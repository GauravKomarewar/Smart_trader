"""
Smart Trader — Unified Symbols Database
==========================================

PostgreSQL-backed symbols database with exchange_token as the primary
cross-broker identifier.

Key design:
  • (exchange, exchange_token) is the composite unique key
  • Each row stores per-broker identifiers: fyers_symbol, shoonya_token, etc.
  • Populated from broker ScriptMasters on startup / manual refresh
  • Provides fast lookup by token, trading_symbol, fyers_symbol, underlying

Usage:
    from db.symbols_db import (
        init_symbols_schema, populate_symbols_db,
        lookup_by_token, lookup_by_trading_symbol,
        lookup_by_fyers_symbol, search_symbols,
    )
"""
from __future__ import annotations

import logging
import time
import threading
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

import psycopg2.extras
from core.daily_refresh import IST, current_refresh_cycle_start, next_refresh_time
from db.trading_db import get_trading_conn

logger = logging.getLogger("smart_trader.symbols_db")

_IST = IST

# Track last successful population time (in IST)
_last_populated_at: Optional[datetime] = None

# ── Schema ─────────────────────────────────────────────────────────────────────

_CREATE_SYMBOLS = """
CREATE TABLE IF NOT EXISTS symbols (
    id                  BIGSERIAL PRIMARY KEY,

    /* ── Universal identifier ─────────────────────────────── */
    exchange            TEXT NOT NULL,          -- NSE, NFO, BSE, BFO, MCX, CDS
    exchange_token      TEXT NOT NULL,          -- exchange-assigned numeric token

    /* ── Instrument info ──────────────────────────────────── */
    symbol              TEXT NOT NULL,          -- underlying: NIFTY, RELIANCE
    trading_symbol      TEXT NOT NULL,          -- compact: NIFTY26APR24500CE
    instrument_type     TEXT NOT NULL,          -- EQ, FUT, OPT, IDX
    lot_size            INTEGER NOT NULL DEFAULT 1,
    tick_size           REAL NOT NULL DEFAULT 0.05,
    isin                TEXT NOT NULL DEFAULT '',
    expiry              DATE,                   -- NULL for equity / index
    strike              REAL NOT NULL DEFAULT 0,
    option_type         TEXT NOT NULL DEFAULT '', -- CE, PE, or ''
    description         TEXT NOT NULL DEFAULT '',

    /* ── Broker-specific identifiers ──────────────────────── */
    fyers_symbol        TEXT NOT NULL DEFAULT '',  -- NSE:RELIANCE-EQ
    fyers_token         TEXT NOT NULL DEFAULT '',  -- Fyers FyToken (long unique)
    shoonya_token       TEXT NOT NULL DEFAULT '',  -- Shoonya exchange token
    shoonya_tsym        TEXT NOT NULL DEFAULT '',  -- Shoonya TradingSymbol
    angelone_token      TEXT NOT NULL DEFAULT '',
    angelone_tsym       TEXT NOT NULL DEFAULT '',
    dhan_security_id    TEXT NOT NULL DEFAULT '',
    kite_instrument_token TEXT NOT NULL DEFAULT '',
    kite_exchange_token TEXT NOT NULL DEFAULT '',
    upstox_ikey         TEXT NOT NULL DEFAULT '',  -- Upstox instrument_key
    groww_token         TEXT NOT NULL DEFAULT '',

    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(exchange, exchange_token)
);

/* ── Indexes for fast lookups ──────────────────────────────── */
CREATE INDEX IF NOT EXISTS idx_sym_trading
    ON symbols(trading_symbol);
CREATE INDEX IF NOT EXISTS idx_sym_fyers
    ON symbols(fyers_symbol) WHERE fyers_symbol != '';
CREATE INDEX IF NOT EXISTS idx_sym_underlying
    ON symbols(symbol, exchange, instrument_type);
CREATE INDEX IF NOT EXISTS idx_sym_underlying_expiry
    ON symbols(symbol, exchange, instrument_type, expiry)
    WHERE instrument_type IN ('FUT', 'OPT');
CREATE INDEX IF NOT EXISTS idx_sym_shoonya
    ON symbols(shoonya_token, exchange) WHERE shoonya_token != '';
CREATE INDEX IF NOT EXISTS idx_sym_search
    ON symbols USING gin (to_tsvector('simple', symbol || ' ' || trading_symbol || ' ' || description));
"""


def init_symbols_schema() -> None:
    """Create the symbols table. Safe to call on every startup."""
    conn = get_trading_conn()
    try:
        cur = conn.cursor()
        cur.execute(_CREATE_SYMBOLS)
        conn.commit()
        logger.info("Symbols DB schema initialized")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Population ─────────────────────────────────────────────────────────────────

_pop_lock = threading.Lock()


def populate_symbols_db(force: bool = False, only_if_empty: bool = False) -> Dict[str, int]:
    """
    Populate the symbols table from all loaded ScriptMasters.

    Skips if already populated today (after 8:45 AM IST), unless force=True.
    Runs inside a transaction: TRUNCATE + INSERT for consistency.
    Returns count of symbols inserted per broker.
    """
    global _last_populated_at

    with _pop_lock:
        t0 = time.time()
        counts: Dict[str, int] = {}

        conn = get_trading_conn()
        try:
            cur = conn.cursor()
            total_rows, last_populated_at = _get_symbols_state(cur)

            if only_if_empty and total_rows > 0:
                logger.info(
                    "Symbols DB already has %d rows (last updated %s IST) — startup rebuild skipped",
                    total_rows,
                    last_populated_at.strftime("%Y-%m-%d %H:%M") if last_populated_at else "unknown",
                )
                return {}

            if not force and last_populated_at is not None:
                cycle_start = current_refresh_cycle_start()
                if last_populated_at >= cycle_start:
                    logger.info(
                        "Symbols DB already fresh for the current refresh cycle (last updated %s IST), skipping.",
                        last_populated_at.strftime("%Y-%m-%d %H:%M"),
                    )
                    return {}

            # Use a temp table + swap for zero-downtime refresh
            cur.execute("CREATE TEMP TABLE _sym_staging (LIKE symbols INCLUDING DEFAULTS) ON COMMIT DROP")
            cur.execute("ALTER TABLE _sym_staging DROP COLUMN id")
            cur.execute("ALTER TABLE _sym_staging ADD CONSTRAINT _sym_staging_uq UNIQUE (exchange, exchange_token)")

            # ── Fyers ──────────────────────────────────────────────
            fyers_count = _load_fyers(cur)
            counts["fyers"] = fyers_count

            # ── Shoonya ────────────────────────────────────────────
            shoonya_count = _load_shoonya(cur)
            counts["shoonya"] = shoonya_count

            # ── Angel One ──────────────────────────────────────────
            angelone_count = _load_angelone(cur)
            counts["angelone"] = angelone_count

            # ── Dhan ───────────────────────────────────────────────
            dhan_count = _load_dhan(cur)
            counts["dhan"] = dhan_count

            # ── Kite ───────────────────────────────────────────────
            kite_count = _load_kite(cur)
            counts["kite"] = kite_count

            # Now merge staging into main table
            _merge_staging(cur)

            conn.commit()
            elapsed = time.time() - t0
            total = sum(counts.values())
            logger.info(
                "Symbols DB populated: %d records from %d brokers in %.1fs — %s",
                total, len([c for c in counts.values() if c > 0]), elapsed, counts,
            )
            _last_populated_at = datetime.now(_IST)
        except Exception:
            conn.rollback()
            logger.exception("Failed to populate symbols DB")
            raise
        finally:
            conn.close()

        return counts


def _get_symbols_state(cur) -> tuple[int, Optional[datetime]]:
    """Return current row count and last population timestamp in IST."""
    global _last_populated_at
    cur.execute("SELECT COUNT(*), MAX(updated_at) FROM symbols")
    total_rows, last_updated = cur.fetchone()
    last_populated_at = _coerce_ist(last_updated)
    if last_populated_at is not None:
        _last_populated_at = last_populated_at
    return int(total_rows or 0), last_populated_at


def _coerce_ist(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc).astimezone(_IST)
    return value.astimezone(_IST)


def _load_fyers(cur) -> int:
    """Insert Fyers ScriptMaster records into staging table."""
    try:
        from scripts.fyers_scriptmaster import FYERS_SCRIPTMASTER
    except ImportError:
        return 0

    count = 0
    batch = []
    for key, rec in FYERS_SCRIPTMASTER.items():
        token = str(rec.get("Token", "")).strip()
        exchange = rec.get("Exchange", "")
        if not token or not exchange:
            continue

        fyers_sym = rec.get("FyersSymbol", "")
        tsym = fyers_sym.split(":", 1)[-1] if ":" in fyers_sym else fyers_sym

        # Convert expiry epoch to date
        expiry_val = None
        exp_epoch = rec.get("ExpiryEpoch")
        if exp_epoch and isinstance(exp_epoch, (int, float)) and exp_epoch > 0:
            from datetime import datetime, timezone
            expiry_val = datetime.fromtimestamp(exp_epoch, tz=timezone.utc).strftime("%Y-%m-%d")

        batch.append((
            exchange, token,
            rec.get("Underlying", "") or rec.get("Symbol", ""),
            tsym,
            rec.get("Instrument", "EQ"),
            int(rec.get("LotSize", 1) or 1),
            float(rec.get("TickSize", 0.05) or 0.05),
            rec.get("ISIN", "") or "",
            expiry_val,
            float(rec.get("StrikePrice") or 0),
            rec.get("OptionType") or "",
            rec.get("Description", "") or "",
            fyers_sym,
            str(rec.get("FyToken", "")),
        ))
        count += 1

        if len(batch) >= 5000:
            _insert_fyers_batch(cur, batch)
            batch.clear()

    if batch:
        _insert_fyers_batch(cur, batch)

    return count


def _insert_fyers_batch(cur, batch):
    """Bulk insert Fyers records into staging."""
    from psycopg2.extras import execute_values
    execute_values(cur, """
        INSERT INTO _sym_staging (
            exchange, exchange_token, symbol, trading_symbol,
            instrument_type, lot_size, tick_size, isin,
            expiry, strike, option_type, description,
            fyers_symbol, fyers_token
        ) VALUES %s
        ON CONFLICT (exchange, exchange_token)
        DO UPDATE SET
            fyers_symbol = EXCLUDED.fyers_symbol,
            fyers_token = EXCLUDED.fyers_token,
            description = COALESCE(NULLIF(EXCLUDED.description, ''), _sym_staging.description),
            isin = COALESCE(NULLIF(EXCLUDED.isin, ''), _sym_staging.isin)
    """, batch, page_size=2000)


def _load_shoonya(cur) -> int:
    """Insert Shoonya ScriptMaster records into staging."""
    try:
        from scripts.shoonya_scriptmaster import SHOONYA_SCRIPTMASTER
    except ImportError:
        return 0

    _instr_map = {
        "FUTIDX": "FUT", "FUTSTK": "FUT", "FUTCOM": "FUT",
        "FUTCUR": "FUT", "FUTIRC": "FUT",
        "OPTIDX": "OPT", "OPTSTK": "OPT", "OPTFUT": "OPT",
        "OPTCOM": "OPT", "OPTCUR": "OPT",
        "EQ": "EQ", "": "EQ",
    }

    count = 0
    batch = []
    for exch, tokens in SHOONYA_SCRIPTMASTER.items():
        for token, rec in tokens.items():
            raw_instr = rec.get("Instrument", "")
            canonical = _instr_map.get(raw_instr, raw_instr)

            expiry_val = None
            raw_exp = rec.get("Expiry", "")
            if raw_exp:
                expiry_val = _parse_expiry(raw_exp)

            batch.append((
                exch, str(token),
                rec.get("Underlying", "") or rec.get("Symbol", ""),
                rec.get("TradingSymbol", ""),
                canonical,
                int(rec.get("LotSize", 1) or 1),
                float(rec.get("TickSize", 0.05) or 0.05),
                expiry_val,
                float(rec.get("StrikePrice") or 0),
                rec.get("OptionType") or "",
                str(token),
                rec.get("TradingSymbol", ""),
            ))
            count += 1

            if len(batch) >= 5000:
                _insert_shoonya_batch(cur, batch)
                batch.clear()

    if batch:
        _insert_shoonya_batch(cur, batch)

    return count


def _insert_shoonya_batch(cur, batch):
    """Bulk insert Shoonya records into staging."""
    from psycopg2.extras import execute_values
    execute_values(cur, """
        INSERT INTO _sym_staging (
            exchange, exchange_token, symbol, trading_symbol,
            instrument_type, lot_size, tick_size,
            expiry, strike, option_type,
            shoonya_token, shoonya_tsym
        ) VALUES %s
        ON CONFLICT (exchange, exchange_token)
        DO UPDATE SET
            shoonya_token = EXCLUDED.shoonya_token,
            shoonya_tsym = EXCLUDED.shoonya_tsym,
            symbol = COALESCE(NULLIF(EXCLUDED.symbol, ''), _sym_staging.symbol),
            trading_symbol = COALESCE(NULLIF(EXCLUDED.trading_symbol, ''), _sym_staging.trading_symbol)
    """, batch, page_size=2000)


def _load_angelone(cur) -> int:
    """Insert Angel One records into staging."""
    try:
        from scripts.angelone_scriptmaster import ANGELONE_SCRIPTMASTER
    except ImportError:
        return 0

    count = 0
    batch = []
    for key, rec in ANGELONE_SCRIPTMASTER.items():
        token = str(rec.get("Token", "")).strip()
        exchange = rec.get("Exchange", "")
        if not token or not exchange:
            continue

        expiry_val = None
        raw_exp = rec.get("Expiry", "")
        if raw_exp:
            expiry_val = _parse_expiry(raw_exp)

        batch.append((
            exchange, token,
            rec.get("Underlying", "") or rec.get("Symbol", ""),
            rec.get("TradingSymbol", "") or rec.get("Symbol", ""),
            rec.get("Instrument", "EQ"),
            int(rec.get("LotSize", 1) or 1),
            float(rec.get("TickSize", 0.05) or 0.05),
            expiry_val,
            float(rec.get("StrikePrice") or 0),
            rec.get("OptionType") or "",
            token,
            rec.get("TradingSymbol", "") or rec.get("Symbol", ""),
        ))
        count += 1

        if len(batch) >= 5000:
            _insert_angelone_batch(cur, batch)
            batch.clear()

    if batch:
        _insert_angelone_batch(cur, batch)

    return count


def _insert_angelone_batch(cur, batch):
    from psycopg2.extras import execute_values
    execute_values(cur, """
        INSERT INTO _sym_staging (
            exchange, exchange_token, symbol, trading_symbol,
            instrument_type, lot_size, tick_size,
            expiry, strike, option_type,
            angelone_token, angelone_tsym
        ) VALUES %s
        ON CONFLICT (exchange, exchange_token)
        DO UPDATE SET
            angelone_token = EXCLUDED.angelone_token,
            angelone_tsym = EXCLUDED.angelone_tsym
    """, batch, page_size=2000)


def _load_dhan(cur) -> int:
    """Insert Dhan records into staging."""
    try:
        from scripts.dhan_scriptmaster import DHAN_SCRIPTMASTER
    except ImportError:
        return 0

    count = 0
    batch = []
    for key, rec in DHAN_SCRIPTMASTER.items():
        token = str(rec.get("Token", "")).strip()
        exchange = rec.get("Exchange", "")
        security_id = str(rec.get("SecurityId", "") or rec.get("SEM_SMST_SECURITY_ID", "") or key)
        if not token or not exchange:
            continue

        expiry_val = None
        raw_exp = rec.get("Expiry", "")
        if raw_exp:
            expiry_val = _parse_expiry(raw_exp)

        batch.append((
            exchange, token,
            rec.get("Underlying", "") or rec.get("Symbol", ""),
            rec.get("TradingSymbol", "") or rec.get("Symbol", ""),
            rec.get("Instrument", "EQ"),
            int(rec.get("LotSize", 1) or 1),
            float(rec.get("TickSize", 0.05) or 0.05),
            expiry_val,
            float(rec.get("StrikePrice") or 0),
            rec.get("OptionType") or "",
            security_id,
        ))
        count += 1

        if len(batch) >= 5000:
            _insert_dhan_batch(cur, batch)
            batch.clear()

    if batch:
        _insert_dhan_batch(cur, batch)

    return count


def _insert_dhan_batch(cur, batch):
    from psycopg2.extras import execute_values
    execute_values(cur, """
        INSERT INTO _sym_staging (
            exchange, exchange_token, symbol, trading_symbol,
            instrument_type, lot_size, tick_size,
            expiry, strike, option_type,
            dhan_security_id
        ) VALUES %s
        ON CONFLICT (exchange, exchange_token)
        DO UPDATE SET
            dhan_security_id = EXCLUDED.dhan_security_id
    """, batch, page_size=2000)


def _load_kite(cur) -> int:
    """Insert Zerodha Kite records into staging."""
    try:
        from scripts.kite_scriptmaster import KITE_SCRIPTMASTER
    except ImportError:
        return 0

    count = 0
    batch = []
    for key, rec in KITE_SCRIPTMASTER.items():
        token = str(rec.get("Token", "") or rec.get("ExchangeToken", "")).strip()
        exchange = rec.get("Exchange", "")
        instrument_token = str(rec.get("InstrumentToken", "") or "")
        if not token or not exchange:
            continue

        expiry_val = None
        raw_exp = rec.get("Expiry", "")
        if raw_exp:
            expiry_val = _parse_expiry(raw_exp)

        batch.append((
            exchange, token,
            rec.get("Underlying", "") or rec.get("Symbol", ""),
            rec.get("TradingSymbol", "") or rec.get("Symbol", ""),
            rec.get("Instrument", "EQ"),
            int(rec.get("LotSize", 1) or 1),
            float(rec.get("TickSize", 0.05) or 0.05),
            expiry_val,
            float(rec.get("StrikePrice") or 0),
            rec.get("OptionType") or "",
            instrument_token,
            token,
        ))
        count += 1

        if len(batch) >= 5000:
            _insert_kite_batch(cur, batch)
            batch.clear()

    if batch:
        _insert_kite_batch(cur, batch)

    return count


def _insert_kite_batch(cur, batch):
    from psycopg2.extras import execute_values
    execute_values(cur, """
        INSERT INTO _sym_staging (
            exchange, exchange_token, symbol, trading_symbol,
            instrument_type, lot_size, tick_size,
            expiry, strike, option_type,
            kite_instrument_token, kite_exchange_token
        ) VALUES %s
        ON CONFLICT (exchange, exchange_token)
        DO UPDATE SET
            kite_instrument_token = EXCLUDED.kite_instrument_token,
            kite_exchange_token = EXCLUDED.kite_exchange_token
    """, batch, page_size=2000)


def _merge_staging(cur):
    """Merge staging table into main symbols table using atomic swap."""
    # Delete all existing, insert from staging
    cur.execute("TRUNCATE symbols RESTART IDENTITY")
    cur.execute("""
        INSERT INTO symbols (
            exchange, exchange_token, symbol, trading_symbol,
            instrument_type, lot_size, tick_size, isin,
            expiry, strike, option_type, description,
            fyers_symbol, fyers_token,
            shoonya_token, shoonya_tsym,
            angelone_token, angelone_tsym,
            dhan_security_id,
            kite_instrument_token, kite_exchange_token,
            upstox_ikey, groww_token,
            updated_at
        )
        SELECT
            exchange, exchange_token, symbol, trading_symbol,
            instrument_type, lot_size, tick_size, isin,
            expiry, strike, option_type, description,
            fyers_symbol, fyers_token,
            shoonya_token, shoonya_tsym,
            angelone_token, angelone_tsym,
            dhan_security_id,
            kite_instrument_token, kite_exchange_token,
            upstox_ikey, groww_token,
            NOW()
        FROM _sym_staging
    """)
    count = cur.rowcount
    logger.info("Merged %d symbols into main table", count)


# ── Lookup Functions ───────────────────────────────────────────────────────────

def lookup_by_token(exchange: str, token: str) -> Optional[Dict[str, Any]]:
    """Fast lookup by (exchange, exchange_token). O(1) via unique index."""
    conn = get_trading_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT * FROM symbols WHERE exchange = %s AND exchange_token = %s LIMIT 1",
            (exchange.upper(), str(token)),
        )
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def lookup_by_trading_symbol(trading_symbol: str, exchange: str = "") -> Optional[Dict[str, Any]]:
    """Lookup by trading symbol (e.g. 'NIFTY26APR24500CE')."""
    conn = get_trading_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        tsym = trading_symbol.upper().strip()
        # Strip exchange prefix if present
        if ":" in tsym:
            parts = tsym.split(":", 1)
            if not exchange:
                exchange = parts[0]
            tsym = parts[1]
        # Remove common suffixes for index/equity matching
        variants = [tsym]
        if tsym.endswith("-EQ"):
            variants.append(tsym[:-3])
        elif tsym.endswith("-INDEX"):
            variants.append(tsym[:-6])

        if exchange:
            cur.execute(
                "SELECT * FROM symbols WHERE trading_symbol = ANY(%s) AND exchange = %s LIMIT 1",
                (variants, exchange.upper()),
            )
        else:
            cur.execute(
                "SELECT * FROM symbols WHERE trading_symbol = ANY(%s) LIMIT 1",
                (variants,),
            )
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def lookup_by_fyers_symbol(fyers_symbol: str) -> Optional[Dict[str, Any]]:
    """Lookup by Fyers symbol (e.g. 'NSE:RELIANCE-EQ')."""
    conn = get_trading_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT * FROM symbols WHERE fyers_symbol = %s LIMIT 1",
            (fyers_symbol,),
        )
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def lookup_by_components(
    symbol: str,
    exchange: str = "NFO",
    instrument_type: str = "",
    expiry: Optional[str] = None,
    strike: Optional[float] = None,
    option_type: str = "",
) -> Optional[Dict[str, Any]]:
    """Lookup a symbol row by canonical components."""
    conn = get_trading_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        conditions = ["exchange = %s", "UPPER(symbol) = %s"]
        params: list[Any] = [exchange.upper(), symbol.upper().strip()]

        if instrument_type:
            conditions.append("instrument_type = %s")
            params.append(instrument_type.upper().strip())

        expiry_iso = _parse_expiry(expiry) if expiry else None
        if expiry_iso:
            conditions.append("expiry = %s")
            params.append(expiry_iso)

        if strike is not None:
            conditions.append("ABS(strike - %s) < 0.001")
            params.append(float(strike))

        if option_type:
            conditions.append("option_type = %s")
            params.append(option_type.upper().strip())

        cur.execute(
            f"""
            SELECT * FROM symbols
            WHERE {' AND '.join(conditions)}
            ORDER BY expiry NULLS LAST, strike, trading_symbol
            LIMIT 1
            """,
            params,
        )
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def search_symbols(
    query: str,
    exchange: str = "",
    instrument_type: str = "",
    limit: int = 30,
) -> List[Dict[str, Any]]:
    """
    Full-text search on symbols. Uses PostgreSQL GIN index for speed.
    Falls back to ILIKE for short queries.
    """
    conn = get_trading_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        q = query.upper().strip()
        conditions = []
        params: list = []

        if len(q) >= 3:
            # Use full-text search
            conditions.append(
                "to_tsvector('simple', symbol || ' ' || trading_symbol || ' ' || description) "
                "@@ to_tsquery('simple', %s)"
            )
            params.append(q + ":*")
        else:
            conditions.append("(symbol ILIKE %s OR trading_symbol ILIKE %s)")
            params.extend([f"%{q}%", f"%{q}%"])

        if exchange:
            conditions.append("exchange = %s")
            params.append(exchange.upper())

        if instrument_type:
            conditions.append("instrument_type = %s")
            params.append(instrument_type.upper())

        where = " AND ".join(conditions)

        cur.execute(
            f"SELECT * FROM symbols WHERE {where} "
            f"ORDER BY CASE WHEN symbol = %s THEN 0 "
            f"WHEN symbol LIKE %s THEN 1 ELSE 2 END, "
            f"instrument_type, symbol LIMIT %s",
            params + [q, q + "%", limit],
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_futures(symbol: str, exchange: str = "NFO") -> List[Dict[str, Any]]:
    """Return futures rows for an underlying sorted by nearest expiry."""
    conn = get_trading_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT * FROM symbols
            WHERE exchange = %s
              AND instrument_type = 'FUT'
              AND UPPER(symbol) = %s
              AND expiry IS NOT NULL
              AND expiry >= CURRENT_DATE
            ORDER BY expiry, trading_symbol
            """,
            (exchange.upper(), symbol.upper().strip()),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_future(symbol: str, exchange: str = "NFO", result: int = 0) -> Optional[Dict[str, Any]]:
    """Return the Nth nearest futures row for an underlying."""
    futures = get_futures(symbol, exchange)
    return futures[result] if len(futures) > result else None


def get_options(
    symbol: str,
    exchange: str = "NFO",
    expiry: Optional[str] = None,
    strike: Optional[float] = None,
    option_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return option rows filtered by underlying and optional expiry/strike/type."""
    conn = get_trading_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        conditions = [
            "exchange = %s",
            "instrument_type = 'OPT'",
            "UPPER(symbol) = %s",
            "expiry IS NOT NULL",
            "expiry >= CURRENT_DATE",
        ]
        params: list[Any] = [exchange.upper(), symbol.upper().strip()]

        expiry_iso = _parse_expiry(expiry) if expiry else None
        if expiry_iso:
            conditions.append("expiry = %s")
            params.append(expiry_iso)

        if strike is not None:
            conditions.append("ABS(strike - %s) < 0.001")
            params.append(float(strike))

        if option_type:
            conditions.append("option_type = %s")
            params.append(option_type.upper().strip())

        cur.execute(
            f"""
            SELECT * FROM symbols
            WHERE {' AND '.join(conditions)}
            ORDER BY expiry, strike, option_type, trading_symbol
            """,
            params,
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_expiry_list(symbol: str, exchange: str = "NFO", instrument_type: str = "OPT") -> List[str]:
    """Return sorted ISO expiries for a symbol and instrument type."""
    conn = get_trading_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT expiry
            FROM symbols
            WHERE exchange = %s
              AND instrument_type = %s
              AND UPPER(symbol) = %s
              AND expiry IS NOT NULL
              AND expiry >= CURRENT_DATE
            ORDER BY expiry
            """,
            (exchange.upper(), instrument_type.upper().strip(), symbol.upper().strip()),
        )
        return [row[0].strftime("%Y-%m-%d") for row in cur.fetchall() if row and row[0]]
    finally:
        conn.close()


def get_lot_size(symbol: str, exchange: str = "NFO") -> int:
    """Return lot size for an underlying/trading symbol."""
    conn = get_trading_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT lot_size
            FROM symbols
            WHERE exchange = %s
              AND (UPPER(symbol) = %s OR UPPER(trading_symbol) = %s)
              AND lot_size > 0
            ORDER BY
                CASE instrument_type
                    WHEN 'FUT' THEN 0
                    WHEN 'OPT' THEN 1
                    WHEN 'IDX' THEN 2
                    ELSE 3
                END,
                expiry NULLS LAST,
                trading_symbol
            LIMIT 1
            """,
            (exchange.upper(), symbol.upper().strip(), symbol.upper().strip()),
        )
        row = cur.fetchone()
        return int(row[0]) if row and row[0] else 1
    finally:
        conn.close()


def get_broker_symbols_by_trading_symbol(trading_symbol: str, exchange: str = "") -> Dict[str, str]:
    """Resolve all broker symbol identifiers from a canonical trading symbol."""
    rec = lookup_by_trading_symbol(trading_symbol, exchange)
    if not rec:
        clean = trading_symbol.split(":", 1)[-1] if ":" in trading_symbol else trading_symbol
        return {
            "fyers": f"{exchange.upper()}:{clean}" if exchange else clean,
            "shoonya": clean,
            "angelone": clean,
            "dhan": clean,
            "kite": clean,
            "upstox": clean,
            "groww": clean,
        }

    clean = rec.get("trading_symbol", "")
    resolved_exchange = rec.get("exchange", exchange.upper())
    return {
        "fyers": rec.get("fyers_symbol") or f"{resolved_exchange}:{clean}",
        "shoonya": rec.get("shoonya_tsym") or clean,
        "angelone": rec.get("angelone_tsym") or clean,
        "dhan": rec.get("dhan_security_id") or clean,
        "kite": clean,
        "upstox": rec.get("upstox_ikey") or clean,
        "groww": clean,
    }


def get_symbol_count() -> int:
    """Return total number of symbols in the DB."""
    conn = get_trading_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT COUNT(*) as cnt FROM symbols")
        row = cur.fetchone()
        return row["cnt"] if row else 0
    except Exception:
        return 0
    finally:
        conn.close()


def get_broker_symbol(exchange: str, token: str, broker: str) -> Optional[str]:
    """
    Get the broker-specific trading symbol for an instrument.

    Args:
        exchange:  "NSE", "NFO", etc.
        token:     Exchange token number
        broker:    "fyers", "shoonya", "angelone", "dhan", "kite"

    Returns:
        The broker-specific symbol string, or None.
    """
    rec = lookup_by_token(exchange, token)
    if not rec:
        return None

    _broker_sym_map = {
        "fyers":    "fyers_symbol",
        "shoonya":  "shoonya_tsym",
        "angelone": "angelone_tsym",
        "dhan":     "trading_symbol",  # Dhan uses standard trading symbol
        "kite":     "trading_symbol",  # Kite uses standard trading symbol
        "upstox":   "upstox_ikey",
        "groww":    "trading_symbol",
    }
    field = _broker_sym_map.get(broker, "trading_symbol")
    return rec.get(field) or rec.get("trading_symbol") or None


# ── Utility ────────────────────────────────────────────────────────────────────

def _parse_expiry(raw: str) -> Optional[str]:
    """Parse DD-Mon-YYYY or YYYY-MM-DD to ISO date string."""
    if not raw:
        return None
    # Already ISO?
    if len(raw) == 10 and raw[4] == "-":
        return raw
    # DD-Mon-YYYY
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


# ── Daily 8:45 AM IST refresh scheduler ───────────────────────────────────────

_scheduler_thread: Optional[threading.Thread] = None


def _daily_refresh_loop():
    """Background thread: refreshes ScriptMasters + symbols DB at 8:45 AM IST daily."""
    while True:
        try:
            now = datetime.now(_IST)
            target = next_refresh_time(now)
            wait_secs = (target - now).total_seconds()
            logger.info("Symbols DB next refresh at %s IST (in %.0f min)",
                        target.strftime("%Y-%m-%d %H:%M"), wait_secs / 60)
            # Sleep until 8:45 AM IST
            time.sleep(wait_secs)

            # 1) Re-download ScriptMasters from broker APIs
            logger.info("8:45 AM IST — refreshing ScriptMasters...")
            try:
                from scripts.unified_scriptmaster import refresh_all
                counts = refresh_all()
                logger.info("ScriptMaster refresh done: %s", counts)
            except Exception as exc:
                logger.warning("ScriptMaster refresh failed: %s", exc)

            # 2) Rebuild symbols DB
            logger.info("Rebuilding symbols DB...")
            try:
                result = populate_symbols_db(force=True)
                logger.info("Symbols DB daily refresh complete: %s", result)
            except Exception as exc:
                logger.warning("Symbols DB daily refresh failed: %s", exc)
        except Exception as exc:
            logger.exception("Symbols DB scheduler loop error: %s", exc)
            time.sleep(60)  # back off on unexpected error


def start_daily_refresh_scheduler():
    """Start the background thread for daily 8:45 AM IST symbols refresh."""
    global _scheduler_thread
    if _scheduler_thread and _scheduler_thread.is_alive():
        logger.debug("Symbols refresh scheduler already running")
        return
    _scheduler_thread = threading.Thread(
        target=_daily_refresh_loop, name="symbols-daily-refresh", daemon=True
    )
    _scheduler_thread.start()
    logger.info("Symbols DB daily refresh scheduler started")
