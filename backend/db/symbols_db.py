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
        lookup_by_token, lookup_by_row_key, lookup_by_trading_symbol,
        lookup_by_fyers_symbol, search_symbols,
    )
"""
from __future__ import annotations

import logging
import re
import time
import threading
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

import psycopg2.extras

# Regex that strips broker-specific expiry/strike/contract suffixes from the
# `symbol` column so we can recover the clean underlying base name.
# Handles Shoonya-style long-form names across ALL FNO exchanges:
#   "GOLDPETAL FUT 30 APR 26"        → "GOLDPETAL"
#   "360ONE 1000 CE 26 MAY 26"       → "360ONE"
#   "JPYINR 62.25 CE 25 JUN 26"      → "JPYINR"
#   "GOLDPETAL26APR" / "GOLDPETAL26APRFUT" → "GOLDPETAL"
# NOTE: the dated-suffix alternative uses explicit month abbreviations instead
# of [A-Z]{3,} to avoid eating valid trailing letters in names like "360ONE".
_MONTHS = "JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC"
_BROKER_SUFFIX_RE = re.compile(
    r"(?:"
    r"\s+(?:FUT|OPT)\s+\d+\s+[A-Z]{3}\s+\d+"         # " FUT 30 APR 26"
    r"|\s+[\d]+(?:\.\d+)?\s+(?:CE|PE)\s+\d+\s+[A-Z]{3}\s+\d+"  # " 1000 CE 26 MAY 26"
    r"|\d{2}(?:" + _MONTHS + r")[A-Z0-9]*"             # "26APR", "26APRFUT"
    r")$",
    re.IGNORECASE,
)

# Symbols whose base name contains 5+ consecutive digits are likely mis-stored
# option trading_symbols (e.g. "NIFTY2642123600"), not real underlyings.
_STRIKE_LIKE_RE = re.compile(r"\d{5,}")


def _extract_base_symbol(symbol: str) -> str:
    """Strip broker-specific expiry/strike suffix to get the underlying base name."""
    return _BROKER_SUFFIX_RE.sub("", symbol).strip()
from core.daily_refresh import IST, current_refresh_cycle_start, next_refresh_time
from db.trading_db import get_trading_conn

logger = logging.getLogger("smart_trader.symbols_db")

_IST = IST

_EQ_LIKE_TYPES = {"EQ", "BE", "BL", "SM", "ST", "TB"}
_IDX_LIKE_TYPES = {"IDX", "INDEX"}

# Track last successful population time (in IST)
_last_populated_at: Optional[datetime] = None


def _clean_text(value: Any) -> str:
    return str(value or "").strip().upper()


def _instrument_family(value: Any) -> str:
    inst = _clean_text(value)
    if not inst:
        return ""
    if inst in _EQ_LIKE_TYPES:
        return "EQ"
    if inst in _IDX_LIKE_TYPES:
        return "IDX"
    if inst.startswith("OPT"):
        return "OPT"
    if inst.startswith("FUT"):
        return "FUT"
    return inst


def _normalize_trading_symbol_value(trading_symbol: Any, instrument_type: Any = "") -> str:
    tsym = _clean_text(trading_symbol)
    if not tsym:
        return ""
    if ":" in tsym:
        tsym = tsym.split(":", 1)[1].strip()
    family = _instrument_family(instrument_type)
    if tsym.endswith("-EQ"):
        return tsym[:-3]
    if tsym.endswith("-INDEX"):
        return tsym[:-6]
    if family in {"EQ", "IDX"}:
        return tsym
    return tsym


def _lookup_symbol_variants(trading_symbol: str) -> List[str]:
    raw = _clean_text(trading_symbol)
    if not raw:
        return []
    if ":" in raw:
        raw = raw.split(":", 1)[1].strip()
    normalized = _normalize_trading_symbol_value(raw)
    variants: List[str] = []
    for candidate in (raw, normalized, f"{normalized}-EQ", f"{normalized}-INDEX"):
        cleaned = _clean_text(candidate)
        if cleaned and cleaned not in variants:
            variants.append(cleaned)
    return variants


def _broker_coverage_score(row: Dict[str, Any]) -> int:
    score = 0
    for field, weight in (
        ("fyers_symbol", 3),
        ("fyers_token", 1),
        ("shoonya_token", 2),
        ("shoonya_tsym", 1),
        ("angelone_token", 2),
        ("angelone_tsym", 1),
        ("dhan_security_id", 2),
        ("kite_exchange_token", 2),
        ("kite_instrument_token", 1),
        ("upstox_ikey", 2),
        ("groww_token", 1),
        ("description", 1),
        ("isin", 1),
    ):
        if str(row.get(field) or "").strip():
            score += weight
    return score


def _broker_coverage_sql(alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return f"""
        (
            CASE WHEN NULLIF({prefix}fyers_symbol, '') IS NOT NULL THEN 3 ELSE 0 END +
            CASE WHEN NULLIF({prefix}fyers_token, '') IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN NULLIF({prefix}shoonya_token, '') IS NOT NULL THEN 2 ELSE 0 END +
            CASE WHEN NULLIF({prefix}shoonya_tsym, '') IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN NULLIF({prefix}angelone_token, '') IS NOT NULL THEN 2 ELSE 0 END +
            CASE WHEN NULLIF({prefix}angelone_tsym, '') IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN NULLIF({prefix}dhan_security_id, '') IS NOT NULL THEN 2 ELSE 0 END +
            CASE WHEN NULLIF({prefix}kite_exchange_token, '') IS NOT NULL THEN 2 ELSE 0 END +
            CASE WHEN NULLIF({prefix}kite_instrument_token, '') IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN NULLIF({prefix}upstox_ikey, '') IS NOT NULL THEN 2 ELSE 0 END +
            CASE WHEN NULLIF({prefix}groww_token, '') IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN NULLIF({prefix}description, '') IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN NULLIF({prefix}isin, '') IS NOT NULL THEN 1 ELSE 0 END
        )
    """

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
    normalized_underlying TEXT NOT NULL DEFAULT '',
    normalized_trading_symbol TEXT NOT NULL DEFAULT '',
    identity_key        TEXT NOT NULL DEFAULT '',
    row_key             TEXT NOT NULL DEFAULT '',
    broker_coverage     INTEGER NOT NULL DEFAULT 0,

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
        cur.execute("ALTER TABLE symbols ADD COLUMN IF NOT EXISTS normalized_underlying TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE symbols ADD COLUMN IF NOT EXISTS normalized_trading_symbol TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE symbols ADD COLUMN IF NOT EXISTS identity_key TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE symbols ADD COLUMN IF NOT EXISTS row_key TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE symbols ADD COLUMN IF NOT EXISTS broker_coverage INTEGER NOT NULL DEFAULT 0")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sym_norm_trading ON symbols(exchange, normalized_trading_symbol)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sym_identity_key ON symbols(identity_key)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sym_row_key ON symbols(row_key)")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_sym_row_key_nonempty ON symbols(row_key) WHERE row_key <> ''")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sym_shoonya_tsym ON symbols(shoonya_tsym) WHERE shoonya_tsym <> ''")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sym_angelone_tsym ON symbols(angelone_tsym) WHERE angelone_tsym <> ''")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sym_groww_token ON symbols(groww_token) WHERE groww_token <> ''")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sym_upstox_ikey ON symbols(upstox_ikey) WHERE upstox_ikey <> ''")
        _create_symbols_fast_lookup_view(cur)
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

            # ── Upstox ─────────────────────────────────────────────
            upstox_count = _load_upstox(cur)
            counts["upstox"] = upstox_count

            # ── Groww ──────────────────────────────────────────────
            groww_count = _load_groww(cur)
            counts["groww"] = groww_count

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
            angelone_tsym = EXCLUDED.angelone_tsym,
            symbol = CASE
                WHEN NULLIF(_sym_staging.symbol, '') IS NULL THEN EXCLUDED.symbol
                WHEN NULLIF(EXCLUDED.symbol, '') IS NULL THEN _sym_staging.symbol
                WHEN LENGTH(EXCLUDED.symbol) > LENGTH(_sym_staging.symbol) THEN EXCLUDED.symbol
                ELSE _sym_staging.symbol
            END,
            trading_symbol = CASE
                WHEN NULLIF(_sym_staging.trading_symbol, '') IS NULL THEN EXCLUDED.trading_symbol
                WHEN NULLIF(EXCLUDED.trading_symbol, '') IS NULL THEN _sym_staging.trading_symbol
                WHEN LENGTH(EXCLUDED.trading_symbol) > LENGTH(_sym_staging.trading_symbol) THEN EXCLUDED.trading_symbol
                ELSE _sym_staging.trading_symbol
            END
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
            dhan_security_id = EXCLUDED.dhan_security_id,
            symbol = CASE
                WHEN NULLIF(_sym_staging.symbol, '') IS NULL THEN EXCLUDED.symbol
                WHEN NULLIF(EXCLUDED.symbol, '') IS NULL THEN _sym_staging.symbol
                WHEN LENGTH(EXCLUDED.symbol) > LENGTH(_sym_staging.symbol) THEN EXCLUDED.symbol
                ELSE _sym_staging.symbol
            END,
            trading_symbol = CASE
                WHEN NULLIF(_sym_staging.trading_symbol, '') IS NULL THEN EXCLUDED.trading_symbol
                WHEN NULLIF(EXCLUDED.trading_symbol, '') IS NULL THEN _sym_staging.trading_symbol
                WHEN LENGTH(EXCLUDED.trading_symbol) > LENGTH(_sym_staging.trading_symbol) THEN EXCLUDED.trading_symbol
                ELSE _sym_staging.trading_symbol
            END
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
            kite_exchange_token = EXCLUDED.kite_exchange_token,
            symbol = CASE
                WHEN NULLIF(_sym_staging.symbol, '') IS NULL THEN EXCLUDED.symbol
                WHEN NULLIF(EXCLUDED.symbol, '') IS NULL THEN _sym_staging.symbol
                WHEN LENGTH(EXCLUDED.symbol) > LENGTH(_sym_staging.symbol) THEN EXCLUDED.symbol
                ELSE _sym_staging.symbol
            END,
            trading_symbol = CASE
                WHEN NULLIF(_sym_staging.trading_symbol, '') IS NULL THEN EXCLUDED.trading_symbol
                WHEN NULLIF(EXCLUDED.trading_symbol, '') IS NULL THEN _sym_staging.trading_symbol
                WHEN LENGTH(EXCLUDED.trading_symbol) > LENGTH(_sym_staging.trading_symbol) THEN EXCLUDED.trading_symbol
                ELSE _sym_staging.trading_symbol
            END
    """, batch, page_size=2000)


def _load_upstox(cur) -> int:
    """Insert Upstox ScriptMaster records into staging table."""
    try:
        from scripts.upstox_scriptmaster import UPSTOX_SCRIPTMASTER
    except ImportError:
        return 0

    count = 0
    batch = []
    for key, rec in UPSTOX_SCRIPTMASTER.items():
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
            rec.get("Underlying", "") or rec.get("ShortName", ""),
            rec.get("TradingSymbol", ""),
            rec.get("Instrument", "EQ"),
            int(rec.get("LotSize", 1) or 1),
            float(rec.get("TickSize", 0.05) or 0.05),
            rec.get("ISIN", "") or "",
            expiry_val,
            float(rec.get("StrikePrice") or 0),
            rec.get("OptionType") or "",
            str(rec.get("Name", "") or ""),
            str(key),  # InstrumentKey
        ))
        count += 1

        if len(batch) >= 5000:
            _insert_upstox_batch(cur, batch)
            batch.clear()

    if batch:
        _insert_upstox_batch(cur, batch)

    return count


def _insert_upstox_batch(cur, batch):
    from psycopg2.extras import execute_values
    execute_values(cur, """
        INSERT INTO _sym_staging (
            exchange, exchange_token, symbol, trading_symbol,
            instrument_type, lot_size, tick_size, isin,
            expiry, strike, option_type, description,
            upstox_ikey
        ) VALUES %s
        ON CONFLICT (exchange, exchange_token)
        DO UPDATE SET
            upstox_ikey = EXCLUDED.upstox_ikey,
            isin = COALESCE(NULLIF(EXCLUDED.isin, ''), _sym_staging.isin),
            description = COALESCE(NULLIF(EXCLUDED.description, ''), _sym_staging.description),
            symbol = CASE
                WHEN NULLIF(_sym_staging.symbol, '') IS NULL THEN EXCLUDED.symbol
                WHEN NULLIF(EXCLUDED.symbol, '') IS NULL THEN _sym_staging.symbol
                WHEN LENGTH(EXCLUDED.symbol) > LENGTH(_sym_staging.symbol) THEN EXCLUDED.symbol
                ELSE _sym_staging.symbol
            END
    """, batch, page_size=2000)


def _load_groww(cur) -> int:
    """Insert Groww ScriptMaster records into staging table."""
    try:
        from scripts.groww_scriptmaster import GROWW_SCRIPTMASTER
    except ImportError:
        return 0

    count = 0
    batch = []
    for key, rec in GROWW_SCRIPTMASTER.items():
        token = str(rec.get("ExchangeToken", rec.get("Token", ""))).strip()
        exchange = rec.get("Exchange", "")
        if not token or not exchange:
            continue

        expiry_val = None
        raw_exp = rec.get("Expiry", "")
        if raw_exp:
            expiry_val = _parse_expiry(raw_exp)

        groww_sym = str(rec.get("GrowwSymbol", "") or rec.get("TradingSymbol", "") or "")

        batch.append((
            exchange, token,
            rec.get("Underlying", "") or rec.get("TradingSymbol", ""),
            rec.get("TradingSymbol", ""),
            rec.get("Instrument", "EQ"),
            int(rec.get("LotSize", 1) or 1),
            float(rec.get("TickSize", 0.05) or 0.05),
            rec.get("ISIN", "") or "",
            expiry_val,
            float(rec.get("StrikePrice") or 0),
            rec.get("OptionType") or "",
            str(rec.get("Name", "") or ""),
            groww_sym,
        ))
        count += 1

        if len(batch) >= 5000:
            _insert_groww_batch(cur, batch)
            batch.clear()

    if batch:
        _insert_groww_batch(cur, batch)

    return count


def _insert_groww_batch(cur, batch):
    from psycopg2.extras import execute_values
    execute_values(cur, """
        INSERT INTO _sym_staging (
            exchange, exchange_token, symbol, trading_symbol,
            instrument_type, lot_size, tick_size, isin,
            expiry, strike, option_type, description,
            groww_token
        ) VALUES %s
        ON CONFLICT (exchange, exchange_token)
        DO UPDATE SET
            groww_token = EXCLUDED.groww_token,
            isin = COALESCE(NULLIF(EXCLUDED.isin, ''), _sym_staging.isin),
            description = COALESCE(NULLIF(EXCLUDED.description, ''), _sym_staging.description),
            symbol = CASE
                WHEN NULLIF(_sym_staging.symbol, '') IS NULL THEN EXCLUDED.symbol
                WHEN NULLIF(EXCLUDED.symbol, '') IS NULL THEN _sym_staging.symbol
                WHEN LENGTH(EXCLUDED.symbol) > LENGTH(_sym_staging.symbol) THEN EXCLUDED.symbol
                ELSE _sym_staging.symbol
            END
    """, batch, page_size=2000)


def _finalize_normalized_columns(cur, table_name: str) -> None:
    raw_symbol = "UPPER(BTRIM(COALESCE(symbol, '')))"
    raw_tsym = (
        "UPPER(BTRIM(CASE "
        "WHEN POSITION(':' IN COALESCE(trading_symbol, '')) > 0 "
        "THEN SPLIT_PART(trading_symbol, ':', 2) "
        "ELSE COALESCE(trading_symbol, '') END))"
    )
    family = (
        "CASE "
        "WHEN UPPER(COALESCE(instrument_type, '')) IN ('EQ','BE','BL','SM','ST','TB') THEN 'EQ' "
        "WHEN UPPER(COALESCE(instrument_type, '')) IN ('IDX','INDEX') THEN 'IDX' "
        "WHEN UPPER(COALESCE(instrument_type, '')) LIKE 'OPT%' THEN 'OPT' "
        "WHEN UPPER(COALESCE(instrument_type, '')) LIKE 'FUT%' THEN 'FUT' "
        "ELSE UPPER(COALESCE(instrument_type, '')) END"
    )
    normalized_tsym = (
        f"CASE "
        f"WHEN {raw_tsym} = '' THEN '' "
        f"WHEN {family} IN ('EQ', 'IDX') THEN REGEXP_REPLACE({raw_tsym}, '(-EQ|-INDEX)$', '') "
        f"ELSE {raw_tsym} END"
    )
    normalized_underlying = f"CASE WHEN {raw_symbol} = '' THEN {normalized_tsym} ELSE {raw_symbol} END"
    strike_text = "COALESCE(TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM strike::text)), '0')"
    identity_key = (
        f"CONCAT_WS('|', exchange, {normalized_underlying}, {normalized_tsym}, {family}, "
        "COALESCE(TO_CHAR(expiry, 'YYYY-MM-DD'), ''), "
        f"{strike_text}, UPPER(COALESCE(option_type, '')))"
    )
    row_key = "CONCAT(exchange, ':', exchange_token)"
    coverage = _broker_coverage_sql()

    # Normalize derivative typing before metadata derivation.
    cur.execute(
        f"""
        UPDATE {table_name}
        SET option_type = UPPER(RIGHT(BTRIM(trading_symbol), 2))
        WHERE UPPER(COALESCE(instrument_type, '')) = 'OPT'
          AND COALESCE(option_type, '') = ''
          AND UPPER(BTRIM(trading_symbol)) ~ '(CE|PE)$'
        """
    )
    cur.execute(
        f"""
        UPDATE {table_name}
        SET instrument_type = 'OPT'
        WHERE UPPER(COALESCE(instrument_type, '')) = 'FUT'
          AND (
            UPPER(COALESCE(option_type, '')) IN ('CE', 'PE')
            OR UPPER(BTRIM(trading_symbol)) ~ '(CE|PE)$'
          )
        """
    )
    cur.execute(
        f"""
        UPDATE {table_name}
        SET instrument_type = 'FUT'
        WHERE UPPER(COALESCE(instrument_type, '')) = 'OPT'
          AND COALESCE(option_type, '') = ''
          AND COALESCE(strike, 0) = 0
          AND UPPER(BTRIM(trading_symbol)) LIKE '%FUT'
        """
    )
    # Second pass after reclassification: fill option_type for any newly retyped OPT rows.
    cur.execute(
        f"""
        UPDATE {table_name}
        SET option_type = UPPER(RIGHT(BTRIM(trading_symbol), 2))
        WHERE UPPER(COALESCE(instrument_type, '')) = 'OPT'
          AND COALESCE(option_type, '') = ''
          AND UPPER(BTRIM(trading_symbol)) ~ '(CE|PE)$'
        """
    )
    # Infer strike from symbols like EURINR-AUG2025-100.5-PE where strike stayed 0.
    cur.execute(
        f"""
        UPDATE {table_name}
        SET strike = REGEXP_REPLACE(
                UPPER(BTRIM(trading_symbol)),
                '^.*-([0-9]+(?:\\.[0-9]+)?)-(CE|PE)$',
                '\\1'
            )::DOUBLE PRECISION
        WHERE UPPER(COALESCE(instrument_type, '')) = 'OPT'
          AND COALESCE(strike, 0) = 0
          AND UPPER(BTRIM(trading_symbol)) ~ '^.*-[0-9]+(?:\\.[0-9]+)?-(CE|PE)$'
        """
    )

    cur.execute(
        f"""
        UPDATE {table_name}
        SET normalized_underlying = {normalized_underlying},
            normalized_trading_symbol = {normalized_tsym},
            identity_key = {identity_key},
            row_key = {row_key},
            broker_coverage = {coverage}
        """
    )
    # Backfill expiry from trading_symbol for rows where expiry is NULL but the
    # instrument name embeds it (e.g. Upstox NSE_COM "ZINC 325 PE 23 APR 26").
    # Pattern: "<UNDERLYING> <STRIKE> <CE|PE|FUT> <DD> <MON> <YY>"
    cur.execute(
        f"""
        UPDATE {table_name}
        SET expiry = TO_DATE(
                REGEXP_REPLACE(
                    trading_symbol,
                    '^.+\\s+(\\d{{1,2}})\\s+([A-Za-z]{{3}})\\s+(\\d{{2}})\\s*$',
                    '\\1 \\2 \\3'
                ),
                'DD Mon RR'
            )
        WHERE expiry IS NULL
          AND instrument_type IN ('FUT', 'OPT')
          AND trading_symbol ~ '^.+\\s+\\d{{1,2}}\\s+[A-Za-z]{{3}}\\s+\\d{{2}}\\s*$'
        """
    )


def _enrich_broker_columns_by_identity(cur, table_name: str) -> None:
    """
    Fill missing broker identifiers from sibling rows with the same identity_key.

    This keeps (exchange, exchange_token) as the physical row key while still
    propagating known broker mappings for equivalent contracts.
    """
    cur.execute(
        f"""
        WITH agg AS (
            SELECT
                identity_key,
                MAX(NULLIF(fyers_symbol, '')) AS fyers_symbol,
                MAX(NULLIF(fyers_token, '')) AS fyers_token,
                MAX(NULLIF(shoonya_token, '')) AS shoonya_token,
                MAX(NULLIF(shoonya_tsym, '')) AS shoonya_tsym,
                MAX(NULLIF(angelone_token, '')) AS angelone_token,
                MAX(NULLIF(angelone_tsym, '')) AS angelone_tsym,
                MAX(NULLIF(dhan_security_id, '')) AS dhan_security_id,
                MAX(NULLIF(kite_instrument_token, '')) AS kite_instrument_token,
                MAX(NULLIF(kite_exchange_token, '')) AS kite_exchange_token,
                MAX(NULLIF(upstox_ikey, '')) AS upstox_ikey,
                MAX(NULLIF(groww_token, '')) AS groww_token
            FROM {table_name}
            WHERE identity_key <> ''
            GROUP BY identity_key
        )
        UPDATE {table_name} s
        SET
            fyers_symbol = COALESCE(NULLIF(s.fyers_symbol, ''), agg.fyers_symbol, ''),
            fyers_token = COALESCE(NULLIF(s.fyers_token, ''), agg.fyers_token, ''),
            shoonya_token = COALESCE(NULLIF(s.shoonya_token, ''), agg.shoonya_token, ''),
            shoonya_tsym = COALESCE(NULLIF(s.shoonya_tsym, ''), agg.shoonya_tsym, ''),
            angelone_token = COALESCE(NULLIF(s.angelone_token, ''), agg.angelone_token, ''),
            angelone_tsym = COALESCE(NULLIF(s.angelone_tsym, ''), agg.angelone_tsym, ''),
            dhan_security_id = COALESCE(NULLIF(s.dhan_security_id, ''), agg.dhan_security_id, ''),
            kite_instrument_token = COALESCE(NULLIF(s.kite_instrument_token, ''), agg.kite_instrument_token, ''),
            kite_exchange_token = COALESCE(NULLIF(s.kite_exchange_token, ''), agg.kite_exchange_token, ''),
            upstox_ikey = COALESCE(NULLIF(s.upstox_ikey, ''), agg.upstox_ikey, ''),
            groww_token = COALESCE(NULLIF(s.groww_token, ''), agg.groww_token, '')
        FROM agg
        WHERE s.identity_key = agg.identity_key
        """
    )


def _refresh_broker_coverage(cur, table_name: str) -> None:
    coverage = _broker_coverage_sql()
    cur.execute(f"UPDATE {table_name} SET broker_coverage = {coverage}")


def backfill_symbols_metadata() -> int:
    """Backfill normalized/quality columns for an existing symbols table."""
    conn = get_trading_conn()
    try:
        cur = conn.cursor()
        _finalize_normalized_columns(cur, "symbols")
        _enrich_broker_columns_by_identity(cur, "symbols")
        _refresh_broker_coverage(cur, "symbols")
        cur.execute("SELECT COUNT(*) FROM symbols")
        updated = cur.fetchone()[0]
        _create_symbols_fast_lookup_view(cur)
        conn.commit()
        logger.info("Symbols metadata backfilled for %d rows", updated)
        validate_symbols_accuracy()
        return updated
    except Exception:
        conn.rollback()
        logger.exception("Failed to backfill symbols metadata")
        raise
    finally:
        conn.close()


def _merge_staging(cur):
    """Merge staging table into main symbols table using atomic swap."""
    _finalize_normalized_columns(cur, "_sym_staging")
    _enrich_broker_columns_by_identity(cur, "_sym_staging")
    _refresh_broker_coverage(cur, "_sym_staging")
    # Delete all existing, insert from staging
    cur.execute("TRUNCATE symbols RESTART IDENTITY")
    cur.execute("""
        INSERT INTO symbols (
            exchange, exchange_token, symbol, trading_symbol,
            instrument_type, lot_size, tick_size, isin,
            expiry, strike, option_type, description,
            normalized_underlying, normalized_trading_symbol,
            identity_key, row_key, broker_coverage,
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
            normalized_underlying, normalized_trading_symbol,
            identity_key, row_key, broker_coverage,
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
    _create_symbols_fast_lookup_view(cur)


def _create_symbols_fast_lookup_view(cur) -> None:
    """Create compatibility view with fast-lookup columns requested by clients."""
    cur.execute(
        """
        CREATE OR REPLACE VIEW symbols_fast_lookup AS
        SELECT
            id,
            row_key,
            exchange,
            exchange_token,
            trading_symbol,
            normalized_trading_symbol,
            instrument_type,
            lot_size,
            tick_size,
            broker_coverage,
            fyers_symbol,
            shoonya_tsym AS shoonya_symbol,
            angelone_tsym AS angelone_symbol,
            dhan_security_id AS dhan_symbol,
            COALESCE(NULLIF(kite_exchange_token, ''), kite_instrument_token, '') AS kite_symbol,
            upstox_ikey AS upstox_symbol,
            groww_token AS grow_symbol
        FROM symbols
        """
    )


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


def lookup_by_row_key(row_key: str) -> Optional[Dict[str, Any]]:
    """Fast lookup by row_key (format: EXCHANGE:EXCHANGE_TOKEN)."""
    conn = get_trading_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT * FROM symbols_fast_lookup WHERE row_key = %s LIMIT 1",
            (str(row_key).upper().strip(),),
        )
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def lookup_by_trading_symbol(trading_symbol: str, exchange: str = "") -> Optional[Dict[str, Any]]:
    """Lookup by trading symbol (e.g. 'NIFTY26APR24500CE').

    Prefers the highest-quality canonical row within the normalized symbol group.
    """
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

        variants = _lookup_symbol_variants(tsym)
        normalized = _normalize_trading_symbol_value(tsym)

        if exchange:
            cur.execute(
                """
                SELECT * FROM symbols
                WHERE exchange = %s
                  AND (trading_symbol = ANY(%s) OR normalized_trading_symbol = %s)
                ORDER BY broker_coverage DESC,
                         CASE WHEN trading_symbol = %s THEN 0 ELSE 1 END,
                         CASE WHEN NULLIF(description, '') IS NOT NULL THEN 0 ELSE 1 END,
                         trading_symbol
                LIMIT 1
                """,
                (exchange.upper(), variants, normalized, tsym),
            )
        else:
            cur.execute(
                """
                SELECT * FROM symbols
                WHERE trading_symbol = ANY(%s) OR normalized_trading_symbol = %s
                ORDER BY broker_coverage DESC,
                         CASE WHEN trading_symbol = %s THEN 0 ELSE 1 END,
                         CASE WHEN NULLIF(description, '') IS NOT NULL THEN 0 ELSE 1 END,
                         trading_symbol
                LIMIT 1
                """,
                (variants, normalized, tsym),
            )
        row = cur.fetchone()
        if row:
            return dict(row)

        # ── Broker-symbol fallback ──────────────────────────────────────────
        # Some symbols (e.g. Shoonya MCX "GOLDPETAL30APR26") differ from the
        # canonical trading_symbol stored on the primary row.  Try matching
        # against per-broker symbol columns so callers can use any broker's
        # native format.
        broker_conditions = (
            "shoonya_tsym = %s OR angelone_tsym = %s OR "
            "fyers_symbol = %s OR fyers_symbol = %s OR "
            "dhan_security_id = %s OR groww_token = %s"
        )
        fyers_prefixed = f"{exchange}:{tsym}" if exchange else tsym
        broker_params = [tsym, tsym, tsym, fyers_prefixed, tsym, tsym]

        if exchange:
            cur.execute(
                f"""
                SELECT * FROM symbols
                WHERE exchange = %s AND ({broker_conditions})
                ORDER BY broker_coverage DESC LIMIT 1
                """,
                [exchange.upper()] + broker_params,
            )
        else:
            cur.execute(
                f"""
                SELECT * FROM symbols
                WHERE {broker_conditions}
                ORDER BY broker_coverage DESC LIMIT 1
                """,
                broker_params,
            )
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


# ── Broker-field mapping ──────────────────────────────────────────────────────
_BROKER_FIELD: Dict[str, str] = {
    "shoonya":  "shoonya_tsym",
    "fyers":    "fyers_symbol",
    "angelone": "angelone_tsym",
    "angel":    "angelone_tsym",
    "dhan":     "dhan_security_id",
    "kite":     "kite_instrument_token",
    "zerodha":  "kite_instrument_token",
    "upstox":   "upstox_ikey",
    "groww":    "groww_token",
}
# Secondary field returned alongside the primary (e.g. token for AngelOne/Dhan/Shoonya)
_BROKER_TOKEN_FIELD: Dict[str, str] = {
    "angelone": "angelone_token",
    "angel":    "angelone_token",
    "shoonya":  "shoonya_token",   # numeric exchange token for get_quotes
}


def resolve_broker_symbol(
    trading_symbol: str,
    broker_id: str,
    exchange: str = "",
) -> Dict[str, str]:
    """
    Resolve the broker-specific symbol for a given canonical/any-format trading_symbol.

    Design:
      The symbols DB may have multiple rows for the same instrument imported from
      different broker scriptmasters. Only the high-coverage row (typically the
      'UNDERLYING-MMMYYYY-STRIKE-TYPE' format) has all broker-specific fields
      populated. A low-coverage row (e.g. Fyers/Kite format 'NIFTY2642124500CE')
      may have an empty shoonya_tsym/angelone_tsym/etc.

      This function:
        1. Looks up the primary row for the input symbol.
        2. Returns the broker field if non-empty.
        3. If empty (or no row), falls back to a secondary query by
           expiry + strike + option_type + exchange (picks highest-coverage row
           with the requested broker field non-empty).

    Returns dict with keys (all may be empty string if not found):
      - 'symbol'        : broker-specific trading symbol
      - 'token'         : broker-specific token/security_id (where applicable)
      - 'exchange'      : exchange from the resolved row
      - 'tick_size'     : float as string
      - 'lot_size'      : int as string
    """
    field = _BROKER_FIELD.get(broker_id.lower(), "")
    token_field = _BROKER_TOKEN_FIELD.get(broker_id.lower(), "")
    empty = {"symbol": "", "token": "", "exchange": exchange, "tick_size": "0.05", "lot_size": "1"}

    if not field:
        return empty

    conn = get_trading_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # ── Step 1: primary lookup ────────────────────────────────────────────
        rec = lookup_by_trading_symbol(trading_symbol, exchange)
        if rec:
            broker_sym = (rec.get(field) or "").strip()
            if broker_sym:
                return {
                    "symbol":    broker_sym,
                    "token":     (rec.get(token_field) or "").strip() if token_field else "",
                    "exchange":  (rec.get("exchange") or exchange or "").upper(),
                    "tick_size": str(rec.get("tick_size") or 0.05),
                    "lot_size":  str(rec.get("lot_size") or 1),
                }
            # Primary row exists but broker field is empty — use its components for fallback
            db_expiry  = rec.get("expiry")
            db_strike  = rec.get("strike")
            db_opttype = rec.get("option_type")
            db_exch    = (rec.get("exchange") or exchange or "").upper()
        else:
            # No primary row found — try to parse components from the symbol string itself
            db_expiry = db_strike = db_opttype = None
            db_exch   = (exchange or "").upper()

        # ── Step 2: partner-row fallback (same expiry/strike/option_type) ─────
        if db_expiry and db_strike is not None and db_opttype and db_exch:
            cur.execute(
                f"""
                SELECT {field}, {token_field + ',' if token_field else ''}
                       exchange, tick_size, lot_size
                FROM symbols
                WHERE expiry     = %s
                  AND ABS(strike - %s) < 0.001
                  AND option_type = %s
                  AND exchange    = %s
                  AND {field} != ''
                ORDER BY broker_coverage DESC
                LIMIT 1
                """,
                (str(db_expiry), float(db_strike), db_opttype, db_exch),
            )
            row = cur.fetchone()
            if row:
                return {
                    "symbol":    (row[field] or "").strip(),
                    "token":     (row[token_field] or "").strip() if token_field else "",
                    "exchange":  (row["exchange"] or db_exch).upper(),
                    "tick_size": str(row.get("tick_size") or 0.05),
                    "lot_size":  str(row.get("lot_size") or 1),
                }

        return empty
    except Exception as exc:
        logger.warning("resolve_broker_symbol(%s, %s) failed: %s", trading_symbol, broker_id, exc)
        return empty
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
            f"ORDER BY CASE WHEN UPPER(symbol) = %s THEN 0 "
            f"WHEN UPPER(symbol) LIKE %s THEN 1 ELSE 2 END, "
            f"CASE instrument_type WHEN 'EQ' THEN 0 WHEN 'IDX' THEN 1 ELSE 2 END, "
            f"expiry ASC NULLS LAST, "
            f"broker_coverage DESC, symbol LIMIT %s",
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
    """Return sorted ISO expiries (nearest first) for a symbol and instrument type.

    For MCX/CDS the ``symbol`` column stores full contract names such as
    "GOLDPETAL FUT 30 APR 26" so we also match via a regex pattern on the
    base symbol name in addition to an exact-equals check.
    """
    conn = get_trading_conn()
    try:
        cur = conn.cursor()
        sym_upper = symbol.upper().strip()
        exch_upper = exchange.upper()
        itype_upper = instrument_type.upper().strip()

        if exch_upper in ("MCX", "CDS"):
            # Broker ScriptMaster stores full contract names in symbol column,
            # e.g. "GOLDPETAL FUT 30 APR 26" or "GOLDPETAL26APR".  Use a
            # case-insensitive regex anchor so "GOLDPETAL" matches both without
            # accidentally matching "GOLDPETALCOM".
            base_pattern = f"^{re.escape(sym_upper)}(\\s|\\d)"
            cur.execute(
                """
                SELECT DISTINCT expiry
                FROM symbols
                WHERE exchange = %s
                  AND (instrument_type = %s OR instrument_type LIKE %s)
                  AND (UPPER(symbol) = %s OR UPPER(symbol) ~* %s)
                  AND expiry IS NOT NULL
                  AND expiry >= CURRENT_DATE
                ORDER BY expiry
                """,
                (exch_upper, itype_upper, itype_upper + "%", sym_upper, base_pattern),
            )
        else:
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
                (exch_upper, itype_upper, sym_upper),
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


def get_fno_underlyings() -> List[Dict[str, Any]]:
    """
    Return distinct underlying symbols that have option (OPT) or futures (FUT)
    contracts in FNO segments: NFO, BFO, MCX, CDS.

    Returns a list of dicts:
        {"symbol": "NIFTY", "exchange": "NFO", "instrument_types": ["OPT", "FUT"]}
    Sorted by exchange priority (NFO first) then symbol alphabetically.

    Broker ScriptMasters may store contract-specific names in `symbol` while the
    canonical base can often be inferred from normalized trading symbol columns.
    We prefer row-level normalized/base extraction when available, then fallback
    to suffix stripping from `symbol`.
    """

    def _sanitize_base_symbol(value: str) -> str:
        s = _clean_text(value)
        if not s:
            return ""
        # Common BFO artifact forms: BANKBARODA7. / BANDHANBNK2.
        s = re.sub(r"^([A-Z0-9&-]+)[0-9]\.$", r"\1", s)
        # Single trailing dot artifact: INOXWIND.
        s = re.sub(r"^([A-Z0-9&-]+)\.$", r"\1", s)
        return s

    def _is_valid_base_symbol(value: str) -> bool:
        s = _sanitize_base_symbol(value)
        if not s or len(s) < 2:
            return False
        if _STRIKE_LIKE_RE.search(s):
            return False
        return True

    def _base_from_normalized_trading_symbol(tsym: str) -> str:
        t = _clean_text(tsym)
        if not t:
            return ""

        # Canonical hyphenated form: GOLD-30APR2026-121100-PE -> GOLD
        if "-" in t:
            first = t.split("-", 1)[0].strip()
            if _is_valid_base_symbol(first):
                return first

        # Compact contract form: NIFTY26APR24500CE / BANKNIFTY26MAYFUT -> NIFTY/BANKNIFTY
        compact = re.match(r"^([A-Z0-9&-]+?)\d{2}(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\d{2}.*$", t)
        if compact:
            base = compact.group(1).strip()
            if _is_valid_base_symbol(base):
                return base

        # Space form: GOLD 121100 PE 30 APR 26 -> GOLD
        spaced = re.match(r"^([A-Z0-9&]+)\s+\d+(?:\.\d+)?\s+(?:CE|PE|FUT)\s+\d{1,2}\s+[A-Z]{3}\s+\d{2}$", t)
        if spaced:
            base = spaced.group(1).strip()
            if _is_valid_base_symbol(base):
                return base

        return ""

    def _row_base_symbol(row: Dict[str, Any]) -> str:
        # Prefer row-level normalized signals before generic stripping.
        for candidate in (
            _base_from_normalized_trading_symbol(row.get("normalized_trading_symbol", "")),
            _base_from_normalized_trading_symbol(row.get("trading_symbol", "")),
            row.get("normalized_underlying", ""),
            _extract_base_symbol(row.get("symbol", "")),
        ):
            c = _sanitize_base_symbol(_extract_base_symbol(str(candidate or "")))
            if _is_valid_base_symbol(c):
                return c
        return ""

    conn = get_trading_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT
                symbol,
                normalized_underlying,
                normalized_trading_symbol,
                trading_symbol,
                exchange,
                array_agg(DISTINCT
                    CASE
                        WHEN instrument_type LIKE 'OPT%' THEN 'OPT'
                        WHEN instrument_type LIKE 'FUT%' THEN 'FUT'
                        ELSE instrument_type
                    END
                ) AS instrument_types
            FROM symbols
                        WHERE exchange IN ('NFO', 'BFO', 'MCX')
              AND (instrument_type LIKE 'OPT%' OR instrument_type LIKE 'FUT%')
              AND symbol IS NOT NULL
              AND symbol != ''
                            AND broker_coverage >= 5
                            AND UPPER(symbol) NOT LIKE '%TEST%'
            GROUP BY symbol, normalized_underlying, normalized_trading_symbol, trading_symbol, exchange
            """
        )
        rows = cur.fetchall()
        if not rows:
            return []

        # Deduplicate by (base_symbol, exchange) using row-level normalized base.
        groups: Dict[tuple, set] = {}
        for r in rows:
            base = _row_base_symbol(r)
            if not base:
                continue
            key = (base, r["exchange"])
            if key not in groups:
                groups[key] = set()
            for it in (r["instrument_types"] or []):
                if it:
                    groups[key].add(it)

        _exchange_priority = {"NFO": 1, "BFO": 2, "MCX": 3, "CDS": 4}
        result = [
            {"symbol": sym, "exchange": exch, "instrument_types": sorted(itypes)}
            for (sym, exch), itypes in groups.items()
        ]
        result.sort(key=lambda x: (_exchange_priority.get(x["exchange"], 5), x["symbol"]))
        return result
    except Exception as exc:
        logger.warning("get_fno_underlyings failed: %s", exc)
        return []
    finally:
        conn.close()


def get_option_contract_tokens(
    symbol: str,
    exchange: str = "NFO",
    expiry: Optional[str] = None,
    limit: int = 2000,
) -> List[Dict[str, Any]]:
    """
    Return option-contract rows + per-broker tokens for one underlying.

    Designed for intraday token subscription preparation.
    """
    base = _clean_text(symbol)
    exch = _clean_text(exchange)
    lim = max(1, min(int(limit or 2000), 20000))
    expiry_iso = _parse_expiry(expiry) if expiry else None

    conn = get_trading_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        params: List[Any] = [exch, base, base, f"{base}-%"]
        where_parts = [
            "exchange = %s",
            "instrument_type = 'OPT'",
            """(
                UPPER(COALESCE(normalized_underlying, '')) = %s
                OR UPPER(COALESCE(symbol, '')) = %s
                OR UPPER(COALESCE(normalized_trading_symbol, '')) LIKE %s
            )""",
        ]
        if expiry_iso:
            where_parts.append("expiry = %s")
            params.append(expiry_iso)

        params.append(lim)
        cur.execute(
            f"""
            SELECT
                exchange,
                exchange_token,
                symbol,
                trading_symbol,
                normalized_underlying,
                normalized_trading_symbol,
                expiry,
                strike,
                option_type,
                lot_size,
                tick_size,
                broker_coverage,
                fyers_symbol,
                fyers_token,
                shoonya_tsym,
                shoonya_token,
                angelone_tsym,
                angelone_token,
                dhan_security_id,
                kite_instrument_token,
                kite_exchange_token,
                upstox_ikey,
                groww_token
            FROM symbols
            WHERE {' AND '.join(where_parts)}
            ORDER BY expiry ASC NULLS LAST, strike ASC, option_type ASC, broker_coverage DESC
            LIMIT %s
            """,
            params,
        )
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    except Exception as exc:
        logger.warning(
            "get_option_contract_tokens(%s, %s, %s) failed: %s",
            symbol,
            exchange,
            expiry,
            exc,
        )
        return []
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
    """Parse a wide variety of expiry date strings to ISO YYYY-MM-DD.

    Handles:
        - 2026-04-30  (ISO)
        - 30-Apr-2026 / 30-April-2026
        - 30/04/2026
        - 30APR2026   (compact, Shoonya)
        - 30-Apr-26   (two-digit year, Upstox/Shoonya)
        - 30 APR 26   (Kite/Upstox NSE_COM space-separated two-digit year)
    """
    if not raw:
        return None
    raw = raw.strip()
    # Already ISO YYYY-MM-DD?
    if len(raw) == 10 and raw[4] == "-":
        return raw
    # Standard four-digit-year formats
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d", "%d/%m/%Y",
                "%d%b%Y", "%d%B%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Two-digit year variants: "30-Apr-26", "30 Apr 26", "30APR26"
    normalized = raw.replace(" ", "-")
    for fmt in ("%d-%b-%y", "%d-%B-%y", "%d%b%y"):
        try:
            return datetime.strptime(normalized, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def validate_symbols_accuracy() -> Dict[str, Any]:
    """
    Run accuracy checks on the symbols table and log a structured report.
    Raises no exceptions — returns a dict of check results for callers to inspect.
    """
    report: Dict[str, Any] = {}
    try:
        conn = get_trading_conn()
        try:
            cur = conn.cursor()

            cur.execute("SELECT COUNT(*) FROM symbols")
            report["total_rows"] = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM symbols
                WHERE lot_size <= 0 OR tick_size <= 0
                   OR symbol = '' OR trading_symbol = ''
                   OR exchange = '' OR exchange_token = ''
            """)
            report["critical_missing_fields"] = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM symbols
                WHERE instrument_type IN ('FUT','OPT') AND expiry IS NULL
            """)
            report["fut_opt_missing_expiry"] = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM symbols
                WHERE fyers_symbol <> '' AND fyers_symbol NOT LIKE '%:%'
            """)
            report["bad_fyers_format"] = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM symbols
                WHERE upstox_ikey <> '' AND upstox_ikey NOT LIKE '%|%'
            """)
            report["bad_upstox_format"] = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT exchange, exchange_token
                    FROM symbols
                    GROUP BY exchange, exchange_token
                    HAVING COUNT(*) > 1
                ) t
            """)
            report["duplicate_primary_keys"] = cur.fetchone()[0]

            cur.execute("""
                SELECT
                  SUM(CASE WHEN fyers_symbol <> '' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN shoonya_tsym <> '' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN angelone_tsym <> '' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN dhan_security_id <> '' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN (kite_exchange_token <> '' OR kite_instrument_token <> '') THEN 1 ELSE 0 END),
                  SUM(CASE WHEN upstox_ikey <> '' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN groww_token <> '' THEN 1 ELSE 0 END),
                  COUNT(*)
                FROM symbols
            """)
            row = cur.fetchone()
            total = row[7] or 1
            report["broker_coverage"] = {
                "fyers":    {"rows": row[0], "pct": round(row[0]/total*100, 1)},
                "shoonya":  {"rows": row[1], "pct": round(row[1]/total*100, 1)},
                "angelone": {"rows": row[2], "pct": round(row[2]/total*100, 1)},
                "dhan":     {"rows": row[3], "pct": round(row[3]/total*100, 1)},
                "kite":     {"rows": row[4], "pct": round(row[4]/total*100, 1)},
                "upstox":   {"rows": row[5], "pct": round(row[5]/total*100, 1)},
                "groww":    {"rows": row[6], "pct": round(row[6]/total*100, 1)},
            }
        finally:
            conn.close()
    except Exception as exc:
        report["error"] = str(exc)
        logger.error("Symbol accuracy validation failed: %s", exc)
        return report

    # Evaluate pass/fail
    failures = []
    if report.get("critical_missing_fields", 0) > 0:
        failures.append(f"critical_missing_fields={report['critical_missing_fields']}")
    if report.get("duplicate_primary_keys", 0) > 0:
        failures.append(f"duplicate_primary_keys={report['duplicate_primary_keys']}")
    if report.get("bad_fyers_format", 0) > 0:
        failures.append(f"bad_fyers_format={report['bad_fyers_format']}")
    if report.get("bad_upstox_format", 0) > 0:
        failures.append(f"bad_upstox_format={report['bad_upstox_format']}")

    remaining_expiry = report.get("fut_opt_missing_expiry", 0)

    report["pass"] = len(failures) == 0
    report["warnings"] = []
    if remaining_expiry > 0:
        report["warnings"].append(f"fut_opt_missing_expiry={remaining_expiry} (may be non-tradeable reference rows)")

    if failures:
        logger.error("SYMBOL DB ACCURACY FAILURES: %s", "; ".join(failures))
    elif report["warnings"]:
        logger.warning("Symbol DB accuracy warnings: %s", "; ".join(report["warnings"]))
    else:
        logger.info("Symbol DB accuracy check PASSED — %d rows, no critical issues", report["total_rows"])

    return report


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

            # 3) Clear normalizer LRU cache so fresh data is picked up immediately
            try:
                from broker.symbol_normalizer import get_normalizer
                get_normalizer().invalidate_cache()
            except Exception:
                pass  # normalizer not critical to the refresh cycle
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
