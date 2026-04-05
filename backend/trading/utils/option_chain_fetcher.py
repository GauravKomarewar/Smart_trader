"""
option_chain_fetcher.py — Fetch option chain from broker and persist to SQLite
================================================================================

Creates SQLite databases in the format expected by MarketReader:
    data/option_chain/{EXCHANGE}_{SYMBOL}_{DD-MMM-YYYY}.sqlite

Tables:
    meta         — key/value pairs (spot_ltp, atm, fut_ltp, snapshot_ts, etc.)
    option_chain — strike, option_type, ltp, delta, gamma, theta, vega, iv, oi, volume, bid, ask, lot_size, trading_symbol

Data source priority:
    1. FyersDataClient singleton (REST API, no auth needed for option chain)
    2. Shoonya BrokerSession (if logged in)
"""

import logging
import sqlite3
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("smart_trader.option_chain_fetcher")

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_FOLDER = _PROJECT_ROOT / "data" / "option_chain"


def _ensure_db_folder():
    DB_FOLDER.mkdir(parents=True, exist_ok=True)


def _get_db_path(exchange: str, symbol: str, expiry: str) -> Path:
    _ensure_db_folder()
    return DB_FOLDER / f"{exchange.upper()}_{symbol.upper()}_{expiry}.sqlite"


def create_or_update_chain_db(
    exchange: str,
    symbol: str,
    expiry: str,
    spot_price: float,
    atm_strike: float,
    fut_ltp: float,
    chain_rows: List[Dict[str, Any]],
    lot_size: int = 1,
) -> str:
    """
    Create or update a SQLite option chain database.

    Returns:
        Path to the created/updated SQLite file.
    """
    db_path = _get_db_path(exchange, symbol, expiry)

    conn = sqlite3.connect(str(db_path), timeout=5)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS option_chain (
                strike         REAL NOT NULL,
                option_type    TEXT NOT NULL,
                ltp            REAL DEFAULT 0,
                delta          REAL DEFAULT 0,
                gamma          REAL DEFAULT 0,
                theta          REAL DEFAULT 0,
                vega           REAL DEFAULT 0,
                iv             REAL DEFAULT 0,
                oi             INTEGER DEFAULT 0,
                volume         INTEGER DEFAULT 0,
                bid            REAL DEFAULT 0,
                ask            REAL DEFAULT 0,
                lot_size       INTEGER DEFAULT 1,
                trading_symbol TEXT DEFAULT '',
                PRIMARY KEY (strike, option_type)
            )
        """)

        # Update meta
        now_ts = time.time()
        meta_entries = {
            "spot_ltp":    str(spot_price),
            "atm":         str(atm_strike),
            "fut_ltp":     str(fut_ltp),
            "snapshot_ts": str(now_ts),
            "exchange":    exchange.upper(),
            "symbol":      symbol.upper(),
            "expiry":      expiry,
            "lot_size":    str(lot_size),
            "updated_at":  datetime.now().isoformat(),
        }
        for key, value in meta_entries.items():
            conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                (key, value),
            )

        # Upsert chain rows
        for row in chain_rows:
            conn.execute("""
                INSERT OR REPLACE INTO option_chain
                    (strike, option_type, ltp, delta, gamma, theta, vega, iv,
                     oi, volume, bid, ask, lot_size, trading_symbol)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                float(row.get("strike", 0)),
                str(row.get("option_type", "CE")),
                float(row.get("ltp", 0)),
                float(row.get("delta", 0)),
                float(row.get("gamma", 0)),
                float(row.get("theta", 0)),
                float(row.get("vega", 0)),
                float(row.get("iv", 0)),
                int(row.get("oi", 0)),
                int(row.get("volume", 0)),
                float(row.get("bid", 0)),
                float(row.get("ask", 0)),
                int(row.get("lot_size", lot_size)),
                str(row.get("trading_symbol", "")),
            ))

        conn.commit()
        logger.info(
            "Option chain DB updated: %s | rows=%d | spot=%.2f | atm=%s",
            db_path.name, len(chain_rows), spot_price, atm_strike,
        )
        return str(db_path)

    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# FETCH FROM BROKER AND STORE
# ══════════════════════════════════════════════════════════════════════════════

def fetch_and_store_from_broker(
    exchange: str,
    symbol: str,
    expiry_hint: str = "current",
) -> Optional[str]:
    """
    Fetch option chain from the best available broker source and persist to SQLite.

    Priority:
      1. FyersDataClient.get_option_chain()  (works without separate login)
      2. ShoonyaClient.get_option_chain()    (needs active session)

    Returns path to the SQLite file or None if no data available.
    """
    sym = symbol.upper()

    # ── Source 1: Fyers Data Client (singleton, no per-user session needed) ──
    try:
        from broker.fyers_client import get_fyers_client
        fyers = get_fyers_client()
        if fyers.is_live:
            raw = fyers.get_option_chain(sym)
            if raw:
                result = _process_fyers_chain(exchange, sym, raw)
                if result:
                    return result
    except Exception as exc:
        logger.debug("Fyers option chain fetch for %s: %s", sym, exc)

    # ── Source 2: Shoonya client (legacy direct session) ──
    try:
        from broker.shoonya_client import get_session
        session = get_session()
        if not session.is_demo and hasattr(session, 'get_option_chain'):
            data = session.get_option_chain(exchange=exchange, symbol=sym)
            if data:
                return _process_shoonya_chain(exchange, sym, data)
    except Exception as exc:
        logger.debug("Shoonya option chain fetch for %s: %s", sym, exc)

    logger.debug("No option chain source available for %s %s", exchange, sym)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# PROCESS FYERS OPTION CHAIN → SQLite
# ══════════════════════════════════════════════════════════════════════════════

def _process_fyers_chain(exchange: str, symbol: str, data: Dict) -> Optional[str]:
    """
    Process Fyers optionchain API response into SQLite.

    Fyers response structure:
        data = {
            "expiryData": [{"date": "2026-04-10", "expiry": 1775932200}, ...],
            "optionsChain": [
                {
                    "strike_price": 25000,           # -1 for index row
                    "option_type": "CE" / "PE",
                    "ltp": 150.5,
                    "ask": 151.0, "bid": 150.0,
                    "oi": 5000000,
                    "oich": 100000,                  # OI change
                    "volume": 50000,
                    "symbol": "NSE:NIFTY2541025000CE",
                    ...
                },
                ...
            ]
        }
    """
    chain_list = data.get("optionsChain", [])
    expiry_data = data.get("expiryData", [])

    if not chain_list:
        logger.debug("Fyers: empty optionsChain for %s", symbol)
        return None

    # Extract spot from the index row (strike_price == -1)
    spot = 0.0
    for row in chain_list:
        if row.get("strike_price", 0) == -1:
            spot = float(row.get("ltp", 0))
            break

    if spot <= 0:
        logger.debug("Fyers: no spot found for %s", symbol)
        return None

    # Get nearest expiry date
    expiry_str = ""
    if expiry_data:
        # Fyers returns dates like "2026-04-10"
        raw_date = expiry_data[0].get("date", "")
        expiry_str = _fyers_date_to_dd_mmm_yyyy(raw_date)
    if not expiry_str:
        logger.debug("Fyers: no expiry date for %s", symbol)
        return None

    # Get lot size from scriptmaster
    try:
        from scripts.unified_scriptmaster import get_lot_size
        lot_size = get_lot_size(symbol, exchange)
    except Exception:
        lot_size = 1

    # Group by strike and build rows
    by_strike: Dict[float, Dict[str, Dict]] = defaultdict(dict)
    for row in chain_list:
        strike_price = row.get("strike_price", -1)
        if strike_price <= 0:
            continue
        opt_type = row.get("option_type", "")
        if opt_type not in ("CE", "PE"):
            continue
        by_strike[strike_price][opt_type] = row

    # Calculate ATM
    all_strikes = sorted(by_strike.keys())
    atm = min(all_strikes, key=lambda s: abs(s - spot)) if all_strikes else 0

    # Build chain rows with Greeks computation
    chain_rows: List[Dict[str, Any]] = []
    for strike, sides in sorted(by_strike.items()):
        for ot in ("CE", "PE"):
            leg = sides.get(ot)
            if not leg:
                continue
            ltp = float(leg.get("ltp", 0) or 0)

            # Compute Greeks
            delta = gamma = theta = vega = iv_val = 0.0
            if ltp > 0.01 and spot > 0:
                try:
                    from trading.utils.bs_greeks import (
                        implied_volatility, bs_greeks, time_to_expiry_seconds,
                    )
                    T = time_to_expiry_seconds(expiry_str, "15:30")
                    if T > 0:
                        iv_val = implied_volatility(ltp, spot, strike, T, 0.065, ot) or 0.0
                        sigma = (iv_val / 100.0) if iv_val > 1 else (iv_val if iv_val > 0 else 0.20)
                        greeks = bs_greeks(spot, strike, T, 0.065, sigma, ot) or {}
                        delta = greeks.get("delta", 0)
                        gamma = greeks.get("gamma", 0)
                        theta = greeks.get("theta", 0)
                        vega  = greeks.get("vega", 0)
                except Exception:
                    pass

            chain_rows.append({
                "strike":         strike,
                "option_type":    ot,
                "ltp":            ltp,
                "delta":          delta,
                "gamma":          gamma,
                "theta":          theta,
                "vega":           vega,
                "iv":             iv_val,
                "oi":             int(leg.get("oi", 0) or 0),
                "volume":         int(leg.get("volume", 0) or 0),
                "bid":            float(leg.get("bid", 0) or 0),
                "ask":            float(leg.get("ask", 0) or 0),
                "lot_size":       lot_size,
                "trading_symbol": str(leg.get("symbol", "")),
            })

    if not chain_rows:
        return None

    return create_or_update_chain_db(
        exchange=exchange,
        symbol=symbol,
        expiry=expiry_str,
        spot_price=spot,
        atm_strike=atm,
        fut_ltp=spot,  # Use spot as proxy when futures LTP not available
        chain_rows=chain_rows,
        lot_size=lot_size,
    )


# ══════════════════════════════════════════════════════════════════════════════
# PROCESS SHOONYA OPTION CHAIN → SQLite
# ══════════════════════════════════════════════════════════════════════════════

def _process_shoonya_chain(exchange: str, symbol: str, data: Dict) -> Optional[str]:
    """
    Process Shoonya-format option chain response into SQLite.

    Shoonya OC format (from shoonya_client.get_option_chain):
        {
            "underlying": "NIFTY",
            "underlyingLtp": 25912.5,
            "expiry": "10-Apr-2026",
            "rows": [
                {"strike": 25000, "isATM": false,
                 "call": {"ltp": 900, "iv": 0, "oi": ..., "volume": ..., "bid": ..., "ask": ...},
                 "put":  {"ltp": ..., ...}},
                ...
            ]
        }
    """
    if not data or not isinstance(data, dict):
        return None

    spot = float(data.get("underlyingLtp", 0) or 0)
    expiry = str(data.get("expiry", ""))
    rows_data = data.get("rows", [])

    if not spot or not expiry or not rows_data:
        return None

    try:
        from scripts.unified_scriptmaster import get_lot_size
        lot_size = get_lot_size(symbol, exchange)
    except Exception:
        lot_size = 1

    atm = 0.0
    chain_rows: List[Dict[str, Any]] = []
    for entry in rows_data:
        strike = float(entry.get("strike", 0))
        if entry.get("isATM"):
            atm = strike

        for ot, side_key in [("CE", "call"), ("PE", "put")]:
            side_data = entry.get(side_key, {})
            if not side_data:
                continue
            ltp = float(side_data.get("ltp", 0) or 0)
            chain_rows.append({
                "strike":         strike,
                "option_type":    ot,
                "ltp":            ltp,
                "delta":          float(side_data.get("delta", 0) or 0),
                "gamma":          float(side_data.get("gamma", 0) or 0),
                "theta":          float(side_data.get("theta", 0) or 0),
                "vega":           float(side_data.get("vega", 0) or 0),
                "iv":             float(side_data.get("iv", 0) or 0),
                "oi":             int(side_data.get("oi", 0) or 0),
                "volume":         int(side_data.get("volume", 0) or 0),
                "bid":            float(side_data.get("bid", 0) or 0),
                "ask":            float(side_data.get("ask", 0) or 0),
                "lot_size":       lot_size,
                "trading_symbol": str(side_data.get("tsym", "")),
            })

    if not chain_rows:
        return None

    if not atm:
        atm = min((r["strike"] for r in chain_rows), key=lambda s: abs(s - spot), default=0)

    return create_or_update_chain_db(
        exchange=exchange,
        symbol=symbol,
        expiry=expiry,
        spot_price=spot,
        atm_strike=atm,
        fut_ltp=spot,
        chain_rows=chain_rows,
        lot_size=lot_size,
    )


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _fyers_date_to_dd_mmm_yyyy(date_str: str) -> str:
    """
    Convert Fyers date format to MarketReader format.
    '2026-04-10' → '10-Apr-2026'
    """
    if not date_str:
        return ""
    try:
        dt = datetime.strptime(date_str.strip(), "%Y-%m-%d")
        return dt.strftime("%-d-%b-%Y")  # e.g. "10-Apr-2026"
    except ValueError:
        # Already in DD-Mon-YYYY format?
        return date_str


def get_db_path(exchange: str, symbol: str, expiry: str) -> Path:
    """Public accessor for the DB path."""
    return _get_db_path(exchange, symbol, expiry)
