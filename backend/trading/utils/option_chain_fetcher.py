"""
option_chain_fetcher.py — Fetch option chain from broker and persist to SQLite
================================================================================

Creates SQLite databases in the format expected by MarketReader:
    data/option_chain/{EXCHANGE}_{SYMBOL}_{DD-MMM-YYYY}.sqlite

Tables:
    meta         — key/value pairs (spot_ltp, atm, fut_ltp, snapshot_ts, etc.)
    option_chain — strike, option_type, ltp, delta, gamma, theta, vega, iv, oi, volume, bid, ask, lot_size, trading_symbol
"""

import logging
import sqlite3
import time
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

    Args:
        exchange: NFO, MCX, BFO, etc.
        symbol: NIFTY, BANKNIFTY, etc.
        expiry: DD-MMM-YYYY format (e.g. 05-JUN-2025)
        spot_price: Current spot price
        atm_strike: ATM strike price
        fut_ltp: Futures LTP
        chain_rows: List of dicts with keys:
            strike, option_type (CE/PE), ltp, delta, gamma, theta, vega, iv,
            oi, volume, bid, ask, lot_size, trading_symbol
        lot_size: Default lot size for the underlying

    Returns:
        Path to the created/updated SQLite file.
    """
    db_path = _get_db_path(exchange, symbol, expiry)

    conn = sqlite3.connect(str(db_path), timeout=5)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")

        # Create tables
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
            "spot_ltp": str(spot_price),
            "atm": str(atm_strike),
            "fut_ltp": str(fut_ltp),
            "snapshot_ts": str(now_ts),
            "exchange": exchange.upper(),
            "symbol": symbol.upper(),
            "expiry": expiry,
            "lot_size": str(lot_size),
            "updated_at": datetime.now().isoformat(),
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


def fetch_and_store_from_broker(
    exchange: str,
    symbol: str,
    expiry_hint: str = "current",
) -> Optional[str]:
    """
    Fetch option chain from any available broker adapter and store to SQLite.

    Args:
        exchange: NFO, MCX, BFO
        symbol: NIFTY, BANKNIFTY, etc.
        expiry_hint: "current" or specific expiry string

    Returns:
        Path to the SQLite file or None if no data available.
    """
    try:
        from broker.multi_broker import registry
    except ImportError:
        logger.warning("Broker registry not available")
        return None

    sessions = registry.get_all_sessions()
    for sess in sessions:
        if not (sess.is_live or sess.is_paper):
            continue
        adapter = getattr(sess, '_adapter', None)
        if adapter is None:
            continue

        # Try Fyers adapter (has get_option_chain via API)
        if hasattr(adapter, 'get_option_chain'):
            try:
                data = adapter.get_option_chain(symbol, expiry_hint)
                if data and data.get("entries"):
                    return _process_fyers_chain(exchange, symbol, data)
            except Exception as e:
                logger.debug("Fyers option chain fetch failed: %s", e)

        # Try via market router directly
        try:
            from routers.market import _get_option_chain_data
            data = _get_option_chain_data(symbol, expiry_hint)
            if data:
                return _process_market_chain(exchange, symbol, data)
        except Exception as e:
            logger.debug("Market router fallback failed: %s", e)

    logger.warning("No broker data source available for %s %s", exchange, symbol)
    return None


def _process_fyers_chain(exchange: str, symbol: str, data: Dict) -> Optional[str]:
    """Process Fyers-format option chain data into SQLite."""
    spot = float(data.get("spot", 0))
    entries = data.get("entries", {})
    if not entries or spot <= 0:
        return None

    from trading.utils.bs_greeks import implied_volatility, bs_greeks, time_to_expiry_seconds

    # Determine expiry from the entries
    expiry = data.get("expiry", "")
    if not expiry:
        return None

    # Calculate ATM
    strikes = [int(k) for k in entries.keys()]
    atm = min(strikes, key=lambda s: abs(s - spot)) if strikes else 0

    rows = []
    for strike_raw, sides in entries.items():
        strike = int(strike_raw)
        for ot in ("CE", "PE"):
            leg = sides.get(ot, {})
            ltp = float(leg.get("ltp", 0) or 0)

            # Compute Greeks if we have LTP
            delta = gamma = theta = vega = iv_val = 0.0
            if ltp > 0.01:
                try:
                    T = time_to_expiry_seconds(expiry, "15:30")
                    if T > 0:
                        iv_val = implied_volatility(ltp, spot, strike, T, 0.065, ot) or 0.0
                        sigma = (iv_val / 100) if iv_val else 0.20
                        greeks = bs_greeks(spot, strike, T, 0.065, sigma, ot) or {}
                        delta = greeks.get("delta", 0)
                        gamma = greeks.get("gamma", 0)
                        theta = greeks.get("theta", 0)
                        vega = greeks.get("vega", 0)
                except Exception:
                    pass

            rows.append({
                "strike": strike,
                "option_type": ot,
                "ltp": ltp,
                "delta": delta,
                "gamma": gamma,
                "theta": theta,
                "vega": vega,
                "iv": iv_val,
                "oi": int(leg.get("oi", 0) or 0),
                "volume": int(leg.get("vol", 0) or leg.get("volume", 0) or 0),
                "bid": float(leg.get("bid", 0) or 0),
                "ask": float(leg.get("ask", 0) or 0),
            })

    _DEFAULT_LOTS = {"NIFTY": 75, "BANKNIFTY": 30, "FINNIFTY": 40, "MIDCPNIFTY": 75, "SENSEX": 10}
    lot_size = _DEFAULT_LOTS.get(symbol.upper(), 1)

    return create_or_update_chain_db(
        exchange=exchange,
        symbol=symbol,
        expiry=expiry,
        spot_price=spot,
        atm_strike=atm,
        fut_ltp=spot,
        chain_rows=rows,
        lot_size=lot_size,
    )


def _process_market_chain(exchange: str, symbol: str, data: Any) -> Optional[str]:
    """Process market router option chain format into SQLite."""
    # This handles the format from routers/market.py get_option_chain
    if not data or not isinstance(data, list):
        return None

    rows = []
    spot = 0.0
    atm = 0.0
    for entry in data:
        strike = float(entry.get("strike", 0))
        is_atm = entry.get("isATM", False)
        if is_atm:
            atm = strike

        for ot, side_data in [("CE", entry.get("call", {})), ("PE", entry.get("put", {}))]:
            if not side_data:
                continue
            ltp = float(side_data.get("ltp", 0) or 0)
            if ltp <= 0:
                continue
            rows.append({
                "strike": strike,
                "option_type": ot,
                "ltp": ltp,
                "delta": float(side_data.get("delta", 0) or 0),
                "gamma": float(side_data.get("gamma", 0) or 0),
                "theta": float(side_data.get("theta", 0) or 0),
                "vega": float(side_data.get("vega", 0) or 0),
                "iv": float(side_data.get("iv", 0) or 0),
                "oi": int(side_data.get("oi", 0) or 0),
                "volume": int(side_data.get("volume", 0) or 0),
                "bid": float(side_data.get("bid", 0) or 0),
                "ask": float(side_data.get("ask", 0) or 0),
            })

    if not rows:
        return None

    if spot <= 0 and atm > 0:
        spot = atm

    _DEFAULT_LOTS = {"NIFTY": 75, "BANKNIFTY": 30, "FINNIFTY": 40, "MIDCPNIFTY": 75, "SENSEX": 10}
    lot_size = _DEFAULT_LOTS.get(symbol.upper(), 1)

    # Determine expiry from data if available
    expiry = ""
    for entry in data:
        if entry.get("expiry"):
            expiry = entry["expiry"]
            break

    if not expiry:
        # Use a placeholder expiry
        from datetime import date, timedelta
        today = date.today()
        # Next Thursday
        days_ahead = 3 - today.weekday()
        if days_ahead <= 0:
            days_ahead += 7
        next_thu = today + timedelta(days=days_ahead)
        expiry = next_thu.strftime("%d-%b-%Y").upper()

    return create_or_update_chain_db(
        exchange=exchange,
        symbol=symbol,
        expiry=expiry,
        spot_price=spot,
        atm_strike=atm,
        fut_ltp=spot,
        chain_rows=rows,
        lot_size=lot_size,
    )
