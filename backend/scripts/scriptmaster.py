"""
scriptmaster.py — Instrument Master for Smart Trader
=====================================================

Delegates to the Fyers ScriptMaster (broker/fyers_scriptmaster.py) which
downloads and caches real instrument CSV files from Fyers public endpoints.

Provides the same interface expected by:
  - trading/strategy_runner/market_reader.py
  - trading/strategy_runner/entry_engine.py
  - trading/utils/option_chain_fetcher.py

Data source (no auth):
    https://public.fyers.in/sym_details/NSE_FO.csv
    https://public.fyers.in/sym_details/BSE_FO.csv
    https://public.fyers.in/sym_details/NSE_CM.csv
    https://public.fyers.in/sym_details/BSE_CM.csv

Downloaded once per day, served from disk cache for the rest of the session.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger("smart_trader.scriptmaster")

# ── Lazy init flag ─────────────────────────────────────────────────────────────
_init_lock = threading.Lock()
_initialized = False


def _ensure_loaded() -> None:
    """Ensure fyers_scriptmaster data is loaded (once per process)."""
    global _initialized
    if _initialized:
        return
    with _init_lock:
        if _initialized:
            return
        try:
            from broker.fyers_scriptmaster import refresh, FYERS_SCRIPTMASTER
            refresh()  # downloads once/day or loads from disk
            count = len(FYERS_SCRIPTMASTER)
            logger.info("ScriptMaster loaded: %d instruments", count)
            _initialized = True
        except Exception as exc:
            logger.warning("ScriptMaster load failed: %s — using fallback data", exc)
            _initialized = True  # mark done to avoid retry loops


# ── Fallback constants (used when real data unavailable) ───────────────────────

_LOT_SIZES: Dict[str, int] = {
    "NIFTY": 75, "BANKNIFTY": 30, "FINNIFTY": 40,
    "MIDCPNIFTY": 75, "SENSEX": 10, "BANKEX": 15,
    "CRUDEOIL": 100, "CRUDEOILM": 100, "GOLD": 100,
    "GOLDM": 100, "GOLDPETAL": 100, "SILVER": 30,
    "SILVERM": 100, "NATURALGAS": 1250, "COPPER": 2500,
}

_STRIKE_GAPS: Dict[str, int] = {
    "NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50,
    "MIDCPNIFTY": 25, "SENSEX": 100, "BANKEX": 100,
    "CRUDEOIL": 50, "GOLD": 100, "SILVER": 1000,
}

# Fyers index symbols for spot quotes
_INDEX_SYMBOLS: Dict[str, str] = {
    "NIFTY":      "NSE:NIFTY50-INDEX",
    "BANKNIFTY":  "NSE:NIFTYBANK-INDEX",
    "FINNIFTY":   "NSE:FINNIFTY-INDEX",
    "MIDCPNIFTY": "NSE:MIDCPNIFTY-INDEX",
    "SENSEX":     "BSE:SENSEX-INDEX",
}


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC API — consumed by strategy_runner modules
# ══════════════════════════════════════════════════════════════════════════════

def refresh(force: bool = False) -> int:
    """
    Download / reload instrument master data.
    Returns total symbol count or 0 on failure.
    """
    global _initialized
    try:
        from broker.fyers_scriptmaster import refresh as _refresh, FYERS_SCRIPTMASTER
        _refresh(force=force)
        _initialized = True
        return len(FYERS_SCRIPTMASTER)
    except Exception as exc:
        logger.error("ScriptMaster refresh failed: %s", exc)
        return 0


def get_future(
    symbol: str,
    exchange: str = "NFO",
    result: int = 0,
) -> Optional[Dict[str, Any]]:
    """
    Look up futures contract for a given underlying.

    Args:
        symbol:   e.g. "NIFTY", "BANKNIFTY"
        exchange: e.g. "NFO", "BFO", "MCX"
        result:   0 = nearest expiry, 1 = next, etc.

    Returns dict with TradingSymbol, LotSize, Expiry, Exchange, Token, etc.
    """
    _ensure_loaded()
    try:
        from broker.fyers_scriptmaster import get_futures
        futs = get_futures(symbol.upper(), exchange.upper())
        if futs and len(futs) > result:
            rec = futs[result]
            return _fyers_to_standard(rec)
    except Exception as exc:
        logger.debug("get_future(%s, %s) from fyers_scriptmaster: %s", symbol, exchange, exc)

    # Fallback: construct a synthetic record
    lot = _LOT_SIZES.get(symbol.upper(), 1)
    return {
        "TradingSymbol": f"{symbol.upper()}FUT",
        "Symbol": symbol.upper(),
        "Underlying": symbol.upper(),
        "Exchange": exchange.upper(),
        "LotSize": lot,
        "Expiry": "",
        "Instrument": "FUT",
        "Token": "",
        "FyersSymbol": "",
    }


def get_futures_list(
    symbol: str,
    exchange: str = "NFO",
) -> List[Dict[str, Any]]:
    """Return all futures contracts for a symbol, sorted by expiry."""
    _ensure_loaded()
    try:
        from broker.fyers_scriptmaster import get_futures
        return [_fyers_to_standard(r) for r in get_futures(symbol.upper(), exchange.upper())]
    except Exception:
        fut = get_future(symbol, exchange)
        return [fut] if fut else []


def get_option(
    symbol: str,
    exchange: str = "NFO",
    expiry: Optional[str] = None,
    strike: Optional[float] = None,
    option_type: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Look up a specific option contract.
    Returns the first matching record or None.
    """
    results = get_options_list(symbol, exchange, expiry, strike, option_type)
    return results[0] if results else None


def get_options_list(
    symbol: str,
    exchange: str = "NFO",
    expiry: Optional[str] = None,
    strike: Optional[float] = None,
    option_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Return options matching the given filters, sorted by (expiry, strike).
    """
    _ensure_loaded()
    try:
        from broker.fyers_scriptmaster import get_options
        raw = get_options(
            underlying=symbol.upper(),
            exchange=exchange.upper(),
            expiry=expiry,
            strike=strike,
            option_type=option_type.upper() if option_type else None,
        )
        return [_fyers_to_standard(r) for r in raw]
    except Exception as exc:
        logger.debug("get_options_list error: %s", exc)
        return []


def universal_symbol_search(
    symbol: str,
    exchange: str = "NFO",
) -> List[Dict[str, Any]]:
    """
    Search instrument master for matching symbols/underlyings.
    Returns list of dicts with TradingSymbol, LotSize, Expiry, etc.
    """
    _ensure_loaded()
    try:
        from broker.fyers_scriptmaster import search
        raw = search(query=symbol, exchange=exchange)
        return [_fyers_to_standard(r) for r in raw]
    except Exception as exc:
        logger.debug("universal_symbol_search error: %s", exc)
        # Fallback
        fut = get_future(symbol, exchange)
        return [fut] if fut else []


def options_expiry(
    symbol: str,
    exchange: str = "NFO",
    result: Optional[int] = None,
) -> Any:
    """
    Return option expiry dates for the symbol.

    Args:
        symbol:   e.g. "NIFTY"
        exchange: e.g. "NFO"
        result:   None = full list, 0 = nearest, 1 = next, etc.

    Returns:
        list[str] if result is None, or str if result is int,
        or empty list / "" if nothing found.
    """
    _ensure_loaded()
    try:
        from broker.fyers_scriptmaster import get_expiry_list
        expiries = get_expiry_list(symbol.upper(), exchange.upper(), "OPTION")
        if expiries:
            # Filter to future dates only
            today = datetime.now().date()
            expiries = [e for e in expiries if _parse_expiry_date(e) and _parse_expiry_date(e) >= today]
        if result is not None:
            return expiries[result] if len(expiries) > result else ""
        return expiries
    except Exception as exc:
        logger.debug("options_expiry error: %s", exc)
        return "" if result is not None else []


def fut_expiry(
    symbol: str,
    exchange: str = "NFO",
    result: Optional[int] = None,
) -> Any:
    """Return futures expiry dates for the symbol."""
    _ensure_loaded()
    try:
        from broker.fyers_scriptmaster import get_expiry_list
        expiries = get_expiry_list(symbol.upper(), exchange.upper(), "FUTURE")
        if expiries:
            today = datetime.now().date()
            expiries = [e for e in expiries if _parse_expiry_date(e) and _parse_expiry_date(e) >= today]
        if result is not None:
            return expiries[result] if len(expiries) > result else ""
        return expiries
    except Exception as exc:
        logger.debug("fut_expiry error: %s", exc)
        return "" if result is not None else []


def requires_limit_order(
    exchange: str = "",
    token: str = "",
    tradingsymbol: str = "",
) -> bool:
    """
    Returns True if the instrument requires LIMIT orders.
    Stock options and MCX options typically must be LIMIT.
    """
    _ensure_loaded()
    try:
        from broker.fyers_scriptmaster import get_symbol, FYERS_SCRIPTMASTER
        # Try lookup by FyersSymbol
        rec = None
        if tradingsymbol:
            rec = FYERS_SCRIPTMASTER.get(tradingsymbol)
        if rec:
            instr = rec.get("Instrument", "")
            return instr == "OPT" and rec.get("Exchange", "") in ("NFO", "BFO", "MCX")
    except Exception:
        pass
    # Conservative default: require LIMIT for stock options
    return "OPT" in tradingsymbol.upper() if tradingsymbol else False


def get_lot_size(symbol: str, exchange: str = "NFO") -> int:
    """Return lot size for the underlying."""
    _ensure_loaded()
    try:
        from broker.fyers_scriptmaster import get_near_futures
        rec = get_near_futures(symbol.upper(), exchange.upper())
        if rec and rec.get("LotSize", 0) > 0:
            return rec["LotSize"]
    except Exception:
        pass
    return _LOT_SIZES.get(symbol.upper(), 1)


def get_strike_gap(symbol: str, exchange: str = "NFO") -> int:
    """Return strike gap for the underlying."""
    return _STRIKE_GAPS.get(symbol.upper(), 50)


def get_spot_symbol(symbol: str) -> Optional[str]:
    """Return the Fyers spot/index symbol for an underlying (for quotes)."""
    return _INDEX_SYMBOLS.get(symbol.upper())


def build_option_trading_symbol(
    symbol: str,
    expiry: str,
    strike: float,
    option_type: str,
    exchange: str = "NFO",
) -> str:
    """
    Build a Fyers-format option trading symbol.
    e.g. "NSE:NIFTY2541024500CE"

    Uses the real instrument master if available, otherwise constructs
    from conventions.
    """
    _ensure_loaded()
    # Try to find the exact match in scriptmaster
    try:
        from broker.fyers_scriptmaster import get_options
        matches = get_options(
            underlying=symbol.upper(),
            exchange=exchange.upper(),
            expiry=expiry,
            strike=strike,
            option_type=option_type.upper(),
        )
        if matches:
            return matches[0].get("FyersSymbol", "")
    except Exception:
        pass

    # Fallback: construct from convention
    # Fyers format: NSE:NIFTY{YYMM}{DD}{STRIKE}{CE/PE}
    # This is approximate — real symbol may differ
    try:
        dt = _parse_expiry_date(expiry)
        if dt:
            yymm = dt.strftime("%y%m")
            dd = dt.strftime("%d")
            prefix = "NSE" if exchange in ("NFO", "NSE") else "BSE"
            strike_int = int(strike)
            return f"{prefix}:{symbol.upper()}{yymm}{dd}{strike_int}{option_type.upper()}"
    except Exception:
        pass

    return f"{symbol.upper()}{expiry}{int(strike)}{option_type.upper()}"


def get_instrument_count() -> int:
    """Return total loaded instrument count."""
    try:
        from broker.fyers_scriptmaster import FYERS_SCRIPTMASTER
        return len(FYERS_SCRIPTMASTER)
    except Exception:
        return 0


# ══════════════════════════════════════════════════════════════════════════════
# INTERNAL HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _fyers_to_standard(rec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a fyers_scriptmaster record to the standard interface
    expected by market_reader.py, entry_engine.py, etc.
    """
    return {
        "TradingSymbol": rec.get("FyersSymbol", ""),
        "FyersSymbol":   rec.get("FyersSymbol", ""),
        "Symbol":        rec.get("Underlying", ""),
        "Underlying":    rec.get("Underlying", ""),
        "Exchange":      rec.get("Exchange", ""),
        "Token":         rec.get("Token", ""),
        "FyToken":       rec.get("FyToken", ""),
        "LotSize":       rec.get("LotSize", 1),
        "TickSize":      rec.get("TickSize", 0.05),
        "Expiry":        rec.get("Expiry", ""),
        "Instrument":    rec.get("Instrument", ""),
        "OptionType":    rec.get("OptionType"),
        "StrikePrice":   rec.get("StrikePrice"),
        "Description":   rec.get("Description", ""),
        # Shoonya-compatible aliases
        "tsym":          rec.get("FyersSymbol", ""),
        "exch":          rec.get("Exchange", ""),
        "ls":            str(rec.get("LotSize", 1)),
    }


def _parse_expiry_date(expiry_str: str) -> Optional[Any]:
    """Parse DD-Mon-YYYY expiry string to date, or None."""
    if not expiry_str:
        return None
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(expiry_str, fmt).date()
        except ValueError:
            continue
    return None
