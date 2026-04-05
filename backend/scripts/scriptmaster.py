"""
scriptmaster.py — Stub module for Smart Trader
================================================

Provides minimal implementations of functions imported by strategy_runner modules.
In production, this should be replaced with actual ScriptMaster integration that
queries instrument metadata from NSE/BSE/MCX master files.
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def get_future(symbol: str, exchange: str = "NFO", result: int = 0) -> Optional[Dict[str, Any]]:
    """
    Look up the nearest futures contract for a given underlying.
    Returns dict with TradingSymbol, LotSize, Expiry, etc. or None.
    """
    # Default lot sizes for common underlyings
    _LOT_SIZES = {
        "NIFTY": 75, "BANKNIFTY": 30, "FINNIFTY": 40,
        "MIDCPNIFTY": 75, "SENSEX": 10, "BANKEX": 15,
    }
    lot = _LOT_SIZES.get(symbol.upper(), 1)
    return {
        "TradingSymbol": f"{symbol}FUT",
        "LotSize": lot,
        "Exchange": exchange,
        "Symbol": symbol,
    }


def universal_symbol_search(symbol: str, exchange: str = "NFO") -> List[Dict[str, Any]]:
    """
    Search instrument master for matching symbols.
    Returns list of dicts with TradingSymbol, LotSize, Expiry, etc.
    """
    fut = get_future(symbol, exchange)
    return [fut] if fut else []


def requires_limit_order(exchange: str, symbol: str) -> bool:
    """
    Returns True if the instrument requires LIMIT orders (e.g. illiquid options).
    Default: False (allow MARKET orders).
    """
    return False


def options_expiry(symbol: str, exchange: str = "NFO") -> List[str]:
    """
    Return list of available option expiry dates for the symbol.
    Returns empty list when instrument data is unavailable.
    """
    return []
