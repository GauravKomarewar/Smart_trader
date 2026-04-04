"""
Smart Trader — Option Chain Store
===================================

In-memory option chain with PostgreSQL persistence (optional cache).
Stores processed option chain data keyed by (underlying, expiry).
"""

import logging
import threading
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger("smart_trader.option_chain.store")


class OptionChainStore:
    """
    Thread-safe in-memory store for option chains.
    Each entry: {strike -> {CE: {...}, PE: {...}}}
    """

    def __init__(self):
        self._lock  = threading.RLock()
        self._data: Dict[str, Dict] = {}     # key: "UNDERLYING:EXPIRY"
        self._meta: Dict[str, Dict] = {}     # metadata: spot, atm_strike, ts, iv_stats

    def _key(self, underlying: str, expiry: str) -> str:
        return f"{underlying.upper()}:{expiry.upper()}"

    def update(
        self,
        underlying: str,
        expiry: str,
        chain: Dict[int, Dict],   # strike -> {"CE": {...}, "PE": {...}}
        spot: float,
        atm_strike: Optional[int] = None,
    ) -> None:
        k = self._key(underlying, expiry)
        with self._lock:
            self._data[k] = chain
            self._meta[k] = {
                "underlying": underlying,
                "expiry":     expiry,
                "spot":       spot,
                "atm_strike": atm_strike or self._find_atm(spot, list(chain.keys())),
                "last_update": datetime.utcnow().isoformat(),
                "strikes":    sorted(chain.keys()),
            }

    def get(self, underlying: str, expiry: str) -> Optional[Dict]:
        with self._lock:
            return self._data.get(self._key(underlying, expiry))

    def get_meta(self, underlying: str, expiry: str) -> Optional[Dict]:
        with self._lock:
            return self._meta.get(self._key(underlying, expiry))

    def get_full(self, underlying: str, expiry: str) -> Optional[Dict]:
        with self._lock:
            k = self._key(underlying, expiry)
            if k not in self._data:
                return None
            return {"chain": self._data[k], "meta": self._meta.get(k, {})}

    def get_atm_strikes(self, underlying: str, expiry: str, count: int = 5) -> List[int]:
        """Return count ATM strikes above and below the ATM."""
        with self._lock:
            meta = self._meta.get(self._key(underlying, expiry))
            if not meta:
                return []
            all_strikes = sorted(meta.get("strikes", []))
            atm = meta.get("atm_strike")
            if not atm or not all_strikes:
                return []
            idx = min(range(len(all_strikes)), key=lambda i: abs(all_strikes[i] - atm))
            lo = max(0, idx - count)
            hi = min(len(all_strikes), idx + count + 1)
            return all_strikes[lo:hi]

    def list_keys(self) -> List[str]:
        with self._lock:
            return list(self._data.keys())

    def clear(self, underlying: str = "", expiry: str = "") -> None:
        with self._lock:
            if underlying:
                k = self._key(underlying, expiry)
                self._data.pop(k, None)
                self._meta.pop(k, None)
            else:
                self._data.clear()
                self._meta.clear()

    @staticmethod
    def _find_atm(spot: float, strikes: List[int]) -> Optional[int]:
        if not strikes:
            return None
        return min(strikes, key=lambda s: abs(s - spot))
