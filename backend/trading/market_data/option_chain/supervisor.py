"""
Smart Trader — Option Chain Supervisor
========================================

Polls the broker for option chain data, attaches Black-Scholes Greeks,
and fans out to OptionChainStore. Runs as a background daemon thread.

Port of shoonya_platform/market_data/option_chain/supervisor.py
"""

import logging
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from trading.market_data.option_chain.store import OptionChainStore
from trading.utils.bs_greeks import bs_greeks, implied_volatility, time_to_expiry_seconds

logger = logging.getLogger("smart_trader.option_chain.supervisor")

_RISK_FREE_RATE = 0.065        # ~6.5% Indian Tbill
_MARKET_CLOSE   = "15:30"


class OptionChainSupervisor:
    """
    Polls broker for option chain data, enriches with computed Greeks,
    stores in OptionChainStore, and optionally persists a snapshot to DB.
    """

    def __init__(
        self,
        bot,
        store: Optional[OptionChainStore] = None,
        poll_interval: float = 30.0,
    ):
        self.bot           = bot
        self.store         = store or OptionChainStore()
        self.poll_interval = poll_interval

        self._lock          = threading.RLock()
        self._running       = False
        self._thread: Optional[threading.Thread] = None
        self._subscriptions: List[Dict] = []   # [{underlying, expiry, strikes?}]

    # -- Subscription management ----------------------------------------------

    def subscribe(self, underlying: str, expiry: str, strikes: Optional[List[int]] = None):
        with self._lock:
            entry: Dict[str, Any] = {"underlying": underlying.upper(), "expiry": expiry.upper()}
            if strikes:
                entry["strikes"] = strikes
            if not any(e["underlying"] == entry["underlying"] and
                       e["expiry"] == entry["expiry"]
                       for e in self._subscriptions):
                self._subscriptions.append(entry)
                logger.info("Subscribed: %s %s", underlying, expiry)

    def unsubscribe(self, underlying: str, expiry: str):
        with self._lock:
            self._subscriptions = [
                s for s in self._subscriptions
                if not (s["underlying"] == underlying.upper() and s["expiry"] == expiry.upper())
            ]

    # -- Lifecycle ------------------------------------------------------------

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._loop, daemon=True,
            name=f"OC-Supervisor:{self.bot.client_id}")
        self._thread.start()
        logger.info("OptionChainSupervisor started | interval=%.0fs", self.poll_interval)

    def stop(self):
        self._running = False

    def force_refresh(self, underlying: str, expiry: str) -> Optional[Dict]:
        """Synchronous single fetch + enrich + store. Returns full data."""
        return self._fetch_and_store(underlying.upper(), expiry.upper())

    # -- Main loop ------------------------------------------------------------

    def _loop(self):
        while self._running:
            with self._lock:
                subs = list(self._subscriptions)
            for sub in subs:
                try:
                    self._fetch_and_store(sub["underlying"], sub["expiry"],
                                          sub.get("strikes"))
                except Exception:
                    logger.exception("OC fetch failed: %s %s",
                                     sub["underlying"], sub["expiry"])
            time.sleep(self.poll_interval)

    # -- Fetch + enrich -------------------------------------------------------

    def _fetch_and_store(
        self, underlying: str, expiry: str,
        requested_strikes: Optional[List[int]] = None,
    ) -> Optional[Dict]:
        """Fetch raw option chain from broker, attach Greeks, store it."""
        raw = self._fetch_raw(underlying, expiry)
        if not raw:
            return None

        spot    = raw.get("spot", 0.0)
        entries = raw.get("entries", {})   # {strike: {"CE":{ltp,oi,vol,...}, "PE":{...}}}

        if not entries or spot <= 0:
            return None

        T = time_to_expiry_seconds(expiry, _MARKET_CLOSE)
        enriched: Dict[int, Dict] = {}
        atm_strike = None
        min_dist   = float("inf")

        for strike_raw, sides in entries.items():
            strike = int(strike_raw)
            dist   = abs(strike - spot)
            if dist < min_dist:
                min_dist   = dist
                atm_strike = strike

            row: Dict = {"strike": strike}
            for side in ("CE", "PE"):
                leg = sides.get(side, {})
                ltp = float(leg.get("ltp", 0) or 0)
                if ltp > 0.01 and T > 1e-9:
                    iv = implied_volatility(ltp, spot, strike, T, _RISK_FREE_RATE, side)
                    sigma = (iv / 100) if iv else 0.20
                    greeks = bs_greeks(spot, strike, T, _RISK_FREE_RATE, sigma, side)
                    bs_price_val = greeks  # dict
                else:
                    iv, greeks, sigma = None, {}, 0.0

                row[side] = {
                    "ltp":    ltp,
                    "oi":     int(leg.get("oi",  0) or 0),
                    "volume": int(leg.get("vol", 0) or 0),
                    "bid":    float(leg.get("bid", 0) or 0),
                    "ask":    float(leg.get("ask", 0) or 0),
                    "iv":     iv,
                    "delta":  round(greeks.get("delta", 0), 4),
                    "gamma":  round(greeks.get("gamma", 0), 6),
                    "theta":  round(greeks.get("theta", 0), 4),
                    "vega":   round(greeks.get("vega", 0), 4),
                }
            enriched[strike] = row

        self.store.update(underlying, expiry, enriched, spot, atm_strike)
        return self.store.get_full(underlying, expiry)

    def _fetch_raw(self, underlying: str, expiry: str) -> Optional[Dict]:
        """
        Call broker adapter to get option chain.
        Returns {"spot": float, "entries": {strike: {"CE":{...}, "PE":{...}}}}

        Broker adapters should implement get_option_chain(underlying, expiry).
        Falls back to get_quotes() for a list of symbols when not supported.
        """
        if not hasattr(self.bot.api, "get_option_chain"):
            logger.debug("Broker has no get_option_chain — skipping %s %s", underlying, expiry)
            return None
        try:
            return self.bot.api.get_option_chain(underlying, expiry)
        except Exception as e:
            logger.warning("get_option_chain(%s, %s) failed: %s", underlying, expiry, e)
            return None

    # -- Query helpers --------------------------------------------------------

    def get_chain(self, underlying: str, expiry: str) -> Optional[Dict]:
        return self.store.get_full(underlying.upper(), expiry.upper())

    def get_atm_strikes(
        self, underlying: str, expiry: str, count: int = 5
    ) -> List[int]:
        return self.store.get_atm_strikes(underlying.upper(), expiry.upper(), count)
