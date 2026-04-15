"""
Smart Trader — Option Chain Supervisor  (v3)
=============================================

Polls the broker for option chain data, attaches Black-Scholes Greeks,
and fans out to OptionChainStore. Runs as a background daemon thread.

v3 features (ported from shoonya_platform supervisor v3.0):
  - Feed stall detection + recovery with cooldown
  - Heartbeat file for external monitoring
  - Per-chain retry with exponential backoff
  - Session validation in main loop
  - Health monitoring & graceful degradation
  - Market-hours-aware staleness checks
"""

import json
import logging
import threading
import time
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from trading.market_data.option_chain.store import OptionChainStore
from trading.utils.bs_greeks import bs_greeks, implied_volatility, time_to_expiry_seconds

logger = logging.getLogger("smart_trader.option_chain.supervisor")

_RISK_FREE_RATE = 0.065        # ~6.5% Indian Tbill
_MARKET_CLOSE   = "15:30"

# ── Feed monitoring ──────────────────────────────────────────────────────────
FEED_STALL_THRESHOLD    = 60      # seconds without a successful fetch → stall
FEED_RECOVERY_COOLDOWN  = 300     # 5 min between recovery attempts
MAX_CHAIN_RETRY         = 3       # retries per chain on first failure
CHAIN_RETRY_BASE_DELAY  = 2       # exponential backoff base (seconds)
HEARTBEAT_INTERVAL      = 10      # seconds between heartbeat file writes
SESSION_CHECK_INTERVAL  = 300     # 5 min between session validity checks

# ── Market hours (IST) ──────────────────────────────────────────────────────
EXCHANGE_MARKET_HOURS: Dict[str, Tuple[dtime, dtime]] = {
    "NFO": (dtime(9, 15), dtime(15, 30)),
    "BFO": (dtime(9, 15), dtime(15, 30)),
    "MCX": (dtime(9, 0),  dtime(23, 30)),
    "NSE": (dtime(9, 15), dtime(15, 30)),
    "BSE": (dtime(9, 15), dtime(15, 30)),
}


class OptionChainSupervisor:
    """
    Polls broker for option chain data, enriches with computed Greeks,
    stores in OptionChainStore, and optionally persists a snapshot to DB.

    v3: Feed stall recovery, heartbeat, chain retry, session validation.
    """

    def __init__(
        self,
        bot,
        store: Optional[OptionChainStore] = None,
        poll_interval: float = 5.0,
    ):
        self.bot           = bot
        self.store         = store or OptionChainStore()
        self.poll_interval = poll_interval

        self._lock          = threading.RLock()
        self._running       = False
        self._thread: Optional[threading.Thread] = None
        self._subscriptions: List[Dict] = []   # [{underlying, expiry, strikes?}]

        # ── v3: Health & recovery state ─────────────────────────────────
        self._last_successful_fetch: float = time.time()
        self._last_feed_recovery:    float = 0.0
        self._last_session_check:    float = 0.0
        self._last_heartbeat:        float = 0.0
        self._feed_stall_count:      int   = 0
        self._total_fetches:         int   = 0
        self._total_errors:          int   = 0
        self._failed_chains: Dict[str, Tuple[float, int]] = {}  # key -> (last_attempt, count)
        self._heartbeat_file = Path("logs/oc_supervisor_heartbeat.json")

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
        self._heartbeat_file.parent.mkdir(parents=True, exist_ok=True)
        self._thread = threading.Thread(
            target=self._loop, daemon=True,
            name=f"OC-Supervisor:{self.bot.client_id}")
        self._thread.start()
        logger.info("OptionChainSupervisor v3 started | interval=%.0fs", self.poll_interval)

    def stop(self):
        self._running = False

    def force_refresh(self, underlying: str, expiry: str) -> Optional[Dict]:
        """Synchronous single fetch + enrich + store. Returns full data."""
        return self._fetch_and_store(underlying.upper(), expiry.upper())

    def get_health(self) -> Dict:
        """Return supervisor health snapshot."""
        with self._lock:
            subs = len(self._subscriptions)
        stale_secs = time.time() - self._last_successful_fetch
        return {
            "running":             self._running,
            "subscriptions":       subs,
            "total_fetches":       self._total_fetches,
            "total_errors":        self._total_errors,
            "feed_stall_count":    self._feed_stall_count,
            "last_fetch_age_secs": round(stale_secs, 1),
            "failed_chains":       len(self._failed_chains),
        }

    # -- Main loop (v3: with stall detection, heartbeat, session check) -------

    def _loop(self):
        while self._running:
            t0 = time.time()

            # ── Heartbeat ─────────────────────────────────────────────
            if t0 - self._last_heartbeat >= HEARTBEAT_INTERVAL:
                self._write_heartbeat()
                self._last_heartbeat = t0

            # ── Session validation (every 5 min) ─────────────────────
            if t0 - self._last_session_check >= SESSION_CHECK_INTERVAL:
                self._validate_session()
                self._last_session_check = t0

            # ── Feed stall detection ─────────────────────────────────
            stale_secs = t0 - self._last_successful_fetch
            if stale_secs > FEED_STALL_THRESHOLD and self._is_market_active():
                self._feed_stall_count += 1
                logger.warning(
                    "OC feed stall detected: %.0fs since last fetch (stall #%d)",
                    stale_secs, self._feed_stall_count,
                )
                self._attempt_feed_recovery()

            # ── Fetch all subscriptions ──────────────────────────────
            with self._lock:
                subs = list(self._subscriptions)

            any_success = False
            for sub in subs:
                key = f"{sub['underlying']}:{sub['expiry']}"

                # Check if this chain is in backoff
                if key in self._failed_chains:
                    last_attempt, count = self._failed_chains[key]
                    backoff = min(CHAIN_RETRY_BASE_DELAY * (2 ** min(count, 5)), 60)
                    if time.time() - last_attempt < backoff:
                        continue

                try:
                    result = self._fetch_and_store(
                        sub["underlying"], sub["expiry"], sub.get("strikes"),
                    )
                    if result:
                        any_success = True
                        self._failed_chains.pop(key, None)
                        self._total_fetches += 1
                    else:
                        self._record_chain_failure(key)
                except Exception:
                    self._total_errors += 1
                    self._record_chain_failure(key)
                    logger.exception("OC fetch failed: %s %s",
                                     sub["underlying"], sub["expiry"])

            if any_success:
                self._last_successful_fetch = time.time()

            elapsed = time.time() - t0
            remaining = max(0, self.poll_interval - elapsed)
            # Sleep in small increments so stop is responsive
            deadline = time.time() + remaining
            while self._running and time.time() < deadline:
                time.sleep(min(1.0, deadline - time.time()))

    # -- v3: Feed stall recovery ─────────────────────────────────────────────

    def _attempt_feed_recovery(self):
        """Try to recover from feed stall with cooldown."""
        now = time.time()
        if now - self._last_feed_recovery < FEED_RECOVERY_COOLDOWN:
            logger.debug("Feed recovery cooldown active (%.0fs remaining)",
                         FEED_RECOVERY_COOLDOWN - (now - self._last_feed_recovery))
            return

        logger.warning("Attempting feed recovery — reconnecting broker session...")
        self._last_feed_recovery = now

        try:
            # Try to re-validate broker session
            if hasattr(self.bot, "api") and hasattr(self.bot.api, "ensure_session"):
                self.bot.api.ensure_session()
                logger.info("Feed recovery: broker session re-validated")
                self._last_successful_fetch = time.time()
                self._feed_stall_count = 0
            elif hasattr(self.bot, "reconnect"):
                self.bot.reconnect()
                logger.info("Feed recovery: broker reconnected")
                self._last_successful_fetch = time.time()
                self._feed_stall_count = 0
            else:
                logger.debug("Feed recovery: no reconnect method available")
        except Exception as e:
            logger.error("Feed recovery failed: %s", e)

    def _validate_session(self):
        """Periodic session liveness check."""
        try:
            if hasattr(self.bot, "api") and hasattr(self.bot.api, "ensure_session"):
                self.bot.api.ensure_session()
        except Exception as e:
            logger.warning("Session validation failed: %s", e)

    def _record_chain_failure(self, key: str):
        """Track failed chain with backoff counter."""
        if key in self._failed_chains:
            _, count = self._failed_chains[key]
            self._failed_chains[key] = (time.time(), count + 1)
        else:
            self._failed_chains[key] = (time.time(), 1)

    def _is_market_active(self) -> bool:
        """Return True if any tracked exchange is in active market hours."""
        now = datetime.now()
        if now.weekday() >= 5:  # Weekend
            return False
        now_t = now.time()
        # Check if any subscription's exchange is active
        with self._lock:
            exchanges = {s.get("exchange", "NFO") for s in self._subscriptions}
        if not exchanges:
            exchanges = {"NFO"}
        for exch in exchanges:
            hours = EXCHANGE_MARKET_HOURS.get(exch.upper())
            if hours and hours[0] <= now_t <= hours[1]:
                return True
        return False

    def _write_heartbeat(self):
        """Write heartbeat JSON for external monitoring."""
        try:
            data = {
                "ts": datetime.now().isoformat(),
                "running": self._running,
                "fetches": self._total_fetches,
                "errors": self._total_errors,
                "stalls": self._feed_stall_count,
                "subs": len(self._subscriptions),
                "last_fetch_age": round(time.time() - self._last_successful_fetch, 1),
            }
            self._heartbeat_file.write_text(json.dumps(data))
        except Exception:
            pass  # Heartbeat is best-effort

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
                    greeks = bs_greeks(spot, strike, T, _RISK_FREE_RATE, sigma, side) or {}
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
