"""
Smart Trader — Copy Trading Service
=====================================

Fan out master trade alerts to follower endpoints.
Port of shoonya_platform/services/copy_trading_service.py

SECURITY: HMAC-SHA256 signed payloads. Secret must match on master & follower.
MODES:
  - mirror : copy alert as-is
  - scaled : multiply qty by COPY_TRADING_SCALE_FACTOR
"""

import hashlib
import hmac
import json
import logging
import os
import threading
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("smart_trader.copy_trading")


# ---------------------------------------------------------------------------
# Circuit Breaker
# ---------------------------------------------------------------------------

class _FollowerCircuitBreaker:
    FAILURE_THRESHOLD  = 3
    BASE_BACKOFF       = 30.0
    MAX_BACKOFF        = 600.0

    def __init__(self):
        self._lock  = threading.Lock()
        self._state: Dict[str, Dict] = {}

    def is_available(self, url: str) -> bool:
        with self._lock:
            info = self._state.get(url)
            if not info:
                return True
            if info["failures"] < self.FAILURE_THRESHOLD:
                return True
            return time.monotonic() >= info["tripped_until"]

    def record_success(self, url: str):
        with self._lock:
            self._state.pop(url, None)

    def record_failure(self, url: str):
        with self._lock:
            info = self._state.setdefault(
                url, {"failures": 0, "tripped_until": 0.0, "backoff": self.BASE_BACKOFF}
            )
            info["failures"] += 1
            if info["failures"] >= self.FAILURE_THRESHOLD:
                info["tripped_until"] = time.monotonic() + info["backoff"]
                info["backoff"] = min(info["backoff"] * 2, self.MAX_BACKOFF)
                logger.warning("CircuitBreaker TRIPPED: %s | failures=%d", url, info["failures"])

    def get_status(self) -> Dict:
        with self._lock:
            now = time.monotonic()
            return {
                url: {
                    "failures": s["failures"],
                    "tripped":  s["failures"] >= self.FAILURE_THRESHOLD and now < s["tripped_until"],
                }
                for url, s in self._state.items()
            }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_signature(payload_bytes: bytes, secret: str) -> str:
    return hmac.new(secret.encode(), payload_bytes, hashlib.sha256).hexdigest()


def _scale_qty(alert_data: Dict, factor: float) -> Dict:
    """Return deep copy of alert_data with all qty fields multiplied by factor."""
    def _scale(node: Any) -> Any:
        if isinstance(node, dict):
            return {
                k: max(1, int(v * factor)) if k.lower() in ("qty", "quantity", "lot_qty")
                   and isinstance(v, (int, float)) else _scale(v)
                for k, v in node.items()
            }
        if isinstance(node, list):
            return [_scale(i) for i in node]
        return node
    return _scale(json.loads(json.dumps(alert_data)))


# ---------------------------------------------------------------------------
# CopyTradingService
# ---------------------------------------------------------------------------

class CopyTradingService:
    """Thread-safe copy trading fanout + validation."""

    TIMEOUT_SECONDS = 5
    MAX_WORKERS     = 10

    def __init__(
        self,
        role: str = "standalone",             # standalone|master|follower
        secret: str = "",
        followers: Optional[List[str]] = None,
        mode: str = "mirror",                 # mirror|scaled
        scale_factor: float = 1.0,
        client_id: str = "",
    ):
        self._role         = role
        self._secret       = secret
        self._followers    = followers or []
        self._mode         = mode
        self._scale_factor = scale_factor
        self._client_id    = client_id
        self._enabled      = role in ("master", "follower")
        self._cb           = _FollowerCircuitBreaker()

        if role == "master":
            logger.info("CopyTradingService | MASTER | followers=%d | mode=%s",
                        len(self._followers), mode)
        elif role == "follower":
            logger.info("CopyTradingService | FOLLOWER")

    @classmethod
    def from_env(cls, client_id: str = "") -> "CopyTradingService":
        return cls(
            role=os.getenv("COPY_TRADING_ROLE", "standalone"),
            secret=os.getenv("COPY_TRADING_SECRET", ""),
            followers=list(filter(None, os.getenv("COPY_TRADING_FOLLOWERS", "").split(","))),
            mode=os.getenv("COPY_TRADING_MODE", "mirror"),
            scale_factor=float(os.getenv("COPY_TRADING_SCALE_FACTOR", "1.0")),
            client_id=client_id,
        )

    @property
    def is_master(self) -> bool:
        return self._role == "master"

    @property
    def is_follower(self) -> bool:
        return self._role == "follower"

    # -- Master: fan out ------------------------------------------------------

    def fan_out_alert(self, alert_data: Dict, master_result: Dict) -> Dict:
        """
        Called on master after a successful alert execution.
        Delivers signed payload to all followers in parallel.
        Returns per-follower delivery results.
        """
        if not self.is_master or not self._followers:
            return {}

        if self._mode == "scaled":
            alert_to_send = _scale_qty(alert_data, self._scale_factor)
        else:
            alert_to_send = alert_data

        payload = self._build_payload(alert_to_send, master_result)
        payload_bytes = json.dumps(payload, separators=(",", ":")).encode()
        sig = _build_signature(payload_bytes, self._secret) if self._secret else ""

        results = {}
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as pool:
            futures = {
                pool.submit(self._deliver, url, payload_bytes, sig): url
                for url in self._followers
                if self._cb.is_available(url)
            }
            tripped = [url for url in self._followers if not self._cb.is_available(url)]
            for url in tripped:
                results[url] = {"ok": False, "reason": "circuit_open"}

            for future in as_completed(futures):
                url = futures[future]
                try:
                    ok, reason = future.result()
                    results[url] = {"ok": ok, "reason": reason}
                    if ok:
                        self._cb.record_success(url)
                    else:
                        self._cb.record_failure(url)
                except Exception as e:
                    results[url] = {"ok": False, "reason": str(e)}
                    self._cb.record_failure(url)

        return results

    def _deliver(self, url: str, payload_bytes: bytes, sig: str) -> Tuple[bool, str]:
        try:
            req = urllib.request.Request(
                url, data=payload_bytes,
                headers={
                    "Content-Type": "application/json",
                    "X-Copy-Signature": sig,
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=self.TIMEOUT_SECONDS) as resp:
                if resp.status == 200:
                    return True, "ok"
                return False, f"HTTP {resp.status}"
        except urllib.error.HTTPError as e:
            return False, f"HTTP {e.code}"
        except Exception as e:
            return False, str(e)

    def _build_payload(self, alert_data: Dict, master_result: Dict) -> Dict:
        return {
            "master_client_id": self._client_id,
            "timestamp": datetime.utcnow().isoformat(),
            "alert": alert_data,
            "master_result": master_result,
        }

    # -- Follower: validation -------------------------------------------------

    def validate_copy_signature(self, payload_bytes: bytes, received_sig: str) -> bool:
        """Verify HMAC signature on incoming copy-alert. Returns True if valid."""
        if not self._secret:
            return True   # No secret configured — allow (insecure but explicit)
        expected = _build_signature(payload_bytes, self._secret)
        return hmac.compare_digest(expected, received_sig)

    def get_status(self) -> Dict:
        return {
            "role":         self._role,
            "enabled":      self._enabled,
            "followers":    len(self._followers),
            "mode":         self._mode,
            "scale_factor": self._scale_factor,
            "circuit_breaker": self._cb.get_status(),
        }
