"""
TradebookManager — Fetches tradebook (fills) from all live broker sessions
and upserts into mgr_tradebook.
"""

from __future__ import annotations
import logging
import time
from managers.base_manager import BaseManager

logger = logging.getLogger("smart_trader.mgr.tradebook")

# Per-session backoff tracking: config_id -> backoff_until timestamp
_rate_limit_until: dict[str, float] = {}
_BACKOFF_SECONDS = 60  # Wait 60s after a rate-limit error before retrying


class TradebookManager(BaseManager):
    MANAGER_NAME = "tradebook_manager"
    REFRESH_INTERVAL = 10.0  # Tradebook polls every 5s

    def refresh_user(self, user_id: str, sessions: list):
        self._refresh_sessions_concurrent(user_id, sessions, self._refresh_session)

    def _refresh_session(self, user_id: str, sess):
        # Skip if we're in a rate-limit backoff period for this session
        if time.monotonic() < _rate_limit_until.get(sess.config_id, 0):
            return
        try:
            raw_trades = sess.get_tradebook()
        except Exception as e:
            err_str = str(e).lower()
            if "429" in err_str or "rate" in err_str or "limit" in err_str:
                _rate_limit_until[sess.config_id] = time.monotonic() + _BACKOFF_SECONDS
                self._log.warning("Rate limit hit for %s/%s — backing off %ds",
                                  sess.broker_id, sess.client_id, _BACKOFF_SECONDS)
            else:
                self._log.debug("get_tradebook failed %s/%s: %s",
                                sess.broker_id, sess.client_id, e)
            return

        rows = []
        current_keys = []
        for t in (raw_trades or []):
            tid = str(t.get("trade_id") or t.get("fillid") or
                      t.get("tradeId") or t.get("id") or "")
            if not tid:
                continue
            current_keys.append((tid,))
            rows.append({
                "user_id": user_id,
                "config_id": sess.config_id,
                "broker_id": sess.broker_id,
                "client_id": sess.client_id,
                "trade_id": tid,
                "data": t,
            })

        self._upsert_jsonb_rows(
            "mgr_tradebook",
            unique_cols=["user_id", "config_id", "trade_id"],
            rows=rows,
            extra_cols=["broker_id", "client_id"],
        )
        # Skip delete when session is stale or when transient empty result detected
        if not sess.is_data_stale and self._should_delete_stale(
            user_id, sess.config_id, len(current_keys)
        ):
            self._delete_stale(
                "mgr_tradebook", user_id, sess.config_id,
                current_keys, ["trade_id"],
            )
