"""
PositionManager — Fetches positions from all live broker sessions,
normalises them, and upserts into mgr_positions.
"""

from __future__ import annotations
import logging
from managers.base_manager import BaseManager

logger = logging.getLogger("smart_trader.mgr.positions")


class PositionManager(BaseManager):
    MANAGER_NAME = "position_manager"
    REFRESH_INTERVAL = 5.0  # 5s refresh — reduces Fyers REST rate-limit pressure

    def refresh_user(self, user_id: str, sessions: list):
        self._refresh_sessions_concurrent(user_id, sessions, self._refresh_session)

    def _refresh_session(self, user_id: str, sess):
        try:
            raw_positions = sess.get_positions()  # Returns list[dict]
        except Exception as e:
            self._log.warning("get_positions failed %s/%s: %s",
                            sess.broker_id, sess.client_id, e)
            return

        rows = []
        current_keys = []
        for p in (raw_positions or []):
            sym = (p.get("symbol") or p.get("tsym") or
                   p.get("tradingsymbol") or "")
            exch = (p.get("exchange") or p.get("exch") or "")
            if not sym:
                continue
            current_keys.append((sym, exch))
            rows.append({
                "user_id": user_id,
                "config_id": sess.config_id,
                "broker_id": sess.broker_id,
                "client_id": sess.client_id,
                "symbol": sym,
                "exchange": exch,
                "data": p,
            })

        self._upsert_jsonb_rows(
            "mgr_positions",
            unique_cols=["user_id", "config_id", "symbol", "exchange"],
            rows=rows,
            extra_cols=["broker_id", "client_id"],
        )
        # Remove positions that no longer exist (closed)
        # Skip when session is stale or when transient empty result detected
        if not sess.is_data_stale and self._should_delete_stale(
            user_id, sess.config_id, len(current_keys)
        ):
            self._delete_stale(
                "mgr_positions", user_id, sess.config_id,
                current_keys, ["symbol", "exchange"],
            )
