"""
TradebookManager — Fetches tradebook (fills) from all live broker sessions
and upserts into mgr_tradebook.
"""

from __future__ import annotations
import logging
from managers.base_manager import BaseManager

logger = logging.getLogger("smart_trader.mgr.tradebook")


class TradebookManager(BaseManager):
    MANAGER_NAME = "tradebook_manager"
    REFRESH_INTERVAL = 5.0  # Trades don't change frequently

    def refresh_user(self, user_id: str, sessions: list):
        for sess in sessions:
            if not sess.is_live:
                continue
            try:
                raw_trades = sess.get_tradebook()
            except Exception as e:
                self._log.debug("get_tradebook failed %s/%s: %s",
                                sess.broker_id, sess.client_id, e)
                continue

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
            self._delete_stale(
                "mgr_tradebook", user_id, sess.config_id,
                current_keys, ["trade_id"],
            )
