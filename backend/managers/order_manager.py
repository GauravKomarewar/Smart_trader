"""
OrderManager — Fetches order book from all live broker sessions,
normalises, and upserts into mgr_orders.
"""

from __future__ import annotations
import logging
from managers.base_manager import BaseManager

logger = logging.getLogger("smart_trader.mgr.orders")


class OrderManager(BaseManager):
    MANAGER_NAME = "order_manager"
    REFRESH_INTERVAL = 2.0

    def refresh_user(self, user_id: str, sessions: list):
        self._refresh_sessions_concurrent(user_id, sessions, self._refresh_session)

    def _refresh_session(self, user_id: str, sess):
        try:
            raw_orders = sess.get_order_book()
        except Exception as e:
            self._log.debug("get_order_book failed %s/%s: %s",
                            sess.broker_id, sess.client_id, e)
            return

        rows = []
        current_keys = []
        for o in (raw_orders or []):
            oid = str(o.get("order_id") or o.get("norenordno") or
                      o.get("orderId") or o.get("id") or "")
            if not oid:
                continue
            current_keys.append((oid,))
            rows.append({
                "user_id": user_id,
                "config_id": sess.config_id,
                "broker_id": sess.broker_id,
                "client_id": sess.client_id,
                "order_id": oid,
                "data": o,
            })

        self._upsert_jsonb_rows(
            "mgr_orders",
            unique_cols=["user_id", "config_id", "order_id"],
            rows=rows,
            extra_cols=["broker_id", "client_id"],
        )
        # Skip delete when session is stale or when transient empty result detected
        if not sess.is_data_stale and self._should_delete_stale(
            user_id, sess.config_id, len(current_keys)
        ):
            self._delete_stale(
                "mgr_orders", user_id, sess.config_id,
                current_keys, ["order_id"],
            )
