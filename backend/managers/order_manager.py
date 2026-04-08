"""
OrderManager — Fetches order book from all live broker sessions,
normalises, and upserts into mgr_orders.
"""

from __future__ import annotations
import logging
import psycopg2.extras
from managers.base_manager import BaseManager
from db.trading_db import get_trading_conn

logger = logging.getLogger("smart_trader.mgr.orders")

# Terminal statuses that should not be overwritten by lower-priority broker data
_TERMINAL_STATUSES = {"CANCELLED", "REJECTED"}
# Statuses that are non-terminal and can be overwritten
_NON_TERMINAL = {"PENDING", "OPEN", "TRIGGER_PENDING"}


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
        order_ids = []
        for o in (raw_orders or []):
            oid = str(o.get("order_id") or o.get("norenordno") or
                      o.get("orderId") or o.get("id") or "")
            if not oid:
                continue
            current_keys.append((oid,))
            order_ids.append(oid)
            rows.append({
                "user_id": user_id,
                "config_id": sess.config_id,
                "broker_id": sess.broker_id,
                "client_id": sess.client_id,
                "order_id": oid,
                "data": o,
            })

        # Preserve locally-set terminal statuses (e.g. CANCELLED via cancel_order API)
        # that haven't propagated to the broker yet
        if rows and order_ids:
            try:
                existing = self._get_existing_statuses(user_id, sess.config_id, order_ids)
                for row in rows:
                    oid = row["order_id"]
                    broker_status = str(row["data"].get("status", "")).upper()
                    cached_status = existing.get(oid, "")
                    # If our cache says CANCELLED/REJECTED but broker still says PENDING/OPEN,
                    # preserve the terminal status
                    if (cached_status in _TERMINAL_STATUSES
                            and broker_status in _NON_TERMINAL):
                        row["data"]["status"] = cached_status
                        row["data"]["remarks"] = row["data"].get("remarks", "") or f"Cancel confirmed (pending broker sync)"
            except Exception as e:
                self._log.debug("Terminal status preservation failed: %s", e)

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

    @staticmethod
    def _get_existing_statuses(user_id: str, config_id: str, order_ids: list) -> dict:
        """Read current cached statuses from mgr_orders for comparison."""
        conn = get_trading_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                "SELECT order_id, data->>'status' AS status "
                "FROM mgr_orders WHERE user_id = %s AND config_id = %s "
                "AND order_id = ANY(%s)",
                (user_id, config_id, order_ids),
            )
            return {r["order_id"]: (r["status"] or "").upper() for r in cur.fetchall()}
        finally:
            conn.close()
