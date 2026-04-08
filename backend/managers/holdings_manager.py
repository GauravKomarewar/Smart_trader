"""
HoldingsManager — Fetches equity holdings from all live broker sessions
and upserts into mgr_holdings.
"""

from __future__ import annotations
import logging
from managers.base_manager import BaseManager

logger = logging.getLogger("smart_trader.mgr.holdings")


class HoldingsManager(BaseManager):
    MANAGER_NAME = "holdings_manager"
    REFRESH_INTERVAL = 30.0  # Holdings change slowly; refresh every 30s

    def refresh_user(self, user_id: str, sessions: list):
        self._refresh_sessions_concurrent(user_id, sessions, self._refresh_session)

    def _refresh_session(self, user_id: str, sess):
        try:
            raw_holdings = sess.get_holdings()
        except Exception as e:
            self._log.warning("get_holdings failed %s/%s: %s",
                            sess.broker_id, sess.client_id, e)
            return

        rows = []
        current_keys = []
        for h in (raw_holdings or []):
            sym = (h.get("symbol") or h.get("tsym") or
                   h.get("tradingsymbol") or "")
            exch = (h.get("exchange") or h.get("exch") or "NSE")
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
                "data": h,
            })

        self._upsert_jsonb_rows(
            "mgr_holdings",
            unique_cols=["user_id", "config_id", "symbol", "exchange"],
            rows=rows,
            extra_cols=["broker_id", "client_id"],
        )
        # Skip delete when session is stale or when transient empty result detected
        if not sess.is_data_stale and self._should_delete_stale(
            user_id, sess.config_id, len(current_keys)
        ):
            self._delete_stale(
                "mgr_holdings", user_id, sess.config_id,
                current_keys, ["symbol", "exchange"],
            )
