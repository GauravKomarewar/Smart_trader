"""
AccountManager — Fetches funds/limits from all live broker sessions
and upserts into mgr_funds.  One row per broker account.
"""

from __future__ import annotations
import logging
from managers.base_manager import BaseManager

logger = logging.getLogger("smart_trader.mgr.account")


class AccountManager(BaseManager):
    MANAGER_NAME = "account_manager"
    REFRESH_INTERVAL = 30.0  # 30s — funds change rarely, avoids rate limiting

    def refresh_user(self, user_id: str, sessions: list):
        self._refresh_sessions_concurrent(user_id, sessions, self._refresh_session)

    def _refresh_session(self, user_id: str, sess):
        try:
            raw_limits = sess.get_limits()
        except Exception as e:
            self._log.debug("get_limits failed %s/%s: %s",
                            sess.broker_id, sess.client_id, e)
            return

        if not raw_limits:
            return

        rows = [{
            "user_id": user_id,
            "config_id": sess.config_id,
            "broker_id": sess.broker_id,
            "client_id": sess.client_id,
            "data": raw_limits,
        }]

        self._upsert_jsonb_rows(
            "mgr_funds",
            unique_cols=["user_id", "config_id"],
            rows=rows,
            extra_cols=["broker_id", "client_id"],
        )
