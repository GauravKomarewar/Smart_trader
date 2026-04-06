"""
SessionManager — Monitors all broker session states and upserts into mgr_sessions.
Tracks connectivity, login status, errors, heartbeats across all brokers.
"""

from __future__ import annotations
import logging
from datetime import datetime, timezone
from managers.base_manager import BaseManager
from db.trading_db import get_trading_conn
import psycopg2.extras

logger = logging.getLogger("smart_trader.mgr.session")


class SessionManager(BaseManager):
    MANAGER_NAME = "session_manager"
    REFRESH_INTERVAL = 5.0

    def refresh_user(self, user_id: str, sessions: list):
        conn = get_trading_conn()
        try:
            cur = conn.cursor()
            now = datetime.now(timezone.utc)
            for sess in sessions:
                state = "disconnected"
                if sess.is_live:
                    state = "live"
                elif sess.is_paper:
                    state = "paper"
                elif sess.is_demo:
                    state = "demo"

                # Check if stale
                if hasattr(sess, "_stale_since") and sess._stale_since is not None:
                    state = "stale"

                cur.execute("""
                    INSERT INTO mgr_sessions
                        (user_id, config_id, broker_id, client_id, state, mode,
                         is_live, error, connected_at, last_heartbeat, extra, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (user_id, config_id)
                    DO UPDATE SET state = EXCLUDED.state,
                                  mode = EXCLUDED.mode,
                                  is_live = EXCLUDED.is_live,
                                  error = EXCLUDED.error,
                                  connected_at = EXCLUDED.connected_at,
                                  last_heartbeat = EXCLUDED.last_heartbeat,
                                  extra = EXCLUDED.extra,
                                  updated_at = EXCLUDED.updated_at
                """, (
                    user_id,
                    sess.config_id,
                    sess.broker_id,
                    sess.client_id,
                    state,
                    sess.mode,
                    sess.is_live,
                    sess.error,
                    sess.connected_at.isoformat() if sess.connected_at else None,
                    sess.last_heartbeat.isoformat() if sess.last_heartbeat else None,
                    psycopg2.extras.Json({
                        "consecutive_failures": getattr(sess, "_consecutive_data_failures", 0),
                    }),
                    now,
                ))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
