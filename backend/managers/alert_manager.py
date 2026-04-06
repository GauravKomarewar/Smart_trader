"""
AlertManager — Handles alerts from multiple sources:
  - Risk engine (position watcher)
  - Webhook events (TradingView)
  - Manager health issues
  - Session expiry notifications

Alerts are written to mgr_alerts for persistence and streamed to WS.
"""

from __future__ import annotations
import logging
from datetime import datetime, timezone
from managers.base_manager import BaseManager
from db.trading_db import get_trading_conn
import psycopg2.extras

logger = logging.getLogger("smart_trader.mgr.alerts")


class AlertManager(BaseManager):
    MANAGER_NAME = "alert_manager"
    REFRESH_INTERVAL = 5.0

    def refresh_user(self, user_id: str, sessions: list):
        """AlertManager doesn't poll brokers — it processes queued alerts."""
        pass  # Alerts are pushed via push_alert()

    @staticmethod
    def push_alert(user_id: str, level: str, title: str, message: str,
                   source: str = "system", data: dict | None = None):
        """
        Insert an alert into mgr_alerts.  Called by any component.
        Levels: INFO, WARNING, CRITICAL
        """
        conn = get_trading_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO mgr_alerts (user_id, source, level, title, message, data, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
            """, (
                user_id, source, level.upper(), title, message,
                psycopg2.extras.Json(data or {}),
            ))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def get_recent_alerts(user_id: str, limit: int = 50,
                          unacknowledged_only: bool = False) -> list[dict]:
        """Read recent alerts for a user."""
        conn = get_trading_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            where = "WHERE user_id = %s"
            params = [user_id]
            if unacknowledged_only:
                where += " AND acknowledged = FALSE"
            cur.execute(
                f"SELECT * FROM mgr_alerts {where} ORDER BY created_at DESC LIMIT %s",
                params + [limit],
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    @staticmethod
    def acknowledge_alert(alert_id: int):
        """Mark an alert as acknowledged."""
        conn = get_trading_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                "UPDATE mgr_alerts SET acknowledged = TRUE WHERE id = %s",
                (alert_id,),
            )
            conn.commit()
        finally:
            conn.close()
