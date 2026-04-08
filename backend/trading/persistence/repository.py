"""
Smart Trader — OrderRepository (PostgreSQL)
============================================

SINGLE SOURCE OF TRUTH for order persistence.
- Multi-tenant: every query is scoped to (user_id, client_id)
- No business logic
- No execution logic
- Full audit trail

OMS STATUS CONTRACT:
  CREATED        : Intent kept, not sent to broker
  SENT_TO_BROKER : Accepted by broker, broker_order_id assigned
  EXECUTED       : Confirmed fill (from OrderWatcher)
  CANCELLED      : Cancelled by user / broker
  FAILED         : Broker rejected / expired / unrecoverable failure
"""

import logging
from datetime import datetime, timezone
from typing import List, Optional

from db.trading_db import get_trading_conn, get_trading_cursor
from trading.persistence.order_record import OrderRecord

logger = logging.getLogger("smart_trader.order_repo")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_audit(conn, user_id: int, client_id: str, command_id: str, action: str,
                 old_value: Optional[str] = None, new_value: Optional[str] = None,
                 source: str = "system", detail: Optional[str] = None):
    """Append audit event. Best-effort — never raises."""
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO audit_log (user_id, client_id, command_id, action,
                                   old_value, new_value, source, detail)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (user_id, client_id, command_id, action,
             old_value, new_value, source, detail),
        )
    except Exception:
        pass  # audit is best-effort


def _row_to_record(row: dict) -> OrderRecord:
    """Convert a RealDictCursor row to OrderRecord."""
    def ts(v):
        if v is None:
            return None
        if hasattr(v, "isoformat"):
            return v.isoformat()
        return str(v)

    return OrderRecord(
        command_id      = row["command_id"],
        source          = row["source"],
        broker_user     = row["broker_user"],
        strategy_name   = row["strategy_name"],
        exchange        = row["exchange"],
        symbol          = row["symbol"],
        side            = row["side"],
        quantity        = int(row["quantity"]),
        product         = row["product"],
        order_type      = row["order_type"],
        price           = float(row["price"] or 0),
        stop_loss       = row.get("stop_loss"),
        target          = row.get("target"),
        trailing_type   = row.get("trailing_type"),
        trailing_value  = row.get("trailing_value"),
        trail_when      = row.get("trail_when"),
        managed_anchor_ltp      = row.get("managed_anchor_ltp"),
        managed_base_stop_loss  = row.get("managed_base_stop_loss"),
        client_id       = row.get("client_id"),
        broker_order_id = row.get("broker_order_id"),
        execution_type  = row.get("execution_type", "ENTRY"),
        status          = row.get("status", "CREATED"),
        created_at      = ts(row.get("created_at")),
        updated_at      = ts(row.get("updated_at")),
        tag             = row.get("tag"),
    )


class OrderRepository:
    """
    PostgreSQL-backed order repository, scoped to (user_id, client_id).
    """

    def __init__(self, user_id, client_id: str):
        self.user_id = user_id
        self.client_id = client_id

    # ── CREATE ──────────────────────────────────────────────────────────────

    def create(self, record: OrderRecord):
        now = _now()
        conn = get_trading_conn()
        try:
            cur = get_trading_cursor(conn)
            cur.execute(
                """
                INSERT INTO orders (
                    user_id, client_id,
                    command_id, source, broker_user, strategy_name,
                    exchange, symbol, side, quantity, product,
                    order_type, price,
                    stop_loss, target, trailing_type, trailing_value,
                    trail_when, managed_anchor_ltp, managed_base_stop_loss,
                    broker_order_id, execution_type,
                    status, created_at, updated_at, tag
                )
                VALUES (
                    %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s
                )
                ON CONFLICT (command_id, user_id) DO NOTHING
                """,
                (
                    self.user_id, self.client_id,
                    record.command_id, record.source, record.broker_user, record.strategy_name,
                    record.exchange, record.symbol, record.side, record.quantity, record.product,
                    record.order_type, record.price,
                    record.stop_loss, record.target, record.trailing_type, record.trailing_value,
                    record.trail_when, record.managed_anchor_ltp, record.managed_base_stop_loss,
                    record.broker_order_id, record.execution_type,
                    record.status, record.created_at or now, record.updated_at or now, record.tag,
                ),
            )
            _write_audit(
                conn, self.user_id, self.client_id, record.command_id,
                "ORDER_CREATED", new_value=record.status, source=record.source,
                detail=f"{record.exchange}:{record.symbol} {record.side} qty={record.quantity}",
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ── UPDATE ──────────────────────────────────────────────────────────────

    def update_status(self, command_id: str, status: str):
        conn = get_trading_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE orders
                SET status = %s, updated_at = NOW()
                WHERE command_id = %s AND user_id = %s
                """,
                (status, command_id, self.user_id),
            )
            _write_audit(conn, self.user_id, self.client_id, command_id, "STATUS_CHANGE",
                         new_value=status)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def update_broker_id(self, command_id: str, broker_order_id: str):
        conn = get_trading_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE orders
                SET broker_order_id = %s, status = 'SENT_TO_BROKER', updated_at = NOW()
                WHERE command_id = %s AND user_id = %s
                """,
                (broker_order_id, command_id, self.user_id),
            )
            _write_audit(conn, self.user_id, self.client_id, command_id,
                         "BROKER_ID_ASSIGNED", new_value=broker_order_id)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def update_status_by_broker_id(self, broker_order_id: str, status: str):
        conn = get_trading_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE orders
                SET status = %s, updated_at = NOW()
                WHERE broker_order_id = %s AND user_id = %s
                """,
                (status, broker_order_id, self.user_id),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def update_tag(self, command_id: str, tag: str):
        conn = get_trading_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE orders SET tag = %s, updated_at = NOW()
                WHERE command_id = %s AND user_id = %s
                """,
                (tag, command_id, self.user_id),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def update_stop_loss(self, command_id: str, new_sl: float, new_target: Optional[float] = None):
        conn = get_trading_conn()
        try:
            cur = conn.cursor()
            if new_target is not None:
                cur.execute(
                    """
                    UPDATE orders SET stop_loss = %s, target = %s, updated_at = NOW()
                    WHERE command_id = %s AND user_id = %s
                    """,
                    (new_sl, new_target, command_id, self.user_id),
                )
            else:
                cur.execute(
                    """
                    UPDATE orders SET stop_loss = %s, updated_at = NOW()
                    WHERE command_id = %s AND user_id = %s
                    """,
                    (new_sl, command_id, self.user_id),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def update_risk_fields(self, command_id: str, fields: dict):
        """Update arbitrary risk fields (managed_anchor_ltp, managed_base_stop_loss, etc)."""
        allowed = {
            "stop_loss", "target", "trailing_type", "trailing_value",
            "trail_when", "managed_anchor_ltp", "managed_base_stop_loss",
        }
        safe = {k: v for k, v in fields.items() if k in allowed}
        if not safe:
            return
        set_clause = ", ".join(f"{k} = %s" for k in safe)
        vals = list(safe.values()) + [command_id, self.user_id]
        conn = get_trading_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                f"UPDATE orders SET {set_clause}, updated_at = NOW() "
                f"WHERE command_id = %s AND user_id = %s",
                vals,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ── READ ────────────────────────────────────────────────────────────────

    def get_open_orders(self) -> List[OrderRecord]:
        """Orders needing OrderWatcher attention."""
        conn = get_trading_conn()
        try:
            cur = get_trading_cursor(conn)
            cur.execute(
                """
                SELECT * FROM orders
                WHERE status IN ('CREATED', 'SENT_TO_BROKER')
                  AND user_id = %s
                ORDER BY created_at
                """,
                (self.user_id,),
            )
            return [_row_to_record(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def get_by_id(self, command_id: str) -> Optional[OrderRecord]:
        conn = get_trading_conn()
        try:
            cur = get_trading_cursor(conn)
            cur.execute(
                "SELECT * FROM orders WHERE command_id = %s AND user_id = %s",
                (command_id, self.user_id),
            )
            row = cur.fetchone()
            return _row_to_record(row) if row else None
        finally:
            conn.close()

    def get_by_broker_id(self, broker_order_id: str) -> Optional[OrderRecord]:
        conn = get_trading_conn()
        try:
            cur = get_trading_cursor(conn)
            cur.execute(
                """
                SELECT * FROM orders
                WHERE broker_order_id = %s AND user_id = %s
                """,
                (broker_order_id, self.user_id),
            )
            row = cur.fetchone()
            return _row_to_record(row) if row else None
        finally:
            conn.close()

    def get_all(self, limit: int = 200) -> List[OrderRecord]:
        conn = get_trading_conn()
        try:
            cur = get_trading_cursor(conn)
            cur.execute(
                """
                SELECT * FROM orders
                WHERE user_id = %s
                ORDER BY updated_at DESC
                LIMIT %s
                """,
                (self.user_id, limit),
            )
            return [_row_to_record(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def get_open_orders_by_strategy(self, strategy_name: str) -> List[OrderRecord]:
        conn = get_trading_conn()
        try:
            cur = get_trading_cursor(conn)
            cur.execute(
                """
                SELECT * FROM orders
                WHERE status IN ('CREATED', 'SENT_TO_BROKER')
                  AND strategy_name = %s AND user_id = %s
                """,
                (strategy_name, self.user_id),
            )
            return [_row_to_record(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def get_open_leg_count(self, strategy_name: str) -> int:
        conn = get_trading_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT COUNT(*) FROM orders
                WHERE status IN ('CREATED', 'SENT_TO_BROKER')
                  AND strategy_name = %s AND user_id = %s
                """,
                (strategy_name, self.user_id),
            )
            row = cur.fetchone()
            return int(row[0]) if row else 0
        finally:
            conn.close()

    def get_open_positions_by_strategy(self, strategy_name: str) -> list:
        """Return position-like dicts for a strategy (for recovery/RMS)."""
        conn = get_trading_conn()
        try:
            cur = get_trading_cursor(conn)
            cur.execute(
                """
                SELECT * FROM orders
                WHERE status = 'SENT_TO_BROKER'
                  AND strategy_name = %s AND user_id = %s
                """,
                (strategy_name, self.user_id),
            )
            rows = cur.fetchall()
            return [
                {
                    "exchange":      r["exchange"],
                    "symbol":        r["symbol"],
                    "product":       r.get("product"),
                    "qty":           int(r["quantity"]),
                    "side":          r["side"],
                    "price":         r.get("price"),
                    "source":        r.get("source"),
                    "tag":           r.get("tag"),
                    "stop_loss":     r.get("stop_loss"),
                    "target":        r.get("target"),
                    "trailing_type": r.get("trailing_type"),
                    "trailing_value":r.get("trailing_value"),
                    "broker_order_id": r.get("broker_order_id"),
                }
                for r in rows
            ]
        finally:
            conn.close()

    def get_last_order_source(self, symbol: str) -> Optional[str]:
        conn = get_trading_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT source FROM orders
                WHERE symbol = %s AND user_id = %s
                ORDER BY updated_at DESC LIMIT 1
                """,
                (symbol, self.user_id),
            )
            row = cur.fetchone()
            return row[0] if row else None
        finally:
            conn.close()

    def get_last_exit_source(self, symbol: str) -> Optional[str]:
        conn = get_trading_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT source FROM orders
                WHERE symbol = %s AND user_id = %s AND status = 'EXECUTED'
                ORDER BY updated_at DESC LIMIT 1
                """,
                (symbol, self.user_id),
            )
            row = cur.fetchone()
            return row[0] if row else None
        finally:
            conn.close()

    def count_open_orders(self) -> int:
        conn = get_trading_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM orders WHERE status IN ('CREATED','SENT_TO_BROKER') AND user_id = %s",
                (self.user_id,),
            )
            row = cur.fetchone()
            return int(row[0]) if row else 0
        finally:
            conn.close()

    def get_last_order_time(self) -> Optional[str]:
        conn = get_trading_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT updated_at FROM orders WHERE user_id = %s ORDER BY updated_at DESC LIMIT 1",
                (self.user_id,),
            )
            row = cur.fetchone()
            return str(row[0]) if row else None
        finally:
            conn.close()

    def cleanup_old_closed_orders(self, days: int = 7) -> int:
        conn = get_trading_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                DELETE FROM orders
                WHERE status NOT IN ('CREATED', 'SENT_TO_BROKER')
                  AND updated_at < NOW() - INTERVAL '%s days'
                  AND user_id = %s
                """,
                (days, self.user_id),
            )
            deleted = cur.rowcount
            conn.commit()
            return deleted
        except Exception:
            conn.rollback()
            return 0
        finally:
            conn.close()
