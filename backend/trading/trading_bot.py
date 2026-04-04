"""
Smart Trader — Trading Bot & Alert Processor
==============================================

Handles webhook alerts from TradingView / Chartink with full
entry/exit/adjustment/test-mode support.

Alert JSON format (compatible with shoonya_platform):
{
  "strategy":    "nifty_straddle",
  "action":      "entry" | "exit" | "adjust" | "status",
  "symbol":      "NIFTY26APR24500CE",
  "exchange":    "NFO",
  "side":        "BUY" | "SELL",
  "quantity":    50,
  "order_type":  "MARKET" | "LIMIT",
  "product":     "MIS" | "NRML" | "CNC",
  "price":       0,
  "trigger_price": 0,
  "test_mode":   false,
  "remarks":     "optional note",

  -- Multi-leg (legs array overrides top-level symbol) --
  "legs": [
    {"symbol": "NIFTY26APR24500CE", "side": "SELL", "quantity": 50},
    {"symbol": "NIFTY26APR24400PE", "side": "SELL", "quantity": 50}
  ],

  -- Auth (optional webhook secret) --
  "secret": "GK_TRADINGVIEW_BOT_2408"
}
"""

import json
import hmac
import hashlib
import logging
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.orm import Session

from core.deps import current_user
from db.database import get_db
from trading.oms import OrderManagementSystem, OrderRequest, OrderSide, OrderType, ProductType, get_oms
from trading.execution_guard import ExecutionGuard
from trading.supreme_risk import SupremeRiskManager, get_risk

logger = logging.getLogger("smart_trader.trading_bot")

alert_router = APIRouter(prefix="/alerts", tags=["alerts"])

# Per-user guard instances
_guards: Dict[str, ExecutionGuard] = {}
_guard_lock = threading.Lock()


def get_guard(user_id: str) -> ExecutionGuard:
    with _guard_lock:
        if user_id not in _guards:
            _guards[user_id] = ExecutionGuard()
        return _guards[user_id]


# ── Alert schema ────────────────────────────────────────────────────────────

class AlertLeg(BaseModel):
    symbol:      str
    side:        str = "BUY"
    quantity:    int = 1
    price:       float = 0.0
    order_type:  str = "MARKET"


class AlertPayload(BaseModel):
    strategy:    str
    action:      str                    # entry | exit | adjust | status | exit_all
    symbol:      Optional[str] = None
    exchange:    str = "NFO"
    side:        str = "BUY"
    quantity:    int = 1
    order_type:  str = "MARKET"
    product:     str = "MIS"
    price:       float = 0.0
    trigger_price: float = 0.0
    test_mode:   bool = False
    remarks:     str = ""
    legs:        Optional[List[AlertLeg]] = None
    secret:      Optional[str] = None   # webhook auth secret


# ── Webhook (public) ─────────────────────────────────────────────────────────

@alert_router.post("/webhook/{user_id}")
async def webhook_alert(
    user_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    """
    Public webhook endpoint. Receives TradingView / Chartink alerts.
    URL: POST /api/alerts/webhook/{user_id}
    Auth: secret in payload or X-Webhook-Secret header.
    """
    import os

    body = await request.body()
    try:
        payload = json.loads(body)
    except Exception:
        raise HTTPException(400, "Invalid JSON payload")

    # Webhook secret validation
    expected_secret = os.getenv("WEBHOOK_SECRET_KEY", "")
    if expected_secret:
        provided = (payload.get("secret")
                    or request.headers.get("X-Webhook-Secret", ""))
        if not hmac.compare_digest(provided, expected_secret):
            logger.warning("Webhook secret mismatch from %s", request.client.host)
            raise HTTPException(403, "Invalid webhook secret")

    alert = AlertPayload(**payload)
    return _process_alert(user_id=user_id, alert=alert, db=db)


@alert_router.post("/process")
def process_alert_authenticated(
    alert: AlertPayload,
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """Authenticated alert processing (from UI strategy panel)."""
    return _process_alert(user_id=payload["sub"], alert=alert, db=db)


@alert_router.get("/history")
def alert_history(
    strategy: Optional[str] = None,
    limit: int = 100,
    payload: dict = Depends(current_user),
):
    """Return recent alert history for the user."""
    history = _get_alert_history(payload["sub"])
    if strategy:
        history = [h for h in history if h.get("strategy") == strategy]
    return history[:limit]


# ── Core alert processing ─────────────────────────────────────────────────────

# In-memory alert history per user (ring buffer)
_alert_history: Dict[str, List[dict]] = {}
_history_lock  = threading.Lock()
_MAX_HISTORY   = 500


def _record_history(user_id: str, entry: dict):
    with _history_lock:
        buf = _alert_history.setdefault(user_id, [])
        buf.insert(0, entry)
        if len(buf) > _MAX_HISTORY:
            buf.pop()


def _get_alert_history(user_id: str) -> List[dict]:
    with _history_lock:
        return list(_alert_history.get(user_id, []))


def _process_alert(user_id: str, alert: AlertPayload, db) -> dict:
    action = alert.action.lower()
    logger.info("Alert | user=%s strategy=%s action=%s test=%s",
                user_id, alert.strategy, action, alert.test_mode)

    oms   = get_oms(user_id)
    guard = get_guard(user_id)
    risk  = get_risk(user_id)

    # Attach guard to OMS
    oms.guard = guard

    # Risk check (skip for status queries)
    if action not in ("status",):
        allowed, reason = risk.is_trading_allowed()
        if not allowed:
            result = {
                "success": False,
                "strategy": alert.strategy,
                "action": action,
                "message": f"RISK BLOCK: {reason}",
                "test_mode": alert.test_mode,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            _record_history(user_id, result)
            return result

    if action == "status":
        return {
            "success": True,
            "strategy": alert.strategy,
            "action": "status",
            "risk_status": risk.get_status().to_dict(),
            "open_positions": oms.get_positions(alert.strategy),
            "guard_positions": guard.get_strategy_positions(alert.strategy),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    if action in ("exit", "exit_all"):
        return _handle_exit(user_id, alert, oms, guard)

    if action in ("entry", "adjust"):
        return _handle_entry(user_id, alert, oms, guard, action)

    raise HTTPException(400, f"Unknown action: {action}")


def _handle_entry(user_id: str, alert: AlertPayload, oms: OrderManagementSystem,
                  guard: ExecutionGuard, action: str) -> dict:
    tag = "ENTRY" if action == "entry" else "ADJUST"
    legs = _build_legs(alert)
    orders = []

    for leg in legs:
        req = OrderRequest(
            symbol=leg.symbol,
            exchange=alert.exchange,
            side=OrderSide(leg.side.upper()),
            order_type=OrderType(leg.order_type.upper()),
            product=ProductType(alert.product.upper()),
            quantity=leg.quantity,
            price=leg.price,
            trigger_price=alert.trigger_price,
            strategy_id=alert.strategy,
            tag=tag,
            test_mode=alert.test_mode,
            remarks=alert.remarks or f"{tag}:{alert.strategy}",
        )
        order = oms.place_order(req)
        if order.status.value != "REJECTED":
            guard.on_fill(alert.strategy, leg.symbol, leg.side.upper(), leg.quantity)
        orders.append(order.to_dict())

    result = {
        "success": True,
        "strategy": alert.strategy,
        "action": action,
        "tag": tag,
        "test_mode": alert.test_mode,
        "orders": orders,
        "order_count": len(orders),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    _record_history(user_id, result)
    return result


def _handle_exit(user_id: str, alert: AlertPayload, oms: OrderManagementSystem,
                 guard: ExecutionGuard) -> dict:
    positions = guard.get_strategy_positions(alert.strategy)
    if not positions:
        return {
            "success": True,
            "strategy": alert.strategy,
            "action": "exit",
            "message": "No open positions to exit",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    orders = []
    for pos in positions:
        # Exit means flip side
        exit_side = "SELL" if pos["direction"] == "BUY" else "BUY"
        req = OrderRequest(
            symbol=pos["symbol"],
            exchange=alert.exchange,
            side=OrderSide(exit_side),
            order_type=OrderType.MARKET,
            product=ProductType(alert.product.upper()),
            quantity=pos["qty"],
            strategy_id=alert.strategy,
            tag="EXIT",
            test_mode=alert.test_mode,
            remarks=f"EXIT:{alert.strategy}",
        )
        order = oms.place_order(req)
        if order.status.value != "REJECTED":
            guard.on_exit_fill(alert.strategy, pos["symbol"], pos["qty"])
        orders.append(order.to_dict())

    if not guard.has_open_position(alert.strategy):
        guard.clear_strategy(alert.strategy)

    result = {
        "success": True,
        "strategy": alert.strategy,
        "action": "exit",
        "test_mode": alert.test_mode,
        "orders": orders,
        "order_count": len(orders),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    _record_history(user_id, result)
    return result


def _build_legs(alert: AlertPayload) -> List[AlertLeg]:
    if alert.legs:
        return alert.legs
    if alert.symbol:
        return [AlertLeg(
            symbol=alert.symbol,
            side=alert.side,
            quantity=alert.quantity,
            price=alert.price,
            order_type=alert.order_type,
        )]
    raise HTTPException(422, "Alert must have 'symbol' or 'legs'")
