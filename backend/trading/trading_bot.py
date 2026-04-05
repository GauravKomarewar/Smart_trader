"""
Smart Trader — Trading Bot & Alert Processor
==============================================

THE SINGLE CHECKPOINT for ALL orders — webhook + frontend + strategy.

All alerts are routed through the unified OrderIntentProcessor:
  Alert → Validate auth → Resolve broker accounts → Per-account:
    TradingBot → CommandService → ExecutionGuard → Risk Manager → Broker
    → PostgreSQL order persistence → OrderWatcher tracking

Alert JSON format (Shoonya-compatible):
{
  "api_key":        "st_...",               // user's API key (required for webhooks)
  "execution_type": "ENTRY",                // ENTRY | EXIT | ADJUST
  "strategy_name":  "nifty_straddle",
  "exchange":       "NFO",
  "broker_accounts": ["config_id_1"],       // specific broker accounts to route to
  "legs": [
    {
      "tradingsymbol": "NIFTY26JUN24500CE",
      "direction":     "SELL",
      "qty":           50,
      "order_type":    "MARKET",
      "price":         0,
      "product_type":  "MIS",
      "stop_loss":     null,
      "target":        null,
      "trailing_type": null,
      "trailing_value": null
    }
  ],
  "test_mode": false,
  "remarks":   ""
}

Legacy format (backward-compatible):
{
  "strategy":  "name",
  "action":    "entry" | "exit" | "adjust" | "status",
  "symbol":    "NIFTY26APR24500CE",
  "exchange":  "NFO",
  "side":      "BUY",
  "quantity":  50,
  "secret":    "webhook_secret"
}
"""

import json
import hmac
import logging
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.orm import Session

from core.deps import current_user
from db.database import get_db, User
from trading.order_intent import (
    OrderIntentPayload, LegPayload,
    process_order_intent, IntentResult,
)
from broker.account_risk import list_all_risk_statuses

logger = logging.getLogger("smart_trader.trading_bot")

alert_router = APIRouter(prefix="/alerts", tags=["alerts"])


# ── Alert schema (backward-compatible + new format) ──────────────────────────

class AlertLeg(BaseModel):
    symbol: Optional[str] = None
    tradingsymbol: Optional[str] = None
    side: Optional[str] = None
    direction: Optional[str] = None
    quantity: Optional[int] = None
    qty: Optional[int] = None
    price: float = 0.0
    order_type: str = "MARKET"
    product_type: Optional[str] = None
    stop_loss: Optional[float] = None
    target: Optional[float] = None
    trailing_type: Optional[str] = None
    trailing_value: Optional[float] = None
    trail_when: Optional[float] = None

    def to_leg_payload(self) -> LegPayload:
        return LegPayload(
            tradingsymbol=self.tradingsymbol or self.symbol or "",
            direction=self.direction or self.side or "BUY",
            qty=self.qty or self.quantity or 1,
            order_type=self.order_type,
            price=self.price,
            product_type=self.product_type or "MIS",
            stop_loss=self.stop_loss,
            target=self.target,
            trailing_type=self.trailing_type,
            trailing_value=self.trailing_value,
            trail_when=self.trail_when,
        )


class AlertPayload(BaseModel):
    # New format fields
    api_key: Optional[str] = None
    execution_type: Optional[str] = None      # ENTRY | EXIT | ADJUST
    strategy_name: Optional[str] = None
    broker_accounts: Optional[List[str]] = None

    # Legacy format fields
    strategy: Optional[str] = None
    action: Optional[str] = None              # entry | exit | adjust | status
    symbol: Optional[str] = None
    exchange: str = "NFO"
    side: str = "BUY"
    quantity: int = 1
    order_type: str = "MARKET"
    product: str = "MIS"
    price: float = 0.0
    trigger_price: float = 0.0
    test_mode: bool = False
    remarks: str = ""
    legs: Optional[List[AlertLeg]] = None
    secret: Optional[str] = None              # legacy webhook secret

    # Risk fields
    stop_loss: Optional[float] = None
    target: Optional[float] = None
    trailing_type: Optional[str] = None
    trailing_value: Optional[float] = None
    trail_when: Optional[float] = None

    def to_order_intent(self) -> OrderIntentPayload:
        """Convert to unified OrderIntentPayload."""
        # Resolve execution type
        exec_type = self.execution_type
        if not exec_type and self.action:
            action_map = {
                "entry": "ENTRY", "exit": "EXIT",
                "adjust": "ADJUST", "exit_all": "EXIT",
            }
            exec_type = action_map.get(self.action.lower(), "ENTRY")

        # Resolve strategy name
        strat = self.strategy_name or self.strategy or "manual"

        # Convert legs
        intent_legs = []
        if self.legs:
            for leg in self.legs:
                intent_legs.append(leg.to_leg_payload())
        elif self.symbol:
            intent_legs.append(LegPayload(
                tradingsymbol=self.symbol,
                direction=self.side,
                qty=self.quantity,
                order_type=self.order_type,
                price=self.price,
                trigger_price=self.trigger_price,
                product_type=self.product,
                stop_loss=self.stop_loss,
                target=self.target,
                trailing_type=self.trailing_type,
                trailing_value=self.trailing_value,
                trail_when=self.trail_when,
            ))

        return OrderIntentPayload(
            api_key=self.api_key or self.secret,
            execution_type=exec_type or "ENTRY",
            strategy_name=strat,
            exchange=self.exchange,
            test_mode=self.test_mode,
            remarks=self.remarks,
            broker_accounts=self.broker_accounts or [],
            legs=intent_legs,
        )


# ── Webhook (public — api_key authenticated) ─────────────────────────────────

@alert_router.post("/webhook/{user_id}")
async def webhook_alert(
    user_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    """
    Public webhook endpoint for TradingView / Chartink / external alerts.

    URL: POST /api/alerts/webhook/{user_id}

    Auth: api_key in payload body (recommended) or X-Webhook-Secret header
          or legacy 'secret' field matching WEBHOOK_SECRET_KEY env var.

    The api_key corresponds to the user's unique key generated at registration.
    The user_id in the URL must match the api_key's owner.
    """
    import os

    body = await request.body()
    try:
        payload = json.loads(body)
    except Exception:
        raise HTTPException(400, "Invalid JSON payload")

    alert = AlertPayload(**payload)

    # Auth: api_key validation (primary method)
    api_key = alert.api_key or request.headers.get("X-API-Key", "")
    if api_key:
        user = db.query(User).filter(
            User.api_key == api_key,
            User.id == user_id,
            User.is_active == True,
        ).first()
        if not user:
            logger.warning("Webhook api_key mismatch for user_id=%s from %s",
                          user_id[:8], request.client.host)
            raise HTTPException(403, "Invalid api_key or user_id mismatch")
    else:
        # Fallback: legacy secret validation
        expected_secret = os.getenv("WEBHOOK_SECRET_KEY", "")
        if expected_secret:
            provided = (alert.secret
                        or request.headers.get("X-Webhook-Secret", ""))
            if not hmac.compare_digest(provided, expected_secret):
                logger.warning("Webhook secret mismatch from %s", request.client.host)
                raise HTTPException(403, "Invalid webhook secret")

    # Convert to unified intent
    intent = alert.to_order_intent()

    # If no broker_accounts specified, route to ALL live broker accounts
    if not intent.broker_accounts:
        intent.broker_accounts = _get_all_live_config_ids(user_id)

    if not intent.broker_accounts:
        return {
            "success": False,
            "message": "No live broker accounts connected",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    # Handle status action (legacy compatibility)
    if alert.action and alert.action.lower() == "status":
        return _handle_status(user_id, alert)

    # Process through unified pipeline
    result = process_order_intent(user_id=user_id, intent=intent, db_session=db)

    # Record history
    _record_history(user_id, {
        "source": "webhook",
        "strategy": intent.strategy_name,
        "execution_type": intent.execution_type,
        "test_mode": intent.test_mode,
        **result.to_dict(),
    })

    return result.to_dict()


@alert_router.post("/process")
def process_alert_authenticated(
    alert: AlertPayload,
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """
    Authenticated alert processing (from UI strategy panel / frontend).
    JWT-authenticated — no api_key needed.
    """
    user_id = payload["sub"]
    intent = alert.to_order_intent()

    # If no broker_accounts specified, route to ALL live broker accounts
    if not intent.broker_accounts:
        intent.broker_accounts = _get_all_live_config_ids(user_id)

    if not intent.broker_accounts:
        return {
            "success": False,
            "message": "No live broker accounts connected",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    if alert.action and alert.action.lower() == "status":
        return _handle_status(user_id, alert)

    result = process_order_intent(user_id=user_id, intent=intent, db_session=db)

    _record_history(user_id, {
        "source": "frontend",
        "strategy": intent.strategy_name,
        "execution_type": intent.execution_type,
        "test_mode": intent.test_mode,
        **result.to_dict(),
    })

    return result.to_dict()


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


# ── Helper functions ─────────────────────────────────────────────────────────

def _get_all_live_config_ids(user_id: str) -> List[str]:
    """Get all live broker account config_ids for a user."""
    try:
        from broker.multi_broker import registry as mb
        sessions = mb.list_live_sessions(user_id)
        return [s.config_id for s in sessions if hasattr(s, 'config_id') and s.config_id]
    except Exception:
        return []


def _handle_status(user_id: str, alert: AlertPayload) -> dict:
    """Handle legacy status action."""
    strategy = alert.strategy_name or alert.strategy or "manual"
    all_risk = list_all_risk_statuses(user_id)
    return {
        "success": True,
        "strategy": strategy,
        "action": "status",
        "risk_status": all_risk,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ── In-memory alert history (ring buffer) ────────────────────────────────────

_alert_history: Dict[str, List[dict]] = {}
_history_lock = threading.Lock()
_MAX_HISTORY = 500


def _record_history(user_id: str, entry: dict):
    with _history_lock:
        buf = _alert_history.setdefault(user_id, [])
        buf.insert(0, entry)
        if len(buf) > _MAX_HISTORY:
            buf.pop()


def _get_alert_history(user_id: str) -> List[dict]:
    with _history_lock:
        return list(_alert_history.get(user_id, []))
