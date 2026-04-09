"""
Smart Trader — Unified Order Intent Processor
===============================================

THE SINGLE CHECKPOINT for ALL trading orders.

Every order — whether from the frontend PlaceOrderModal, TradingView webhook,
or strategy engine — MUST flow through this module.

Flow:
  Frontend / Webhook / Strategy
      ↓
  OrderIntentProcessor.process()
      ↓
  Validate api_key / JWT auth
      ↓
  Resolve broker accounts
      ↓
  For each broker account:
      TradingBot → CommandService → ExecutionGuard → Risk Manager → Broker
      ↓
  All orders persisted to PostgreSQL with full tracking

Alert Format (Shoonya-compatible):
{
    "api_key":         "st_...",          # user's API key (webhook) or omit for JWT auth
    "execution_type":  "ENTRY",           # ENTRY | EXIT | ADJUST
    "strategy_name":   "nifty_straddle",
    "exchange":        "NFO",
    "broker_accounts": ["config_id_1"],   # which broker accounts to route to
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
            "trailing_value": null,
            "trail_when":    null
        }
    ],
    "test_mode": false,
    "remarks":   ""
}
"""

import logging
import threading
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

from pydantic import BaseModel

from trading.execution.intent import UniversalOrderCommand, validate_order
from trading.execution_guard import ExecutionGuard, LegIntent
from trading.bot_core import TradingBot, get_bot

logger = logging.getLogger("smart_trader.order_intent")


# ── Alert/Intent Data Models ─────────────────────────────────────────────────

class LegPayload(BaseModel):
    tradingsymbol: str
    direction: str = "BUY"                   # BUY | SELL
    qty: int = 1
    order_type: str = "MARKET"               # MARKET | LIMIT | SL | SL-M
    price: float = 0.0
    trigger_price: float = 0.0
    product_type: str = "MIS"                # MIS | NRML | CNC
    stop_loss: Optional[float] = None
    target: Optional[float] = None
    trailing_type: Optional[str] = None      # POINTS | PERCENT | ABSOLUTE
    trailing_value: Optional[float] = None
    trail_when: Optional[float] = None


class OrderIntentPayload(BaseModel):
    """
    Unified order intent accepted from ALL sources:
    - Frontend PlaceOrderModal
    - TradingView webhook
    - Strategy engine
    - API calls
    """
    # Auth (one of: api_key for webhook, or JWT-authenticated user_id injected)
    api_key: Optional[str] = None

    # Execution
    execution_type: str = "ENTRY"            # ENTRY | EXIT | ADJUST
    strategy_name: str = "manual"
    exchange: str = "NFO"
    test_mode: bool = False
    remarks: str = ""

    # Broker routing — list of config_ids to send orders to
    broker_accounts: List[str] = []

    # Legs (Shoonya-compatible multi-leg format)
    legs: List[LegPayload] = []

    # Legacy single-symbol fields (converted to legs internally)
    symbol: Optional[str] = None
    side: Optional[str] = None
    quantity: Optional[int] = None
    order_type: Optional[str] = None
    product: Optional[str] = None
    price: Optional[float] = None
    trigger_price: Optional[float] = None
    stop_loss: Optional[float] = None
    target: Optional[float] = None
    trailing_type: Optional[str] = None
    trailing_value: Optional[float] = None

    def get_legs(self) -> List[LegPayload]:
        """Return legs — convert single-symbol fields to a single leg if no legs provided."""
        if self.legs:
            return self.legs
        if self.symbol:
            return [LegPayload(
                tradingsymbol=self.symbol,
                direction=self.side or "BUY",
                qty=self.quantity or 1,
                order_type=self.order_type or "MARKET",
                price=self.price or 0.0,
                trigger_price=self.trigger_price or 0.0,
                product_type=self.product or "MIS",
                stop_loss=self.stop_loss,
                target=self.target,
                trailing_type=self.trailing_type,
                trailing_value=self.trailing_value,
            )]
        return []


# ── Per-strategy locking ─────────────────────────────────────────────────────

_strategy_locks: Dict[str, threading.Lock] = {}
_lock_lock = threading.Lock()


def _get_strategy_lock(user_id: str, strategy: str) -> threading.Lock:
    key = f"{user_id}:{strategy}"
    with _lock_lock:
        if key not in _strategy_locks:
            _strategy_locks[key] = threading.Lock()
        return _strategy_locks[key]


# ── Result model ─────────────────────────────────────────────────────────────

class IntentResult:
    def __init__(self):
        self.success = True
        self.orders: List[dict] = []
        self.errors: List[str] = []
        self.account_results: List[dict] = []

    def to_dict(self) -> dict:
        return {
            "success": self.success and len(self.errors) == 0,
            "order_count": len(self.orders),
            "orders": self.orders,
            "errors": self.errors,
            "account_results": self.account_results,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


# ── Core Processor ───────────────────────────────────────────────────────────

class OrderIntentProcessor:
    """
    THE SINGLE CHECKPOINT for all trading orders.

    Usage:
        processor = OrderIntentProcessor()
        result = processor.process(user_id="...", intent=OrderIntentPayload(...))
    """

    def process(
        self,
        *,
        user_id: str,
        intent: OrderIntentPayload,
        db_session=None,
    ) -> IntentResult:
        """
        Process an order intent through the full pipeline:
        1. Resolve broker accounts
        2. Per-strategy lock
        3. For each broker account:
           a. Get/create TradingBot
           b. ExecutionGuard validation
           c. Risk Manager check
           d. CommandService.submit() → broker
           e. Persist to PostgreSQL
        """
        result = IntentResult()

        legs = intent.get_legs()
        if not legs:
            result.errors.append("No legs or symbol provided")
            result.success = False
            return result

        execution_type = intent.execution_type.upper()
        if execution_type not in ("ENTRY", "EXIT", "ADJUST"):
            result.errors.append(f"Invalid execution_type: {execution_type}")
            result.success = False
            return result

        # Resolve broker accounts
        broker_accounts = intent.broker_accounts
        if not broker_accounts:
            result.errors.append("No broker accounts specified")
            result.success = False
            return result

        # Per-strategy lock to prevent duplicate concurrent execution
        strategy_lock = _get_strategy_lock(user_id, intent.strategy_name)

        with strategy_lock:
            for config_id in broker_accounts:
                acct_result = self._process_for_account(
                    user_id=user_id,
                    config_id=config_id,
                    intent=intent,
                    legs=legs,
                    execution_type=execution_type,
                )
                result.account_results.append(acct_result)
                if acct_result.get("success"):
                    result.orders.extend(acct_result.get("orders", []))
                else:
                    result.errors.append(
                        f"Account {config_id[:8]}: {acct_result.get('error', 'Unknown error')}"
                    )

        if not result.orders and result.errors:
            result.success = False

        return result

    def _process_for_account(
        self,
        *,
        user_id: str,
        config_id: str,
        intent: OrderIntentPayload,
        legs: List[LegPayload],
        execution_type: str,
    ) -> dict:
        """Process intent for a single broker account through TradingBot pipeline."""
        try:
            # Resolve broker session and adapter
            from broker.multi_broker import registry as mb_registry
            mb_sess = mb_registry.get_session(user_id, config_id)
            if not mb_sess or mb_sess.is_demo:
                return {
                    "success": False,
                    "config_id": config_id,
                    "error": "Broker account not connected or in demo mode",
                }

            client_id = mb_sess.client_id
            broker_id = mb_sess.broker_id

            # Get or create TradingBot for this account
            from trading.execution_guard import ExecutionGuard
            guard = _get_or_create_guard(user_id, config_id)
            bot = get_bot(
                user_id=user_id,
                client_id=client_id,
                api=mb_sess._adapter,
                execution_guard=guard,
            )
            if not bot:
                return {
                    "success": False,
                    "config_id": config_id,
                    "error": "Failed to initialize TradingBot",
                }

            # Ensure bot has correct config_id and broker_id for risk manager
            if not hasattr(bot, '_config_id') or bot._config_id != config_id:
                bot._config_id = config_id
                bot.risk = get_bot_risk(user_id, config_id, broker_id, client_id)

            # Risk Manager — pre-trade check
            if execution_type != "EXIT":
                for leg in legs:
                    allowed, reason = bot.risk.validate_order({
                        "transaction_type": leg.direction,
                        "symbol": leg.tradingsymbol,
                        "exchange": intent.exchange,
                        "quantity": leg.qty,
                        "price": leg.price,
                        "tag": execution_type,
                    })
                    if not allowed:
                        logger.warning(
                            "Risk block [user=%s client=%s]: %s",
                            user_id[:8], client_id, reason,
                        )
                        return {
                            "success": False,
                            "config_id": config_id,
                            "client_id": client_id,
                            "error": f"Risk blocked: {reason}",
                        }

            # ExecutionGuard validation
            # For manual orders, use a unique strategy_id per order so the guard
            # doesn't block sequential manual ENTRY orders as "duplicate ENTRY".
            # For actual strategies, the strategy_name is used as-is for proper
            # duplicate position detection.
            guard_strategy_id = intent.strategy_name
            if intent.strategy_name.lower() == "manual":
                guard_strategy_id = f"manual_{uuid.uuid4().hex[:8]}"

            leg_intents = [
                LegIntent(
                    strategy_id=guard_strategy_id,
                    symbol=leg.tradingsymbol,
                    direction=leg.direction.upper(),
                    qty=leg.qty,
                    tag=execution_type,
                )
                for leg in legs
            ]

            if execution_type in ("ENTRY", "ADJUST"):
                try:
                    allowed_intents = guard.validate_and_prepare(leg_intents, execution_type)
                    if not allowed_intents:
                        return {
                            "success": False,
                            "config_id": config_id,
                            "client_id": client_id,
                            "error": "ExecutionGuard: all legs blocked (duplicate or no delta)",
                        }
                except RuntimeError as e:
                    return {
                        "success": False,
                        "config_id": config_id,
                        "client_id": client_id,
                        "error": f"ExecutionGuard: {e}",
                    }
            else:
                # EXIT — resolve positions to exit
                allowed_intents = leg_intents

            # Route through CommandService for each leg
            orders = []
            for i, leg in enumerate(legs):
                # Match with allowed intent (may have adjusted qty)
                actual_qty = leg.qty
                if i < len(allowed_intents):
                    actual_qty = allowed_intents[i].qty

                cmd = UniversalOrderCommand(
                    command_id=str(uuid.uuid4()),
                    source="FRONTEND" if not intent.api_key else "WEBHOOK",
                    intent=execution_type,
                    user=client_id,
                    strategy_name=intent.strategy_name,
                    exchange=intent.exchange,
                    symbol=leg.tradingsymbol,
                    side=leg.direction.upper(),
                    quantity=actual_qty,
                    product=leg.product_type.upper(),
                    order_type=leg.order_type.upper(),
                    price=leg.price,
                    stop_loss=leg.stop_loss,
                    target=leg.target,
                    trailing_type=leg.trailing_type,
                    trailing_value=leg.trailing_value,
                    trail_when=leg.trail_when,
                    comment=intent.remarks,
                    test_mode=intent.test_mode,
                    user_id=int(user_id) if user_id.isdigit() else 0,
                    client_id=client_id,
                )

                try:
                    if execution_type == "EXIT":
                        # EXIT: register as intent, OrderWatcher dispatches
                        bot.command_svc.register(cmd)
                        orders.append({
                            "command_id": cmd.command_id,
                            "symbol": cmd.symbol,
                            "side": cmd.side,
                            "quantity": cmd.quantity,
                            "status": "CREATED",
                            "execution_type": "EXIT",
                            "message": "Exit intent registered — OrderWatcher will execute",
                        })
                    else:
                        # ENTRY / ADJUST: submit immediately through CommandService
                        exec_result = bot.command_svc.submit(cmd, execution_type=execution_type)
                        broker_order_id = ""
                        if exec_result and hasattr(exec_result, 'broker_order_id'):
                            broker_order_id = exec_result.broker_order_id

                        # Update ExecutionGuard on success
                        if exec_result and exec_result.success:
                            guard.on_fill(
                                guard_strategy_id,
                                leg.tradingsymbol,
                                leg.direction.upper(),
                                actual_qty,
                            )

                        orders.append({
                            "command_id": cmd.command_id,
                            "symbol": cmd.symbol,
                            "side": cmd.side,
                            "quantity": cmd.quantity,
                            "status": "SENT_TO_BROKER" if (exec_result and exec_result.success) else "FAILED",
                            "execution_type": execution_type,
                            "broker_order_id": broker_order_id,
                            "message": exec_result.error_message if (exec_result and not exec_result.success) else "OK",
                        })

                except Exception as e:
                    logger.error(
                        "CommandService error [%s %s]: %s",
                        cmd.symbol, execution_type, e,
                    )
                    orders.append({
                        "command_id": cmd.command_id,
                        "symbol": cmd.symbol,
                        "side": cmd.side,
                        "quantity": cmd.quantity,
                        "status": "FAILED",
                        "execution_type": execution_type,
                        "error": str(e),
                    })

            success = any(o.get("status") != "FAILED" for o in orders)
            return {
                "success": success,
                "config_id": config_id,
                "client_id": client_id,
                "orders": orders,
            }

        except Exception as e:
            logger.exception("Intent processing error for account %s", config_id[:8])
            return {
                "success": False,
                "config_id": config_id,
                "error": str(e),
            }


# ── Guard registry (per user+account) ────────────────────────────────────────

_guards: Dict[str, ExecutionGuard] = {}
_guard_lock = threading.Lock()


def _get_or_create_guard(user_id: str, config_id: str) -> ExecutionGuard:
    key = f"{user_id}:{config_id}"
    with _guard_lock:
        if key not in _guards:
            _guards[key] = ExecutionGuard()
        return _guards[key]


def get_bot_risk(user_id: str, config_id: str, broker_id: str, client_id: str):
    """Get or create AccountRiskManager for a broker account."""
    from broker.account_risk import get_account_risk
    return get_account_risk(
        user_id=user_id,
        config_id=config_id,
        broker_id=broker_id,
        client_id=client_id,
    )


# ── Module-level singleton ───────────────────────────────────────────────────

_processor = OrderIntentProcessor()


def process_order_intent(
    *,
    user_id: str,
    intent: OrderIntentPayload,
    db_session=None,
) -> IntentResult:
    """Module-level convenience function."""
    return _processor.process(user_id=user_id, intent=intent, db_session=db_session)
