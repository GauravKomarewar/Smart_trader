"""
Smart Trader — CommandService
==============================

THE SINGLE GATE for all trading actions.

Ported from shoonya_platform/execution/command_service.py

Guarantees:
  - No direct broker access from business logic
  - Full audit + recovery support
  - Validation before persistence
  - Trailing engine wired into execution

EXIT FLOW:
  handle_exit_intent() → PositionExitService → DB EXIT record
  → OrderWatcherEngine polls → broker execution

ENTRY FLOW:
  submit() → validate → persist → execute_command()

Multi-tenant: requires user_id + client_id on all bot operations.
"""

import logging
from datetime import datetime, timezone
from typing import List, Optional

from trading.execution.intent import UniversalOrderCommand, validate_order
from trading.execution.trailing import make_trailing_engine
from trading.execution.position_exit_service import PositionExitService
from trading.persistence.order_record import OrderRecord
from trading.persistence.repository import OrderRepository

logger = logging.getLogger("smart_trader.command_service")


class CommandService:
    """
    Single gate for all trading actions.
    Injected into TradingBot instances.
    """

    def __init__(self, bot):
        self.bot = bot
        self.user_id = bot.user_id
        self.client_id = bot.client_id
        self.order_repo: OrderRepository = bot.order_repo
        self.position_exit_service = PositionExitService(
            broker_client=bot.api,
            order_repo=self.order_repo,
            user_id=self.user_id,
            client_id=self.client_id,
        )

    def register(self, cmd: UniversalOrderCommand):
        """
        Register EXIT intent only (NO execution).
        EXIT execution is deferred to OrderWatcherEngine.
        """
        cmd = cmd.with_intent("EXIT")
        validate_order(cmd)

        _tag = "EXIT"
        comment = str(getattr(cmd, "comment", "") or "").upper()
        if "TEST_MODE_SUCCESS" in comment:
            _tag = "EXIT|TEST_MODE_SUCCESS"
        elif "TEST_MODE_FAILURE" in comment:
            _tag = "EXIT|TEST_MODE_FAILURE"

        now = datetime.now(timezone.utc).isoformat()
        record = OrderRecord(
            command_id      = cmd.command_id,
            broker_order_id = None,
            execution_type  = "EXIT",
            source          = cmd.source,
            broker_user     = cmd.user or self.client_id,
            strategy_name   = cmd.strategy_name,
            exchange        = cmd.exchange,
            symbol          = cmd.symbol,
            side            = cmd.side,
            quantity        = cmd.quantity,
            product         = cmd.product,
            order_type      = cmd.order_type or "MARKET",
            price           = cmd.price or 0.0,
            stop_loss       = cmd.stop_loss,
            target          = cmd.target,
            trailing_type   = cmd.trailing_type,
            trailing_value  = cmd.trailing_value,
            trail_when      = cmd.trail_when,
            status          = "CREATED",
            created_at      = now,
            updated_at      = now,
            tag             = _tag,
        )
        self.order_repo.create(record)
        logger.info("REGISTER_EXIT | cmd_id=%s | %s %s qty=%s",
                    cmd.command_id, cmd.symbol, cmd.side, cmd.quantity)

    def submit(self, cmd: UniversalOrderCommand, *, execution_type: str):
        """
        Validate, persist, and submit ENTRY / ADJUST commands.

        EXIT commands MUST use handle_exit_intent() instead.
        """
        if execution_type == "EXIT" or cmd.intent == "EXIT":
            raise RuntimeError(
                "EXIT submission forbidden via submit(). "
                "Use handle_exit_intent() → PositionExitService → OrderWatcher."
            )

        now = datetime.now(timezone.utc).isoformat()

        # 1. Hard validation
        try:
            validate_order(cmd)
        except Exception as ve:
            self.order_repo.create(OrderRecord(
                command_id      = cmd.command_id,
                broker_order_id = None,
                execution_type  = execution_type,
                source          = cmd.source,
                broker_user     = cmd.user or self.client_id,
                strategy_name   = cmd.strategy_name,
                exchange        = cmd.exchange,
                symbol          = cmd.symbol,
                side            = cmd.side,
                quantity        = cmd.quantity,
                product         = cmd.product,
                order_type      = cmd.order_type or "MARKET",
                price           = cmd.price or 0.0,
                stop_loss       = cmd.stop_loss,
                target          = cmd.target,
                trailing_type   = cmd.trailing_type,
                trailing_value  = cmd.trailing_value,
                trail_when      = cmd.trail_when,
                status          = "FAILED",
                created_at      = now,
                updated_at      = now,
                tag             = "VALIDATION_FAILED",
            ))
            raise

        # 2. Build trailing engine
        trailing_engine = make_trailing_engine(
            cmd.trailing_type, cmd.trailing_value, cmd.trail_step
        )

        # 3. Persist
        record = OrderRecord(
            command_id      = cmd.command_id,
            broker_order_id = None,
            execution_type  = execution_type,
            source          = cmd.source,
            broker_user     = cmd.user or self.client_id,
            strategy_name   = cmd.strategy_name,
            exchange        = cmd.exchange,
            symbol          = cmd.symbol,
            side            = cmd.side,
            quantity        = cmd.quantity,
            product         = cmd.product,
            order_type      = cmd.order_type or "MARKET",
            price           = cmd.price or 0.0,
            stop_loss       = cmd.stop_loss,
            target          = cmd.target,
            trailing_type   = cmd.trailing_type,
            trailing_value  = cmd.trailing_value,
            trail_when      = cmd.trail_when,
            status          = "CREATED",
            created_at      = now,
            updated_at      = now,
            tag             = None,
        )
        self.order_repo.create(record)

        # 4. Execute via bot
        return self.bot.execute_command(
            command=cmd,
            trailing_engine=trailing_engine,
        )

    def handle_exit_intent(
        self,
        *,
        scope: str,
        strategy_name: Optional[str] = None,
        symbols: Optional[List[str]] = None,
        product_type: str = "ALL",
        reason: str = "EXIT",
        source: str = "SYSTEM",
        stop_loss: Optional[float] = None,
        target: Optional[float] = None,
        trailing_type: Optional[str] = None,
        trailing_value: Optional[float] = None,
        trail_when: Optional[float] = None,
    ):
        """
        Route EXIT intent to PositionExitService.
        This is the SINGLE GATEWAY for all exit requests.
        """
        logger.info(
            "EXIT_INTENT | scope=%s strategy=%s symbols=%s reason=%s source=%s",
            scope, strategy_name, symbols, reason, source,
        )
        self.position_exit_service.exit_positions(
            scope=scope,
            strategy_name=strategy_name,
            symbols=symbols,
            product_scope=product_type,
            reason=reason,
            source=source,
            stop_loss=stop_loss,
            target=target,
            trailing_type=trailing_type,
            trailing_value=trailing_value,
            trail_when=trail_when,
        )
