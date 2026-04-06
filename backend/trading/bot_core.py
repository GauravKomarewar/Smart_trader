"""
Smart Trader — TradingBot Core
================================

Central bot object that owns all trading services.
One instance per (user_id, client_id) combination.

Services wired in:
  - OrderRepository (PostgreSQL)
  - CommandService (single gate for all orders)
  - OrderWatcherEngine (SL/target/trailing daemon)
  - AccountRiskManager (per-account PnL + max-loss)
  - TelegramNotifier (optional)
  - HistoricalAnalyticsService
  - OptionChainSupervisor
  - CopyTradingService
  - RecoveryBootstrap (run once on start)
  - TradingScheduler (daily tasks)
"""

import logging
import os
import threading
from typing import Dict, List, Optional

from trading.persistence.repository import OrderRepository
from trading.execution.command_service import CommandService
from trading.execution.order_watcher import OrderWatcherEngine
from trading.execution.intent import UniversalOrderCommand
from broker.account_risk import get_account_risk
from trading.analytics.historical_service import HistoricalAnalyticsService
from trading.market_data.option_chain.supervisor import OptionChainSupervisor
from trading.market_data.option_chain.store import OptionChainStore
from trading.services.copy_trading_service import CopyTradingService
from trading.services.recovery_service import RecoveryBootstrap, OrphanPositionManager
from trading.services.scheduler import TradingScheduler
from trading.notifications.telegram import TelegramNotifier

logger = logging.getLogger("smart_trader.bot_core")


class _ExecuteResult:
    """Minimal result object returned by execute_command()."""
    def __init__(self, success: bool, broker_order_id: str = "", error_message: str = ""):
        self.success         = success
        self.broker_order_id = broker_order_id
        self.error_message   = error_message

    def to_dict(self):
        return {
            "success":          self.success,
            "broker_order_id":  self.broker_order_id,
            "error_message":    self.error_message,
        }


class TradingBot:
    """
    Central trading bot instance (one per user+client).

    Args:
        user_id   : integer user ID (from auth DB)
        client_id : broker account ID (e.g. "AB1234")
        api       : broker adapter instance (Shoonya / Fyers / Paper)
        execution_guard : ExecutionGuard instance from the alert router
    """

    def __init__(
        self,
        user_id,
        client_id: str,
        api,
        execution_guard=None,
        telegram_token: str = "",
        telegram_chat_id: str = "",
        config_id: str = "",
        broker_id: str = "",
    ):
        self.user_id        = user_id
        self.client_id      = client_id
        self.api            = api
        self.execution_guard = execution_guard

        # Core services
        self.order_repo = OrderRepository(user_id, client_id)
        self.command_svc = CommandService(self)
        self.order_watcher = OrderWatcherEngine(self)
        self.risk = get_account_risk(
            user_id=str(user_id),
            config_id=config_id or client_id,
            broker_id=broker_id or "unknown",
            client_id=client_id,
        )
        self.analytics = HistoricalAnalyticsService(user_id, client_id)

        # Option chain
        self._oc_store = OptionChainStore()
        self.oc_supervisor = OptionChainSupervisor(self, self._oc_store)

        # Notifications
        token    = telegram_token    or os.getenv("TELEGRAM_BOT_TOKEN", "")
        chat_id  = telegram_chat_id  or os.getenv("TELEGRAM_CHAT_ID", "")
        self.telegram_enabled = bool(token and chat_id)
        self.telegram: Optional[TelegramNotifier] = (
            TelegramNotifier(token, chat_id) if self.telegram_enabled else None
        )

        # Copy trading
        self.copy_trading = CopyTradingService.from_env(client_id=client_id)

        # Scheduler
        self.scheduler = TradingScheduler(self)

        logger.info("TradingBot created | user=%s | client=%s | telegram=%s",
                    user_id, client_id, self.telegram_enabled)

    # -- Lifecycle ------------------------------------------------------------

    def start(self, run_recovery: bool = True):
        """Start all background services. Call once after construction."""
        if run_recovery:
            try:
                RecoveryBootstrap(self).run()
            except Exception:
                logger.exception("Recovery failed — continuing anyway")

        self.order_watcher.start()
        self.scheduler.start()

        if self.telegram_enabled and self.telegram:
            self.telegram.send_ready(self.client_id)

        logger.info("TradingBot STARTED | client=%s", self.client_id)

    def stop(self):
        """Stop all background services gracefully."""
        self.order_watcher.stop()
        self.scheduler.stop()
        self.oc_supervisor.stop()
        logger.info("TradingBot STOPPED | client=%s", self.client_id)

    # -- Interfaces required by sub-systems -----------------------------------

    def execute_command(
        self,
        command: UniversalOrderCommand,
        trailing_engine=None,
    ) -> _ExecuteResult:
        """
        Execute a single order command via the broker adapter.
        Called by CommandService.submit() for ENTRY/ADJUST orders.
        Updates DB record to SENT_TO_BROKER.
        """
        from trading.persistence.repository import OrderRepository
        repo = self.order_repo

        # Map UniversalOrderCommand → broker-specific place_order dict
        try:
            order_type = command.order_type or "MARKET"
            price = command.price or 0.0

            # ── Smart MKT → LIMIT conversion (SEBI compliance) ────────────
            if order_type == "MARKET" and hasattr(self.api, "get_ltp"):
                try:
                    ltp = self.api.get_ltp(command.exchange, command.symbol)
                    if ltp and ltp > 0:
                        buffer = 1.005 if command.side == "BUY" else 0.995
                        smart_price = round(ltp * buffer, 2)
                        smart_price = round(round(smart_price / 0.05) * 0.05, 2)
                        order_type = "LIMIT"
                        price = smart_price
                        logger.info(
                            "SMART_LIMIT: MKT→LIMIT %s %s @ %.2f (LTP=%.2f, buffer=%.1f%%)",
                            command.side, command.symbol, smart_price, ltp,
                            (buffer - 1) * 100,
                        )
                    else:
                        logger.warning(
                            "SMART_LIMIT: LTP unavailable for %s:%s — falling back to MARKET",
                            command.exchange, command.symbol,
                        )
                except Exception as ltp_err:
                    logger.warning(
                        "SMART_LIMIT: LTP fetch failed for %s — falling back to MARKET: %s",
                        command.symbol, ltp_err,
                    )

            resp = self.api.place_order({
                "symbol":        command.symbol,
                "exchange":      command.exchange,
                "side":          command.side,
                "order_type":    order_type,
                "product":       command.product,
                "qty":           command.quantity,
                "price":         price,
                "trigger_price": getattr(command, "trigger_price", 0.0) or 0.0,
                "remarks":       getattr(command, "comment", "") or "",
            })
            broker_order_id = (
                (resp or {}).get("norenordno")
                or (resp or {}).get("order_id")
                or (resp or {}).get("orderid")
                or ""
            )

            # Update DB: CREATED → SENT_TO_BROKER
            record = repo.get_by_id(command.command_id)
            if record:
                repo.update_status(command.command_id, "SENT_TO_BROKER")
                if broker_order_id:
                    repo.update_broker_id(command.command_id, broker_order_id)

            logger.info("EXECUTED | %s %s x%d @ %s | broker_id=%s",
                        command.side, command.symbol, command.quantity,
                        price if price else "MKT", broker_order_id)
            return _ExecuteResult(True, broker_order_id)

        except Exception as e:
            logger.error("execute_command failed: %s | %s", command.symbol, e)
            repo.update_status(command.command_id, "FAILED")
            return _ExecuteResult(False, error_message=str(e))

    def request_exit(
        self,
        scope: str = "ALL",
        symbols: Optional[List[str]] = None,
        product_type: str = "ALL",
        reason: str = "EXIT",
        source: str = "SYSTEM",
        stop_loss: Optional[float] = None,
        target: Optional[float] = None,
        trailing_type: Optional[str] = None,
        trailing_value: Optional[float] = None,
        trail_when: Optional[float] = None,
        strategy_name: Optional[str] = None,
    ):
        """Route to CommandService.handle_exit_intent(). The only exit gateway."""
        self.command_svc.handle_exit_intent(
            scope=scope,
            strategy_name=strategy_name,
            symbols=symbols,
            product_type=product_type,
            reason=reason,
            source=source,
            stop_loss=stop_loss,
            target=target,
            trailing_type=trailing_type,
            trailing_value=trailing_value,
            trail_when=trail_when,
        )

    def notify_fill(
        self,
        symbol: str,
        side: str,
        qty: int,
        price: float,
        strategy_name: str = "",
    ):
        """Called by OrderWatcher when a fill is confirmed."""
        if self.execution_guard:
            try:
                self.execution_guard.on_fill(strategy_name, symbol, side, qty)
            except Exception:
                pass
        if self.telegram_enabled and self.telegram:
            self.telegram.send_order(symbol, side, qty, price, strategy_name)

    def send_telegram(self, msg: str, category: str = "system"):
        if self.telegram_enabled and self.telegram:
            try:
                if self.telegram.is_category_enabled(category):
                    self.telegram.send_message(msg)
            except Exception:
                pass

    # -- Passthrough properties used across modules ---------------------------

    def get_positions(self) -> list:
        try:
            return self.api.get_positions() or []
        except Exception:
            return []

    def get_order_book(self) -> list:
        try:
            return self.api.get_order_book() or []
        except Exception:
            return []


# ---------------------------------------------------------------------------
# Per-user singleton registry
# ---------------------------------------------------------------------------

_bots: Dict[str, "TradingBot"] = {}
_bot_lock = threading.Lock()


def get_bot(user_id, client_id: str = "", api=None, execution_guard=None) -> Optional["TradingBot"]:
    """
    Return the existing TradingBot for (user_id, client_id) or create a new one.
    If api is provided and no instance exists, creates + starts the bot.
    """
    key = f"{user_id}:{client_id}"
    with _bot_lock:
        if key not in _bots:
            if api is None:
                return None
            bot = TradingBot(
                user_id=user_id,
                client_id=client_id,
                api=api,
                execution_guard=execution_guard,
            )
            bot.start(run_recovery=True)
            _bots[key] = bot
        return _bots[key]


def remove_bot(user_id, client_id: str = ""):
    key = f"{user_id}:{client_id}"
    with _bot_lock:
        bot = _bots.pop(key, None)
        if bot:
            bot.stop()
