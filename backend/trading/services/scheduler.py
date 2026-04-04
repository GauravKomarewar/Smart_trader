"""
Smart Trader — Scheduler (Daily Tasks)
========================================

Handles:
  - Daily state reset at configured time (default 08:55)
  - EOD report at 15:35
  - Periodic status heartbeat
  - Cooldown check on startup

Port of shoonya_platform/bot_status_scheduling.py
"""

import logging
import threading
import time
from datetime import date, datetime, timedelta
from typing import Callable, Optional

logger = logging.getLogger("smart_trader.scheduler")


class _ScheduledTask:
    def __init__(self, name: str, hour: int, minute: int, callback: Callable, run_once: bool = False):
        self.name      = name
        self.hour      = hour
        self.minute    = minute
        self.callback  = callback
        self.run_once  = run_once
        self.last_run: Optional[date] = None


class TradingScheduler:
    """
    Minute-granularity task scheduler that runs as a daemon thread.
    Checks every 30 seconds if any tasks should fire.
    """

    def __init__(self, bot, poll_seconds: float = 30.0):
        self.bot          = bot
        self.poll_seconds = poll_seconds
        self._tasks: list = []
        self._running     = False
        self._thread: Optional[threading.Thread] = None

        # Register default tasks
        self._register_defaults()

    def _register_defaults(self):
        # Morning reset (pre-market)
        self.add_task("morning_reset", 8, 55, self._morning_reset)
        # EOD report
        self.add_task("eod_report", 15, 35, self._eod_report)
        # Mid-day status
        self.add_task("midday_status", 12, 0, self._midday_status)

    def add_task(self, name: str, hour: int, minute: int,
                 callback: Callable, run_once: bool = False):
        self._tasks.append(_ScheduledTask(name, hour, minute, callback, run_once))
        logger.info("Scheduler task registered: %s @ %02d:%02d", name, hour, minute)

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._loop, daemon=True,
            name=f"Scheduler:{self.bot.client_id}")
        self._thread.start()
        logger.info("TradingScheduler started | poll=%.0fs", self.poll_seconds)

    def stop(self):
        self._running = False

    def _loop(self):
        while self._running:
            try:
                self._tick()
            except Exception:
                logger.exception("Scheduler tick error")
            time.sleep(self.poll_seconds)

    def _tick(self):
        now  = datetime.now()
        today = now.date()

        for task in self._tasks:
            if task.last_run == today:
                continue
            if now.hour == task.hour and now.minute >= task.minute:
                try:
                    logger.info("Running scheduled task: %s", task.name)
                    task.callback()
                    task.last_run = today
                except Exception:
                    logger.exception("Task %s failed", task.name)

    # -- Default callbacks ----------------------------------------------------

    def _morning_reset(self):
        """Reset daily state, run recovery, log startup."""
        logger.info("MORNING RESET: %s", date.today())
        # Trigger risk manager daily reset if already initialized
        if hasattr(self.bot, "risk") and self.bot.risk:
            try:
                self.bot.risk._reset_daily_state(date.today())
            except Exception:
                pass
        # Send startup notification
        if hasattr(self.bot, "telegram") and getattr(self.bot, "telegram_enabled", False):
            try:
                self.bot.telegram.send_startup(
                    self.bot.client_id, version="2.0"
                )
            except Exception:
                pass

    def _eod_report(self):
        """Record daily PnL, send EOD summary."""
        logger.info("EOD REPORT: %s", date.today())
        pnl = 0.0
        try:
            if hasattr(self.bot, "risk") and self.bot.risk:
                pnl = self.bot.risk.daily_pnl
        except Exception:
            pass

        # Persist historical PnL
        if hasattr(self.bot, "analytics") and self.bot.analytics:
            try:
                self.bot.analytics.record_eod(pnl=pnl)
            except Exception:
                logger.exception("EOD: failed to record analytics")

        # Send Telegram EOD summary
        if hasattr(self.bot, "telegram") and getattr(self.bot, "telegram_enabled", False):
            try:
                self.bot.telegram.send_daily(pnl, 0, date.today().strftime("%d %b %Y"))
            except Exception:
                pass

    def _midday_status(self):
        """Send mid-day heartbeat."""
        if not (hasattr(self.bot, "telegram") and getattr(self.bot, "telegram_enabled", False)):
            return
        try:
            risk   = getattr(self.bot, "risk", None)
            pnl    = risk.daily_pnl if risk else 0.0
            repo   = getattr(self.bot, "order_repo", None)
            count  = len(repo.get_open_orders()) if repo else 0
            self.bot.telegram.send_heartbeat(self.bot.client_id, pnl, count)
        except Exception:
            pass
