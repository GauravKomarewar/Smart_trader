"""
Smart Trader — PositionWatcher (Real-Time Risk Scheduler)
=========================================================

Continuously polls ALL live broker accounts every 1-2 seconds,
feeds PnL into each AccountRiskManager.heartbeat(), and pushes
risk events to the WebSocket notification queue for instant
frontend updates.

Architecture (modelled on Shoonya Platform SupremeRiskManager):
  • Single background daemon thread per application
  • Iterates all live BrokerAccountSessions from MultiAccountRegistry
  • Calls AccountRiskManager.heartbeat(session) for each account
  • On risk breach → force-exit positions, cancel broker orders,
    cancel system entries, block new trades
  • Post-breach enforcement: if user places trades directly on
    broker terminal, next heartbeat detects and kills them

Usage:
    from trading.position_watcher import position_watcher

    # Start on app boot:
    position_watcher.start()

    # Stop on shutdown:
    position_watcher.stop()

    # Get latest snapshot for WebSocket push:
    snapshot = position_watcher.get_latest_snapshot(user_id)

    # Read risk alerts:
    alerts = position_watcher.drain_alerts(user_id)
"""

import threading
import logging
import time
import queue
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from collections import defaultdict

logger = logging.getLogger("smart_trader.position_watcher")


class RiskAlert:
    """A single risk event pushed to frontend via WebSocket."""

    __slots__ = ("user_id", "config_id", "broker_id", "client_id",
                 "level", "message", "pnl", "limit", "action", "ts")

    def __init__(
        self,
        user_id: str,
        config_id: str,
        broker_id: str,
        client_id: str,
        level: str,        # "INFO" | "WARNING" | "CRITICAL"
        message: str,
        pnl: float = 0.0,
        limit: float = 0.0,
        action: str = "",  # "FORCE_EXIT" | "POST_BREACH_FLATTEN" | "WARNING" | ""
    ):
        self.user_id = user_id
        self.config_id = config_id
        self.broker_id = broker_id
        self.client_id = client_id
        self.level = level
        self.message = message
        self.pnl = pnl
        self.limit = limit
        self.action = action
        self.ts = int(time.time())

    def to_dict(self) -> dict:
        return {
            "user_id": self.user_id,
            "config_id": self.config_id,
            "broker_id": self.broker_id,
            "client_id": self.client_id,
            "level": self.level,
            "message": self.message,
            "pnl": self.pnl,
            "limit": self.limit,
            "action": self.action,
            "ts": self.ts,
        }


class PositionWatcher:
    """
    Daemon that polls all live broker accounts every POLL_INTERVAL seconds.

    Lifecycle:
      1. start() — spawns the polling thread
      2. Each cycle: iterate all live sessions → heartbeat() each
      3. On breach: push RiskAlert, trigger force-exit
      4. stop() — graceful shutdown
    """

    POLL_INTERVAL = 1.5  # seconds between cycles

    def __init__(self):
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

        # Per-user latest snapshot: {user_id: {config_id: heartbeat_result}}
        self._snapshots: Dict[str, Dict[str, dict]] = defaultdict(dict)

        # Per-user alert queue (bounded to prevent memory leak)
        self._alerts: Dict[str, list] = defaultdict(list)
        self._alerts_lock = threading.Lock()
        self._MAX_ALERTS_PER_USER = 50

        # Force-exit callback registry: {(user_id, config_id): callable}
        self._exit_callbacks: Dict[tuple, Any] = {}

    # ── Lifecycle ─────────────────────────────────────────────────────────

    def start(self) -> None:
        with self._lock:
            if self._running:
                logger.info("PositionWatcher already running")
                return
            self._running = True
            self._thread = threading.Thread(
                target=self._loop,
                name="PositionWatcher",
                daemon=True,
            )
            self._thread.start()
            logger.info("PositionWatcher STARTED (poll=%.1fs)", self.POLL_INTERVAL)

    def stop(self) -> None:
        with self._lock:
            self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)
        logger.info("PositionWatcher STOPPED")

    @property
    def is_running(self) -> bool:
        return self._running

    # ── Main loop ─────────────────────────────────────────────────────────

    def _loop(self) -> None:
        logger.info("PositionWatcher loop started")
        while self._running:
            try:
                self._tick()
            except Exception:
                logger.exception("PositionWatcher tick error")
            time.sleep(self.POLL_INTERVAL)

    def _tick(self) -> None:
        """One monitoring cycle across all live broker accounts."""
        from broker.multi_broker import registry as mb
        from broker.account_risk import get_account_risk

        all_sessions = mb.get_all_sessions()
        if not all_sessions:
            return

        for sess in all_sessions:
            if sess.is_demo:
                continue

            user_id = sess.user_id
            config_id = sess.config_id

            try:
                # Get or create the risk manager
                risk = get_account_risk(
                    user_id=user_id,
                    config_id=config_id,
                    broker_id=sess.broker_id,
                    client_id=sess.client_id,
                )

                # Wire up force-exit callback if not already set
                if not risk._force_exit_cb:
                    risk.set_force_exit_callback(self._make_force_exit_cb())

                # Wire up warning callback
                if not risk._warning_cb:
                    risk.set_warning_callback(self._make_warning_cb())

                # Run the heartbeat cycle
                hb_result = risk.heartbeat(sess)

                # Store snapshot for WebSocket push
                self._snapshots[user_id][config_id] = hb_result

                # If breach action occurred, push alert
                if hb_result.get("action"):
                    self._push_alert(RiskAlert(
                        user_id=user_id,
                        config_id=config_id,
                        broker_id=sess.broker_id,
                        client_id=sess.client_id,
                        level="CRITICAL",
                        message=f"Risk breach on {sess.client_id}: {hb_result['action']}. "
                                f"PnL=₹{hb_result['pnl']:.0f}. "
                                f"Positions killed, orders cancelled.",
                        pnl=hb_result.get("pnl", 0),
                        action=hb_result.get("action", ""),
                    ))

                    # Cancel system entries in PostgreSQL
                    self._cancel_system_entries(user_id, config_id, risk)

            except Exception as e:
                logger.warning(
                    "PositionWatcher: heartbeat failed for user=%s config=%s: %s",
                    user_id[:8] if user_id else "?",
                    config_id[:8] if config_id else "?",
                    e,
                )

    # ── Force-exit callback factory ───────────────────────────────────────

    def _make_force_exit_cb(self):
        """
        Create a force-exit callback that routes through TradingBot.request_exit().
        Falls back to direct broker position squareoff if no bot exists.
        """
        def _force_exit(user_id: str, config_id: str, reason: str):
            logger.critical(
                "FORCE EXIT triggered for user=%s config=%s reason=%s",
                user_id[:8], config_id[:8], reason,
            )

            # Try through TradingBot first (proper exit flow)
            try:
                from trading.bot_core import _bots, _bot_lock
                with _bot_lock:
                    # Find bot matching this config_id
                    for key, bot in _bots.items():
                        if str(bot.user_id) == str(user_id):
                            bot.request_exit(
                                scope="ALL",
                                product_type="ALL",
                                reason=f"RMS: {reason}",
                                source="RISK_MANAGER",
                            )
                            logger.info("Force-exit routed through TradingBot for %s", key)
                            return
            except Exception as e:
                logger.error("TradingBot force-exit failed: %s", e)

            # Fallback: directly square off via broker session
            try:
                from broker.multi_broker import registry as mb
                sess = mb.get_session(user_id, config_id)
                if sess:
                    self._direct_squareoff(sess)
            except Exception as e:
                logger.error("Direct squareoff fallback failed: %s", e)

        return _force_exit

    def _direct_squareoff(self, session) -> None:
        """
        Emergency position squareoff: iterate all open positions
        and place counter-orders via the broker session.
        """
        try:
            positions = session.get_positions() or []
        except Exception:
            return

        for p in positions:
            if not isinstance(p, dict):
                continue
            net_qty = int(p.get("net_qty") or p.get("netqty") or p.get("qty", 0) or 0)
            if net_qty == 0:
                continue

            prd = (p.get("product", "") or p.get("prd", "")).upper()
            if prd in ("CNC", "C", "DELIVERY"):
                continue

            symbol = p.get("symbol") or p.get("tradingsymbol") or p.get("tsym", "")
            exchange = p.get("exchange") or p.get("exch", "NSE")

            # Determine exit side
            side = "SELL" if net_qty > 0 else "BUY"
            qty = abs(net_qty)

            # Map product
            _PRD_MAP = {"MIS": "MIS", "I": "MIS", "NRML": "NRML", "M": "NRML",
                        "INTRADAY": "MIS", "MARGIN": "NRML"}
            product = _PRD_MAP.get(prd, prd)

            try:
                session.place_order({
                    "symbol": symbol,
                    "exchange": exchange,
                    "side": side,
                    "product": product,
                    "order_type": "MARKET",
                    "qty": qty,
                    "price": 0,
                    "trigger_price": 0,
                    "tag": "RMS_EXIT",
                })
                logger.info(
                    "DIRECT SQUAREOFF: %s %s x%d on %s",
                    side, symbol, qty, session.client_id,
                )
            except Exception as e:
                logger.error("Direct squareoff failed for %s: %s", symbol, e)

    def _cancel_system_entries(self, user_id: str, config_id: str, risk) -> None:
        """Cancel pending system entries in PostgreSQL order repo."""
        try:
            from trading.bot_core import _bots, _bot_lock
            with _bot_lock:
                for key, bot in _bots.items():
                    if str(bot.user_id) == str(user_id):
                        risk.cancel_system_entries(bot.order_repo)
                        return
        except Exception as e:
            logger.warning("cancel_system_entries failed: %s", e)

    # ── Warning callback factory ──────────────────────────────────────────

    def _make_warning_cb(self):
        def _warning(user_id: str, config_id: str, pnl: float, limit: float):
            self._push_alert(RiskAlert(
                user_id=user_id,
                config_id=config_id,
                broker_id="",
                client_id="",
                level="WARNING",
                message=f"Risk warning: PnL ₹{pnl:.0f} approaching limit ₹{limit:.0f}",
                pnl=pnl,
                limit=limit,
                action="WARNING",
            ))
        return _warning

    # ── Alert queue ───────────────────────────────────────────────────────

    def _push_alert(self, alert: RiskAlert) -> None:
        with self._alerts_lock:
            user_alerts = self._alerts[alert.user_id]
            user_alerts.append(alert)
            # Trim to max size
            if len(user_alerts) > self._MAX_ALERTS_PER_USER:
                self._alerts[alert.user_id] = user_alerts[-self._MAX_ALERTS_PER_USER:]

    def drain_alerts(self, user_id: str) -> List[dict]:
        """
        Pop all pending risk alerts for a user.
        Called by WebSocket feed to push to frontend.
        """
        with self._alerts_lock:
            alerts = self._alerts.pop(user_id, [])
        return [a.to_dict() for a in alerts]

    def peek_alerts(self, user_id: str) -> List[dict]:
        """Non-destructive read of pending alerts."""
        with self._alerts_lock:
            return [a.to_dict() for a in self._alerts.get(user_id, [])]

    # ── Snapshot access ───────────────────────────────────────────────────

    def get_latest_snapshot(self, user_id: str) -> Dict[str, dict]:
        """
        Returns the latest heartbeat snapshot for all accounts of a user.
        {config_id: {pnl, positions_count, breach, action, ...}}
        """
        return dict(self._snapshots.get(user_id, {}))

    def get_all_snapshots(self) -> Dict[str, Dict[str, dict]]:
        """All snapshots across all users (admin view)."""
        return dict(self._snapshots)


# ── Module-level singleton ────────────────────────────────────────────────────
position_watcher = PositionWatcher()
