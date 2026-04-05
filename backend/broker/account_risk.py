"""
Smart Trader — Per-Account Risk Manager
=========================================

Each broker account (user_id + config_id) gets its own independent
risk manager.  This ensures risk limits are enforced per-account and
trading on one account does not affect another.

Risk checks happen at two points:
  1. PRE-TRADE  — validate_order() before sending to broker
  2. REAL-TIME  — on_pnl_update() called by PnL feed / positions poller

Architecture (ported from shoonya_platform SupremeRiskManager):
  • Daily P&L monitoring with trailing max-loss
  • Configurable warning threshold (default 80%)
  • Consecutive loss-day tracking
  • Force-exit callback when limits breached
  • Cooldown period after breach
  • State persisted to JSON per account

Usage:
    from broker.account_risk import get_account_risk

    risk = get_account_risk(user_id, config_id)
    allowed, reason = risk.validate_order(order_dict)
    if not allowed:
        raise HTTPException(400, reason)

    # Called periodically or on tick:
    risk.on_pnl_update(current_pnl)

    status = risk.get_status()  # dict for API response
"""

import threading
import logging
import json
import os
import time
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Callable, Dict, Any, List
from dataclasses import dataclass, field, asdict

logger = logging.getLogger("smart_trader.account_risk")

# ── Risk Configuration ────────────────────────────────────────────────────────

@dataclass
class AccountRiskConfig:
    """
    Risk parameters for a single broker account.
    Defaults are conservative — admin/user can override via API.
    """
    # P&L limits
    max_daily_loss:         float = -2500.0   # must be negative, e.g. -2500
    trail_step:             float = 100.0     # raise trailing limit by this on profit
    warning_threshold_pct:  float = 0.80      # warn at 80% of max_daily_loss
    max_consecutive_loss_days: int = 3

    # Order-level guards
    max_order_value:        float = 500_000.0  # single order max ₹ value
    max_qty_per_order:      int   = 1800       # single order max qty (1 lot NIFTY = 75)
    max_open_positions:     int   = 20         # max open legs simultaneously
    allowed_exchanges:      tuple = ("NSE", "BSE", "NFO", "BFO", "MCX", "CDS")

    # State persistence
    state_dir: str = "logs/risk"

    def to_dict(self) -> dict:
        d = asdict(self)
        d["allowed_exchanges"] = list(d["allowed_exchanges"])
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "AccountRiskConfig":
        d2 = dict(d)
        if "allowed_exchanges" in d2:
            d2["allowed_exchanges"] = tuple(d2["allowed_exchanges"])
        return cls(**{k: v for k, v in d2.items() if k in cls.__dataclass_fields__})


# ── Risk Status ───────────────────────────────────────────────────────────────

@dataclass
class AccountRiskStatus:
    account_id:           str   = ""
    broker_id:            str   = ""
    client_id:            str   = ""
    daily_pnl:            float = 0.0
    dynamic_max_loss:     float = 0.0
    highest_profit:       float = 0.0
    daily_loss_hit:       bool  = False
    warning_sent:         bool  = False
    cooldown_until:       Optional[str] = None
    consecutive_loss_days: int  = 0
    force_exit_triggered: bool  = False
    trading_allowed:      bool  = True
    halt_reason:          str   = ""
    checked_at:           str   = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> dict:
        return asdict(self)


# ── Account Risk Manager ──────────────────────────────────────────────────────

class AccountRiskManager:
    """
    Thread-safe, state-persistent risk manager for one broker account.

    Ported from shoonya_platform SupremeRiskManager and extended with:
    - Per-account identity (user_id, config_id, client_id)
    - Pre-trade order validation (value, qty, exchange guards)
    - Force-exit callback for integration with order execution
    """

    def __init__(
        self,
        user_id:   str,
        config_id: str,
        broker_id: str,
        client_id: str,
        config:    Optional[AccountRiskConfig] = None,
    ):
        self.user_id   = user_id
        self.config_id = config_id
        self.broker_id = broker_id
        self.client_id = client_id
        self._config   = config or AccountRiskConfig()
        self._lock     = threading.RLock()

        # State
        self._daily_pnl:      float = 0.0
        self._highest_profit: float = 0.0
        self._dynamic_max_loss: float = self._config.max_daily_loss
        self._daily_loss_hit: bool = False
        self._warning_sent:   bool = False
        self._cooldown_until: Optional[datetime] = None
        self._consecutive_loss_days: int = 0
        self._force_exit_triggered: bool = False
        self._today: date = date.today()

        # Callbacks
        self._force_exit_cb: Optional[Callable] = None
        self._warning_cb:    Optional[Callable] = None

        # Heartbeat state
        self._last_known_pnl: float = 0.0
        self._force_exit_in_progress: bool = False

        # Setup logging with context
        self._log = logging.LoggerAdapter(
            logging.getLogger("smart_trader.account_risk"),
            {"user_id": user_id[:8], "broker": broker_id, "client": client_id},
        )

        # Ensure state directory exists
        state_dir = Path(self._config.state_dir)
        state_dir.mkdir(parents=True, exist_ok=True)
        self._state_file = state_dir / f"risk_{config_id[:8]}.json"

        self._load_state()
        self._log.info("Risk manager initialised — max_loss=%s", self._config.max_daily_loss)

    # ── Callback registration ─────────────────────────────────────────────────

    def set_force_exit_callback(self, fn: Callable) -> None:
        """Called with (user_id, config_id, reason) when risk limit breached."""
        self._force_exit_cb = fn

    def set_warning_callback(self, fn: Callable) -> None:
        """Called with (user_id, config_id, pnl, limit) at warning threshold."""
        self._warning_cb = fn

    # ── Pre-trade validation ──────────────────────────────────────────────────

    def validate_order(self, order: dict) -> tuple[bool, str]:
        """
        Pre-trade risk check.

        Returns (True, "") if allowed, or (False, reason_string) if blocked.
        EXIT orders (SELL on a position) are never blocked.
        """
        tag = order.get("tag", "").upper()

        # EXIT orders are always allowed (never block covers)
        if "EXIT" in tag or order.get("transaction_type", "").upper() in ("S", "SELL"):
            return True, ""

        with self._lock:
            self._maybe_roll_day()

            # 1. Trading halted?
            if self._daily_loss_hit:
                reason = (
                    f"Daily loss limit breached on {self.client_id} "
                    f"(P&L: ₹{self._daily_pnl:.0f}, limit: ₹{self._dynamic_max_loss:.0f})"
                )
                self._log.warning("ORDER BLOCKED — %s", reason)
                return False, reason

            if self._cooldown_until and datetime.now(timezone.utc) < self._cooldown_until:
                remaining = int((self._cooldown_until - datetime.now(timezone.utc)).total_seconds() / 60)
                reason = f"Account {self.client_id} in cooldown for {remaining} more minutes"
                self._log.warning("ORDER BLOCKED — %s", reason)
                return False, reason

            # 2. Exchange allowed?
            exch = order.get("exchange", "NSE").upper()
            if self._config.allowed_exchanges and exch not in self._config.allowed_exchanges:
                reason = f"Exchange {exch} not allowed for account {self.client_id}"
                self._log.warning("ORDER BLOCKED — %s", reason)
                return False, reason

            # 3. Quantity guard
            qty = int(order.get("quantity", 0))
            if qty <= 0:
                return False, "Quantity must be > 0"
            if qty > self._config.max_qty_per_order:
                reason = (
                    f"Quantity {qty} exceeds max {self._config.max_qty_per_order} "
                    f"for account {self.client_id}"
                )
                self._log.warning("ORDER BLOCKED — %s", reason)
                return False, reason

            # 4. Order value guard
            price = float(order.get("price", 0) or order.get("ltp", 0))
            if price > 0:
                value = qty * price
                if value > self._config.max_order_value:
                    reason = (
                        f"Order value ₹{value:,.0f} exceeds max ₹{self._config.max_order_value:,.0f} "
                        f"for account {self.client_id}"
                    )
                    self._log.warning("ORDER BLOCKED — %s", reason)
                    return False, reason

        return True, ""

    # ── Real-time P&L update ──────────────────────────────────────────────────

    def on_pnl_update(self, pnl: float) -> None:
        """
        Called on each P&L tick (positions polling, WebSocket, etc.).
        Updates trailing max-loss and triggers force-exit if breached.
        """
        with self._lock:
            self._maybe_roll_day()
            self._daily_pnl = pnl

            # Update trailing high-water mark
            if pnl > self._highest_profit:
                self._highest_profit = pnl
                # Raise trailing limit
                steps = int(pnl / self._config.trail_step)
                self._dynamic_max_loss = min(
                    self._config.max_daily_loss + steps * self._config.trail_step,
                    0.0,   # never above 0
                )

            # Warning threshold
            warning_level = self._dynamic_max_loss * self._config.warning_threshold_pct
            if pnl <= warning_level and not self._warning_sent:
                self._warning_sent = True
                self._log.warning(
                    "RISK WARNING — PnL ₹%.0f approaching limit ₹%.0f (account=%s)",
                    pnl, self._dynamic_max_loss, self.client_id,
                )
                if self._warning_cb:
                    try:
                        self._warning_cb(self.user_id, self.config_id, pnl, self._dynamic_max_loss)
                    except Exception as e:
                        self._log.error("Warning callback error: %s", e)

            # Breach check
            if pnl <= self._dynamic_max_loss and not self._daily_loss_hit:
                self._daily_loss_hit = True
                self._force_exit_triggered = True
                self._consecutive_loss_days += 1
                self._log.critical(
                    "RISK BREACH — PnL ₹%.0f hit limit ₹%.0f for account=%s (client=%s). "
                    "Triggering force exit.",
                    pnl, self._dynamic_max_loss, self.config_id[:8], self.client_id,
                )
                self._save_state()
                if self._force_exit_cb:
                    try:
                        self._force_exit_cb(
                            self.user_id,
                            self.config_id,
                            f"Daily loss limit breached: P&L={pnl:.0f} limit={self._dynamic_max_loss:.0f}",
                        )
                    except Exception as e:
                        self._log.error("Force-exit callback error: %s", e)

    # ── Heartbeat — self-driven PnL + enforcement cycle ──────────────────────

    def heartbeat(self, session) -> dict:
        """
        Self-driven monitoring cycle.  Called every 1-2s by PositionWatcher.

        1. Fetches positions from broker via session.get_positions()
        2. Computes intraday PnL (excludes CNC/delivery)
        3. Feeds PnL to on_pnl_update()
        4. POST-BREACH enforcement: if daily_loss_hit AND positions still
           exist → force-exit again (catches human-placed trades on broker)
        5. Returns a status dict for push to frontend

        Args:
            session: BrokerAccountSession (has get_positions, get_order_book,
                     cancel_order, place_order)
        """
        result = {
            "config_id": self.config_id,
            "broker_id": self.broker_id,
            "client_id": self.client_id,
            "pnl": 0.0,
            "positions_count": 0,
            "breach": False,
            "action": None,
        }

        try:
            positions = session.get_positions() or []
        except Exception as e:
            self._log.warning("heartbeat: get_positions failed: %s", e)
            return result

        # ── Empty-position guard (BUG-12) ─────────────────────────────────
        # If broker returns empty positions mid-session, preserve last known
        # PnL to avoid falsely resetting to 0 and missing a breach.
        if not positions and hasattr(self, "_last_known_pnl") and self._last_known_pnl != 0.0:
            result["pnl"] = self._last_known_pnl
            return result

        # ── Compute intraday PnL (exclude CNC/delivery) ──────────────────
        pnl = 0.0
        open_qty_count = 0
        for p in positions:
            if isinstance(p, dict):
                prd = (p.get("product", "") or p.get("prd", "")).upper()
                # Skip delivery/CNC positions for intraday risk
                if prd in ("CNC", "C", "DELIVERY"):
                    continue
                net_qty = int(p.get("net_qty") or p.get("netqty") or p.get("qty", 0) or 0)
                rpnl = float(p.get("realised_pnl") or p.get("rpnl", 0) or 0)
                urmtom = float(
                    p.get("unrealised_pnl")
                    or p.get("urmtom", 0)
                    or p.get("pnl", 0)
                    or 0
                )
                pnl += rpnl + urmtom
                if net_qty != 0:
                    open_qty_count += 1

        self._last_known_pnl = pnl
        result["pnl"] = round(pnl, 2)
        result["positions_count"] = open_qty_count

        # Feed PnL into the standard risk engine
        self.on_pnl_update(pnl)

        # ── Post-breach continuous enforcement ────────────────────────────
        # If daily_loss_hit but positions still exist (e.g. human placed
        # trades on broker terminal), kill them again.
        with self._lock:
            if self._daily_loss_hit and open_qty_count > 0:
                self._log.critical(
                    "POST-BREACH: %d open positions detected after breach "
                    "(client=%s). Re-triggering force exit + order cancellation.",
                    open_qty_count, self.client_id,
                )
                result["breach"] = True
                result["action"] = "POST_BREACH_FLATTEN"

                # Cancel all open broker orders first
                try:
                    self.cancel_all_broker_orders(session)
                except Exception as e:
                    self._log.error("Post-breach cancel_all_broker_orders failed: %s", e)

                # Trigger force-exit callback to flatten positions
                if self._force_exit_cb:
                    try:
                        self._force_exit_cb(
                            self.user_id,
                            self.config_id,
                            "POST_BREACH_FLATTEN: Human-placed trades detected after risk breach",
                        )
                    except Exception as e:
                        self._log.error("Post-breach force_exit_cb failed: %s", e)

            elif self._daily_loss_hit and open_qty_count == 0:
                # All flat after breach — good. Cancel lingering orders just in case.
                try:
                    self.cancel_all_broker_orders(session)
                except Exception:
                    pass

        return result

    # ── Broker order cancellation ─────────────────────────────────────────

    def cancel_all_broker_orders(self, session) -> int:
        """
        Cancel all open/pending/trigger-pending orders on the broker.

        Called on risk breach and during post-breach enforcement.
        Returns the number of orders cancelled.
        """
        cancelled = 0
        try:
            orders = session.get_order_book() or []
        except Exception as e:
            self._log.error("cancel_all_broker_orders: get_order_book failed: %s", e)
            return 0

        _OPEN_STATUSES = {
            "OPEN", "PENDING", "TRIGGER_PENDING", "TRIGGER PENDING",
            "NEW", "AFTER MARKET ORDER REQ RECEIVED", "MODIFIED",
            "WAITING", "PARTIALLY_FILLED", "PARTIALLY_EXECUTED",
        }

        for order in orders:
            if isinstance(order, dict):
                status = (
                    order.get("status", "")
                    or order.get("stat", "")
                    or order.get("Status", "")
                ).upper().strip()
                order_id = (
                    order.get("order_id")
                    or order.get("norenordno")
                    or order.get("orderid")
                    or order.get("oms_order_id")
                    or ""
                )
            else:
                status = str(getattr(order, "status", "")).upper().strip()
                order_id = str(
                    getattr(order, "order_id", "")
                    or getattr(order, "norenordno", "")
                    or ""
                )

            if not order_id or status not in _OPEN_STATUSES:
                continue

            try:
                resp = session.cancel_order(order_id)
                cancelled += 1
                self._log.info(
                    "CANCELLED broker order %s (status=%s) on client=%s",
                    order_id, status, self.client_id,
                )
            except Exception as e:
                self._log.error(
                    "Failed to cancel order %s on client=%s: %s",
                    order_id, self.client_id, e,
                )

        if cancelled:
            self._log.info(
                "cancel_all_broker_orders: cancelled %d orders on client=%s",
                cancelled, self.client_id,
            )
        return cancelled

    # ── System entry cancellation ─────────────────────────────────────────

    def cancel_system_entries(self, order_repo) -> int:
        """
        Mark all CREATED (not yet sent) system orders as FAILED in
        the PostgreSQL order repository.

        Prevents the OrderWatcher from picking up queued orders
        after a risk breach.
        """
        cancelled = 0
        try:
            pending = order_repo.get_open_orders()
            for record in pending:
                if record.status == "CREATED":
                    try:
                        order_repo.update_status(record.command_id, "FAILED")
                        cancelled += 1
                    except Exception as e:
                        self._log.error("cancel_system_entries: update failed for %s: %s",
                                        record.command_id, e)
        except Exception as e:
            self._log.error("cancel_system_entries failed: %s", e)
        if cancelled:
            self._log.info("cancel_system_entries: cancelled %d pending orders", cancelled)
        return cancelled

    # ── Status & control ──────────────────────────────────────────────────────

    def is_trading_allowed(self) -> tuple[bool, str]:
        with self._lock:
            self._maybe_roll_day()
            if self._daily_loss_hit:
                return False, f"Daily loss limit breached for {self.client_id}"
            if self._cooldown_until and datetime.now(timezone.utc) < self._cooldown_until:
                return False, f"In cooldown until {self._cooldown_until.isoformat()}"
            return True, ""

    def get_status(self) -> dict:
        with self._lock:
            allowed, halt_reason = self.is_trading_allowed()
            return AccountRiskStatus(
                account_id=self.config_id,
                broker_id=self.broker_id,
                client_id=self.client_id,
                daily_pnl=self._daily_pnl,
                dynamic_max_loss=self._dynamic_max_loss,
                highest_profit=self._highest_profit,
                daily_loss_hit=self._daily_loss_hit,
                warning_sent=self._warning_sent,
                cooldown_until=self._cooldown_until.isoformat() if self._cooldown_until else None,
                consecutive_loss_days=self._consecutive_loss_days,
                force_exit_triggered=self._force_exit_triggered,
                trading_allowed=allowed,
                halt_reason=halt_reason,
                checked_at=datetime.now(timezone.utc).isoformat(),
            ).to_dict()

    def acknowledge_exit_complete(self) -> None:
        """Call after positions are fully squared off to clear force-exit flag."""
        with self._lock:
            self._force_exit_triggered = False
            self._log.info("Force-exit acknowledged for client=%s", self.client_id)

    def reset_for_new_day(self) -> None:
        """Manually reset daily state. Normally called by _maybe_roll_day()."""
        with self._lock:
            self._do_roll_day()

    def update_config(self, new_config: AccountRiskConfig) -> None:
        """Hot-reload risk config without restart."""
        with self._lock:
            self._config = new_config
            self._dynamic_max_loss = new_config.max_daily_loss
            self._log.info(
                "Risk config updated — new max_loss=%s", new_config.max_daily_loss
            )

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _maybe_roll_day(self) -> None:
        today = date.today()
        if today != self._today:
            self._do_roll_day()
            self._today = today

    def _do_roll_day(self) -> None:
        if self._daily_pnl < 0:
            # Previous day was a loss — already counted in consecutive_loss_days
            pass
        else:
            self._consecutive_loss_days = 0

        self._log.info(
            "Day rolled — resetting daily state for client=%s (prev_pnl=%.0f)",
            self.client_id, self._daily_pnl,
        )
        self._daily_pnl          = 0.0
        self._highest_profit     = 0.0
        self._dynamic_max_loss   = self._config.max_daily_loss
        self._daily_loss_hit     = False
        self._warning_sent       = False
        self._cooldown_until     = None
        self._force_exit_triggered = False
        self._save_state()

    def _save_state(self) -> None:
        try:
            state = {
                "date":                  self._today.isoformat(),
                "daily_pnl":             self._daily_pnl,
                "highest_profit":        self._highest_profit,
                "dynamic_max_loss":      self._dynamic_max_loss,
                "daily_loss_hit":        self._daily_loss_hit,
                "warning_sent":          self._warning_sent,
                "cooldown_until":        self._cooldown_until.isoformat() if self._cooldown_until else None,
                "consecutive_loss_days": self._consecutive_loss_days,
                "force_exit_triggered":  self._force_exit_triggered,
            }
            self._state_file.write_text(json.dumps(state, indent=2))
        except Exception as e:
            self._log.warning("Could not save risk state: %s", e)

    def _load_state(self) -> None:
        try:
            if not self._state_file.exists():
                return
            state = json.loads(self._state_file.read_text())
            if state.get("date") != date.today().isoformat():
                self._log.info("State file is from yesterday — starting fresh")
                return
            self._daily_pnl          = float(state.get("daily_pnl", 0))
            self._highest_profit     = float(state.get("highest_profit", 0))
            self._dynamic_max_loss   = float(state.get("dynamic_max_loss", self._config.max_daily_loss))
            self._daily_loss_hit     = bool(state.get("daily_loss_hit", False))
            self._warning_sent       = bool(state.get("warning_sent", False))
            self._consecutive_loss_days = int(state.get("consecutive_loss_days", 0))
            self._force_exit_triggered  = bool(state.get("force_exit_triggered", False))
            cu = state.get("cooldown_until")
            if cu:
                self._cooldown_until = datetime.fromisoformat(cu)
            self._log.info("Loaded risk state from disk — pnl=%.0f loss_hit=%s",
                           self._daily_pnl, self._daily_loss_hit)
        except Exception as e:
            self._log.warning("Could not load risk state: %s", e)


# ── Module-level registry ─────────────────────────────────────────────────────
# Maps (user_id, config_id) → AccountRiskManager
_risk_registry: Dict[tuple, AccountRiskManager] = {}
_risk_lock = threading.Lock()


def get_account_risk(
    user_id:   str,
    config_id: str,
    broker_id: str = "unknown",
    client_id: str = "unknown",
    config:    Optional[AccountRiskConfig] = None,
) -> AccountRiskManager:
    """
    Return (or create) the risk manager for a specific (user, account) pair.
    thread-safe singleton per account.
    """
    key = (user_id, config_id)
    with _risk_lock:
        if key not in _risk_registry:
            _risk_registry[key] = AccountRiskManager(
                user_id   = user_id,
                config_id = config_id,
                broker_id = broker_id,
                client_id = client_id,
                config    = config or AccountRiskConfig(),
            )
    return _risk_registry[key]


def list_all_risk_statuses(user_id: str) -> list:
    """Return risk status for all accounts of a user."""
    result = []
    with _risk_lock:
        for (uid, cfg_id), rm in _risk_registry.items():
            if uid == user_id:
                result.append(rm.get_status())
    return result
