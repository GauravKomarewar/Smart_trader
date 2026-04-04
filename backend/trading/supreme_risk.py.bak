"""
Smart Trader — Supreme Risk Manager
=====================================

AUTHORITY: Decides WHEN risk is breached.
           NEVER decides HOW to exit.

Responsibilities:
  - Daily P&L monitoring
  - Max-loss enforcement (absolute + trailing)
  - Consecutive loss day tracking
  - Cooldown management
  - Real-time risk status broadcasting
  - Force-exit triggering via callback

Integration:
  risk = SupremeRiskManager(config=risk_config)
  risk.on_pnl_update(pnl=current_pnl)   # call on every P&L tick
  risk.set_force_exit_callback(fn)       # fn() called when limits breached
"""

import threading
import logging
import json
import os
from datetime import date, datetime, timezone
from collections import deque
from typing import Optional, Callable, Dict, Any
from dataclasses import dataclass, field, asdict

logger = logging.getLogger("smart_trader.supreme_risk")


@dataclass
class RiskConfig:
    """Risk parameters — matches primary.env fields."""
    base_max_loss:           float = -2500.0    # RISK_BASE_MAX_LOSS (must be negative)
    trail_step:              float = 100.0      # RISK_TRAIL_STEP
    warning_threshold_pct:   float = 0.80       # RISK_WARNING_THRESHOLD
    max_consecutive_loss_days: int = 3          # RISK_MAX_CONSECUTIVE_LOSS_DAYS
    status_update_interval_min: int = 10        # RISK_STATUS_UPDATE_MIN
    state_file:              str = "logs/risk_state.json"

    @classmethod
    def from_env(cls) -> "RiskConfig":
        return cls(
            base_max_loss=float(os.getenv("RISK_BASE_MAX_LOSS", "-2500")),
            trail_step=float(os.getenv("RISK_TRAIL_STEP", "100")),
            warning_threshold_pct=float(os.getenv("RISK_WARNING_THRESHOLD", "0.80")),
            max_consecutive_loss_days=int(os.getenv("RISK_MAX_CONSECUTIVE_LOSS_DAYS", "3")),
            status_update_interval_min=int(os.getenv("RISK_STATUS_UPDATE_MIN", "10")),
            state_file=os.getenv("RISK_STATE_FILE", "logs/risk_state.json"),
        )


@dataclass
class RiskStatus:
    daily_pnl:         float = 0.0
    dynamic_max_loss:  float = 0.0
    highest_profit:    float = 0.0
    daily_loss_hit:    bool  = False
    warning_sent:      bool  = False
    cooldown_until:    Optional[str] = None
    consecutive_loss_days: int = 0
    force_exit_in_progress: bool = False
    checked_at:        str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict:
        return asdict(self)


class SupremeRiskManager:
    """
    Supreme Risk Manager — thread-safe, state-persistent.

    Attach a force_exit_callback to receive notifications when
    risk limits are breached.
    """

    def __init__(self, config: Optional[RiskConfig] = None, client_id: str = "default"):
        self._lock = threading.RLock()
        self.cfg = config or RiskConfig.from_env()
        self.client_id = client_id

        # State file per client
        base, ext = os.path.splitext(self.cfg.state_file)
        self._state_file = f"{base}_{client_id}{ext}"
        os.makedirs(os.path.dirname(self._state_file), exist_ok=True)

        # Runtime
        self.current_day: date = date.today()
        self.daily_pnl:   float = 0.0
        self.dynamic_max_loss: float = self.cfg.base_max_loss
        self.highest_profit:   float = 0.0
        self.daily_loss_hit:   bool = False
        self.warning_sent:     bool = False
        self.force_exit_in_progress: bool = False
        self.cooldown_until:   Optional[date] = None
        self._consecutive_loss_days: deque = deque(maxlen=self.cfg.max_consecutive_loss_days)

        # Callbacks
        self._force_exit_cb: Optional[Callable] = None
        self._on_warning_cb: Optional[Callable] = None

        self._load_state()
        logger.info("SupremeRiskManager init | client=%s | max_loss=%.0f | trail=%.0f",
                    client_id, self.cfg.base_max_loss, self.cfg.trail_step)

    # ── Configuration ─────────────────────────────────────────────────────────

    def set_force_exit_callback(self, fn: Callable):
        """Called with (reason: str) when max-loss is hit."""
        self._force_exit_cb = fn

    def set_warning_callback(self, fn: Callable):
        """Called with (pnl, threshold) when warning threshold approached."""
        self._on_warning_cb = fn

    # ── P&L intake ────────────────────────────────────────────────────────────

    def on_pnl_update(self, pnl: float):
        """
        Main entry point. Call every time P&L changes.
        Triggers exit callbacks when limits are breached.
        """
        with self._lock:
            self._maybe_roll_day()
            self.daily_pnl = pnl

            if self.daily_loss_hit or self.force_exit_in_progress:
                return

            # Check cooldown
            if self.cooldown_until and date.today() < self.cooldown_until:
                logger.warning("COOLDOWN ACTIVE until %s — no new trades", self.cooldown_until)
                return

            # Update trailing max loss
            if pnl > self.highest_profit:
                self.highest_profit = pnl
                new_trail = self.highest_profit - self.cfg.trail_step
                if new_trail > self.dynamic_max_loss:
                    self.dynamic_max_loss = new_trail
                    logger.info("Trail updated: max_loss=%.0f", self.dynamic_max_loss)

            # Warning threshold
            if not self.warning_sent:
                threshold = self.dynamic_max_loss * self.cfg.warning_threshold_pct
                if pnl <= threshold:
                    self.warning_sent = True
                    logger.warning("RISK WARNING: PnL=%.2f | threshold=%.2f", pnl, threshold)
                    if self._on_warning_cb:
                        try:
                            self._on_warning_cb(pnl, threshold)
                        except Exception as e:
                            logger.error("Warning callback error: %s", e)

            # Max-loss breach
            if pnl <= self.dynamic_max_loss:
                self.daily_loss_hit = True
                self.force_exit_in_progress = True
                self._save_state()
                logger.critical(
                    "MAX LOSS HIT | PnL=%.2f | max_loss=%.2f | triggering FORCE EXIT",
                    pnl, self.dynamic_max_loss,
                )
                if self._force_exit_cb:
                    try:
                        self._force_exit_cb(
                            f"Max loss hit: PnL={pnl:.2f} ≤ {self.dynamic_max_loss:.2f}"
                        )
                    except Exception as e:
                        logger.error("Force-exit callback error: %s", e)

            self._save_state()

    def is_trading_allowed(self) -> tuple[bool, str]:
        """Returns (allowed, reason). Check before placing any order."""
        with self._lock:
            if self.force_exit_in_progress:
                return False, "Force exit in progress — risk limit breached"
            if self.daily_loss_hit:
                return False, f"Daily loss limit reached (max_loss={self.dynamic_max_loss:.0f})"
            if self.cooldown_until and date.today() < self.cooldown_until:
                return False, f"Cooldown active until {self.cooldown_until}"
            return True, "OK"

    def acknowledge_exit_complete(self):
        """Call when force-exit is fully done. Resets the in-progress flag."""
        with self._lock:
            self.force_exit_in_progress = False
            logger.info("Force exit acknowledged — risk manager reset")

    def end_of_day(self):
        """Called at EOD to persist daily result and update consecutive loss tracking."""
        with self._lock:
            if self.daily_pnl < 0:
                self._consecutive_loss_days.append(date.today().isoformat())
                logger.warning("EOD: Loss day recorded | streak=%d", len(self._consecutive_loss_days))

            if len(self._consecutive_loss_days) >= self.cfg.max_consecutive_loss_days:
                # All tracked days are loss days — activate cooldown
                cooldown_days = len(self._consecutive_loss_days)
                self.cooldown_until = date.today().replace(
                    day=date.today().day + cooldown_days
                )
                logger.critical(
                    "CONSECUTIVE LOSS LIMIT: %d days — cooldown until %s",
                    cooldown_days, self.cooldown_until,
                )

            self._save_state()

    def get_status(self) -> RiskStatus:
        with self._lock:
            return RiskStatus(
                daily_pnl=self.daily_pnl,
                dynamic_max_loss=self.dynamic_max_loss,
                highest_profit=self.highest_profit,
                daily_loss_hit=self.daily_loss_hit,
                warning_sent=self.warning_sent,
                cooldown_until=self.cooldown_until.isoformat() if self.cooldown_until else None,
                consecutive_loss_days=len(self._consecutive_loss_days),
                force_exit_in_progress=self.force_exit_in_progress,
            )

    def reset_for_new_day(self):
        """Reset daily counters. Called at market open."""
        with self._lock:
            self.current_day = date.today()
            self.daily_pnl = 0.0
            self.dynamic_max_loss = self.cfg.base_max_loss
            self.highest_profit = 0.0
            self.daily_loss_hit = False
            self.warning_sent = False
            self.force_exit_in_progress = False
            self._save_state()
            logger.info("SupremeRiskManager: new day reset for %s", self.current_day)

    # ── State persistence ─────────────────────────────────────────────────────

    def _maybe_roll_day(self):
        if date.today() != self.current_day:
            self.end_of_day()
            self.reset_for_new_day()

    def _save_state(self):
        try:
            state = {
                "date": self.current_day.isoformat(),
                "daily_pnl": self.daily_pnl,
                "dynamic_max_loss": self.dynamic_max_loss,
                "highest_profit": self.highest_profit,
                "daily_loss_hit": self.daily_loss_hit,
                "warning_sent": self.warning_sent,
                "force_exit_in_progress": self.force_exit_in_progress,
                "cooldown_until": self.cooldown_until.isoformat() if self.cooldown_until else None,
                "consecutive_loss_days": list(self._consecutive_loss_days),
            }
            with open(self._state_file, "w") as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            logger.error("Risk state save failed: %s", e)

    def _load_state(self):
        if not os.path.exists(self._state_file):
            return
        try:
            with open(self._state_file) as f:
                state = json.load(f)
            stored_date = date.fromisoformat(state.get("date", date.today().isoformat()))
            if stored_date == date.today():
                self.daily_pnl = state.get("daily_pnl", 0.0)
                self.dynamic_max_loss = state.get("dynamic_max_loss", self.cfg.base_max_loss)
                self.highest_profit = state.get("highest_profit", 0.0)
                self.daily_loss_hit = state.get("daily_loss_hit", False)
                self.warning_sent = state.get("warning_sent", False)
                self.force_exit_in_progress = state.get("force_exit_in_progress", False)
                cd = state.get("cooldown_until")
                self.cooldown_until = date.fromisoformat(cd) if cd else None
                for d in state.get("consecutive_loss_days", []):
                    self._consecutive_loss_days.append(d)
                logger.info("Risk state loaded from %s", self._state_file)
        except Exception as e:
            logger.warning("Could not load risk state: %s", e)


# ── Risk Router ───────────────────────────────────────────────────────────────

from fastapi import APIRouter, Depends
from core.deps import current_user

risk_router = APIRouter(prefix="/risk", tags=["risk"])
_risk_instances: Dict[str, SupremeRiskManager] = {}
_risk_lock = threading.Lock()


def get_risk(user_id: str) -> SupremeRiskManager:
    with _risk_lock:
        if user_id not in _risk_instances:
            cfg = RiskConfig.from_env()
            _risk_instances[user_id] = SupremeRiskManager(config=cfg, client_id=user_id[:8])
        return _risk_instances[user_id]


@risk_router.get("/status")
def risk_status(payload: dict = Depends(current_user)):
    rm = get_risk(payload["sub"])
    return rm.get_status().to_dict()


@risk_router.post("/reset-day")
def risk_reset_day(payload: dict = Depends(current_user)):
    rm = get_risk(payload["sub"])
    rm.reset_for_new_day()
    return {"success": True, "message": "Daily risk state reset"}
