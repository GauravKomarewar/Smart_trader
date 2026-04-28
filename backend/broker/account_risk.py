"""
Smart Trader — Per-Account Risk Manager  (v3)
================================================

Design
------
Every broker account (user_id + config_id) gets its own independent
AccountRiskManager.  Risk params are loaded from the account's
*credentials dict* stored in broker_configs (the "ENV" for that account).

Risk param keys in credentials
-------------------------------
  DAILY_STOP_LOSS       absolute loss limit e.g.  5000  (stored as positive, internally negative)
  TRAIL_STOP_LOSS       trail the limit below peak profit e.g. 1000
  TRAIL_WHEN_PNL        start trailing once PnL >= this value  e.g. 2000
  WARNING_PCT           warn at this fraction of limit (default 0.80)
  MAX_ORDER_VALUE       single-order rupee cap     (default 500000)
  MAX_QTY_PER_ORDER     single-order qty cap       (default 1800)

State files
-----------
  logs/risk/state_{config_id[:8]}.json   — today's live risk state
  logs/risk/history_{config_id[:8]}.json — daily stats log (appended each night)

Consecutive-loss lockout
------------------------
  If 3 consecutive trading days hit max-loss, account is locked for 4
  calendar days.  Lockout is stored in the state file so it survives
  service restarts.

PnL calculation
---------------
  Realised + Unrealised across ALL open products including CNC/delivery.
  The heartbeat() method computes this from positions every 1-2 s.

Post-breach enforcement
------------------------
  After breach, every heartbeat re-checks if new positions exist on the
  broker (from direct terminal trades) and kills them immediately.
"""

import threading
import logging
import json
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Callable, Dict, List
from dataclasses import dataclass, field, asdict

logger = logging.getLogger("smart_trader.account_risk")

# ── How many consecutive loss days before lockout ─────────────────────────────
CONSEC_LOSS_DAYS_FOR_LOCKOUT = 3
LOCKOUT_CALENDAR_DAYS        = 4


# ── Risk Configuration ────────────────────────────────────────────────────────

@dataclass
class AccountRiskConfig:
    """
    Per-account risk parameters.
    Loaded from the account credentials dict so each broker account can have
    its own limits set at registration time via the Settings UI.
    """
    # P&L limits
    max_daily_loss:         float = -2500.0   # negative; absolute floor e.g. -5000
    trail_stop_loss:        float = 0.0       # trail by this amount below peak profit
    trail_when_pnl:         float = 0.0       # only start trailing once PnL >= this
    warning_threshold_pct:  float = 0.80      # warn at 80% of limit

    # Order-level guards
    max_order_value:        float = 500_000.0
    max_qty_per_order:      int   = 1800
    max_open_positions:     int   = 20
    allowed_exchanges:      tuple = ("NSE", "BSE", "NFO", "BFO", "MCX", "CDS")

    # Consecutive-loss lockout
    consec_loss_threshold:  int   = CONSEC_LOSS_DAYS_FOR_LOCKOUT
    lockout_days:           int   = LOCKOUT_CALENDAR_DAYS

    # State persistence dirs
    state_dir:   str = "logs/risk"
    history_dir: str = "logs/risk"

    def to_dict(self) -> dict:
        d = asdict(self)
        d["allowed_exchanges"] = list(d["allowed_exchanges"])
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "AccountRiskConfig":
        d2 = {k: v for k, v in d.items() if k in cls.__dataclass_fields__}
        if "allowed_exchanges" in d2:
            d2["allowed_exchanges"] = tuple(d2["allowed_exchanges"])
        return cls(**d2)

    @classmethod
    def from_credentials(cls, creds: dict, defaults: Optional["AccountRiskConfig"] = None) -> "AccountRiskConfig":
        """
        Build an AccountRiskConfig from a broker credentials dict.

        Keys read (case-insensitive):
          DAILY_STOP_LOSS, TRAIL_STOP_LOSS, TRAIL_WHEN_PNL,
          WARNING_PCT, MAX_ORDER_VALUE, MAX_QTY_PER_ORDER
        """
        base = defaults or cls()

        def _f(key: str, default: float) -> float:
            val = creds.get(key) or creds.get(key.lower())
            if val is None:
                return default
            try:
                return float(val)
            except (TypeError, ValueError):
                return default

        def _i(key: str, default: int) -> int:
            return int(_f(key, float(default)))

        max_loss = _f("DAILY_STOP_LOSS", abs(base.max_daily_loss))
        # Ensure negative internally
        if max_loss > 0:
            max_loss = -max_loss

        return cls(
            max_daily_loss        = max_loss,
            trail_stop_loss       = abs(_f("TRAIL_STOP_LOSS", base.trail_stop_loss)),
            trail_when_pnl        = _f("TRAIL_WHEN_PNL",    base.trail_when_pnl),
            warning_threshold_pct = _f("WARNING_PCT",        base.warning_threshold_pct),
            max_order_value       = _f("MAX_ORDER_VALUE",    base.max_order_value),
            max_qty_per_order     = _i("MAX_QTY_PER_ORDER",  base.max_qty_per_order),
            max_open_positions    = base.max_open_positions,
            allowed_exchanges     = base.allowed_exchanges,
            consec_loss_threshold = base.consec_loss_threshold,
            lockout_days          = base.lockout_days,
            state_dir             = base.state_dir,
            history_dir           = base.history_dir,
        )


# ── Risk Status ───────────────────────────────────────────────────────────────

@dataclass
class AccountRiskStatus:
    account_id:            str   = ""
    broker_id:             str   = ""
    client_id:             str   = ""
    daily_pnl:             float = 0.0
    dynamic_max_loss:      float = 0.0
    highest_profit:        float = 0.0
    daily_loss_hit:        bool  = False
    warning_sent:          bool  = False
    lockout_until:         Optional[str] = None
    consecutive_loss_days: int   = 0
    force_exit_triggered:  bool  = False
    trading_allowed:       bool  = True
    halt_reason:           str   = ""
    checked_at:            str   = ""

    def __post_init__(self):
        if not self.checked_at:
            self.checked_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict:
        return asdict(self)


# ── Account Risk Manager ──────────────────────────────────────────────────────

class AccountRiskManager:
    """
    Thread-safe, state-persistent risk manager for one broker account.

    Key design:
      - heartbeat(session) called every ~1-2 s by PositionWatcher
      - PnL = ALL positions realised + unrealised (including CNC/delivery)
      - Trail logic: once PnL >= trail_when_pnl, dynamic limit trails
        (peak_pnl - trail_stop_loss) but never below max_daily_loss floor
      - Breach: kills ALL open positions + orders, blocks new orders
      - Post-breach: every heartbeat re-checks for new positions and
        kills them immediately (catches direct broker terminal trades)
      - 3 consecutive breach days -> 4-day lockout
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

        # ── Live state ────────────────────────────────────────────────────
        self._daily_pnl:          float = 0.0
        self._highest_profit:     float = 0.0
        self._dynamic_max_loss:   float = self._config.max_daily_loss
        self._daily_loss_hit:     bool  = False
        self._warning_sent:       bool  = False
        self._breach_pnl:         Optional[float] = None
        self._breach_limit:       Optional[float] = None
        self._breach_at:          Optional[str] = None
        self._lockout_until:      Optional[datetime] = None
        self._consecutive_loss_days: int = 0
        self._force_exit_triggered:  bool = False
        self._today:              date = date.today()

        # ── Callbacks ─────────────────────────────────────────────────────
        self._force_exit_cb: Optional[Callable] = None
        self._warning_cb:    Optional[Callable] = None

        # Guard against stale empty-positions reads during close
        self._last_known_pnl: float = 0.0

        # ── Manual trade detection ────────────────────────────────────────
        # Track position signature to detect positions created outside
        # Smart Trader (direct broker terminal trades).
        self._last_position_signature: Optional[str] = None
        self._manual_trade_detected:   bool          = False
        self._manual_detect_cooldown:  float         = 0.0  # monotonic ts

        # ── Logging adapter ───────────────────────────────────────────────
        self._log = logging.LoggerAdapter(
            logging.getLogger("smart_trader.account_risk"),
            {"user_id": user_id[:8], "broker": broker_id, "client": client_id},
        )

        # ── File paths ────────────────────────────────────────────────────
        state_dir   = Path(self._config.state_dir)
        history_dir = Path(self._config.history_dir)
        state_dir.mkdir(parents=True, exist_ok=True)
        history_dir.mkdir(parents=True, exist_ok=True)

        _slug = config_id[:8]
        self._state_file   = state_dir   / f"state_{_slug}.json"
        self._history_file = history_dir / f"history_{_slug}.json"

        self._load_state()
        self._log.info(
            "Risk manager ready — max_loss=%.0f trail_stop=%.0f trail_when=%.0f",
            self._config.max_daily_loss,
            self._config.trail_stop_loss,
            self._config.trail_when_pnl,
        )

    # ── Callback registration ─────────────────────────────────────────────────

    def set_force_exit_callback(self, fn: Callable) -> None:
        self._force_exit_cb = fn

    def set_warning_callback(self, fn: Callable) -> None:
        self._warning_cb = fn

    # ── Pre-trade validation ──────────────────────────────────────────────────

    def validate_order(self, order: dict) -> tuple:
        """
        Pre-trade gate.  Returns (True, "") if allowed, else (False, reason).

        EXIT / COVER orders are always allowed so we can close positions
        even after a breach.
        """
        tag = (order.get("tag") or "").upper()
        txn = (order.get("transaction_type") or order.get("side") or "").upper()

        # Never block exit/cover orders
        is_exit = "EXIT" in tag or "RMS" in tag or txn in ("S", "SELL")

        with self._lock:
            self._maybe_roll_day()

            # Multi-day lockout check
            if self._lockout_until:
                now = datetime.now(timezone.utc)
                if now < self._lockout_until:
                    days_left = max(0, (self._lockout_until.date() - date.today()).days)
                    reason = (
                        f"Account {self.client_id} locked for {days_left} more day(s) "
                        f"after {self._config.consec_loss_threshold} consecutive loss days. "
                        f"Unlocks {self._lockout_until.date().isoformat()}."
                    )
                    self._log.warning("ORDER BLOCKED (lockout) — %s", reason)
                    return False, reason
                else:
                    # Lockout expired
                    self._lockout_until = None
                    self._consecutive_loss_days = 0
                    self._save_state()

            # Daily-loss hit blocks ALL new entries
            if self._daily_loss_hit and not is_exit:
                reason = f"{self._daily_halt_reason()} All new orders blocked for today."
                self._log.warning("ORDER BLOCKED — %s", reason)
                return False, reason

            if is_exit:
                return True, ""

            # Exchange guard
            exch = (order.get("exchange") or "NSE").upper()
            if self._config.allowed_exchanges and exch not in self._config.allowed_exchanges:
                reason = f"Exchange {exch} not permitted for {self.client_id}"
                return False, reason

            # Quantity guard
            qty = int(order.get("quantity", 0))
            if qty <= 0:
                return False, "Quantity must be > 0"
            if qty > self._config.max_qty_per_order:
                reason = (
                    f"Qty {qty} > max {self._config.max_qty_per_order} "
                    f"for {self.client_id}"
                )
                return False, reason

            # Order value guard
            price = float(order.get("price", 0) or order.get("ltp", 0))
            if price > 0:
                value = qty * price
                if value > self._config.max_order_value:
                    reason = (
                        f"Order value Rs.{value:,.0f} > max Rs.{self._config.max_order_value:,.0f} "
                        f"for {self.client_id}"
                    )
                    return False, reason

        return True, ""

    # ── Real-time PnL update ──────────────────────────────────────────────────

    def on_pnl_update(self, pnl: float) -> None:
        """
        Feed the latest combined (realised + unrealised) PnL.
        Called by heartbeat() every 1-2 s.  Thread-safe.
        ALL products are included — CNC, MIS, NRML, delivery.
        """
        with self._lock:
            self._maybe_roll_day()
            self._daily_pnl = pnl

            # ── Update trailing high-water mark ───────────────────────────
            if pnl > self._highest_profit:
                self._highest_profit = pnl

            # ── Compute dynamic loss limit ─────────────────────────────────
            # Trail logic:
            #   once PnL >= trail_when_pnl AND trail_stop_loss > 0,
            #   the limit rises to (peak - trail_stop_loss) but never
            #   below the configured max_daily_loss floor.
            if (
                self._config.trail_stop_loss > 0
                and self._config.trail_when_pnl > 0
                and self._highest_profit >= self._config.trail_when_pnl
            ):
                trail_limit = self._highest_profit - self._config.trail_stop_loss
                # Only raise the limit (never lower it); cap is 0 (don't lock in profit)
                self._dynamic_max_loss = max(
                    self._config.max_daily_loss,  # absolute floor (most negative)
                    min(trail_limit, 0.0),         # cap at break-even
                )
            # else: keep _dynamic_max_loss at config.max_daily_loss

            # ── Warning ───────────────────────────────────────────────────
            warning_level = self._dynamic_max_loss * self._config.warning_threshold_pct
            if pnl <= warning_level and not self._warning_sent:
                self._warning_sent = True
                self._log.warning(
                    "RISK WARNING — PnL Rs.%.0f approaching limit Rs.%.0f (client=%s)",
                    pnl, self._dynamic_max_loss, self.client_id,
                )
                if self._warning_cb:
                    try:
                        self._warning_cb(self.user_id, self.config_id, pnl, self._dynamic_max_loss)
                    except Exception as exc:
                        self._log.error("Warning callback error: %s", exc)

            # ── Breach ────────────────────────────────────────────────────
            if pnl <= self._dynamic_max_loss and not self._daily_loss_hit:
                self._daily_loss_hit       = True
                self._force_exit_triggered = True
                self._consecutive_loss_days += 1
                self._breach_pnl = pnl
                self._breach_limit = self._dynamic_max_loss
                self._breach_at = datetime.now(timezone.utc).isoformat()

                self._log.critical(
                    "RISK BREACH — PnL Rs.%.0f hit limit Rs.%.0f for client=%s "
                    "(consecutive_loss_days=%d)",
                    pnl, self._dynamic_max_loss, self.client_id,
                    self._consecutive_loss_days,
                )

                # Check for consecutive-loss lockout trigger
                self._maybe_apply_lockout()

                # Persist immediately
                self._save_state()
                self._append_history(pnl=pnl, breached=True)

                if self._force_exit_cb:
                    try:
                        self._force_exit_cb(
                            self.user_id,
                            self.config_id,
                            f"Daily loss limit breached: PnL=Rs.{pnl:.0f} "
                            f"limit=Rs.{self._dynamic_max_loss:.0f}",
                        )
                    except Exception as exc:
                        self._log.error("Force-exit callback error: %s", exc)

    # ── Heartbeat (called every ~1-2 s by PositionWatcher) ───────────────────

    def heartbeat(self, session) -> dict:
        """
        One monitoring cycle.

        1. Fetch ALL positions from broker
        2. Compute PnL = sum(realised + unrealised) for EVERY product (CNC included)
        3. Feed into on_pnl_update()
        4. If breached AND positions still exist -> kill them again immediately
           (catches orders placed directly on broker terminal)
        5. Returns status dict for WebSocket push
        """
        result = {
            "config_id":       self.config_id,
            "broker_id":       self.broker_id,
            "client_id":       self.client_id,
            "pnl":             0.0,
            "positions_count": 0,
            "breach":          False,
            "action":          None,
        }

        try:
            positions = session.get_positions() or []
        except Exception as exc:
            self._log.debug("heartbeat: get_positions failed: %s", exc)
            return result

        # Guard against stale empty-positions read mid-session close
        if not positions and self._last_known_pnl != 0.0:
            result["pnl"] = self._last_known_pnl
            return result

        # ── Detect manual / orphan trades ─────────────────────────────
        self._check_manual_trades(positions)

        # ── Compute total PnL — ALL products including CNC/delivery ───────
        pnl = 0.0
        open_count = 0
        for p in positions:
            if not isinstance(p, dict):
                continue
            # Include realised + unrealised for every product type
            rpnl   = float(p.get("realised_pnl")  or p.get("rpnl",   0) or 0)
            urmtom = float(p.get("unrealised_pnl") or p.get("urmtom", 0)
                           or p.get("pnl", 0) or 0)
            pnl += rpnl + urmtom
            net_qty = int(p.get("net_qty") or p.get("netqty") or p.get("qty", 0) or 0)
            if net_qty != 0:
                open_count += 1

        self._last_known_pnl       = pnl
        result["pnl"]              = round(pnl, 2)
        result["positions_count"]  = open_count

        # Feed into risk engine
        self.on_pnl_update(pnl)

        # ── Post-breach continuous enforcement ────────────────────────────
        with self._lock:
            locked_out = (
                self._lockout_until is not None
                and datetime.now(timezone.utc) < self._lockout_until
            )
            blocked = self._daily_loss_hit or locked_out

        if blocked and open_count > 0:
            self._log.critical(
                "POST-BREACH: %d open position(s) detected after breach "
                "(client=%s). Force-killing NOW.",
                open_count, self.client_id,
            )
            result["breach"] = True
            result["action"] = "POST_BREACH_FLATTEN"

            try:
                self.cancel_all_broker_orders(session)
            except Exception as exc:
                self._log.error("Post-breach cancel_all failed: %s", exc)

            if self._force_exit_cb:
                try:
                    self._force_exit_cb(
                        self.user_id,
                        self.config_id,
                        "POST_BREACH_FLATTEN: trades detected after risk breach",
                    )
                except Exception as exc:
                    self._log.error("Post-breach force_exit_cb: %s", exc)

        elif blocked and open_count == 0:
            # All flat — still cancel any stray open orders
            try:
                self.cancel_all_broker_orders(session)
            except Exception:
                pass

        return result

    # ── Broker order cancellation ─────────────────────────────────────────────

    def cancel_all_broker_orders(self, session) -> int:
        """Cancel every open/pending order on the broker. Returns count cancelled."""
        _OPEN_STATUSES = {
            "OPEN", "PENDING", "TRIGGER_PENDING", "TRIGGER PENDING",
            "NEW", "AFTER MARKET ORDER REQ RECEIVED", "MODIFIED",
            "WAITING", "PARTIALLY_FILLED", "PARTIALLY_EXECUTED",
        }
        cancelled = 0
        try:
            orders = session.get_order_book() or []
        except Exception as exc:
            self._log.error("cancel_all_broker_orders: get_order_book failed: %s", exc)
            return 0

        for order in orders:
            if isinstance(order, dict):
                status = (
                    order.get("status") or order.get("stat") or order.get("Status", "")
                ).upper().strip()
                order_id = (
                    order.get("order_id") or order.get("norenordno")
                    or order.get("orderid") or order.get("oms_order_id") or ""
                )
            else:
                status   = str(getattr(order, "status", "")).upper().strip()
                order_id = str(
                    getattr(order, "order_id", "") or getattr(order, "norenordno", "")
                )

            if not order_id or status not in _OPEN_STATUSES:
                continue

            try:
                session.cancel_order(order_id)
                cancelled += 1
                self._log.info("CANCELLED order %s (status=%s) client=%s",
                               order_id, status, self.client_id)
            except Exception as exc:
                self._log.error("Cancel order %s failed: %s", order_id, exc)

        if cancelled:
            self._log.info("cancel_all_broker_orders: cancelled %d orders on client=%s",
                           cancelled, self.client_id)
        return cancelled

    # ── System order cancellation (PostgreSQL DB repo) ────────────────────────

    def cancel_system_entries(self, order_repo) -> int:
        """Fail all CREATED-but-not-sent system orders in the DB order repo."""
        cancelled = 0
        try:
            for record in (order_repo.get_open_orders() or []):
                if record.status == "CREATED":
                    try:
                        order_repo.update_status(record.command_id, "FAILED")
                        cancelled += 1
                    except Exception as exc:
                        self._log.error("cancel_system_entries: %s", exc)
        except Exception as exc:
            self._log.error("cancel_system_entries: %s", exc)
        return cancelled

    # ── Status & control ──────────────────────────────────────────────────────

    def is_trading_allowed(self) -> tuple:
        """Returns (allowed: bool, reason: str).  Checks lockout first, then daily breach."""
        with self._lock:
            self._maybe_roll_day()

            # Multi-day lockout
            if self._lockout_until:
                now = datetime.now(timezone.utc)
                if now < self._lockout_until:
                    days_left = max(0, (self._lockout_until.date() - date.today()).days)
                    return (
                        False,
                        f"Account {self.client_id} locked for {days_left} more day(s) "
                        f"({self._config.consec_loss_threshold} consecutive loss days hit). "
                        f"Unlocks {self._lockout_until.date().isoformat()}.",
                    )
                else:
                    self._lockout_until = None
                    self._consecutive_loss_days = 0
                    self._save_state()

            # Today's breach
            if self._daily_loss_hit:
                return False, self._daily_halt_reason()
            return True, ""

    def get_status(self) -> dict:
        with self._lock:
            allowed, halt_reason = self.is_trading_allowed()
            status = AccountRiskStatus(
                account_id             = self.config_id,
                broker_id              = self.broker_id,
                client_id              = self.client_id,
                daily_pnl              = self._daily_pnl,
                dynamic_max_loss       = self._dynamic_max_loss,
                highest_profit         = self._highest_profit,
                daily_loss_hit         = self._daily_loss_hit,
                warning_sent           = self._warning_sent,
                lockout_until          = (
                    self._lockout_until.isoformat() if self._lockout_until else None
                ),
                consecutive_loss_days  = self._consecutive_loss_days,
                force_exit_triggered   = self._force_exit_triggered,
                trading_allowed        = allowed,
                halt_reason            = halt_reason,
                checked_at             = datetime.now(timezone.utc).isoformat(),
            )
            d = status.to_dict()
            d["manual_trade_detected"] = self._manual_trade_detected
            return d

    def get_config(self) -> dict:
        """Return the current risk config as a dict."""
        return self._config.to_dict()

    def get_history(self) -> list:
        """Return the per-day history list (last 60 trading days)."""
        try:
            if self._history_file.exists():
                return json.loads(self._history_file.read_text())
        except Exception:
            pass
        return []

    def acknowledge_exit_complete(self) -> None:
        with self._lock:
            self._force_exit_triggered = False

    def reset_for_new_day(self) -> None:
        with self._lock:
            self._do_roll_day()

    def force_unlock(self) -> None:
        """Admin override: clear lockout and consecutive counter."""
        with self._lock:
            self._lockout_until         = None
            self._consecutive_loss_days = 0
            self._daily_loss_hit        = False
            self._force_exit_triggered  = False
            self._save_state()
            self._log.warning("FORCE_UNLOCK: lockout cleared by admin for client=%s",
                              self.client_id)

    def update_config(self, new_config: AccountRiskConfig) -> None:
        """Hot-reload config without restart (e.g. from API PATCH)."""
        with self._lock:
            self._config = new_config
            # Recalculate dynamic limit only if trailing hasn't started
            if self._highest_profit < new_config.trail_when_pnl:
                self._dynamic_max_loss = new_config.max_daily_loss
            self._log.info(
                "Config hot-reloaded — max_loss=%.0f trail_stop=%.0f trail_when=%.0f",
                new_config.max_daily_loss,
                new_config.trail_stop_loss,
                new_config.trail_when_pnl,
            )

    # ── Manual trade detection ───────────────────────────────────────────────

    def _check_manual_trades(self, positions: list) -> None:
        """
        Detect positions created outside Smart Trader by tracking a position
        signature (sorted hash of symbol+qty+side).  If the signature changes
        without a corresponding DB order, flag as manual trade.

        Ported from shoonya_platform/risk/supreme_risk.py manual-trade detection.
        """
        import hashlib

        # Build deterministic signature from open positions
        sig_parts = []
        for p in positions:
            if not isinstance(p, dict):
                continue
            sym = p.get("symbol") or p.get("tsym") or ""
            qty = int(p.get("net_qty") or p.get("netqty") or p.get("qty", 0) or 0)
            if qty == 0:
                continue
            side = "B" if qty > 0 else "S"
            sig_parts.append(f"{sym}|{side}|{abs(qty)}")
        sig_parts.sort()
        current_sig = hashlib.sha256("|".join(sig_parts).encode()).hexdigest()[:16] if sig_parts else ""

        with self._lock:
            if self._last_position_signature is None:
                # First heartbeat — just record
                self._last_position_signature = current_sig
                return

            if current_sig == self._last_position_signature:
                return  # No change

            # Signature changed — check cooldown
            import time as _time
            now = _time.monotonic()
            if now - self._manual_detect_cooldown < 5.0:
                return
            self._manual_detect_cooldown = now

            # Check if the change came from Smart Trader DB orders
            # by comparing with recent order records
            try:
                from trading.persistence.repository import OrderRepository
                repo = OrderRepository(self.user_id, self.client_id)
                recent = repo.get_all(limit=20)
                # If there are recent SENT/EXECUTED orders in last 30s, it's our order
                for rec in recent:
                    if rec.status in ("SENT_TO_BROKER", "EXECUTED", "DISPATCHING"):
                        age = (datetime.now(timezone.utc) - rec.updated_at).total_seconds() \
                            if hasattr(rec, "updated_at") and rec.updated_at else 999
                        if age < 30:
                            # Likely our order, not manual
                            self._last_position_signature = current_sig
                            return
            except Exception:
                pass

            # Genuine manual trade detected
            self._manual_trade_detected = True
            self._log.warning(
                "MANUAL TRADE DETECTED — position signature changed for client=%s "
                "(old=%s new=%s). Positions may have been created outside Smart Trader.",
                self.client_id,
                self._last_position_signature[:8],
                current_sig[:8],
            )

            # If we're in a breached state, this is a violation
            if self._daily_loss_hit:
                self._log.critical(
                    "MANUAL TRADE VIOLATION — new positions after risk breach on client=%s",
                    self.client_id,
                )

            self._last_position_signature = current_sig
            self._save_state()

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _maybe_apply_lockout(self) -> None:
        """
        If consecutive loss days >= threshold, apply a calendar-day lockout.
        Called inside the mutex (already locked).
        """
        if self._consecutive_loss_days >= self._config.consec_loss_threshold:
            unlock_dt = datetime.now(timezone.utc) + timedelta(days=self._config.lockout_days)
            self._lockout_until = unlock_dt
            self._log.critical(
                "LOCKOUT APPLIED — %d consecutive loss days on client=%s. "
                "Trading BLOCKED until %s",
                self._consecutive_loss_days, self.client_id,
                unlock_dt.date().isoformat(),
            )

    def _maybe_roll_day(self) -> None:
        today = date.today()
        if today != self._today:
            self._do_roll_day()
            self._today = today

    def _do_roll_day(self) -> None:
        """
        End-of-day roll: save history entry, reset intraday counters.
        Locked by caller (_lock must be held or called with _lock).
        """
        prev_pnl = self._daily_pnl
        breached = self._daily_loss_hit

        # Persist final daily record
        self._append_history(pnl=prev_pnl, breached=breached)

        # Reset consecutive counter only when day was profitable
        if not breached and prev_pnl >= 0:
            self._consecutive_loss_days = 0

        self._log.info(
            "Day rolled — pnl=%.0f breached=%s consec=%d client=%s",
            prev_pnl, breached, self._consecutive_loss_days, self.client_id,
        )

        # Reset intraday state
        self._daily_pnl          = 0.0
        self._highest_profit     = 0.0
        self._dynamic_max_loss   = self._config.max_daily_loss
        self._daily_loss_hit     = False
        self._warning_sent       = False
        self._breach_pnl         = None
        self._breach_limit       = None
        self._breach_at          = None
        self._force_exit_triggered = False
        self._manual_trade_detected = False
        self._last_position_signature = None
        # NOTE: _lockout_until is intentionally NOT reset here
        self._save_state()

    def _daily_halt_reason(self) -> str:
        """Human-readable reason for current-day loss halt."""
        breach_pnl = self._breach_pnl if self._breach_pnl is not None else self._daily_pnl
        breach_limit = self._breach_limit if self._breach_limit is not None else self._dynamic_max_loss
        return (
            f"Daily loss limit breached for {self.client_id} "
            f"(breached at P&L Rs.{breach_pnl:.0f} vs limit Rs.{breach_limit:.0f}; "
            f"current P&L Rs.{self._daily_pnl:.0f})."
        )

    # ── Persistence ───────────────────────────────────────────────────────────

    def _save_state(self) -> None:
        """Persist today's live risk state to JSON (called under lock)."""
        try:
            state = {
                "date":                  self._today.isoformat(),
                "client_id":             self.client_id,
                "broker_id":             self.broker_id,
                "daily_pnl":             self._daily_pnl,
                "highest_profit":        self._highest_profit,
                "dynamic_max_loss":      self._dynamic_max_loss,
                "daily_loss_hit":        self._daily_loss_hit,
                "warning_sent":          self._warning_sent,
                "breach_pnl":            self._breach_pnl,
                "breach_limit":          self._breach_limit,
                "breach_at":             self._breach_at,
                "lockout_until":         (
                    self._lockout_until.isoformat() if self._lockout_until else None
                ),
                "consecutive_loss_days": self._consecutive_loss_days,
                "force_exit_triggered":  self._force_exit_triggered,
                "manual_trade_detected": self._manual_trade_detected,
                "position_signature":    self._last_position_signature,
                # Snapshot of active risk params for diagnostics
                "config": {
                    "max_daily_loss":    self._config.max_daily_loss,
                    "trail_stop_loss":   self._config.trail_stop_loss,
                    "trail_when_pnl":    self._config.trail_when_pnl,
                    "warning_pct":       self._config.warning_threshold_pct,
                },
            }
            self._state_file.write_text(json.dumps(state, indent=2))
        except Exception as exc:
            self._log.warning("Could not save risk state: %s", exc)

    def _load_state(self) -> None:
        """
        Restore persisted state on startup.
        Intraday stats only loaded if saved date == today.
        Consecutive-day counter and lockout always loaded (survive day roll).
        Falls back to old 'risk_*.json' naming for migration.
        """
        # Backward-compat: also check old 'risk_' prefix from v1/v2
        _slug = self.config_id[:8]
        old_file = Path(self._config.state_dir) / f"risk_{_slug}.json"
        state_file = self._state_file if self._state_file.exists() else (
            old_file if old_file.exists() else None
        )
        try:
            if not state_file:
                return
            state = json.loads(state_file.read_text())
            saved_date_str = state.get("date", "")

            # Always restore: consecutive count + lockout
            self._consecutive_loss_days = int(state.get("consecutive_loss_days", 0))

            # New key: lockout_until; old key was cooldown_until (ignored — no lockout in v1)
            lu = state.get("lockout_until")
            if lu:
                try:
                    self._lockout_until = datetime.fromisoformat(lu)
                except ValueError:
                    self._lockout_until = None

            # Intraday state: only restore if same calendar day
            if saved_date_str != date.today().isoformat():
                self._log.info(
                    "Stale state date=%s kept consec=%d lockout=%s",
                    saved_date_str, self._consecutive_loss_days, self._lockout_until,
                )
                return

            self._daily_pnl          = float(state.get("daily_pnl",        0))
            self._highest_profit     = float(state.get("highest_profit",    0))
            self._dynamic_max_loss   = float(state.get("dynamic_max_loss",  self._config.max_daily_loss))
            self._daily_loss_hit     = bool( state.get("daily_loss_hit",    False))
            self._warning_sent       = bool( state.get("warning_sent",      False))
            self._breach_pnl         = state.get("breach_pnl")
            self._breach_limit       = state.get("breach_limit")
            self._breach_at          = state.get("breach_at")
            self._force_exit_triggered = bool(state.get("force_exit_triggered", False))
            self._manual_trade_detected = bool(state.get("manual_trade_detected", False))
            self._last_position_signature = state.get("position_signature")

            self._log.info(
                "Restored state — pnl=%.0f loss_hit=%s consec=%d lockout=%s",
                self._daily_pnl, self._daily_loss_hit,
                self._consecutive_loss_days, self._lockout_until,
            )
        except Exception as exc:
            self._log.warning("Could not load risk state: %s", exc)

    def _append_history(self, pnl: float, breached: bool) -> None:
        """Append or update today's record in the daily history JSON file."""
        try:
            history: list = []
            if self._history_file.exists():
                try:
                    history = json.loads(self._history_file.read_text())
                except Exception:
                    history = []

            today_str = date.today().isoformat()
            record = {
                "date":                  today_str,
                "client_id":             self.client_id,
                "broker_id":             self.broker_id,
                "final_pnl":             round(pnl, 2),
                "daily_loss_hit":        breached,
                "highest_profit":        round(self._highest_profit, 2),
                "dynamic_max_loss_used": round(self._dynamic_max_loss, 2),
                "max_daily_loss_config": round(self._config.max_daily_loss, 2),
                "consecutive_loss_days": self._consecutive_loss_days,
                "lockout_until":         (
                    self._lockout_until.isoformat() if self._lockout_until else None
                ),
                "recorded_at":           datetime.now(timezone.utc).isoformat(),
            }

            # Replace if same date already exists (update-in-place)
            history = [h for h in history if h.get("date") != today_str]
            history.append(record)
            # Retain last 60 trading days
            history = history[-60:]

            self._history_file.write_text(json.dumps(history, indent=2))
        except Exception as exc:
            self._log.warning("Could not append risk history: %s", exc)


# ── Module-level registry ─────────────────────────────────────────────────────

_risk_registry: Dict[tuple, AccountRiskManager] = {}
_risk_lock = threading.Lock()


def get_account_risk(
    user_id:   str,
    config_id: str,
    broker_id: str = "unknown",
    client_id: str = "unknown",
    config:    Optional[AccountRiskConfig] = None,
) -> AccountRiskManager:
    """Singleton factory — one AccountRiskManager per (user_id, config_id)."""
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
        elif config is not None:
            # Update existing manager with new config if one is supplied
            _risk_registry[key].update_config(config)
    return _risk_registry[key]


def list_all_risk_statuses(user_id: str) -> list:
    """Return risk status dicts for all accounts belonging to user_id."""
    result = []
    with _risk_lock:
        for (uid, _), rm in _risk_registry.items():
            if uid == user_id:
                result.append(rm.get_status())
    return result
