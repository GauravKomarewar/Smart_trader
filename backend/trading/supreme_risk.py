"""
Smart Trader — SupremeRiskManager (v2.0)
=========================================

Full feature port from shoonya_platform/risk/supreme_risk.py

Features:
  - 2-second heartbeat with live PnL polling
  - Trailing max-loss (locks in profits)
  - Consecutive loss day tracking + cooldown
  - Warning threshold at configurable % of limit
  - PnL OHLC bars (1m, 5m, 1d)
  - JSON state persistence (per user/client)
  - Telegram alerts via bot.send_telegram()

EXIT LAW:
  Risk -> bot.request_exit() -> CommandService.handle_exit_intent()
    -> PositionExitService -> OrderWatcher -> Broker
"""

import os
import json
import time
import logging
import threading
from collections import deque, OrderedDict
from datetime import date, datetime, timedelta
from typing import Dict, Optional, Tuple

logger = logging.getLogger("smart_trader.supreme_risk")


class SupremeRiskManager:
    """Supreme Risk Manager — makes exit DECISIONS only."""

    _ANY_MARKET_OPEN  = datetime.strptime("09:00", "%H:%M").time()
    _ANY_MARKET_CLOSE = datetime.strptime("23:30", "%H:%M").time()

    @classmethod
    def _is_any_market_active(cls) -> bool:
        now = datetime.now().time()
        return cls._ANY_MARKET_OPEN <= now <= cls._ANY_MARKET_CLOSE

    @property
    def PNL_RETENTION(self):
        return {"1m": timedelta(hours=4), "5m": timedelta(days=2), "1d": timedelta(days=30)}

    def __init__(self, bot):
        self.bot = bot
        self._lock = threading.RLock()

        self.BASE_MAX_LOSS             = float(os.getenv("RISK_BASE_MAX_LOSS", "-2500"))
        self.TRAIL_STEP                = float(os.getenv("RISK_TRAIL_STEP", "100"))
        self.WARNING_THRESHOLD_PCT     = float(os.getenv("RISK_WARNING_THRESHOLD", "0.80"))
        self.MAX_CONSECUTIVE_LOSS_DAYS = int(os.getenv("RISK_MAX_CONSECUTIVE_LOSS_DAYS", "3"))
        self.STATUS_UPDATE_INTERVAL    = int(os.getenv("RISK_STATUS_UPDATE_MIN", "10"))

        self.current_day:     date  = date.today()
        self.daily_pnl:       float = 0.0
        self.dynamic_max_loss: float = self.BASE_MAX_LOSS
        self.highest_profit:  float = 0.0
        self.daily_loss_hit:  bool  = False
        self.warning_sent:    bool  = False
        self.force_exit_in_progress: bool = False
        self.cooldown_until:  Optional[date] = None
        self.failed_days:     deque = deque(maxlen=self.MAX_CONSECUTIVE_LOSS_DAYS)
        self.last_known_pnl:  Optional[float] = None
        self.last_status_update: Optional[datetime] = None
        self.human_violation_detected: bool = False

        self.pnl_ohlc: Dict = {"1m": OrderedDict(), "5m": OrderedDict(), "1d": OrderedDict()}

        self._load_state()
        self._hb_running = False
        self._hb_thread: Optional[threading.Thread] = None

        logger.info("SupremeRiskManager v2.0 | client=%s | max_loss=%.0f",
                    bot.client_id, self.BASE_MAX_LOSS)

    # -- Heartbeat ------------------------------------------------------------

    def start_heartbeat(self, interval: float = 2.0):
        if self._hb_running:
            return
        self._hb_running = True
        self._hb_thread = threading.Thread(
            target=self._heartbeat_loop, args=(interval,), daemon=True,
            name=f"RMS-HB:{self.bot.client_id}")
        self._hb_thread.start()
        logger.info("RMS heartbeat started (%.1fs) | client=%s", interval, self.bot.client_id)

    def stop_heartbeat(self):
        self._hb_running = False

    def _heartbeat_loop(self, interval: float):
        while self._hb_running:
            try:
                self.heartbeat()
            except RuntimeError:
                raise
            except Exception:
                logger.exception("RMS heartbeat error")
            time.sleep(interval)

    def heartbeat(self):
        with self._lock:
            try:
                today = date.today()
                if today != self.current_day:
                    self._reset_daily_state(today)

                positions = self._update_pnl()
                has_live = any(int(p.get("netqty", 0)) != 0 for p in positions)

                if positions:
                    if has_live and not self.daily_loss_hit and self.daily_pnl <= self.dynamic_max_loss:
                        self._handle_daily_loss_breach()
                    if self.daily_loss_hit:
                        all_flat = all(int(p.get("netqty", 0)) == 0 for p in positions)
                        if all_flat and self.force_exit_in_progress:
                            self.force_exit_in_progress = False
                            self._cancel_pending_broker_orders()

                if self._is_any_market_active():
                    self._update_trailing_max_loss()
                    self._check_warning_threshold()

                self.track_pnl_ohlc()
                self._save_state()
                self._send_periodic_status()
            except RuntimeError:
                raise
            except Exception:
                logger.exception("RMS heartbeat exception")

    # -- Entry gating ---------------------------------------------------------

    def can_execute(self) -> bool:
        with self._lock:
            today = date.today()
            if today != self.current_day:
                self._reset_daily_state(today)
            if self.cooldown_until and today < self.cooldown_until:
                return False
            if self.force_exit_in_progress or self.daily_loss_hit:
                return False
            return True

    def can_execute_command(self, command) -> Tuple[bool, str]:
        with self._lock:
            if self.force_exit_in_progress:
                return False, "FORCE_EXIT_IN_PROGRESS"
            if self.daily_loss_hit:
                return False, "DAILY_MAX_LOSS_HIT"
            return True, "OK"

    def is_trading_allowed(self) -> Tuple[bool, str]:
        allowed = self.can_execute()
        if not allowed:
            if self.cooldown_until:
                return False, f"Cooldown until {self.cooldown_until}"
            if self.daily_loss_hit:
                return False, "Daily loss limit reached"
            if self.force_exit_in_progress:
                return False, "Force exit in progress"
        return True, "OK"

    # -- PnL update -----------------------------------------------------------

    def _update_pnl(self) -> list:
        try:
            positions = self.bot.api.get_positions() or []
        except Exception as e:
            logger.warning("RMS: get_positions() failed: %s", e)
            return []

        if not positions and self.last_known_pnl not in (None, 0.0):
            return []

        total_rpnl = total_urmtom = 0.0
        live_count = 0
        for p in positions:
            product = (p.get("prd") or "").upper()
            if product in ("CNC", "C"):
                continue
            total_rpnl   += float(p.get("rpnl", 0) or 0)
            total_urmtom += float(p.get("urmtom", 0) or 0)
            if int(p.get("netqty", 0) or 0) != 0:
                live_count += 1

        total = total_rpnl + total_urmtom
        if not self._is_any_market_active() and live_count == 0 and total != 0.0:
            return positions

        self.daily_pnl = total
        self.last_known_pnl = total
        return positions

    # -- Loss breach ----------------------------------------------------------

    def _handle_daily_loss_breach(self):
        if self.daily_loss_hit:
            return
        self.daily_loss_hit = True
        self.force_exit_in_progress = True
        self._save_state()
        self.failed_days.append(self.current_day)

        logger.critical("DAILY MAX LOSS HIT | pnl=%.2f | max_loss=%.2f",
                        self.daily_pnl, self.dynamic_max_loss)

        self._notify(
            f"DAILY MAX LOSS BREACH\n"
            f"PnL: ₹{self.daily_pnl:.2f} | Max: ₹{self.dynamic_max_loss:.2f}\n"
            f"FORCING EXIT ALL POSITIONS", "system")

        self._route_global_exit("RMS_DAILY_MAX_LOSS")
        self._cancel_pending_broker_orders()

        days_list = list(self.failed_days)
        is_consec = True
        if len(days_list) >= 2:
            for i in range(len(days_list) - 1):
                if self._trading_days_between(days_list[i], days_list[i+1]) != 1:
                    is_consec = False
                    break
        if is_consec and len(days_list) >= self.MAX_CONSECUTIVE_LOSS_DAYS:
            self._activate_cooldown()

    def _route_global_exit(self, reason: str):
        try:
            self.bot.request_exit(scope="ALL", symbols=None,
                                  product_type="ALL", reason=reason, source="RISK")
        except Exception:
            logger.exception("_route_global_exit failed")

    # -- Trailing max loss / warning ------------------------------------------

    def _update_trailing_max_loss(self):
        if self.daily_pnl <= self.highest_profit:
            return
        self.highest_profit = self.daily_pnl
        steps = int(self.highest_profit // self.TRAIL_STEP)
        new_loss = self.BASE_MAX_LOSS + steps * self.TRAIL_STEP
        if new_loss > self.dynamic_max_loss:
            self.dynamic_max_loss = new_loss
            self.warning_sent = False
            logger.info("TRAILING STOP UPDATED | hp=%.2f | max_loss=%.2f",
                        self.highest_profit, self.dynamic_max_loss)
            self._notify(
                f"Trailing Stop Updated\n"
                f"Highest Profit: ₹{self.highest_profit:.2f}\n"
                f"New Max Loss: ₹{self.dynamic_max_loss:.2f}", "reports")

    def _check_warning_threshold(self):
        if self.warning_sent or self.daily_loss_hit:
            return
        margin = self.daily_pnl - self.dynamic_max_loss
        warn_dist = abs(self.BASE_MAX_LOSS) * (1.0 - self.WARNING_THRESHOLD_PCT)
        if margin <= warn_dist:
            self.warning_sent = True
            logger.warning("RISK WARNING | pnl=%.2f | max_loss=%.2f",
                           self.daily_pnl, self.dynamic_max_loss)
            self._notify(
                f"RISK WARNING\n"
                f"PnL: ₹{self.daily_pnl:.2f} | Max Loss: ₹{self.dynamic_max_loss:.2f}\n"
                f"Distance: ₹{margin:.2f}", "system")

    # -- PnL OHLC -------------------------------------------------------------

    def track_pnl_ohlc(self):
        now = datetime.now()
        try:
            self._upd_ohlc("1m", now.replace(second=0, microsecond=0), self.daily_pnl)
            self._upd_ohlc("5m", now.replace(minute=(now.minute//5)*5, second=0, microsecond=0), self.daily_pnl)
            self._upd_ohlc("1d", now.date(), self.daily_pnl)
            self._prune_ohlc()
        except Exception:
            pass

    def _upd_ohlc(self, tf: str, key, pnl: float):
        store = self.pnl_ohlc[tf]
        if key not in store:
            store[key] = {"timestamp": key, "open": pnl, "high": pnl, "low": pnl, "close": pnl}
        else:
            c = store[key]
            c["high"]  = max(c["high"], pnl)
            c["low"]   = min(c["low"],  pnl)
            c["close"] = pnl

    def _prune_ohlc(self):
        now = datetime.now()
        for tf, store in self.pnl_ohlc.items():
            cutoff = now - self.PNL_RETENTION[tf]
            for k in list(store.keys()):
                ts = store[k]["timestamp"]
                if not isinstance(ts, datetime):
                    ts = datetime.combine(ts, datetime.min.time()) if hasattr(ts, "year") else now
                if ts < cutoff:
                    del store[k]

    def get_pnl_ohlc(self, tf: str = "1m") -> list:
        store = self.pnl_ohlc.get(tf, {})
        return [{"t": str(v["timestamp"]), "o": v["open"], "h": v["high"],
                 "l": v["low"], "c": v["close"]} for v in store.values()]

    # -- Broker cleanup -------------------------------------------------------

    def _cancel_pending_broker_orders(self):
        try:
            for o in (self.bot.api.get_order_book() or []):
                st = (o.get("status") or "").upper()
                if st in ("OPEN", "PENDING", "TRIGGER_PENDING"):
                    oid = o.get("norenordno") or o.get("orderid")
                    if oid:
                        try:
                            self.bot.api.cancel_order(oid)
                        except Exception:
                            pass
        except Exception:
            logger.exception("RMS: cancel_pending_broker_orders failed")

    # -- Notification helper --------------------------------------------------

    def _notify(self, msg: str, category: str = "system"):
        try:
            if getattr(self.bot, "telegram_enabled", False):
                self.bot.send_telegram(msg, category=category)
        except Exception:
            pass

    # -- State persistence ----------------------------------------------------

    def _state_file_path(self) -> str:
        log_dir = os.getenv("LOG_DIR", "logs")
        client  = (self.bot.client_id or "default").replace("/", "_")
        uid     = str(getattr(self.bot, "user_id", "0"))
        os.makedirs(log_dir, exist_ok=True)
        return os.path.join(log_dir, f"risk_state_{uid}_{client}.json")

    def _load_state(self):
        path = self._state_file_path()
        if not os.path.exists(path):
            return
        try:
            with open(path) as f:
                data = json.load(f)
            if data.get("date") == str(self.current_day):
                self.dynamic_max_loss       = data.get("dynamic_max_loss", self.BASE_MAX_LOSS)
                self.highest_profit         = data.get("highest_profit", 0.0)
                self.daily_loss_hit         = data.get("daily_loss_hit", False)
                self.warning_sent           = data.get("warning_sent", False)
                self.force_exit_in_progress = data.get("force_exit_in_progress", False)
                for d in data.get("failed_days", []):
                    try: self.failed_days.append(date.fromisoformat(d))
                    except Exception: pass
                if data.get("cooldown_until"):
                    try: self.cooldown_until = date.fromisoformat(data["cooldown_until"])
                    except Exception: pass
        except Exception:
            logger.exception("RMS: _load_state failed")

    def _save_state(self):
        try:
            with open(self._state_file_path(), "w") as f:
                json.dump({
                    "date": str(self.current_day), "daily_pnl": self.daily_pnl,
                    "dynamic_max_loss": self.dynamic_max_loss,
                    "highest_profit": self.highest_profit,
                    "daily_loss_hit": self.daily_loss_hit,
                    "warning_sent": self.warning_sent,
                    "force_exit_in_progress": self.force_exit_in_progress,
                    "failed_days": [str(d) for d in self.failed_days],
                    "cooldown_until": str(self.cooldown_until) if self.cooldown_until else None,
                }, f)
        except Exception:
            pass

    # -- Day management -------------------------------------------------------

    def _reset_daily_state(self, new_day: date):
        logger.info("RMS DAILY RESET | %s -> %s | pnl=%.2f",
                    self.current_day, new_day, self.daily_pnl)
        self.current_day = new_day
        self.daily_pnl = 0.0
        self.daily_loss_hit = False
        self.warning_sent = False
        self.force_exit_in_progress = False
        self.dynamic_max_loss = self.BASE_MAX_LOSS
        self.highest_profit = 0.0
        self.pnl_ohlc["1m"].clear()
        self.pnl_ohlc["5m"].clear()
        self._save_state()
        self._notify(
            f"New Trading Day: {new_day.strftime('%d %b %Y')}\n"
            f"Max Loss: ₹{self.BASE_MAX_LOSS:.0f} | RMS Ready", "system")

    def _activate_cooldown(self):
        today = date.today()
        days_to_monday = (7 - today.weekday()) % 7 or 7
        self.cooldown_until = today + timedelta(days=days_to_monday)
        logger.critical("COOLDOWN ACTIVATED until=%s", self.cooldown_until)
        self._notify(f"COOLDOWN ACTIVATED\n"
                     f"Until: {self.cooldown_until.strftime('%d %b %Y')}", "system")

    # -- Periodic status ------------------------------------------------------

    def _send_periodic_status(self):
        now = datetime.now()
        if (self.last_status_update and
                (now - self.last_status_update).total_seconds() < self.STATUS_UPDATE_INTERVAL * 60):
            return
        self.last_status_update = now
        icon = "🟢" if self.daily_pnl > 0 else "🔴"
        self._notify(
            f"RMS STATUS | {icon} PnL: ₹{self.daily_pnl:.2f}\n"
            f"Max Loss: ₹{self.dynamic_max_loss:.2f} | High: ₹{self.highest_profit:.2f}", "reports")

    # -- Status dict for API --------------------------------------------------

    def get_status(self) -> dict:
        with self._lock:
            return {
                "date":                   str(self.current_day),
                "daily_pnl":              self.daily_pnl,
                "dynamic_max_loss":       self.dynamic_max_loss,
                "base_max_loss":          self.BASE_MAX_LOSS,
                "highest_profit":         self.highest_profit,
                "daily_loss_hit":         self.daily_loss_hit,
                "warning_sent":           self.warning_sent,
                "force_exit_in_progress": self.force_exit_in_progress,
                "cooldown_until":         str(self.cooldown_until) if self.cooldown_until else None,
                "pnl_ohlc_1m":            self.get_pnl_ohlc("1m"),
                "pnl_ohlc_5m":            self.get_pnl_ohlc("5m"),
            }

    # -- Legacy shims ---------------------------------------------------------

    def on_pnl_update(self, pnl: float):
        with self._lock:
            self.daily_pnl = pnl
            self.last_known_pnl = pnl

    def acknowledge_exit_complete(self):
        with self._lock:
            self.force_exit_in_progress = False

    # -- Helpers --------------------------------------------------------------

    @staticmethod
    def _trading_days_between(d1: date, d2: date) -> int:
        if d1 >= d2:
            return 0
        delta = (d2 - d1).days
        weeks, rem = divmod(delta, 7)
        days = weeks * 5
        for off in range(1, rem + 1):
            if (d1 + timedelta(days=off)).weekday() < 5:
                days += 1
        return days


# -- Singleton accessor -------------------------------------------------------

_risk_instances: Dict[str, SupremeRiskManager] = {}
_risk_lock = threading.Lock()


def get_risk(user_id: str, bot=None) -> Optional[SupremeRiskManager]:
    with _risk_lock:
        key = str(user_id)
        if bot is not None and key not in _risk_instances:
            _risk_instances[key] = SupremeRiskManager(bot)
        return _risk_instances.get(key)
