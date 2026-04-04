from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
from .state import StrategyState
from .condition_engine import ConditionEngine
from .models import Condition, Comparator, JoinOperator

logger = logging.getLogger(__name__)

class ExitEngine:
    def __init__(self, state: StrategyState):
        self.state = state
        self.condition_engine = ConditionEngine(state)
        self.exit_config = {}
        self.last_triggered_leg_rule: Optional[Dict[str, Any]] = None
        self.last_exit_reason: str = ""

    def load_config(self, exit_config: Dict[str, Any]):
        self.exit_config = exit_config

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            s = value.strip()
            if not s:
                return None
            try:
                return float(s)
            except ValueError:
                return None
        return None

    def check_exits(self, current_time: datetime) -> Optional[str]:
        self.last_triggered_leg_rule = None
        self.last_exit_reason = ""
        # 1. Profit target
        action = self._check_profit_target()
        if action:
            return action

        # 2. Stop loss
        action = self._check_stop_loss()
        if action:
            return action

        # 3. Trailing stop
        action = self._check_trailing_stop()
        if action:
            return action

        # 4. Profit steps
        action = self._check_profit_steps()
        if action:
            return action

        # 5. Greek risk limits
        action = self._check_greek_limits()
        if action:
            return action

        # 6. Time-based exit
        action = self._check_time_exit(current_time)
        if action:
            return action

        # 7. Combined conditions (custom)
        combined = self.exit_config.get("combined_conditions", {})
        if combined.get("operator") == "OR":
            rules = combined.get("rules", [])
            if rules:
                cond_objs = [self._dict_to_condition(c) for c in rules]
                # ✅ BUG FIX: Use any() to OR individual conditions, matching
                # how AND uses all(). Previous code passed all conds to evaluate()
                # which chained with per-condition join fields (defaulting to AND).
                if any(self.condition_engine.evaluate([c]) for c in cond_objs):
                    self.last_exit_reason = "combined_conditions_or"
                    return "combined_conditions"
        elif combined.get("operator") == "AND":
            # ✅ BUG-011 FIX: AND operator was silently ignored — all conditions must hold
            rules = combined.get("rules", [])
            if rules:
                cond_objs = [self._dict_to_condition(c) for c in rules]
                if all(self.condition_engine.evaluate([c]) for c in cond_objs):
                    self.last_exit_reason = "combined_conditions_and"
                    return "combined_conditions"

        # 8. Per-leg exit rules
        leg_rules = self.exit_config.get("leg_rules", [])
        for rule in leg_rules:
            if self._check_leg_rule(rule):
                self.last_triggered_leg_rule = rule
                self.last_exit_reason = f"leg_rule:{rule.get('name') or rule.get('action')}"
                return f"leg_rule_{rule.get('action')}"

        return None

    def _check_profit_target(self) -> Optional[str]:
        cfg = self.exit_config.get("profit_target", {})
        amount = self._to_float(cfg.get("amount"))
        pct = self._to_float(cfg.get("pct"))
        action = cfg.get("action", "exit_all")
        if amount is not None and self.state.combined_pnl >= amount:
            self.last_exit_reason = f"profit_target_amount:{amount}"
            if action in ("trail", "lock_trail"):
                return f"profit_target_{action}"
            return action
        # BUG-A4 FIX: Use combined_pnl_pct (which guards against zero division)
        # instead of raw division by total_premium.
        if pct is not None and self.state.combined_pnl_pct >= pct:
            self.last_exit_reason = f"profit_target_pct:{pct}"
            if action in ("trail", "lock_trail"):
                return f"profit_target_{action}"
            return action
        return None

    def _check_stop_loss(self) -> Optional[str]:
        # ✅ BUG-010 GUARD: never fire stop-loss when no positions are open.
        # combined_pnl == 0 with a positive amount threshold would always pass
        # the <= -amount test if amount is mis-signed, and even with a correctly
        # signed amount a zero-position strategy should never exit on stop-loss.
        if not self.state.any_leg_active:
            return None
        cfg = self.exit_config.get("stop_loss", {})
        amount = self._to_float(cfg.get("amount"))
        pct = self._to_float(cfg.get("pct"))
        # ✅ BUG-010 FIX: `if amount` is False when amount=0. Use `is not None` so
        # a zero stop-loss (lock breakeven) correctly triggers.
        if amount is not None and self.state.combined_pnl <= -amount:
            self.last_exit_reason = f"stop_loss_amount:{amount}"
            return cfg.get("action", "exit_all")
        # BUG-A4 FIX: Use combined_pnl_pct instead of raw division by total_premium.
        if pct is not None and self.state.combined_pnl_pct <= -pct:
            self.last_exit_reason = f"stop_loss_pct:{pct}"
            return cfg.get("action", "exit_all")
        return None

    def _check_trailing_stop(self) -> Optional[str]:
        cfg = self.exit_config.get("trailing", {})
        trail_amt = self._to_float(cfg.get("trail_amount"))
        trail_pct = self._to_float(cfg.get("trail_pct"))
        lock_in = self._to_float(cfg.get("lock_in_at"))
        lock_in_pct = self._to_float(cfg.get("lock_in_at_pct"))
        step_trigger = self._to_float(cfg.get("step_trigger"))

        # ✅ ISSUE-020 FIX: Support percentage-based trailing stop.
        # Convert trail_pct to absolute amount using total_premium as base.
        if trail_amt is None and trail_pct is not None:
            base = abs(self.state.total_premium) or abs(self.state.total_cost_basis)
            if base > 0:
                trail_amt = base * trail_pct / 100.0
            else:
                trail_amt = None

        if lock_in is None and lock_in_pct is not None:
            base = abs(self.state.total_premium) or abs(self.state.total_cost_basis)
            if base > 0:
                lock_in = base * lock_in_pct / 100.0

        if trail_amt is None:
            return None

        current_pnl = self.state.combined_pnl
        if current_pnl > self.state.peak_pnl:
            self.state.peak_pnl = current_pnl

        # BUG-C2 FIX: Use 'is not None' so lock_in_at=0 (breakeven) works
        if not self.state.trailing_stop_active and lock_in is not None and current_pnl >= lock_in:
            self.state.trailing_stop_active = True
            self.state.trailing_stop_level = current_pnl - trail_amt

        if self.state.trailing_stop_active:
            # BUG-C2 FIX: Continuous trailing when step_trigger is not configured
            if step_trigger:
                # Step-based trailing: only ratchet up when gap exceeds step_trigger
                if (self.state.peak_pnl - self.state.trailing_stop_level) >= step_trigger:
                    self.state.trailing_stop_level = self.state.peak_pnl - trail_amt
            else:
                # Continuous trailing: always ratchet the stop level up with peak PnL
                new_level = self.state.peak_pnl - trail_amt
                if new_level > self.state.trailing_stop_level:
                    self.state.trailing_stop_level = new_level

            if current_pnl <= self.state.trailing_stop_level:
                self.last_exit_reason = (
                    f"trailing_stop_hit:level={self.state.trailing_stop_level:.2f},pnl={current_pnl:.2f}"
                )
                return "exit_all"

        return None

    def _check_profit_steps(self) -> Optional[str]:
        cfg = self.exit_config.get("profit_steps", {})
        step_size = self._to_float(cfg.get("step_size"))
        max_steps = self._to_float(cfg.get("max_steps"))
        action = cfg.get("action", "adj")
        if step_size is None or step_size <= 0:
            return None

        current_pnl = self.state.combined_pnl
        step = int(current_pnl // step_size)
        if max_steps is not None and step > int(max_steps):
            step = int(max_steps)
        # BUG-H2 FIX: Fire only the NEXT step to avoid skipping intermediate steps.
        # Each step typically triggers a partial hedge/reduce action.
        next_step = self.state.current_profit_step + 1
        if step >= next_step:
            self.state.current_profit_step = next_step
            self.last_exit_reason = f"profit_step:{next_step}"
            return f"profit_step_{action}"
        return None

    def _check_greek_limits(self) -> Optional[str]:
        """Enforce Greek‑based risk limits from exit.risk section."""
        cfg = self.exit_config.get("risk", {})
        # Max |net delta|
        max_delta = self._to_float(cfg.get("max_delta"))
        if max_delta is not None and abs(self.state.net_delta) > max_delta:
            self.last_exit_reason = f"max_delta_exceeded:{abs(self.state.net_delta)}>{max_delta}"
            # For now we always exit all; could later use delta_breach_action.
            return "exit_all"

        # Max IV / Min IV
        max_iv = self._to_float(cfg.get("max_iv"))
        min_iv = self._to_float(cfg.get("min_iv"))
        atm_iv = self.state.atm_iv
        if max_iv is not None and atm_iv > max_iv:
            self.last_exit_reason = f"max_iv_exceeded:{atm_iv}>{max_iv}"
            return "exit_all"
        if min_iv is not None and atm_iv < min_iv:
            self.last_exit_reason = f"min_iv_breached:{atm_iv}<{min_iv}"
            return "exit_all"

        # Breakeven buffer
        buffer = self._to_float(cfg.get("breakeven_buffer"))
        if buffer is not None and self.state.breakeven_distance <= buffer:
            self.last_exit_reason = f"breakeven_buffer_hit:{self.state.breakeven_distance}<={buffer}"
            return "exit_all"

        return None

    def _check_time_exit(self, current_time: Optional[datetime]) -> Optional[str]:
        # ✅ BUG-010 FIX: Guard against None current_time
        if current_time is None:
            current_time = datetime.now()

        # ✅ BUG-TIMEEXIT FIX: Never fire time-exit when no legs are active.
        # Without this guard, a redundant EXIT_TRIGGERED fires at 15:28 on every
        # day that already had an early SL/PT exit.  In live mode this would send
        # a second close-all order against an already-flat account.
        if not self.state.any_leg_active:
            return None

        exit_time_str = self.exit_config.get("time", {}).get("strategy_exit_time")
        if exit_time_str:
            try:
                exit_t = datetime.strptime(exit_time_str, "%H:%M").time()
                if current_time.time() >= exit_t:
                    self.last_exit_reason = f"time_exit:{exit_time_str}"
                    return "exit_all"
            except ValueError:
                logger.error("Invalid exit time format: %s", exit_time_str)
        return None

    def _check_leg_rule(self, rule: Dict[str, Any]) -> bool:
        ref = rule.get("exit_leg_ref")
        group = rule.get("group")
        applicable_legs = []
        if ref == "all":
            applicable_legs = [leg for leg in self.state.legs.values() if leg.is_active]
        elif ref == "group" and group:
            applicable_legs = [leg for leg in self.state.legs.values() if leg.is_active and leg.group == group]
        elif ref in self.state.legs:
            leg = self.state.legs[ref]
            if leg.is_active:
                applicable_legs = [leg]

        conds = rule.get("conditions", [])
        if not conds:
            return False
        cond_objs = [self._dict_to_condition(c) for c in conds]
        # If any applicable leg, evaluate conditions (they may refer to specific leg via tag)
        for leg in applicable_legs:
            # Condition engine already resolves tag references using state
            if self.condition_engine.evaluate(cond_objs):
                return True
        return False

    def _dict_to_condition(self, d: Dict[str, Any]) -> Condition:
        return Condition(
            parameter=d["parameter"],
            comparator=Comparator(d["comparator"]),
            value=d.get("value"),
            value2=d.get("value2"),
            join=JoinOperator(d["join"]) if d.get("join") else None
        )