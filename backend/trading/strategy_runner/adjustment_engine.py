from typing import List, Dict, Any, Optional
from datetime import datetime, date
import logging
from .state import StrategyState, LegState
from .condition_engine import ConditionEngine
from .market_reader import MarketReader
from .models import (
    Condition, Comparator, JoinOperator, StrikeConfig,
    InstrumentType, OptionType, Side, OrderType, StrikeMode
)

logger = logging.getLogger(__name__)

class AdjustmentEngine:
    def __init__(self, state: StrategyState, market: MarketReader):
        self.state = state
        self.market = market
        self.condition_engine = ConditionEngine(state)
        self.rules_config = []

        self._rule_last_fired: Dict[str, datetime] = {}
        self._rule_last_skip_log: Dict[str, datetime] = {}
        self._last_guard_reason: str = "ok"
        # ✅ BUG-RETRIGGER FIX: track rules that must not retrigger (retrigger=false)
        # keyed by rule_name → date the rule last fired (for day-scoped blocking).
        self._rule_no_retrigger_fired: Dict[str, date] = {}
        # Tracking: last rule that successfully fired (used by monitor snapshot for reason column)
        self._last_triggered_rule_name: str = ""

    def load_rules(self, rules_config: List[Dict[str, Any]]):
        self.rules_config = sorted(rules_config, key=lambda r: r.get("priority", 999))

    def check_and_apply(self, current_time: datetime) -> List[str]:
        actions_taken = []
        for rule in self.rules_config:
            rule_name = str(rule.get("name") or str(id(rule)))
            guards_ok = self._check_guards(rule, current_time)
            if not guards_ok:
                guard_reason = self._last_guard_reason or "guard_blocked"
                self._log_rule_skip(rule_name, f"GUARD_BLOCKED:{guard_reason}", current_time)
                continue

            if_conds = rule.get("conditions", [])
            cond_objs = [self._dict_to_condition(c) for c in if_conds]
            if self.condition_engine.evaluate(cond_objs):
                action = rule["action"]
                # ✅ BUG-COUNTER FIX: snapshot leg keys before execute to detect NOOP
                legs_before = {t: (l.is_active, l.strike, l.option_type, l.expiry, l.qty)
                               for t, l in self.state.legs.items()}
                self._execute_action(action, "if", rule)
                legs_after = {t: (l.is_active, l.strike, l.option_type, l.expiry, l.qty)
                              for t, l in self.state.legs.items()}
                if legs_before == legs_after:
                    # Action was a complete NOOP — consume NO budget
                    logger.info(
                        "ADJUSTMENT_NOOP | rule=%s | branch=IF | leg_state_unchanged — counter NOT incremented",
                        rule_name,
                    )
                    continue
                actions_taken.append(f"Rule {rule.get('name')}: IF triggered")
                self._rule_last_fired[rule_name] = current_time
                self._last_triggered_rule_name = rule_name
                self.state.last_adjustment_time = current_time
                self.state.adjustments_today += 1
                self.state.lifetime_adjustments += 1
                # ✅ BUG-RETRIGGER FIX: mark no-retrigger rules as permanently fired today
                retrigger = bool(rule.get("retrigger", rule.get("retriger", True)))
                if not retrigger:
                    self._rule_no_retrigger_fired[rule_name] = current_time.date()
                # ✅ BUG-002 FIX: Record adjustment event for audit trail
                self.state.record_adjustment(
                    rule_name=rule_name,
                    action_type=action.get("type", "unknown"),
                    affected_legs=[action.get("close_tag", ""), action.get("target_leg", "")],
                    reason="IF conditions met",
                )
                logger.info(
                    "ADJUSTMENT_TRIGGERED | rule=%s | branch=IF | action=%s | adjustments_today=%s",
                    rule_name,
                    action.get("type"),
                    self.state.adjustments_today,
                )

            elif rule.get("else_enabled"):
                else_conds = rule.get("else_conditions", [])
                else_cond_objs = [self._dict_to_condition(c) for c in else_conds]
                if self.condition_engine.evaluate(else_cond_objs):
                    else_action = rule["else_action"]
                    # ✅ BUG-COUNTER FIX: snapshot leg keys before execute to detect NOOP
                    legs_before = {t: (l.is_active, l.strike, l.option_type, l.expiry, l.qty)
                                   for t, l in self.state.legs.items()}
                    self._execute_action(else_action, "else", rule)
                    legs_after = {t: (l.is_active, l.strike, l.option_type, l.expiry, l.qty)
                                  for t, l in self.state.legs.items()}
                    if legs_before == legs_after:
                        logger.info(
                            "ADJUSTMENT_NOOP | rule=%s | branch=ELSE | leg_state_unchanged — counter NOT incremented",
                            rule_name,
                        )
                        continue
                    actions_taken.append(f"Rule {rule.get('name')}: ELSE triggered")
                    self._rule_last_fired[rule_name] = current_time
                    self._last_triggered_rule_name = rule_name
                    self.state.last_adjustment_time = current_time
                    self.state.adjustments_today += 1
                    self.state.lifetime_adjustments += 1
                    retrigger = bool(rule.get("retrigger", rule.get("retriger", True)))
                    if not retrigger:
                        self._rule_no_retrigger_fired[rule_name] = current_time.date()
                    # ✅ BUG-002 FIX: Record adjustment event for audit trail
                    self.state.record_adjustment(
                        rule_name=rule_name,
                        action_type=else_action.get("type", "unknown"),
                        affected_legs=[else_action.get("close_tag", ""), else_action.get("target_leg", "")],
                        reason="ELSE conditions met",
                    )
                    logger.info(
                        "ADJUSTMENT_TRIGGERED | rule=%s | branch=ELSE | action=%s | adjustments_today=%s",
                        rule_name,
                        else_action.get("type"),
                        self.state.adjustments_today,
                    )
                else:
                    self._log_rule_skip(
                        rule_name,
                        "ELSE_CONDITIONS_FALSE",
                        current_time,
                        if_conditions=cond_objs,
                        else_conditions=else_cond_objs,
                    )
            else:
                self._log_rule_skip(
                    rule_name,
                    "IF_CONDITIONS_FALSE",
                    current_time,
                    if_conditions=cond_objs,
                )

        return actions_taken

    def _check_guards(self, rule: Dict[str, Any], current_time: Optional[datetime] = None) -> bool:
        cooldown = rule.get("cooldown_sec", 0)
        rule_name = rule.get("name") or str(id(rule))
        now = current_time or datetime.now()
        self._last_guard_reason = "ok"

        # ✅ BUG-RETRIGGER FIX: honour retrigger=false (also accepts legacy typo 'retriger').
        # Default is True (same behaviour as before this fix for all existing rules
        # that don't set the field).  When retrigger=false the rule may fire at most
        # once per calendar day — it is blocked for the rest of that day after its
        # first successful execution.
        retrigger = bool(rule.get("retrigger", rule.get("retriger", True)))
        if not retrigger and rule_name in self._rule_no_retrigger_fired:
            fired_date = self._rule_no_retrigger_fired[rule_name]
            if fired_date == now.date():
                self._last_guard_reason = "retrigger_disabled"
                return False

        # ✅ Per‑rule cooldown (only)
        if cooldown > 0 and rule_name in self._rule_last_fired:
            if (now - self._rule_last_fired[rule_name]).total_seconds() < cooldown:
                self._last_guard_reason = "cooldown"
                return False

        max_day = rule.get("max_per_day")
        # BUG-M3 FIX: Use 'is not None' so max_per_day=0 (no adjustments) is respected.
        if max_day is not None and self.state.adjustments_today >= max_day:
            self._last_guard_reason = "max_per_day"
            return False

        max_total = rule.get("max_total")
        # BUG-M3 FIX: Use 'is not None' so max_total=0 is respected.
        if max_total is not None and self.state.lifetime_adjustments >= max_total:
            self._last_guard_reason = "max_total"
            return False

        leg_guard = rule.get("leg_guard")
        if leg_guard:
            leg = self.state.legs.get(leg_guard)
            if not leg or not leg.is_active:
                self._last_guard_reason = f"leg_guard_inactive:{leg_guard}"
                return False

        return True

    def _describe_conditions(self, conditions: List[Condition]) -> str:
        parts: List[str] = []
        for idx, cond in enumerate(conditions):
            try:
                actual = self.condition_engine._resolve_parameter(cond.parameter)
                passed = self.condition_engine._evaluate_single(cond)
                join_txt = "" if idx == 0 or cond.join is None else f" {cond.join.value} "
                parts.append(
                    f"{join_txt}{cond.parameter}={actual} {cond.comparator.value} {cond.value}"
                    f"{' .. ' + str(cond.value2) if cond.value2 is not None else ''} => {passed}"
                )
            except Exception as exc:
                parts.append(f"{cond.parameter}=<error:{exc}>")
        return "".join(parts) if parts else "<no-conditions>"

    def _log_rule_skip(
        self,
        rule_name: str,
        reason: str,
        now: datetime,
        if_conditions: Optional[List[Condition]] = None,
        else_conditions: Optional[List[Condition]] = None,
    ) -> None:
        last = self._rule_last_skip_log.get(rule_name)
        if last and (now - last).total_seconds() < 60:
            return
        self._rule_last_skip_log[rule_name] = now
        if_debug = self._describe_conditions(if_conditions or []) if if_conditions is not None else ""
        else_debug = self._describe_conditions(else_conditions or []) if else_conditions is not None else ""
        if if_debug or else_debug:
            logger.info(
                "ADJUSTMENT_SKIPPED | rule=%s | reason=%s | if=%s | else=%s",
                rule_name,
                reason,
                if_debug or "-",
                else_debug or "-",
            )
            return
        logger.info("ADJUSTMENT_SKIPPED | rule=%s | reason=%s", rule_name, reason)

    def _execute_action(self, action_cfg: Dict[str, Any], branch: str, rule: Dict[str, Any]):
        action_type = action_cfg["type"]

        if action_type == "close_leg":
            close_tag = self._resolve_close_tag(action_cfg.get("close_tag"))
            if close_tag and close_tag in self.state.legs:
                self.state.legs[close_tag].close()

        elif action_type == "partial_close_lots":
            close_tag = self._resolve_close_tag(action_cfg.get("close_tag"))
            lots_to_close = int(action_cfg.get("lots", 1))

            if lots_to_close <= 0:
                logger.error(
                    "ADJUSTMENT_ERROR | partial_close_lots | "
                    "invalid lots_to_close=%s", lots_to_close,
                )
                return

            if not close_tag or close_tag not in self.state.legs:
                logger.error(
                    "ADJUSTMENT_ERROR | partial_close_lots | "
                    "leg %s not found in state", close_tag,
                )
                return

            leg = self.state.legs[close_tag]

            if not leg.is_active:
                logger.warning(
                    "ADJUSTMENT_SKIP | partial_close_lots | "
                    "leg %s is already inactive", close_tag,
                )
                return

            # Validate lots_to_close <= current qty
            if lots_to_close > leg.qty:
                logger.warning(
                    "ADJUSTMENT_MODIFIED | partial_close_lots | "
                    "requested %s lots but %s only has %s lots "
                    "- closing all instead",
                    lots_to_close, close_tag, leg.qty,
                )
                lots_to_close = leg.qty

            original_qty = leg.qty
            leg.qty -= lots_to_close

            logger.info(
                "ADJUSTMENT_EXECUTED | partial_close_lots | "
                "%s | qty: %s -> %s (closed %s lots)",
                close_tag, original_qty, leg.qty, lots_to_close,
            )

            if leg.qty <= 0:
                leg.close()
                logger.info("ADJUSTMENT_EXECUTED | %s fully closed", close_tag)

        elif action_type == "reduce_by_pct":
            close_tag = self._resolve_close_tag(action_cfg.get("close_tag"))
            pct = float(action_cfg.get("reduce_pct", 50)) / 100.0

            if pct <= 0 or pct > 1.0:
                logger.error(
                    "ADJUSTMENT_ERROR | reduce_by_pct | "
                    "invalid reduce_pct=%.1f%% (must be 0-100)",
                    pct * 100,
                )
                return

            if not close_tag or close_tag not in self.state.legs:
                logger.error(
                    "ADJUSTMENT_ERROR | reduce_by_pct | "
                    "leg %s not found in state", close_tag,
                )
                return

            leg = self.state.legs[close_tag]

            if not leg.is_active:
                logger.warning(
                    "ADJUSTMENT_SKIP | reduce_by_pct | "
                    "leg %s is already inactive", close_tag,
                )
                return

            # Use round() instead of int() to avoid truncation bias
            lots_to_reduce = round(leg.qty * pct)
            # If pct > 0 but rounds to 0, reduce at least 1 lot
            if lots_to_reduce == 0 and pct > 0:
                lots_to_reduce = 1

            original_qty = leg.qty
            new_qty = max(0, leg.qty - lots_to_reduce)

            logger.info(
                "ADJUSTMENT_EXECUTED | reduce_by_pct | "
                "%s | %.1f%% = %s lots | qty: %s -> %s",
                close_tag, pct * 100, lots_to_reduce, original_qty, new_qty,
            )

            leg.qty = new_qty
            if leg.qty <= 0:
                leg.close()
                logger.info("ADJUSTMENT_EXECUTED | %s fully closed", close_tag)

        elif action_type == "open_hedge":
            new_leg_cfg = action_cfg.get("new_leg", {})
            self._open_new_leg(new_leg_cfg)

        elif action_type == "roll_to_next_expiry":
            leg_tag = self._resolve_close_tag(action_cfg.get("leg"))
            target_expiry = action_cfg.get("target_expiry", "weekly_next")
            same_strike = action_cfg.get("same_strike", "yes")
            if leg_tag not in self.state.legs:
                logger.error(f"Roll target leg {leg_tag} not found")
                return
            old_leg = self.state.legs[leg_tag]

            # Ensure the leg is an option (rolling futures not supported)
            if old_leg.instrument != InstrumentType.OPT:
                raise ValueError(f"Roll action can only be applied to option legs, not {old_leg.instrument}")

            # At this point, old_leg.option_type is guaranteed to be not None
            assert old_leg.option_type is not None, "Option leg must have option_type"

            new_expiry = self._resolve_next_expiry(target_expiry, old_leg.expiry)

            # Guard: if next-expiry resolution returned the current expiry (only one
            # SQLite file available), abort the roll instead of silently closing the leg.
            if new_expiry == old_leg.expiry:
                raise ValueError(
                    f"Roll aborted: could not find a next expiry for {old_leg.symbol} "
                    f"after {old_leg.expiry}. Ensure the next-expiry option chain DB file "
                    f"has been fetched."
                )

            # Determine new strike and option data
            opt_data: Optional[Dict[str, Any]] = None
            strike: float = 0.0

            if same_strike == "yes":
                if old_leg.strike is None:
                    raise ValueError(f"Leg {leg_tag} has no strike, cannot roll with same_strike=yes")
                strike = old_leg.strike
                opt_data = self.market.get_option_at_strike(
                    strike, old_leg.option_type, expiry=new_expiry
                )
            elif same_strike == "atm":
                new_atm = self.market.get_atm_strike(new_expiry)
                # ✅ BUG-006 FIX: Round ATM to nearest valid strike step in new expiry.
                # Different expiries can have different strike steps (weekly=50, monthly=100).
                step = self.market._get_strike_step(new_expiry)
                strike = round(new_atm / step) * step
                opt_data = self.market.get_option_at_strike(
                    strike, old_leg.option_type, expiry=new_expiry
                )
            elif same_strike == "delta":
                opt_data = self.market.find_option_by_delta(
                    old_leg.option_type, abs(old_leg.delta), expiry=new_expiry
                )
                if opt_data is not None:
                    strike = opt_data["strike"]
            else:
                raise ValueError(f"Unknown same_strike option: {same_strike}")

            if opt_data is None:
                raise ValueError(f"Could not resolve option for roll: {leg_tag} -> {new_expiry}")

            if opt_data.get("ltp") is None or opt_data["ltp"] <= 0:
                raise ValueError(
                    f"Roll aborted: option {strike} {new_expiry} has no valid LTP "
                    f"(market may not be open yet or chain data not refreshed)"
                )

            # ✅ BUG-015 FIX: Tag collision on multiple rolls — use numeric counter
            base_tag = leg_tag.split("_ROLLED")[0]
            roll_num = 1
            candidate = f"{base_tag}_ROLLED_{roll_num}"
            while candidate in self.state.legs:
                roll_num += 1
                candidate = f"{base_tag}_ROLLED_{roll_num}"
            new_tag = candidate

            # Create new leg first (still pending)
            new_leg = LegState(
                tag=new_tag,
                symbol=old_leg.symbol,
                instrument=old_leg.instrument,
                option_type=old_leg.option_type,
                strike=strike,
                expiry=new_expiry,
                side=old_leg.side,
                qty=old_leg.qty,
                entry_price=opt_data["ltp"],
                ltp=opt_data["ltp"],
                delta=opt_data.get("delta", 0.0),
                gamma=opt_data.get("gamma", 0.0),
                theta=opt_data.get("theta", 0.0),
                vega=opt_data.get("vega", 0.0),
                iv=opt_data.get("iv", 0.0),
                trading_symbol=(
                    opt_data.get("trading_symbol")
                    or opt_data.get("tsym")
                    or opt_data.get("symbol")
                    or old_leg.trading_symbol
                ),
            )
            # ✅ BUG-015 FIX: Carry over lot_size from old leg being rolled
            new_leg.lot_size = old_leg.lot_size
            new_leg.is_active = False
            new_leg.order_status = "PENDING"
            new_leg.order_placed_at = datetime.now()

            # Add new leg to state
            self.state.legs[new_tag] = new_leg

            # **Now** deactivate the old leg
            old_leg.close()

        elif action_type == "convert_to_spread":
            # Convert unlimited-risk position to defined-risk spread.
            target_tag = self._resolve_close_tag(action_cfg.get("target_leg"))
            width = float(action_cfg.get("width", 100))

            # Fallback: legacy config uses wing_leg dict directly
            wing_cfg = action_cfg.get("wing_leg", {})
            if not target_tag and wing_cfg:
                self._open_new_leg(wing_cfg)
                return

            if not target_tag or target_tag not in self.state.legs:
                logger.error(
                    "ADJUSTMENT_ERROR | convert_to_spread | "
                    "target leg %s not found", target_tag,
                )
                return

            target_leg = self.state.legs[target_tag]

            if not target_leg.is_active:
                logger.warning(
                    "ADJUSTMENT_SKIP | convert_to_spread | "
                    "target leg %s is inactive", target_tag,
                )
                return

            if target_leg.instrument != InstrumentType.OPT:
                logger.error(
                    "ADJUSTMENT_ERROR | convert_to_spread | "
                    "can only convert options, not %s", target_leg.instrument,
                )
                return

            if target_leg.side != Side.SELL:
                logger.error(
                    "ADJUSTMENT_ERROR | convert_to_spread | "
                    "can only convert short options (target is %s)", target_leg.side,
                )
                return

            # Determine hedge strike
            if target_leg.option_type == OptionType.CE:
                hedge_strike = target_leg.strike + width
                spread_name = "BEAR_CALL_SPREAD"
            elif target_leg.option_type == OptionType.PE:
                hedge_strike = target_leg.strike - width
                spread_name = "BULL_PUT_SPREAD"
            else:
                logger.error("ADJUSTMENT_ERROR | convert_to_spread | invalid option_type")
                return

            # Validate hedge strike exists in chain
            opt_data = self.market.get_option_at_strike(
                hedge_strike, target_leg.option_type, expiry=target_leg.expiry
            )
            if not opt_data:
                logger.error(
                    "ADJUSTMENT_ERROR | convert_to_spread | "
                    "hedge strike %s %s not found in chain",
                    hedge_strike, target_leg.option_type.value if target_leg.option_type else "?",
                )
                return

            hedge_tag = f"{target_tag}_HEDGE"
            hedge_cfg = {
                "tag": hedge_tag,
                "symbol": target_leg.symbol,
                "option_type": target_leg.option_type.value,
                "side": "BUY",
                "strike_mode": "exact",
                "exact_strike": hedge_strike,
                "lots": target_leg.qty,
                "expiry": target_leg.expiry,
                "group": f"SPREAD_{target_tag}",
            }

            new_tag = self._open_new_leg(hedge_cfg, closing_leg=None)

            # Mark both legs as part of spread group
            target_leg.group = f"SPREAD_{target_tag}"
            if new_tag and new_tag in self.state.legs:
                self.state.legs[new_tag].group = f"SPREAD_{target_tag}"

            logger.info(
                "ADJUSTMENT_EXECUTED | convert_to_spread | "
                "%s -> %s | short=%s long=%s width=%s",
                target_tag, spread_name, target_leg.strike, hedge_strike, width,
            )

        elif action_type == "simple_close_open_new":
            swaps = action_cfg.get("leg_swaps", [])
            for swap in swaps:
                close_tag = self._resolve_close_tag(swap.get("close_tag"))
                new_leg_cfg = swap.get("new_leg", {})
                closing_leg = self.state.legs.get(close_tag) if close_tag else None
                # ✅ BUG FIX: Pre-validate match_leg reference BEFORE deactivating
                # the close_tag leg.  Skip the swap when:
                # (a) the reference cannot be resolved at all (no active legs), OR
                # (b) close_tag == resolved_ref_tag AND no other active leg exists
                #     (would have nothing to match against after closure).
                # Note: closing_leg fallback in _open_new_leg handles the case where
                # close_tag IS the match_leg (single-leg state).
                if new_leg_cfg.get("strike_mode") == "match_leg":
                    ref_name = new_leg_cfg.get("match_leg")
                    resolved_ref_tag = self._resolve_close_tag(ref_name) if ref_name else None
                    ref_leg = self.state.legs.get(resolved_ref_tag) if resolved_ref_tag else None
                    active_legs = [leg_ for leg_ in self.state.legs.values() if leg_.is_active]
                    # Only one active leg AND it is both the close_tag and the match reference
                    same_tag = close_tag and resolved_ref_tag and (close_tag == resolved_ref_tag)
                    if ref_leg is None or not ref_leg.is_active:
                        logger.warning(
                            "ADJUSTMENT_SKIPPED | simple_close_open_new | match_leg ref '%s' "
                            "unresolvable or inactive (no active legs). Skipping swap (close=%s).",
                            ref_name, close_tag,
                        )
                        continue
                    if same_tag and len(active_legs) <= 1:
                        logger.warning(
                            "ADJUSTMENT_SKIPPED | simple_close_open_new | close_tag '%s' is the "
                            "only active leg and also the match_leg reference '%s'. "
                            "Skipping swap to avoid invalid single-leg close+match.",
                            close_tag, ref_name,
                        )
                        continue
                if close_tag and close_tag in self.state.legs:
                    closing_pnl = self.state.legs[close_tag].pnl or 0.0
                    self.state.cumulative_daily_pnl += closing_pnl
                    self.state.legs[close_tag].close()
                new_tag = self._open_new_leg(new_leg_cfg, closing_leg=closing_leg)
                # ✅ No-op detection: if the new leg resolved to the same
                # strike + option_type + expiry as the closing leg, the swap
                # is pointless.  Undo it: remove the new leg and reactivate
                # the closing leg.
                if new_tag and closing_leg and new_tag in self.state.legs:
                    new_leg = self.state.legs[new_tag]
                    same_strike = (
                        new_leg.strike is not None
                        and closing_leg.strike is not None
                        and new_leg.strike == closing_leg.strike
                        and new_leg.option_type == closing_leg.option_type
                        and new_leg.expiry == closing_leg.expiry
                    )
                    if same_strike:
                        logger.info(
                            "ADJUSTMENT_NOOP | simple_close_open_new | new leg '%s' "
                            "resolved to same strike=%s/%s/%s as closing leg '%s'. "
                            "Skipping swap.",
                            new_tag, new_leg.strike, new_leg.option_type,
                            new_leg.expiry, closing_leg.tag,
                        )
                        # Remove new leg, reactivate closing leg and clear
                        # any partial-close fields set by leg.close()
                        self.state.legs.pop(new_tag, None)
                        if close_tag and close_tag in self.state.legs:
                            leg_to_restore = self.state.legs[close_tag]
                            leg_to_restore.is_active = True
                            leg_to_restore.exit_timestamp = None
                            leg_to_restore.exit_price = None
                            self.state.cumulative_daily_pnl -= closing_pnl

    def _open_new_leg(self, leg_cfg: Dict[str, Any], closing_leg: Optional[LegState] = None) -> Optional[str]:
        """Open a new leg based on strike config (from action).

        Returns:
            The tag of the newly created leg, or None on failure.
        """
        # Extract basic fields
        side = Side(leg_cfg.get("side", "BUY"))
        opt_type = self._resolve_option_type(leg_cfg.get("option_type", "CE"), closing_leg)
        lots = int(leg_cfg.get("lots", 1))
        order_type_str = leg_cfg.get("order_type")
        order_type = OrderType(order_type_str) if order_type_str else None

        # Determine strike mode
        mode_str = leg_cfg.get("strike_mode", "standard")
        try:
            mode = StrikeMode(mode_str)
        except ValueError:
            mode = StrikeMode.STANDARD

        # ✅ BUG-019 FIX: Validate strike rounding parameter
        rounding = leg_cfg.get("rounding")
        if rounding is not None:
            rounding = float(rounding)
            if rounding <= 0:
                logger.warning(
                    "ADJUSTMENT_WARNING | invalid rounding=%s, ignoring", rounding,
                )
                rounding = None

        # Build StrikeConfig
        strike_cfg = StrikeConfig(
            mode=mode,
            side=side,
            option_type=opt_type,
            lots=lots,
            order_type=order_type,
            strike_selection=leg_cfg.get("strike_selection"),
            strike_value=leg_cfg.get("strike_value"),
            exact_strike=leg_cfg.get("exact_strike"),
            atm_offset_points=leg_cfg.get("atm_offset_points"),
            atm_offset_pct=leg_cfg.get("atm_offset_pct"),
            match_leg=leg_cfg.get("match_leg"),
            match_param=leg_cfg.get("match_param"),
            match_offset=leg_cfg.get("match_offset", 0.0),
            match_multiplier=leg_cfg.get("match_multiplier", 1.0),
            rounding=rounding,
        )

        # Determine symbol and expiry from existing legs
        symbol: Optional[str] = None
        expiry: Optional[str] = None
        for leg in self.state.legs.values():
            if leg.is_active:
                symbol = leg.symbol
                expiry = leg.expiry
                break
        # ✅ BUG FIX: Use closing_leg's symbol/expiry as primary fallback before
        # iterating all legs.  When `simple_close_open_new` deactivates the only
        # active leg **before** calling _open_new_leg, the loop above finds nothing
        # and expiry previously fell back to "current" which MarketReader cannot resolve.
        if symbol is None and closing_leg is not None:
            symbol = closing_leg.symbol
        if expiry is None and closing_leg is not None:
            expiry = closing_leg.expiry
        if symbol is None:
            # No active legs and no closing_leg — pick symbol from any existing leg
            for leg in self.state.legs.values():
                symbol = leg.symbol
                if expiry is None:
                    expiry = leg.expiry
                break
        # If still no symbol, we cannot proceed
        if symbol is None:
            raise ValueError("Cannot determine symbol for adjustment leg: no active or existing leg found.")
        if expiry is None:
            expiry = None  # MarketReader auto-resolves None to nearest future expiry

        # For match_leg, we may need a reference leg
        reference_leg = None
        if mode == StrikeMode.MATCH_LEG and strike_cfg.match_leg:
            ref_tag = self._resolve_close_tag(strike_cfg.match_leg)
            if ref_tag:
                reference_leg = self.state.legs.get(ref_tag)
            # ✅ BUG-004 FIX: Multi-stage fallback for match_leg resolution.
            # 1. Use closing_leg (semantically: match the leg we just closed)
            if reference_leg is None and closing_leg is not None:
                logger.warning(
                    "MATCH_LEG reference '%s' is None after deactivation; "
                    "falling back to closing_leg '%s' as reference.",
                    strike_cfg.match_leg, closing_leg.tag,
                )
                reference_leg = closing_leg
            # 2. Try any active leg with the exact match_leg tag
            if reference_leg is None:
                for leg in self.state.legs.values():
                    if leg.is_active and leg.tag == strike_cfg.match_leg:
                        reference_leg = leg
                        break
            # 3. Use most recently closed inactive leg as snapshot
            if reference_leg is None:
                inactive_candidates = [
                    leg for leg in self.state.legs.values()
                    if leg.tag == strike_cfg.match_leg and not leg.is_active
                ]
                if inactive_candidates:
                    reference_leg = max(
                        inactive_candidates,
                        key=lambda leg_: leg_.order_placed_at or datetime.min,
                    )
                    logger.warning(
                        "MATCH_LEG reference '%s' not active; using most recent "
                        "inactive snapshot (placed_at=%s).",
                        strike_cfg.match_leg,
                        reference_leg.order_placed_at,
                    )

        strike, opt_data = self.market.resolve_strike(
            strike_cfg, symbol, expiry=expiry, reference_leg_state=reference_leg
        )
        if strike is None or opt_data is None:
            logger.warning(
                "ADJUSTMENT_LEG_SKIPPED | reason=STRIKE_RESOLVE_FAILED | "
                "selection=%s | will skip this adjustment",
                leg_cfg.get("strike_selection"),
            )
            return None

        # Generate unique tag
        base_tag = leg_cfg.get("tag", "NEW_LEG")
        existing_tags = set(self.state.legs.keys())
        tag = base_tag
        counter = 1
        while tag in existing_tags:
            tag = f"{base_tag}_{counter}"
            counter += 1

        # Create the new leg
        leg = LegState(
            tag=tag,
            symbol=symbol,
            instrument=InstrumentType.OPT,
            option_type=opt_type,
            strike=strike,
            expiry=expiry,
            side=side,
            qty=lots,
            entry_price=opt_data["ltp"],
            ltp=opt_data["ltp"],
            delta=opt_data.get("delta", 0.0),
            gamma=opt_data.get("gamma", 0.0),
            theta=opt_data.get("theta", 0.0),
            vega=opt_data.get("vega", 0.0),
            iv=opt_data.get("iv", 0.0),
            trading_symbol=(
                opt_data.get("trading_symbol")
                or opt_data.get("tsym")
                or opt_data.get("symbol")
                or ""
            ),
        )

        # ✅ BUG-015 FIX: Populate lot_size from market reader for adjustment legs
        try:
            leg.lot_size = max(1, int(self.market.get_lot_size(expiry)))
        except Exception:
            pass  # Executor service will stamp lot_size as fallback

        # 🔒 Mark as pending, not active – will become active only after fill
        leg.is_active = False
        leg.order_status = "PENDING"
        leg.order_placed_at = datetime.now()

        self.state.legs[tag] = leg
        return tag

    def _resolve_close_tag(self, close_tag: Optional[str]) -> Optional[str]:
        if not close_tag:
            return None
        if close_tag in self.state.legs and self.state.legs[close_tag].is_active:
            return close_tag
        dynamic_map = {
            "HIGHER_DELTA_LEG": self.state.higher_delta_leg,
            "LOWER_DELTA_LEG": self.state.lower_delta_leg,
            "MOST_PROFITABLE_LEG": self.state.most_profitable_leg,
            "LEAST_PROFITABLE_LEG": self.state.least_profitable_leg,
            "HIGHER_THETA_LEG": self.state.higher_theta_leg,
            "LOWER_THETA_LEG": self.state.lower_theta_leg,
            "HIGHER_IV_LEG": self.state.higher_iv_leg,
            "LOWER_IV_LEG": self.state.lower_iv_leg,
            "DEEPEST_ITM_LEG": self.state.deepest_itm_leg,
            "MOST_OTM_LEG": self.state.most_otm_leg,
        }
        return dynamic_map.get(close_tag)

    def _resolve_option_type(self, raw_option_type: str, closing_leg: Optional[LegState]) -> OptionType:
        if raw_option_type == "MATCH_CLOSING":
            if closing_leg and closing_leg.option_type:
                return closing_leg.option_type
            tag = self.state.higher_delta_leg
            if tag and tag in self.state.legs and self.state.legs[tag].option_type:
                return self.state.legs[tag].option_type  # type: ignore[return-value]
            return OptionType.CE
        if raw_option_type == "MATCH_OPPOSITE":
            if closing_leg and closing_leg.option_type == OptionType.CE:
                return OptionType.PE
            if closing_leg and closing_leg.option_type == OptionType.PE:
                return OptionType.CE
            tag = self.state.higher_delta_leg
            if tag and tag in self.state.legs:
                ref_opt = self.state.legs[tag].option_type
                if ref_opt == OptionType.CE:
                    return OptionType.PE
                if ref_opt == OptionType.PE:
                    return OptionType.CE
            return OptionType.PE
        return OptionType(raw_option_type)

    def _resolve_next_expiry(self, target_expiry: str, current_expiry: str) -> str:
        """
        Resolve expiry string for roll using market reader.
        """
        return self.market.get_next_expiry(current_expiry, target_expiry)

    def _dict_to_condition(self, d: Dict[str, Any]) -> Condition:
        return Condition(
            parameter=d["parameter"],
            comparator=Comparator(d["comparator"]),
            value=d.get("value"),
            value2=d.get("value2"),
            join=JoinOperator(d["join"]) if d.get("join") else None
        )
