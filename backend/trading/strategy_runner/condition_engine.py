#!/usr/bin/env python3
"""
condition_engine.py — Rule & Condition Evaluation Engine (Final)
=================================================================
"""
import re
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from .models import Condition, Comparator, JoinOperator
from .state import StrategyState

logger = logging.getLogger(__name__)

class ConditionEngine:
    def __init__(self, state: StrategyState):
        self.state = state

    def evaluate(self, conditions: List[Condition]) -> bool:
        if not conditions:
            return True

        result = None
        for i, cond in enumerate(conditions):
            val = self._evaluate_single(cond)
            if i == 0:
                result = val
            else:
                join_op = cond.join or JoinOperator.AND
                if join_op == JoinOperator.AND:
                    result = result and val
                else:
                    result = result or val
        return bool(result)

    def _evaluate_single(self, cond: Condition) -> bool:
        param_value = self._resolve_parameter(cond.parameter)
        comp = cond.comparator
        val1 = cond.value
        val2 = cond.value2

        # --- Helper conversion functions ---
        def to_numeric(x: Any) -> Optional[float]:
            if isinstance(x, (int, float)):
                return float(x)
            if isinstance(x, str):
                try:
                    return float(x)
                except ValueError:
                    return None
            return None

        def to_bool(x: Any) -> Optional[bool]:
            if isinstance(x, bool):
                return x
            # BUG-025 FIX: numeric 0/1 must be accepted as boolean bounds.
            if isinstance(x, (int, float)) and not isinstance(x, bool):
                if x == 0:
                    return False
                if x == 1:
                    return True
                return None  # 2, -1, etc. — ambiguous, reject
            # ✅ BUG-011 FIX: Extended string boolean support
            if isinstance(x, str):
                lower = x.lower().strip()
                if lower in ('true', 'yes', '1'):
                    return True
                if lower in ('false', 'no', '0'):
                    return False
            return None

        def to_minutes(x: Any) -> Optional[float]:
            if isinstance(x, str) and ':' in x:
                try:
                    t = datetime.strptime(x, "%H:%M").time()
                    return t.hour * 60 + t.minute
                except ValueError:
                    return None
            return None

        # Determine intended comparison type
        param_is_bool = isinstance(param_value, bool)
        val1_is_bool = isinstance(val1, bool)
        param_is_time = isinstance(param_value, str) and ':' in param_value
        val1_is_time = isinstance(val1, str) and ':' in str(val1)
        param_is_str = isinstance(param_value, str)
        val1_is_str = isinstance(val1, str)

        # -------- BOOLEAN BRANCH --------
        if param_is_bool or val1_is_bool:
            pv = to_bool(param_value)
            # is_true / is_false do not require a value operand from config.
            if comp == Comparator.IS_TRUE:
                return pv is True
            if comp == Comparator.IS_FALSE:
                return pv is False

            v1 = to_bool(val1)
            if pv is None or v1 is None:
                logger.warning(f"Cannot compare boolean with non-boolean: {param_value} vs {val1}")
                return False
            # Both are now bool
            if comp == Comparator.GT:
                return pv > v1
            elif comp == Comparator.GTE:
                return pv >= v1
            elif comp == Comparator.LT:
                return pv < v1
            elif comp == Comparator.LTE:
                return pv <= v1
            elif comp == Comparator.EQ:
                return pv == v1
            elif comp == Comparator.NEQ:
                return pv != v1
            elif comp == Comparator.APPROX:
                # Approximation for bools – treat as equality
                return pv == v1
            elif comp == Comparator.BETWEEN:
                if val2 is None:
                    return False
                v2 = to_bool(val2)
                if v2 is None:
                    return False
                # between with two bools: pv must be between v1 and v2 inclusive
                low, high = (v1, v2) if v1 <= v2 else (v2, v1)
                return low <= pv <= high
            elif comp == Comparator.NOT_BETWEEN:
                if val2 is None:
                    return False
                v2 = to_bool(val2)
                if v2 is None:
                    return False
                low, high = (v1, v2) if v1 <= v2 else (v2, v1)
                return not (low <= pv <= high)
            elif comp in (Comparator.CROSSES_ABOVE, Comparator.CROSSES_BELOW):
                prev = self.state.prev_values.get(cond.parameter)
                self.state.prev_values[cond.parameter] = pv
                if prev is None:
                    return False
                prev_bool = to_bool(prev)
                if prev_bool is None:
                    return False
                if comp == Comparator.CROSSES_ABOVE:
                    return prev_bool <= v1 and pv > v1
                else:
                    return prev_bool >= v1 and pv < v1
            else:
                raise ValueError(f"Unsupported comparator for boolean: {comp}")

        # -------- TIME BRANCH --------
        if param_is_time or val1_is_time:
            pv = to_minutes(param_value)
            v1 = to_minutes(val1)
            if pv is None or v1 is None:
                logger.warning(f"Cannot compare time with non-time: {param_value} vs {val1}")
                return False
            # Both are now float (minutes)
            if comp == Comparator.GT:
                return pv > v1
            elif comp == Comparator.GTE:
                return pv >= v1
            elif comp == Comparator.LT:
                return pv < v1
            elif comp == Comparator.LTE:
                return pv <= v1
            elif comp == Comparator.EQ:
                return pv == v1
            elif comp == Comparator.NEQ:
                return pv != v1
            elif comp == Comparator.APPROX:
                if v1 == 0:
                    return abs(pv) < 0.02 * (abs(pv) + 1e-9)
                return abs((pv - v1) / v1) <= 0.02
            elif comp == Comparator.BETWEEN:
                if val2 is None:
                    return False
                v2 = to_minutes(val2)
                if v2 is None:
                    return False
                return v1 <= pv <= v2
            elif comp == Comparator.NOT_BETWEEN:
                if val2 is None:
                    return False
                v2 = to_minutes(val2)
                if v2 is None:
                    return False
                return not (v1 <= pv <= v2)
            elif comp == Comparator.IS_TRUE:
                return bool(pv) is True
            elif comp == Comparator.IS_FALSE:
                return bool(pv) is False
            elif comp in (Comparator.CROSSES_ABOVE, Comparator.CROSSES_BELOW):
                prev = self.state.prev_values.get(cond.parameter)
                self.state.prev_values[cond.parameter] = pv
                if prev is None:
                    return False
                prev_time = to_minutes(prev)
                if prev_time is None:
                    return False
                if comp == Comparator.CROSSES_ABOVE:
                    return prev_time <= v1 and pv > v1
                else:
                    return prev_time >= v1 and pv < v1
            else:
                raise ValueError(f"Unsupported comparator for time: {comp}")

        # -------- STRING BRANCH --------
        # Only use lexical string comparison when values are genuinely non-numeric.
        # This prevents numeric thresholds serialized as strings (e.g. "300")
        # from being compared lexically against numeric params (e.g. 6.0).
        if (param_is_str or val1_is_str) and not (
            to_numeric(param_value) is not None and to_numeric(val1) is not None
        ):
            pv = str(param_value)
            v1 = str(val1)
            if comp == Comparator.EQ:
                return pv == v1
            elif comp == Comparator.NEQ:
                return pv != v1
            elif comp == Comparator.IS_TRUE:
                return bool(pv) is True
            elif comp == Comparator.IS_FALSE:
                return bool(pv) is False
            elif comp in (Comparator.GT, Comparator.GTE, Comparator.LT, Comparator.LTE):
                if comp == Comparator.GT:
                    return pv > v1
                if comp == Comparator.GTE:
                    return pv >= v1
                if comp == Comparator.LT:
                    return pv < v1
                return pv <= v1
            logger.warning(f"Unsupported string comparator {comp} for values: {pv} vs {v1}")
            return False

        # -------- NUMERIC BRANCH (default) --------
        pv = to_numeric(param_value)
        v1 = to_numeric(val1)
        if pv is None or v1 is None:
            logger.warning(f"Cannot compare numeric with non-numeric: {param_value} vs {val1}")
            return False
        # Both are now float

        if comp == Comparator.GT:
            return pv > v1
        elif comp == Comparator.GTE:
            return pv >= v1
        elif comp == Comparator.LT:
            return pv < v1
        elif comp == Comparator.LTE:
            return pv <= v1
        elif comp == Comparator.EQ:
            return pv == v1
        elif comp == Comparator.NEQ:
            return pv != v1
        elif comp == Comparator.APPROX:
            if v1 == 0:
                return abs(pv) < 0.02 * (abs(pv) + 1e-9)
            return abs((pv - v1) / v1) <= 0.02
        elif comp == Comparator.BETWEEN:
            if val2 is None:
                return False
            v2 = to_numeric(val2)
            if v2 is None:
                return False
            return v1 <= pv <= v2
        elif comp == Comparator.NOT_BETWEEN:
            if val2 is None:
                return False
            v2 = to_numeric(val2)
            if v2 is None:
                return False
            return not (v1 <= pv <= v2)
        elif comp == Comparator.IS_TRUE:
            return bool(pv) is True
        elif comp == Comparator.IS_FALSE:
            return bool(pv) is False
        elif comp in (Comparator.CROSSES_ABOVE, Comparator.CROSSES_BELOW):
            prev = self.state.prev_values.get(cond.parameter)
            self.state.prev_values[cond.parameter] = pv
            if prev is None:
                return False
            prev_num = to_numeric(prev)
            if prev_num is None:
                return False
            if comp == Comparator.CROSSES_ABOVE:
                return prev_num <= v1 and pv > v1
            else:
                return prev_num >= v1 and pv < v1
        else:
            raise ValueError(f"Unknown comparator: {comp}")

    def _resolve_parameter(self, param: str) -> Any:
        # Handle abs(...)
        abs_match = re.match(r'^abs\((.+)\)$', param)
        if abs_match:
            inner = abs_match.group(1)
            return abs(self._resolve_parameter(inner))

        # Handle moneyness (needs spot price)
        # For PE, use (spot - strike) / spot so OTM PE → positive.
        if param in ("ce_moneyness", "pe_moneyness"):
            opt_type = "CE" if param.startswith("ce") else "PE"
            leg = self._find_leg_by_option_type(opt_type)
            if leg and leg.strike and self.state.spot_price:
                if opt_type == "PE":
                    return (self.state.spot_price - leg.strike) / self.state.spot_price
                return (leg.strike - self.state.spot_price) / self.state.spot_price
            return 0.0

        if param == "ce_bid_ask_spread":
            leg = self._find_leg_by_option_type("CE")
            return leg.bid_ask_spread if leg else 0.0
        if param == "pe_bid_ask_spread":
            leg = self._find_leg_by_option_type("PE")
            return leg.bid_ask_spread if leg else 0.0
        
        # New parameters
        if param == "days_to_expiry":
            return self.state.days_to_expiry
        if param == "is_expiry_day":
            return self.state.is_expiry_day
        if param == "session_type":
            return self.state.session_type
        if param == "minutes_to_exit":
            return self.state.minutes_to_exit
        if param == "india_vix":
            idx = self.state.index_data.get("INDIAVIX", {})
            return idx.get("ltp", 0.0)
        if param == "pcr":
            return self.state.pcr
        if param == "pcr_volume":
            return self.state.pcr_volume

        # CE/PE parameter handling
        if param.startswith("ce_") or param.startswith("pe_"):
            opt_type = "CE" if param.startswith("ce_") else "PE"
            attr = param[3:]  # remove ce_ or pe_
            leg = self._find_leg_by_option_type(opt_type)
            if leg and hasattr(leg, attr):
                return getattr(leg, attr)
            return 0.0
        
        # Direct attributes + properties
        if param == "spot_price":
            return self.state.spot_price
        elif param == "spot_ltp":
            return self.state.spot_price
        elif param == "spot_open":
            return self.state.spot_open
        elif param == "atm_strike":
            return self.state.atm_strike
        elif param == "fut_ltp":
            return self.state.fut_ltp
        elif param == "time_current":
            t = self.state.current_time or datetime.now()
            return t.strftime("%H:%M")
        elif param == "net_delta":
            return self.state.net_delta
        elif param == "combined_pnl":
            return self.state.combined_pnl
        elif param == "combined_pnl_pct":
            return self.state.combined_pnl_pct
        elif param == "delta_diff":
            return self.state.delta_diff
        elif param == "unrealised_pnl":
            return self.state.unrealised_pnl
        elif param == "realised_pnl":
            return self.state.realised_pnl
        elif param == "profit_step":
            return self.state.profit_step
        elif param == "premium_collected":
            return self.state.premium_collected
        elif param == "total_cost_basis":
            return self.state.total_cost_basis
        elif param == "ce_premium_decay_pct":
            return self.state.ce_premium_decay_pct
        elif param == "pe_premium_decay_pct":
            return self.state.pe_premium_decay_pct
        elif param == "total_premium_decay_pct":
            return self.state.total_premium_decay_pct
        elif param == "max_profit_potential":
            return self.state.max_profit_potential
        elif param == "iv_skew":
            return self.state.iv_skew
        elif param == "atm_iv":
            return self.state.atm_iv
        elif param == "adjustment_count":
            return self.state.adjustment_count
        elif param == "all_legs_active":
            return self.state.all_legs_active
        elif param == "total_premium":
            return self.state.total_premium
        elif param == "max_leg_delta":
            return self.state.max_leg_delta
        elif param == "min_leg_delta":
            return self.state.min_leg_delta
        elif param == "any_leg_delta_above":
            return self.state.max_leg_delta
        elif param == "all_legs_delta_below":
            return self.state.max_leg_delta
        elif param == "higher_delta_leg":
            return self.state.higher_delta_leg or ""
        elif param == "lower_delta_leg":
            return self.state.lower_delta_leg or ""
        elif param == "most_profitable_leg":
            return self.state.most_profitable_leg
        elif param == "least_profitable_leg":
            return self.state.least_profitable_leg
        elif param == "spot_change":
            return self.state.spot_change
        elif param == "spot_change_pct":
            return self.state.spot_change_pct
        elif param == "ce_iv":
            return self.state.ce_iv
        elif param == "pe_iv":
            return self.state.pe_iv
        elif param == "adj_count_today":
            return self.state.adjustments_today
        # BUG-A5 FIX: Breakeven and market params declared in config_schema KNOWN_PARAMETERS
        # but were missing from _resolve_parameter, causing them to fall through to
        # getattr(state, param, 0.0) which silently returns 0.0 for properties.
        elif param == "breakeven_upper":
            return self.state.breakeven_upper
        elif param == "breakeven_lower":
            return self.state.breakeven_lower
        elif param == "breakeven_distance":
            return self.state.breakeven_distance
        elif param == "spot_vs_upper_be":
            return self.state.spot_vs_upper_be
        elif param == "spot_vs_lower_be":
            return self.state.spot_vs_lower_be
        elif param == "spot_vs_max_pain":
            return self.state.spot_vs_max_pain
        elif param == "max_pain_strike":
            return self.state.max_pain_strike
        elif param == "total_oi_ce":
            return self.state.total_oi_ce
        elif param == "total_oi_pe":
            return self.state.total_oi_pe
        elif param == "oi_buildup_ce":
            return self.state.oi_buildup_ce
        elif param == "oi_buildup_pe":
            return self.state.oi_buildup_pe
        elif param == "portfolio_delta":
            return self.state.portfolio_delta
        elif param == "portfolio_gamma":
            return self.state.portfolio_gamma
        elif param == "portfolio_theta":
            return self.state.portfolio_theta
        elif param == "portfolio_vega":
            return self.state.portfolio_vega
        elif param == "active_legs_count":
            return self.state.active_legs_count
        elif param == "closed_legs_count":
            return self.state.closed_legs_count
        elif param == "any_leg_active":
            return self.state.any_leg_active
        elif param == "time_in_position_sec":
            return self.state.time_in_position_sec
        elif param == "time_since_last_adj_sec":
            return self.state.time_since_last_adj_sec
        elif param.startswith("index_"):
            payload = param[len("index_"):]
            attr = None
            idx = None
            for suffix in ("change_pct", "ltp", "pc", "change", "open", "high", "low", "close"):
                token = f"_{suffix}"
                if payload.endswith(token):
                    idx = payload[: -len(token)].upper()
                    attr = suffix
                    break
            if idx and attr:
                if attr == "pc":
                    attr = "change_pct"
                idx_map = self.state.index_data.get(idx)
                if isinstance(idx_map, dict):
                    return idx_map.get(attr, 0.0)
                fallback_key = f"{idx}_{attr}"
                if fallback_key in self.state.index_data:
                    return self.state.index_data.get(fallback_key, 0.0)
            return 0.0
        elif param.startswith("tag."):
            match = re.match(r"tag\.([^.]+)\.(.+)", param)
            if match:
                tag, metric = match.groups()
                leg = self.state.legs.get(tag)
                if leg:
                    if metric == "pnl":
                        return leg.pnl
                    elif metric == "pnl_pct":
                        return leg.pnl_pct
                    elif metric == "abs_delta":
                        return leg.abs_delta
                    elif metric == "is_itm":
                        if leg.strike is None or leg.option_type is None:
                            return False
                        if leg.option_type.value == "CE":
                            return self.state.spot_price > leg.strike
                        return self.state.spot_price < leg.strike
                    elif metric == "moneyness":
                        if leg.strike is None or leg.option_type is None or not self.state.spot_price:
                            return 0.0
                        # BUG-M1 FIX: For PE, use (spot - strike) / spot so OTM PE → positive.
                        if leg.option_type.value == "PE":
                            return (self.state.spot_price - leg.strike) / self.state.spot_price
                        return (leg.strike - self.state.spot_price) / self.state.spot_price
                    elif hasattr(leg, metric):
                        return getattr(leg, metric)
            return 0.0
        else:
            return getattr(self.state, param, 0.0)

    def _find_leg_by_option_type(self, opt_type: str):
        """Helper to find first active leg with given option type (for CE/PE-centric params)."""
        # ✅ BUG FIX: Filter for is_active to avoid returning stale data from closed legs
        for leg in self.state.legs.values():
            if leg.is_active and leg.option_type and leg.option_type.value == opt_type:
                return leg
        return None

def evaluate_condition(condition_dict: Dict[str, Any], state: StrategyState) -> bool:
    """
    Helper for modules/tests that evaluate one condition dict.
    """
    engine = ConditionEngine(state)
    cond = Condition(
        parameter=condition_dict["parameter"],
        comparator=Comparator(condition_dict["comparator"]),
        value=condition_dict.get("value"),
        value2=condition_dict.get("value2"),
        join=JoinOperator(condition_dict["join"]) if condition_dict.get("join") else None,
    )
    return engine.evaluate([cond])
