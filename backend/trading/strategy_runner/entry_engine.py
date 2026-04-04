from typing import List, Dict, Any, Optional
import logging
from .models import (
    Condition, Side, InstrumentType, OptionType, OrderType,
    StrikeMode, StrikeConfig, Comparator, JoinOperator
)
from .state import StrategyState, LegState
from .condition_engine import ConditionEngine
from .market_reader import MarketReader

logger = logging.getLogger(__name__)

class EntryEngine:
    def __init__(self, state: StrategyState, market: MarketReader):
        self.state = state
        self.market = market
        self.condition_engine = ConditionEngine(state)

    def process_entry(self, entry_config: Dict[str, Any], symbol: str, default_expiry: str) -> List[LegState]:
        """
        Process entry legs according to JSON config.
        Returns list of newly created LegState objects.
        """
        # ✅ BUG-014 FIX: Entry guards — max entries per day, cooldown, max total
        max_entries_per_day = entry_config.get("max_entries_per_day")
        if max_entries_per_day is not None and self.state.total_trades_today >= int(max_entries_per_day):
            logger.info(
                "ENTRY_BLOCKED | max_entries_per_day=%s reached (today=%s)",
                max_entries_per_day, self.state.total_trades_today,
            )
            return []

        entry_cooldown = int(entry_config.get("entry_cooldown_sec", 0))
        if entry_cooldown > 0 and self.state.entry_time:
            from datetime import datetime as _dt
            elapsed = (_dt.now() - self.state.entry_time).total_seconds()
            if elapsed < entry_cooldown:
                logger.info(
                    "ENTRY_BLOCKED | cooldown %.0fs < %ss",
                    elapsed, entry_cooldown,
                )
                return []

        # 1. Evaluate global conditions
        global_conds = entry_config.get("global_conditions", [])
        cond_objs = [self._dict_to_condition(c) for c in global_conds]
        if not self.condition_engine.evaluate(cond_objs):
            logger.info(
                "ENTRY_SKIPPED_GLOBAL_CONDITIONS | strategy_symbol=%s | conditions=%s",
                symbol,
                self._condition_summary(global_conds),
            )
            return []  # global gate blocks entry

        # 2. Process each leg — every leg is resolved independently per its own
        #    strike_selection config.  No auto-detection / overriding of the user's
        #    intent (straddle vs strangle etc.) is performed.
        legs_config = entry_config.get("legs", [])
        new_legs = []
        sequence = entry_config.get("entry_sequence", "parallel")

        for leg_cfg in legs_config:
            leg_state = self._process_single_leg(leg_cfg, symbol, default_expiry)
            if leg_state:
                new_legs.append(leg_state)
                if sequence == "sequential":
                    # In a real system we would wait for fill confirmation.
                    # For now, we just continue.
                    logger.warning("Sequential entry mode is not fully implemented; legs are being placed together.")

        if not new_legs:
            logger.info(
                "ENTRY_SKIPPED_ALL_LEGS_FILTERED | strategy_symbol=%s | leg_count=%s",
                symbol,
                len(legs_config),
            )
        return new_legs

    def _process_single_leg(self, leg_cfg: Dict[str, Any], symbol: str, default_expiry: str) -> Optional[LegState]:
        """Process one entry leg: evaluate IF/ELSE, resolve strike, create LegState."""
        # --- Tag validation ---
        tag = leg_cfg.get("tag")
        if not tag or not isinstance(tag, str):
            raise ValueError("Each leg must have a non-empty string 'tag' field")

        # Evaluate IF conditions
        if_conds = leg_cfg.get("conditions", [])
        cond_objs = [self._dict_to_condition(c) for c in if_conds]
        if_condition_true = self.condition_engine.evaluate(cond_objs)

        # Decide which branch to use (IF or ELSE)
        use_else = leg_cfg.get("else_enabled", False) and not if_condition_true
        if use_else:
            else_conds = leg_cfg.get("else_conditions", [])
            else_cond_objs = [self._dict_to_condition(c) for c in else_conds]
            if not self.condition_engine.evaluate(else_cond_objs):
                logger.info(
                    "ENTRY_LEG_SKIPPED | tag=%s | reason=ELSE_CONDITIONS_FALSE | if_conditions=%s | else_conditions=%s",
                    tag,
                    self._condition_summary(if_conds),
                    self._condition_summary(else_conds),
                )
                return None
            exec_config = leg_cfg.get("else_action", {})
        else:
            if not if_condition_true:
                logger.info(
                    "ENTRY_LEG_SKIPPED | tag=%s | reason=IF_CONDITIONS_FALSE | conditions=%s",
                    tag,
                    self._condition_summary(if_conds),
                )
                return None
            exec_config = leg_cfg

        # Determine instrument type
        instrument = InstrumentType(exec_config.get("instrument", "OPT"))
        side = Side(exec_config.get("side", "SELL"))
        lots = int(exec_config.get("lots", 1))
        order_type_str = exec_config.get("order_type")
        order_type = OrderType(order_type_str) if order_type_str else None

        # --- Expiry resolution ---
        expiry_mode = exec_config.get("expiry", "strategy_default")
        if expiry_mode == "strategy_default":
            expiry_mode = default_expiry

        # Resolve the mode to an actual date string using market reader
        try:
            expiry = self.market.resolve_expiry_mode(expiry_mode)
        except Exception as e:
            logger.error(f"Failed to resolve expiry mode '{expiry_mode}': {e}")
            return None

        if instrument == InstrumentType.FUT:
            # Futures leg
            ltp = self.market.get_fut_ltp(expiry) if hasattr(self.market, 'get_fut_ltp') else self.market.get_spot_price()
            # ✅ BUG FIX: Resolve futures trading_symbol so broker orders use the
            # actual contract symbol (e.g. "NIFTY26MARFUT") instead of just "NIFTY".
            fut_tsym = ""
            try:
                from scripts.scriptmaster import get_future
                # BUG-M5 FIX: Use the strategy's configured exchange instead of hardcoded "NFO".
                _exchange = getattr(self.market, 'exchange', 'NFO')
                fut_info = get_future(symbol, _exchange, result=0)
                if isinstance(fut_info, dict):
                    fut_tsym = str(fut_info.get("TradingSymbol") or fut_info.get("tsym") or "")
            except Exception:
                pass
            leg = LegState(
                tag=tag,
                symbol=symbol,
                instrument=InstrumentType.FUT,
                option_type=None,
                strike=None,
                expiry=expiry,
                side=side,
                qty=lots,
                entry_price=ltp,
                ltp=ltp,
                trading_symbol=fut_tsym,
            )
            # ✅ BUG-015 FIX: Populate lot_size from market reader at entry time
            try:
                leg.lot_size = max(1, int(self.market.get_lot_size(expiry)))
            except Exception:
                pass  # Executor service will stamp lot_size as fallback
            return leg

        else:
            # Option leg
            opt_type = OptionType(exec_config.get("option_type", "CE"))
            strike_mode = StrikeMode(exec_config.get("strike_mode", "standard"))

            # ✅ BUG-019 FIX: Validate strike rounding parameter
            rounding = exec_config.get("rounding")
            if rounding is not None:
                try:
                    rounding = float(rounding)
                    if rounding <= 0:
                        logger.warning(
                            "ENTRY_WARNING | tag=%s | invalid rounding=%s, ignoring",
                            tag, rounding,
                        )
                        rounding = None
                except (ValueError, TypeError):
                    logger.warning(
                        "ENTRY_WARNING | tag=%s | non-numeric rounding=%s, ignoring",
                        tag, rounding,
                    )
                    rounding = None

            strike_cfg = StrikeConfig(
                mode=strike_mode,
                side=side,
                option_type=opt_type,
                lots=lots,
                order_type=order_type,
                strike_selection=exec_config.get("strike_selection"),
                strike_value=exec_config.get("strike_value"),
                exact_strike=exec_config.get("exact_strike"),
                atm_offset_points=exec_config.get("atm_offset_points"),
                atm_offset_pct=exec_config.get("atm_offset_pct"),
                match_leg=exec_config.get("match_leg"),
                match_param=exec_config.get("match_param"),
                match_offset=exec_config.get("match_offset", 0.0),
                match_multiplier=exec_config.get("match_multiplier", 1.0),
                rounding=rounding,
            )

            reference_leg = None
            if strike_mode == StrikeMode.MATCH_LEG and strike_cfg.match_leg:
                reference_leg = self.state.legs.get(strike_cfg.match_leg)

            strike, opt_data = self.market.resolve_strike(
                strike_cfg, symbol, expiry, reference_leg
            )

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
            # ✅ BUG-015 FIX: Populate lot_size from market reader at entry time
            try:
                leg.lot_size = max(1, int(self.market.get_lot_size(expiry)))
            except Exception:
                pass  # Executor service will stamp lot_size as fallback
            return leg

    def _dict_to_condition(self, d: Dict[str, Any]) -> Condition:
        return Condition(
            parameter=d["parameter"],
            comparator=Comparator(d["comparator"]),
            value=d.get("value"),
            value2=d.get("value2"),
            join=JoinOperator(d["join"]) if d.get("join") else None
        )

    @staticmethod
    def _condition_summary(conditions: List[Dict[str, Any]]) -> str:
        if not conditions:
            return "[]"
        parts: List[str] = []
        for c in conditions:
            if not isinstance(c, dict):
                continue
            param = c.get("parameter", "?")
            comp = c.get("comparator", "?")
            value = c.get("value")
            value2 = c.get("value2")
            join = c.get("join")
            expr = f"{param} {comp} {value}"
            if value2 is not None:
                expr += f", {value2}"
            if join:
                expr += f" [{join}]"
            parts.append(expr)
        return "; ".join(parts) if parts else "[]"
