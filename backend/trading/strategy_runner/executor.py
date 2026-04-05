import time
import json
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Any

from .state import StrategyState, LegState
from .models import Side, InstrumentType
from .market_reader import MarketReader
from .condition_engine import ConditionEngine
from .entry_engine import EntryEngine
from .adjustment_engine import AdjustmentEngine
from .exit_engine import ExitEngine
from .reconciliation import BrokerReconciliation
from .persistence import StatePersistence

# Stub for index subscriber — Smart_trader does not use shoonya_platform feeds
class _NullIndexSubscriber:
    @staticmethod
    def get_index_prices():
        return {}

index_tokens_subscriber = _NullIndexSubscriber()

logger = logging.getLogger(__name__)

class StrategyExecutor:
    def __init__(self, config_path: str, state_path: Optional[str] = None):
        with open(config_path, 'r') as f:
            self.config = json.load(f)

        self.state_path = state_path or "state.json"  # ✅ BUG-006: JSON instead of pkl
        self.state = self._load_or_create_state()

        # Initialize market reader
        identity = self.config["identity"]
        self.market = MarketReader(
            exchange=identity["exchange"],
            symbol=identity["underlying"],
            max_stale_seconds=30
        )
        self._cycle_expiry_date = self._resolve_cycle_expiry()

        # Initialize engines
        self.condition_engine = ConditionEngine(self.state)
        self.entry_engine = EntryEngine(self.state, self.market)
        self.adjustment_engine = AdjustmentEngine(self.state, self.market)
        self.exit_engine = ExitEngine(self.state)
        self.reconciliation = BrokerReconciliation(
            self.state,
            lot_size_resolver=self.market.get_lot_size,
        )

        # Load rules
        self.adjustment_engine.load_rules(self.config.get("adjustment", {}).get("rules", []))
        self.exit_engine.load_config(self.config.get("exit", {}))

        # OMS + broker integration (injected externally via set_oms)
        self._oms: Optional[Any] = None
        self._paper_mode: bool = identity.get("paper_mode", True)
        self._strategy_id: str = self.config.get("id", self.config.get("name", "unknown"))

        # NEW: Sequential entry state (not fully implemented in base executor)
        self._sequential_pending = False
        self._sequential_legs: List[LegState] = []
        self._sequential_index = 0

    def _resolve_cycle_expiry(self) -> str:
        schedule_mode = str(
            (self.config.get("schedule", {}) or {}).get("expiry_mode", "weekly_current")
        ).strip() or "weekly_current"
        if schedule_mode == "custom":
            logger.warning(
                "schedule.expiry_mode=custom is deprecated; falling back to weekly_current"
            )
            schedule_mode = "weekly_current"

        try:
            resolved = self.market.resolve_expiry_mode(schedule_mode)
            logger.info(
                "Resolved cycle expiry: %s (mode=%s)",
                resolved,
                schedule_mode,
            )
            return resolved
        except Exception as e:
            logger.warning(
                "Failed to resolve cycle expiry with mode=%s: %s; trying weekly_current",
                schedule_mode,
                e,
            )
            try:
                return self.market.resolve_expiry_mode("weekly_current")
            except Exception as e2:
                # No option chain data available — use placeholder.
                # The executor will still run but entry conditions won't trigger
                # until market data becomes available.
                logger.warning(
                    "No option chain data available: %s. "
                    "Executor will run idle until data is populated.",
                    e2,
                )
                return "NO_EXPIRY"

    def _load_or_create_state(self) -> StrategyState:
        state = StatePersistence.load(self.state_path)
        if state is None:
            state = StrategyState()
        return state

    def set_oms(self, oms: Any) -> None:
        """Inject OMS for live/paper order placement."""
        self._oms = oms
        logger.info("OMS injected into executor (paper=%s)", self._paper_mode)

    def _place_entry_order(self, leg: LegState) -> None:
        """Place an entry order for a leg via OMS."""
        if not self._oms:
            # No OMS — state-only tracking (paper simulation within executor)
            leg.order_status = "SIMULATED"
            logger.info(
                "ORDER_SIM | ENTRY %s %s %s strike=%s lots=%d @ %.2f",
                leg.side.value, leg.symbol, leg.option_type.value if leg.option_type else "FUT",
                leg.strike, leg.qty, leg.entry_price,
            )
            return

        try:
            from trading.oms import OrderRequest, OrderSide, OrderType, ProductType
            side = OrderSide.BUY if leg.side == Side.BUY else OrderSide.SELL
            tsym = leg.trading_symbol or leg.symbol
            exchange = getattr(self.market, 'exchange', 'NFO')

            req = OrderRequest(
                symbol=tsym,
                exchange=exchange,
                side=side,
                order_type=OrderType.MARKET,
                product=ProductType.NRML,
                quantity=leg.order_qty,
                price=leg.entry_price,
                strategy_id=self._strategy_id,
                tag="ENTRY",
                test_mode=self._paper_mode,
                remarks=f"strat={self._strategy_id} leg={leg.tag}",
            )
            order = self._oms.place_order(req)
            leg.order_id = order.id
            leg.broker_order_id = order.broker_order_id
            leg.order_status = order.status.value if hasattr(order.status, 'value') else str(order.status)

            if order.avg_price and order.avg_price > 0:
                leg.entry_price = order.avg_price

            logger.info(
                "ORDER_PLACED | ENTRY %s %s %s strike=%s qty=%d | order_id=%s status=%s",
                leg.side.value, tsym,
                leg.option_type.value if leg.option_type else "FUT",
                leg.strike, leg.order_qty,
                order.id[:8], leg.order_status,
            )
        except Exception as exc:
            leg.order_status = "FAILED"
            logger.error("ORDER_FAILED | ENTRY %s: %s", leg.tag, exc)

    def _place_exit_order(self, leg: LegState) -> None:
        """Place an exit order for an active leg via OMS."""
        if not self._oms:
            leg.order_status = "SIM_CLOSED"
            logger.info(
                "ORDER_SIM | EXIT %s %s strike=%s pnl=%.2f",
                leg.tag, leg.symbol, leg.strike, leg.pnl,
            )
            return

        try:
            from trading.oms import OrderRequest, OrderSide, OrderType, ProductType
            # Exit is opposite side
            exit_side = OrderSide.SELL if leg.side == Side.BUY else OrderSide.BUY
            tsym = leg.trading_symbol or leg.symbol
            exchange = getattr(self.market, 'exchange', 'NFO')

            req = OrderRequest(
                symbol=tsym,
                exchange=exchange,
                side=exit_side,
                order_type=OrderType.MARKET,
                product=ProductType.NRML,
                quantity=leg.order_qty,
                price=leg.ltp,
                strategy_id=self._strategy_id,
                tag="EXIT",
                test_mode=self._paper_mode,
                remarks=f"strat={self._strategy_id} leg={leg.tag}",
            )
            order = self._oms.place_order(req)
            leg.order_status = "CLOSED"
            logger.info(
                "ORDER_PLACED | EXIT %s %s | order_id=%s pnl=%.2f",
                leg.tag, tsym, order.id[:8], leg.pnl,
            )
        except Exception as exc:
            logger.error("ORDER_FAILED | EXIT %s: %s", leg.tag, exc)

    def run(self, interval_sec: int = 1):
        logger.info("Starting strategy executor...")
        _backoff = 1  # seconds; reset on successful tick
        while True:
            try:
                self._tick()
                time.sleep(interval_sec)
                _backoff = 1  # reset after clean tick
            except KeyboardInterrupt:
                logger.info("Stopping...")
                self._save_state()
                break
            except Exception as e:
                # ✅ BUG-007 FIX: Fatal exceptions must NOT crash the run loop.
                # Log, save state, then back off before retrying.
                logger.exception(f"Tick error (backing off {_backoff}s): {e}")
                try:
                    self._save_state()
                except Exception:
                    pass
                time.sleep(_backoff)
                _backoff = min(_backoff * 2, 60)  # cap at 60s backoff

    def _tick(self):
        now = datetime.now()
        self.state.current_time = now

        # Daily reset
        if self.state.last_date is None or self.state.last_date != now.date():
            self.state.adjustments_today = 0
            self.state.total_trades_today = 0
            self.state.entered_today = False
            self.state.last_date = now.date()
            logger.info("Daily counters reset")

        # Update market data (including per‑leg data)
        self._update_market_data()

        # Compute minutes to exit
        exit_time_str = self.config.get("exit", {}).get("time", {}).get("strategy_exit_time")
        if exit_time_str:
            try:
                exit_t = datetime.strptime(exit_time_str, "%H:%M").time()
                exit_dt = datetime.combine(now.date(), exit_t)
                if now > exit_dt:
                    self.state.minutes_to_exit = 0
                else:
                    delta = exit_dt - now
                    self.state.minutes_to_exit = int(delta.total_seconds() / 60)
            except Exception:
                self.state.minutes_to_exit = 0
        else:
            self.state.minutes_to_exit = 0

        # Reconciliation with broker (simulate)
        broker_positions = self._fetch_broker_positions()
        warnings = self.reconciliation.reconcile(broker_positions)
        for w in warnings:
            logger.warning(f"Reconciliation warning: {w}")

        # NEW: Expiry day actions
        self._check_expiry_day_action(now)

        # Check exits first
        exit_action = self.exit_engine.check_exits(now)
        if exit_action and exit_action != "profit_step_adj":
            logger.info(f"Exit triggered: {exit_action}")
            self._execute_exit(exit_action)
            self._save_state()  # Save immediately on significant event
            return

        # Check if we should enter today
        if self._should_enter(now):
            self._execute_entry()
            self._save_state()  # ✅ BUG-022 FIX: Save immediately after entry

        # Check adjustments
        actions = self.adjustment_engine.check_and_apply(now)
        for a in actions:
            logger.info(f"Adjustment: {a}")
        if actions:
            self._save_state()  # ✅ BUG-022 FIX: Save immediately after any adjustment

        # Periodic save (every minute) as a safety net
        if now.second == 0:
            self._save_state()

    def _update_market_data(self):
        """Update spot, ATM, futures, per‑leg LTP, greeks, bid/ask, OI change, and index data."""
        self.state.current_time = datetime.now()
        self.state.spot_price = self.market.get_spot_price(self._cycle_expiry_date)
        if not self.state.spot_open and self.state.spot_price:
            self.state.spot_open = self.state.spot_price
        self.state.atm_strike = self.market.get_atm_strike(self._cycle_expiry_date)
        self.state.fut_ltp = self.market.get_fut_ltp(self._cycle_expiry_date)
        chain_metrics = self.market.get_chain_metrics(self._cycle_expiry_date)
        self.state.pcr = float(chain_metrics.get("pcr", 0.0) or 0.0)
        self.state.pcr_volume = float(chain_metrics.get("pcr_volume", 0.0) or 0.0)
        self.state.max_pain_strike = float(chain_metrics.get("max_pain_strike", 0.0) or 0.0)
        self.state.total_oi_ce = float(chain_metrics.get("total_oi_ce", 0.0) or 0.0)
        self.state.total_oi_pe = float(chain_metrics.get("total_oi_pe", 0.0) or 0.0)
        self.state.oi_buildup_ce = float(chain_metrics.get("oi_buildup_ce", 0.0) or 0.0)
        self.state.oi_buildup_pe = float(chain_metrics.get("oi_buildup_pe", 0.0) or 0.0)

        # Update each active leg
        for leg in self.state.legs.values():
            if not leg.is_active:
                continue
            if leg.instrument == InstrumentType.OPT and leg.strike is not None:
                # Option leg – option_type cannot be None here
                assert leg.option_type is not None, f"Option leg {leg.tag} missing option_type"
                try:
                    opt_data = self.market.get_option_at_strike(leg.strike, leg.option_type, leg.expiry)
                    if opt_data:
                        # Update standard fields
                        leg.ltp = opt_data.get("ltp", leg.ltp)
                        leg.delta = opt_data.get("delta", leg.delta)
                        leg.gamma = opt_data.get("gamma", leg.gamma)
                        leg.theta = opt_data.get("theta", leg.theta)
                        leg.vega = opt_data.get("vega", leg.vega)
                        leg.iv = opt_data.get("iv", leg.iv)
                        leg.volume = opt_data.get("volume", leg.volume)

                        # NEW: Bid/Ask fields
                        leg.bid = opt_data.get("bid", leg.bid)
                        leg.ask = opt_data.get("ask", leg.ask)
                        leg.bid_qty = opt_data.get("bid_qty", leg.bid_qty)
                        leg.ask_qty = opt_data.get("ask_qty", leg.ask_qty)
                        # Update bid-ask spread
                        leg.bid_ask_spread = (leg.ask - leg.bid) if leg.ask > 0 and leg.bid > 0 else 0.0

                        # NEW: OI change calculation
                        if "oi" in opt_data:
                            old_oi = leg.oi
                            leg.oi = opt_data["oi"]
                            leg.oi_change = leg.oi - old_oi
                            leg.prev_oi = old_oi

                except Exception as e:
                    logger.debug(f"Could not update leg {leg.tag}: {e}")
            # For futures legs, we might need a separate update – currently not implemented

        # NEW: Fetch index data from live feed
        try:
            index_prices = index_tokens_subscriber.get_index_prices()
            if index_prices:
                # Type ignore: index_prices may contain None, but set_index_ticks handles it.
                self.state.set_index_ticks(index_prices)  # type: ignore
        except Exception as e:
            logger.debug(f"Could not fetch index data: {e}")

    def _fetch_broker_positions(self) -> list:
        # If OMS is available, pull positions from it
        if self._oms:
            try:
                oms_positions = self._oms.get_positions()
                if oms_positions:
                    positions = []
                    for pos in oms_positions:
                        p = pos if isinstance(pos, dict) else pos.to_dict()
                        positions.append(p)
                    return positions
            except Exception as e:
                logger.debug("OMS position fetch failed, falling back to state: %s", e)

        # Fallback: return current legs as positions (paper / no OMS)
        positions = []
        for leg in self.state.legs.values():
            if leg.is_active:
                positions.append({
                    "tag": leg.tag,
                    "symbol": leg.symbol,
                    "instrument": leg.instrument.value,
                    "option_type": leg.option_type.value if leg.option_type else None,
                    "strike": leg.strike,
                    "expiry": leg.expiry,
                    "side": leg.side.value,
                    "qty": leg.qty,
                    "ltp": leg.ltp,
                    "delta": leg.delta
                })
        return positions

    def _should_enter(self, now: datetime) -> bool:
        if self.state.entered_today:
            return False
        # ✅ BUG-008 FIX: Use .get() with safe defaults — hard key access raises KeyError if config section absent
        timing = self.config.get("timing", {})
        entry_start = timing.get("entry_window_start", "09:15")
        entry_end = timing.get("entry_window_end", "15:00")
        try:
            start_t = datetime.strptime(entry_start, "%H:%M").time()
            end_t = datetime.strptime(entry_end, "%H:%M").time()
        except Exception:
            return False

        schedule = self.config.get("schedule", {})

        # Respect entry_on_expiry_day flag (default: allow entry)
        if not schedule.get("entry_on_expiry_day", True) and self.state.is_expiry_day:
            return False

        # Respect max_reentries_per_day (entered_today already blocks re-entry but
        # keep a guard on total_trades_today for completeness)
        max_reentries = schedule.get("max_reentries_per_day")
        if max_reentries is not None and self.state.total_trades_today >= max_reentries:
            return False

        if start_t <= now.time() <= end_t:
            # Also check active days
            day_name = now.strftime("%a").lower()[:3]
            active_days = schedule.get("active_days", [])
            if day_name in active_days:
                return True
        return False

    # NEW: RMS limit check
    def _check_rms_limits(self, additional_lots: int = 0) -> bool:
        """
        Check strategy-level risk limits from config.rms section.
        Returns True if limits are satisfied, False if entry should be blocked.
        """
        rms = self.config.get("rms", {})
        daily = rms.get("daily", {})
        loss_limit = daily.get("loss_limit")
        if loss_limit is not None and self.state.cumulative_daily_pnl <= -loss_limit:
            logger.warning(f"RMS block: daily loss limit {loss_limit} reached (PnL={self.state.cumulative_daily_pnl})")
            return False

        position = rms.get("position", {})
        max_lots = position.get("max_lots")
        if max_lots is not None:
            total_lots = sum(leg.qty for leg in self.state.legs.values() if leg.is_active) + additional_lots
            if total_lots > max_lots:
                logger.warning(f"RMS block: max lots {max_lots} exceeded (would be {total_lots})")
                return False
        return True

    def _execute_entry(self):
        # NEW: Check RMS limits before entry
        if not self._check_rms_limits():
            logger.warning("Entry blocked by RMS limits")
            return

        logger.info("Executing entry...")
        symbol = self.config["identity"]["underlying"]
        default_expiry = self._cycle_expiry_date
        new_legs = self.entry_engine.process_entry(
            self.config["entry"], symbol, default_expiry
        )
        # Place orders for each new leg via OMS
        for leg in new_legs:
            self.state.legs[leg.tag] = leg
            self._place_entry_order(leg)
        self.state.entered_today = True
        self.state.total_trades_today += 1
        self.state.entry_time = datetime.now()
        logger.info(f"Entered {len(new_legs)} legs")

    def _execute_exit(self, action: str):
        # NEW: Handle partial_lots exit action
        if action.startswith("partial_lots"):
            lots_to_close = self.exit_engine.exit_config.get("profit_target", {}).get("lots", 1)
            active = [leg for leg in self.state.legs.values() if leg.is_active]
            if active:
                leg = active[0]
                close_qty = min(lots_to_close, leg.qty)
                logger.info(f"Partial close: closing {close_qty} lots of {leg.tag}")
                self._place_exit_order(leg)
                leg.qty -= close_qty
                if leg.qty == 0:
                    leg.is_active = False
                self.state.cumulative_daily_pnl += leg.pnl
            else:
                logger.warning("partial_lots: no active legs to close")

        elif action.startswith("profit_step_"):
            step_action = action.replace("profit_step_", "")
            if step_action == "adj":
                logger.info("Profit step triggered adjustment (will be handled in next adjustment cycle)")
            elif step_action == "trail":
                current_trail = self.exit_engine.exit_config.get("trailing", {}).get("trail_amount", 0)
                if current_trail > 0:
                    self.state.trailing_stop_level = self.state.peak_pnl - current_trail * 0.75
                logger.info("Profit step tightened trailing stop")
            elif step_action == "partial":
                for leg in self.state.legs.values():
                    if leg.is_active and leg.qty > 0:
                        close_qty = max(1, int(leg.qty * 0.25))
                        self._place_exit_order(leg)
                        leg.qty -= close_qty
                        if leg.qty <= 0:
                            leg.is_active = False
                logger.info("Profit step closed 25% of position")

        elif action.startswith("exit_all") or action in ("combined_conditions", "time_exit"):
            pnl_snapshot = self.state.combined_pnl
            # Place exit orders for all active legs
            for leg in self.state.legs.values():
                if leg.is_active:
                    self._place_exit_order(leg)
                leg.is_active = False
            self.state.cumulative_daily_pnl += pnl_snapshot
            sl_cfg = self.exit_engine.exit_config.get("stop_loss", {})
            if sl_cfg.get("allow_reentry"):
                self.state.entered_today = False
            else:
                self.state.entered_today = True

        # NEW: Handle trail/lock_trail profit target actions
        elif action == "profit_target_trail":
            self.state.trailing_stop_active = True
            trail_amt = self.exit_engine.exit_config.get("trailing", {}).get("trail_amount", 0)
            self.state.trailing_stop_level = self.state.peak_pnl - trail_amt
            logger.info("Trailing stop activated by profit target")

        elif action.startswith("leg_rule_"):
            rule = self.exit_engine.last_triggered_leg_rule
            if rule:
                leg_action = rule.get("action", "close_leg")
                ref = rule.get("exit_leg_ref")
                group = rule.get("group")
                if leg_action == "close_leg":
                    targets = self._resolve_exit_targets(ref, group)
                    for leg in targets:
                        self._place_exit_order(leg)
                        self.state.cumulative_daily_pnl += leg.pnl
                        leg.is_active = False
                elif leg_action == "close_all":
                    pnl_snapshot = self.state.combined_pnl
                    for leg in self.state.legs.values():
                        if leg.is_active:
                            self._place_exit_order(leg)
                        leg.is_active = False
                    self.state.cumulative_daily_pnl += pnl_snapshot
                else:
                    logger.warning(f"Unhandled leg_rule action: {leg_action}")
            else:
                logger.warning("leg_rule exit triggered but no rule context available")

        elif action.startswith("partial_"):
            # Handle partial exits (simplified) - could update state if needed
            logger.info(f"Partial exit triggered: {action}")
            # Actual execution with broker should be overridden in subclass
        else:
            logger.warning(f"Unhandled exit action: {action}")

    def _resolve_exit_targets(self, ref, group):
        """Resolve which legs an exit rule applies to."""
        if ref == "all":
            return [leg for leg in self.state.legs.values() if leg.is_active]
        elif ref == "group" and group:
            return [leg for leg in self.state.legs.values() if leg.is_active and leg.group == group]
        elif ref in self.state.legs:
            leg = self.state.legs[ref]
            return [leg] if leg.is_active else []
        return []

    def _check_expiry_day_action(self, now: datetime):
        """Handle expiry_day_action from exit config."""
        exit_cfg = self.config.get("exit", {}).get("time", {})
        action = exit_cfg.get("expiry_day_action", "none")
        if action == "none" or not self.state.is_expiry_day:
            return

        if action == "time":
            exit_time_str = exit_cfg.get("expiry_day_time")
            if exit_time_str:
                try:
                    exit_t = datetime.strptime(exit_time_str, "%H:%M").time()
                    if now.time() >= exit_t:
                        logger.info("Expiry day time exit triggered")
                        self._execute_exit("exit_all")
                except ValueError:
                    pass
        elif action == "open":
            # Exit at market open – i.e., now is after 09:15 and we haven't exited yet
            if self.state.any_leg_active:
                logger.info("Expiry day open exit triggered")
                self._execute_exit("exit_all")
        elif action == "roll":
            # Roll all positions to next expiry
            self._roll_all_positions_to_next_expiry()

    def _roll_all_positions_to_next_expiry(self):
        """Roll every active leg to the next expiry (same strike, same side)."""
        new_legs = []
        for leg in list(self.state.legs.values()):
            if not leg.is_active or leg.instrument != InstrumentType.OPT:
                continue
            # For option legs, strike and option_type must be present
            assert leg.strike is not None, f"Option leg {leg.tag} has no strike"
            assert leg.option_type is not None, f"Option leg {leg.tag} has no option_type"

            try:
                new_expiry = self.market.get_next_expiry(leg.expiry, "weekly_next")
                opt_data = self.market.get_option_at_strike(leg.strike, leg.option_type, new_expiry)
                if not opt_data:
                    logger.warning(f"Cannot roll {leg.tag}: no data for strike {leg.strike} at {new_expiry}")
                    continue
                # Create new leg (pending)
                new_tag = f"{leg.tag}_ROLLED"
                new_leg = LegState(
                    tag=new_tag,
                    symbol=leg.symbol,
                    instrument=leg.instrument,
                    option_type=leg.option_type,
                    strike=leg.strike,
                    expiry=new_expiry,
                    side=leg.side,
                    qty=leg.qty,
                    entry_price=opt_data["ltp"],
                    ltp=opt_data["ltp"],
                    trading_symbol=opt_data.get("trading_symbol", ""),
                )
                new_leg.order_status = "PENDING"
                new_leg.order_placed_at = datetime.now()
                self.state.legs[new_tag] = new_leg
                new_legs.append(new_leg)
                # Deactivate old leg
                leg.is_active = False
            except Exception as e:
                logger.error(f"Roll failed for {leg.tag}: {e}")
        logger.info(f"Rolled {len(new_legs)} legs to next expiry")

    def _save_state(self):
        StatePersistence.save(self.state, self.state_path)
        logger.info(f"State saved to {self.state_path}")
