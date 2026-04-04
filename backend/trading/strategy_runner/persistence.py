import json
from datetime import datetime, date
from typing import Optional, Dict, Any, List
from .state import StrategyState, LegState, PnLSnapshot, AdjustmentEvent
from .models import InstrumentType, OptionType, Side

class StatePersistence:
    @staticmethod
    def save(state: StrategyState, filepath: str):
        """✅ BUG-006 FIX: Use JSON instead of pickle (pickle allows arbitrary code execution)."""
        data = StatePersistence.to_dict(state)
        # Write atomically via a temp file to avoid partial writes on crash
        tmp = filepath + ".tmp"
        with open(tmp, 'w') as f:
            json.dump(data, f, indent=2)
        import os
        os.replace(tmp, filepath)

    @staticmethod
    def load(filepath: str) -> Optional[StrategyState]:
        """✅ BUG-006 FIX: Load from JSON instead of pickle."""
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            return StatePersistence.from_dict(data)
        except FileNotFoundError:
            return None
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"Failed to load state from {filepath}: {e}")
            return None

    @staticmethod
    def to_dict(state: StrategyState) -> Dict[str, Any]:
        return {
            "legs": {
                tag: {
                    "tag": leg.tag,
                    "symbol": leg.symbol,
                    "instrument": leg.instrument.value,
                    "option_type": leg.option_type.value if leg.option_type else None,
                    "strike": leg.strike,
                    "expiry": leg.expiry,
                    "side": leg.side.value,
                    "qty": leg.qty,
                    "entry_price": leg.entry_price,
                    "ltp": leg.ltp,
                    "delta": leg.delta,
                    "gamma": leg.gamma,
                    "theta": leg.theta,
                    "vega": leg.vega,
                    "iv": leg.iv,
                    "is_active": leg.is_active,
                    "group": leg.group,
                    "label": leg.label,
                    "oi": leg.oi,
                    "oi_change": leg.oi_change,
                    "volume": leg.volume,
                    "trading_symbol": leg.trading_symbol, 
                    "order_id": leg.order_id,
                    "command_id": leg.command_id,
                    "order_status": leg.order_status,
                    "filled_qty": leg.filled_qty,
                    "order_placed_at": leg.order_placed_at.isoformat() if leg.order_placed_at else None,
                    "lot_size": getattr(leg, "lot_size", 1),
                    # ✅ BUG-001 FIX: PnL history and tracking
                    "pnl_history": [
                        {
                            "timestamp": snap.timestamp.isoformat(),
                            "pnl": snap.pnl,
                            "pnl_pct": snap.pnl_pct,
                            "ltp": snap.ltp,
                            "underlying_price": snap.underlying_price,
                        }
                        for snap in (leg.pnl_history or [])[-100:]  # Persist last 100
                    ],
                    "entry_reason": getattr(leg, "entry_reason", ""),
                    "entry_timestamp": leg.entry_timestamp.isoformat() if getattr(leg, "entry_timestamp", None) else None,
                    "exit_timestamp": leg.exit_timestamp.isoformat() if getattr(leg, "exit_timestamp", None) else None,
                    "exit_reason": getattr(leg, "exit_reason", ""),
                    "exit_price": getattr(leg, "exit_price", None),
                } for tag, leg in state.legs.items()
            },
            "spot_price": state.spot_price,
            "spot_open": state.spot_open,
            "atm_strike": state.atm_strike,
            "fut_ltp": state.fut_ltp,
            "adjustments_today": state.adjustments_today,
            "total_trades_today": state.total_trades_today,
            "cumulative_daily_pnl": state.cumulative_daily_pnl,
            "entry_time": state.entry_time.isoformat() if state.entry_time else None,
            "last_adjustment_time": state.last_adjustment_time.isoformat() if state.last_adjustment_time else None,
            "trailing_stop_active": state.trailing_stop_active,
            "trailing_stop_level": state.trailing_stop_level,
            "peak_pnl": state.peak_pnl,
            "prev_values": state.prev_values,
            "lifetime_adjustments": state.lifetime_adjustments,
            "current_profit_step": state.current_profit_step,
            "last_date": state.last_date.isoformat() if state.last_date else None,
            "entered_today": state.entered_today,
            "minutes_to_exit": state.minutes_to_exit,
            # ✅ BUG-002 FIX: Adjustment history and strategy-level reasons
            "adjustment_history": [
                {
                    "timestamp": evt.timestamp.isoformat(),
                    "rule_name": evt.rule_name,
                    "action_type": evt.action_type,
                    "affected_legs": evt.affected_legs,
                    "reason": evt.reason,
                    "market_data_snapshot": evt.market_data_snapshot,
                }
                for evt in (state.adjustment_history or [])[-50:]  # Persist last 50
            ],
            "entry_reason": getattr(state, "entry_reason", ""),
            "exit_reason": getattr(state, "exit_reason", ""),
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> StrategyState:
        legs = {}
        for tag, leg_data in data.get("legs", {}).items():
            leg = LegState(
                tag=leg_data["tag"],
                symbol=leg_data["symbol"],
                instrument=InstrumentType(leg_data["instrument"]),
                option_type=OptionType(leg_data["option_type"]) if leg_data["option_type"] else None,
                strike=leg_data["strike"],
                expiry=leg_data["expiry"],
                side=Side(leg_data["side"]),
                qty=leg_data["qty"],
                entry_price=float(leg_data.get("entry_price") or 0.0),
                ltp=float(leg_data["ltp"]) if leg_data.get("ltp") is not None and float(leg_data.get("ltp", 0)) != 0.0 else float(leg_data.get("entry_price") or 0.0),
                delta=leg_data.get("delta") or 0.0,
                gamma=leg_data.get("gamma") or 0.0,
                theta=leg_data.get("theta") or 0.0,
                vega=leg_data.get("vega") or 0.0,
                iv=leg_data.get("iv") or 0.0,
                is_active=leg_data["is_active"],
                group=leg_data.get("group", ""),
                label=leg_data.get("label", ""),
                oi=leg_data.get("oi", 0),
                oi_change=leg_data.get("oi_change", 0),
                volume=leg_data.get("volume", 0),
                trading_symbol=leg_data.get("trading_symbol", ""),  
                order_id=leg_data.get("order_id"),
                command_id=leg_data.get("command_id"),
                order_status=leg_data.get("order_status", "PENDING"),
                filled_qty=leg_data.get("filled_qty", 0),
                order_placed_at=datetime.fromisoformat(leg_data["order_placed_at"]) if leg_data.get("order_placed_at") else None,
                lot_size=leg_data.get("lot_size", 1),
            )
            # Restore PnL history
            for snap_data in leg_data.get("pnl_history", []):
                try:
                    leg.pnl_history.append(PnLSnapshot(
                        timestamp=datetime.fromisoformat(snap_data["timestamp"]),
                        pnl=snap_data["pnl"],
                        pnl_pct=snap_data["pnl_pct"],
                        ltp=snap_data["ltp"],
                        underlying_price=snap_data["underlying_price"],
                    ))
                except (KeyError, ValueError):
                    pass
            leg.entry_reason = leg_data.get("entry_reason", "")
            leg.entry_timestamp = datetime.fromisoformat(leg_data["entry_timestamp"]) if leg_data.get("entry_timestamp") else None
            leg.exit_timestamp = datetime.fromisoformat(leg_data["exit_timestamp"]) if leg_data.get("exit_timestamp") else None
            leg.exit_reason = leg_data.get("exit_reason", "")
            leg.exit_price = leg_data.get("exit_price")
            legs[tag] = leg

        state = StrategyState(
            legs=legs,
            spot_price=data.get("spot_price", 0.0),
            spot_open=data.get("spot_open", 0.0),
            atm_strike=data.get("atm_strike", 0.0),
            fut_ltp=data.get("fut_ltp", 0.0),
            adjustments_today=data.get("adjustments_today", 0),
            total_trades_today=data.get("total_trades_today", 0),
            cumulative_daily_pnl=data.get("cumulative_daily_pnl", 0.0),
            entry_time=datetime.fromisoformat(data["entry_time"]) if data.get("entry_time") else None,
            last_adjustment_time=datetime.fromisoformat(data["last_adjustment_time"]) if data.get("last_adjustment_time") else None,
            trailing_stop_active=data.get("trailing_stop_active", False),
            trailing_stop_level=data.get("trailing_stop_level", 0.0),
            peak_pnl=data.get("peak_pnl", 0.0),
            prev_values=data.get("prev_values", {}),
            lifetime_adjustments=data.get("lifetime_adjustments", 0),
            current_profit_step=data.get("current_profit_step", -1),
            last_date=date.fromisoformat(data["last_date"]) if data.get("last_date") else None,
            entered_today=data.get("entered_today", False),
            minutes_to_exit=data.get("minutes_to_exit", 0),
        )
        # Restore adjustment history
        for evt_data in data.get("adjustment_history", []):
            try:
                state.adjustment_history.append(AdjustmentEvent(
                    timestamp=datetime.fromisoformat(evt_data["timestamp"]),
                    rule_name=evt_data["rule_name"],
                    action_type=evt_data["action_type"],
                    affected_legs=evt_data.get("affected_legs", []),
                    reason=evt_data.get("reason", ""),
                    market_data_snapshot=evt_data.get("market_data_snapshot", {}),
                ))
            except (KeyError, ValueError):
                pass
        state.entry_reason = data.get("entry_reason", "")
        state.exit_reason = data.get("exit_reason", "")
        return state