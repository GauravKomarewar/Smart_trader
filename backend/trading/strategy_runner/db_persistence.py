"""
Strategy DB Persistence — PostgreSQL-backed state for strategy runs.
====================================================================
Replaces JSON file persistence with database-backed persistence that is
restart-safe and supports full leg history (both active and closed).

Tables used:
  strategy_runs  — one row per strategy execution
  strategy_legs  — one row per leg (active or closed)
"""

import json
import logging
import uuid
from datetime import datetime, date
from typing import Optional, Dict, Any

from db.trading_db import trading_cursor, get_trading_conn

from .state import StrategyState, LegState
from .models import InstrumentType, OptionType, Side

logger = logging.getLogger(__name__)


class DBPersistence:
    """Database-backed persistence for a single strategy run."""

    def __init__(self, strategy_name: str, config: dict):
        self.strategy_name = strategy_name
        self.run_id: Optional[str] = None
        self._config = config

    # ── Create a new run ────────────────────────────────────────────────────

    def create_run(self, state: StrategyState) -> str:
        """Insert a new strategy_runs row, return run_id."""
        self.run_id = f"{self.strategy_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        identity = self._config.get("identity", {})
        with trading_cursor() as cur:
            cur.execute("""
                INSERT INTO strategy_runs
                    (run_id, strategy_name, config_name, symbol, exchange,
                     paper_mode, broker_config_id, status, config_snapshot,
                     entered_today, started_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """, (
                self.run_id,
                self.strategy_name,
                self._config.get("name", self.strategy_name),
                identity.get("underlying", "NIFTY"),
                identity.get("exchange", "NFO"),
                identity.get("paper_mode", True),
                self._config.get("_broker_config_id"),
                "RUNNING",
                json.dumps(self._config),
                state.entered_today,
            ))
        logger.info("DB: Created strategy run %s", self.run_id)
        return self.run_id

    # ── Resume an existing run ──────────────────────────────────────────────

    @classmethod
    def find_resumable_run(cls, strategy_name: str) -> Optional[str]:
        """Find the latest RUNNING run_id for this strategy (for restart recovery)."""
        with trading_cursor() as cur:
            cur.execute("""
                SELECT run_id FROM strategy_runs
                WHERE strategy_name = %s AND status = 'RUNNING'
                ORDER BY started_at DESC LIMIT 1
            """, (strategy_name,))
            row = cur.fetchone()
            return row["run_id"] if row else None

    def attach_run(self, run_id: str) -> None:
        """Attach to an existing run_id (for resume after restart)."""
        self.run_id = run_id
        logger.info("DB: Attached to existing run %s", self.run_id)

    # ── Save state (called every tick or periodically) ──────────────────────

    def save(self, state: StrategyState) -> None:
        """Upsert the strategy_runs row and all legs."""
        if not self.run_id:
            self.create_run(state)

        conn = get_trading_conn()
        try:
            cur = conn.cursor()

            # Update strategy_runs
            cur.execute("""
                UPDATE strategy_runs SET
                    spot_price = %s,
                    spot_open = %s,
                    atm_strike = %s,
                    fut_ltp = %s,
                    entered_today = %s,
                    entry_time = %s,
                    cumulative_daily_pnl = %s,
                    adjustments_today = %s,
                    total_trades_today = %s,
                    trailing_stop_active = %s,
                    trailing_stop_level = %s,
                    peak_pnl = %s,
                    current_profit_step = %s,
                    entry_reason = %s,
                    exit_reason = %s,
                    last_tick_at = NOW(),
                    updated_at = NOW()
                WHERE run_id = %s
            """, (
                state.spot_price,
                state.spot_open,
                state.atm_strike,
                state.fut_ltp,
                state.entered_today,
                state.entry_time,
                state.cumulative_daily_pnl,
                state.adjustments_today,
                state.total_trades_today,
                state.trailing_stop_active,
                state.trailing_stop_level,
                state.peak_pnl,
                state.current_profit_step,
                getattr(state, "entry_reason", ""),
                getattr(state, "exit_reason", ""),
                self.run_id,
            ))

            # Upsert all legs (both active and closed)
            for tag, leg in state.legs.items():
                cur.execute("""
                    INSERT INTO strategy_legs
                        (run_id, tag, symbol, instrument, option_type, strike,
                         expiry, side, qty, lot_size, entry_price, ltp, exit_price,
                         is_active, trading_symbol, order_id, command_id,
                         order_status, filled_qty, order_placed_at,
                         delta, gamma, theta, vega, iv, oi, volume,
                         group_name, label, entry_reason, exit_reason,
                         entry_timestamp, exit_timestamp, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, NOW())
                    ON CONFLICT (run_id, tag) DO UPDATE SET
                        ltp = EXCLUDED.ltp,
                        exit_price = EXCLUDED.exit_price,
                        is_active = EXCLUDED.is_active,
                        order_id = EXCLUDED.order_id,
                        order_status = EXCLUDED.order_status,
                        filled_qty = EXCLUDED.filled_qty,
                        delta = EXCLUDED.delta,
                        gamma = EXCLUDED.gamma,
                        theta = EXCLUDED.theta,
                        vega = EXCLUDED.vega,
                        iv = EXCLUDED.iv,
                        oi = EXCLUDED.oi,
                        volume = EXCLUDED.volume,
                        exit_reason = EXCLUDED.exit_reason,
                        exit_timestamp = EXCLUDED.exit_timestamp,
                        updated_at = NOW()
                """, (
                    self.run_id, tag, leg.symbol,
                    leg.instrument.value,
                    leg.option_type.value if leg.option_type else None,
                    leg.strike, leg.expiry, leg.side.value,
                    leg.qty, getattr(leg, "lot_size", 1),
                    leg.entry_price, leg.ltp,
                    getattr(leg, "exit_price", None),
                    leg.is_active, leg.trading_symbol,
                    leg.order_id, leg.command_id,
                    leg.order_status, leg.filled_qty,
                    leg.order_placed_at,
                    leg.delta, leg.gamma, leg.theta, leg.vega, leg.iv,
                    leg.oi, leg.volume,
                    leg.group, leg.label,
                    getattr(leg, "entry_reason", ""),
                    getattr(leg, "exit_reason", ""),
                    getattr(leg, "entry_timestamp", None),
                    getattr(leg, "exit_timestamp", None),
                ))

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ── Load state from DB (for resume) ─────────────────────────────────────

    def load(self) -> Optional[StrategyState]:
        """Load state from the DB for the attached run_id. Returns None if not found."""
        if not self.run_id:
            return None

        with trading_cursor() as cur:
            # Load run
            cur.execute("SELECT * FROM strategy_runs WHERE run_id = %s", (self.run_id,))
            run = cur.fetchone()
            if not run:
                return None

            # Load legs
            cur.execute(
                "SELECT * FROM strategy_legs WHERE run_id = %s ORDER BY created_at",
                (self.run_id,),
            )
            leg_rows = cur.fetchall()

        # Reconstruct StrategyState
        legs: Dict[str, LegState] = {}
        for lr in leg_rows:
            leg = LegState(
                tag=lr["tag"],
                symbol=lr["symbol"],
                instrument=InstrumentType(lr["instrument"]),
                option_type=OptionType(lr["option_type"]) if lr["option_type"] else None,
                strike=lr["strike"],
                expiry=lr["expiry"] or "",
                side=Side(lr["side"]),
                qty=lr["qty"],
                entry_price=float(lr["entry_price"] or 0),
                ltp=float(lr["ltp"] or 0),
                is_active=lr["is_active"],
                group=lr["group_name"] or "",
                label=lr["label"] or "",
                oi=lr["oi"] or 0,
                volume=lr["volume"] or 0,
                trading_symbol=lr["trading_symbol"] or "",
                order_id=lr["order_id"],
                command_id=lr["command_id"],
                order_status=lr["order_status"] or "PENDING",
                filled_qty=lr["filled_qty"] or 0,
                order_placed_at=lr["order_placed_at"],
                lot_size=lr["lot_size"] or 1,
                delta=float(lr["delta"] or 0),
                gamma=float(lr["gamma"] or 0),
                theta=float(lr["theta"] or 0),
                vega=float(lr["vega"] or 0),
                iv=float(lr["iv"] or 0),
            )
            leg.entry_reason = lr["entry_reason"] or ""
            leg.exit_reason = lr["exit_reason"] or ""
            leg.entry_timestamp = lr["entry_timestamp"]
            leg.exit_timestamp = lr["exit_timestamp"]
            leg.exit_price = float(lr["exit_price"]) if lr["exit_price"] is not None else None
            legs[lr["tag"]] = leg

        state = StrategyState(
            legs=legs,
            spot_price=float(run["spot_price"] or 0),
            spot_open=float(run["spot_open"] or 0),
            atm_strike=float(run["atm_strike"] or 0),
            fut_ltp=float(run["fut_ltp"] or 0),
            entered_today=run["entered_today"],
            entry_time=run["entry_time"],
            cumulative_daily_pnl=float(run["cumulative_daily_pnl"] or 0),
            adjustments_today=run["adjustments_today"] or 0,
            total_trades_today=run["total_trades_today"] or 0,
            trailing_stop_active=run["trailing_stop_active"],
            trailing_stop_level=float(run["trailing_stop_level"] or 0),
            peak_pnl=float(run["peak_pnl"] or 0),
            current_profit_step=run["current_profit_step"] or -1,
        )
        state.entry_reason = run["entry_reason"] or ""
        state.exit_reason = run["exit_reason"] or ""

        logger.info(
            "DB: Loaded run %s — %d legs (%d active)",
            self.run_id, len(legs),
            sum(1 for l in legs.values() if l.is_active),
        )
        return state

    # ── Mark run as stopped ─────────────────────────────────────────────────

    def mark_stopped(self, reason: str = "") -> None:
        """Set the run status to STOPPED."""
        if not self.run_id:
            return
        with trading_cursor() as cur:
            cur.execute("""
                UPDATE strategy_runs
                SET status = 'STOPPED', exit_reason = %s,
                    stopped_at = NOW(), updated_at = NOW()
                WHERE run_id = %s
            """, (reason, self.run_id))
        logger.info("DB: Marked run %s as STOPPED (%s)", self.run_id, reason or "normal")

    def mark_error(self, error: str) -> None:
        """Set the run status to ERROR."""
        if not self.run_id:
            return
        with trading_cursor() as cur:
            cur.execute("""
                UPDATE strategy_runs
                SET status = 'ERROR', exit_reason = %s,
                    stopped_at = NOW(), updated_at = NOW()
                WHERE run_id = %s
            """, (error[:500], self.run_id))
        logger.info("DB: Marked run %s as ERROR", self.run_id)

    # ── Event logging (decision audit trail) ────────────────────────────────

    def log_event(self, event_type: str, reason: str = "",
                  leg_tag: str = None, pnl: float = 0.0,
                  spot: float = 0.0, details: dict = None) -> None:
        """Insert a row into strategy_events for full decision audit trail."""
        if not self.run_id:
            return
        try:
            with trading_cursor() as cur:
                cur.execute("""
                    INSERT INTO strategy_events
                        (run_id, event_type, leg_tag, reason, details, pnl_at_event, spot_at_event)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    self.run_id, event_type, leg_tag, reason,
                    json.dumps(details or {}), pnl, spot,
                ))
        except Exception as e:
            logger.warning("DB: event log failed: %s", e)

    def snapshot_pnl(self, pnl: float, spot: float,
                     active_legs: int = 0, net_delta: float = 0.0) -> None:
        """Insert a PnL snapshot for charting (called every minute)."""
        if not self.run_id:
            return
        try:
            with trading_cursor() as cur:
                cur.execute("""
                    INSERT INTO pnl_snapshots
                        (run_id, pnl, spot_price, active_legs, net_delta)
                    VALUES (%s, %s, %s, %s, %s)
                """, (self.run_id, pnl, spot, active_legs, net_delta))
        except Exception as e:
            logger.warning("DB: pnl snapshot failed: %s", e)
