#!/usr/bin/env python3
"""
strategy_executor_service.py — Drop‑in replacement using Universal Engine
==========================================================================
Implements the same interface as the old service, but uses the new engine.
"""

import json
import logging
import sqlite3
import threading
import time
import copy
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from .models import (
    InstrumentType, OptionType, Side, OrderType, StrikeMode, StrikeConfig
)
from .state import StrategyState, LegState
from .condition_engine import ConditionEngine
from .market_reader import MarketReader
from .entry_engine import EntryEngine
from .adjustment_engine import AdjustmentEngine
from .exit_engine import ExitEngine
from .reconciliation import BrokerReconciliation
from .persistence import StatePersistence
from scripts.unified_scriptmaster import requires_limit_order

logger = logging.getLogger("STRATEGY_EXECUTOR_SERVICE")

@dataclass
class ExecutionState:
    """
    execution snapshot used by dashboard/runtime recovery paths.
    """
    strategy_name: str
    run_id: str
    has_position: bool = False
    entry_timestamp: float = 0.0

    ce_symbol: Optional[str] = None
    ce_side: Optional[str] = None
    ce_qty: int = 0
    ce_entry_price: float = 0.0
    ce_strike: float = 0.0
    ce_delta: float = 0.0

    pe_symbol: Optional[str] = None
    pe_side: Optional[str] = None
    pe_qty: int = 0
    pe_entry_price: float = 0.0
    pe_strike: float = 0.0
    pe_delta: float = 0.0

    updated_at: float = 0.0


class StateManager:
    """
    Minimal SQLite-backed state store for strategy lifecycle state.
    """

    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path, timeout=5)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        conn = self._connect()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS execution_states (
                    strategy_name TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS monitor_leg_rows (
                    strategy_name TEXT NOT NULL,
                    leg_key TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (strategy_name, leg_key)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS completed_monitor_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_name TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    archived_at REAL NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_completed_monitor_history_archived_at
                ON completed_monitor_history(archived_at DESC)
                """
            )
            conn.commit()
        finally:
            conn.close()

    def save(self, state: ExecutionState):
        state.updated_at = time.time()
        payload = json.dumps(asdict(state), default=str)
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO execution_states(strategy_name, payload, updated_at)
                VALUES(?, ?, ?)
                ON CONFLICT(strategy_name) DO UPDATE SET
                    payload=excluded.payload,
                    updated_at=excluded.updated_at
                """,
                (state.strategy_name, payload, state.updated_at),
            )
            conn.commit()
        finally:
            conn.close()

    def load(self, strategy_name: str) -> Optional[ExecutionState]:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT payload FROM execution_states WHERE strategy_name = ?",
                (strategy_name,),
            ).fetchone()
            if not row:
                return None
            data = json.loads(row["payload"])
            return ExecutionState(**data)
        finally:
            conn.close()

    def delete(self, strategy_name: str):
        conn = self._connect()
        try:
            conn.execute("DELETE FROM execution_states WHERE strategy_name = ?", (strategy_name,))
            conn.commit()
        finally:
            conn.close()

    def list_all(self) -> List[str]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT strategy_name FROM execution_states ORDER BY strategy_name"
            ).fetchall()
            return [r["strategy_name"] for r in rows]
        finally:
            conn.close()

    def load_monitor_snapshot(self, strategy_name: str) -> Dict[str, Dict[str, Any]]:
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                SELECT leg_key, payload
                FROM monitor_leg_rows
                WHERE strategy_name = ?
                ORDER BY updated_at DESC
                """,
                (strategy_name,),
            ).fetchall()
            out: Dict[str, Dict[str, Any]] = {}
            for row in rows:
                try:
                    out[str(row["leg_key"])] = json.loads(row["payload"])
                except Exception:
                    continue
            return out
        finally:
            conn.close()

    def save_monitor_snapshot(self, strategy_name: str, cache: Dict[str, Dict[str, Any]]) -> None:
        conn = self._connect()
        try:
            keys = list(cache.keys())
            conn.execute("BEGIN")
            if not keys:
                conn.execute(
                    "DELETE FROM monitor_leg_rows WHERE strategy_name = ?",
                    (strategy_name,),
                )
            else:
                placeholders = ",".join("?" for _ in keys)
                conn.execute(
                    f"""
                    DELETE FROM monitor_leg_rows
                    WHERE strategy_name = ?
                      AND leg_key NOT IN ({placeholders})
                    """,
                    [strategy_name, *keys],
                )
                for leg_key, row in cache.items():
                    updated_at = time.time()
                    try:
                        raw_updated = row.get("updated_at")
                        if raw_updated:
                            updated_at = datetime.fromisoformat(str(raw_updated)).timestamp()
                    except Exception:
                        pass
                    conn.execute(
                        """
                        INSERT INTO monitor_leg_rows(strategy_name, leg_key, payload, updated_at)
                        VALUES(?, ?, ?, ?)
                        ON CONFLICT(strategy_name, leg_key) DO UPDATE SET
                            payload=excluded.payload,
                            updated_at=excluded.updated_at
                        """,
                        (
                            strategy_name,
                            str(leg_key),
                            json.dumps(row, default=str),
                            float(updated_at),
                        ),
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def clear_monitor_snapshot(self, strategy_name: str) -> None:
        conn = self._connect()
        try:
            conn.execute(
                "DELETE FROM monitor_leg_rows WHERE strategy_name = ?",
                (strategy_name,),
            )
            conn.commit()
        finally:
            conn.close()

    def append_completed_monitor_history(self, history_row: Dict[str, Any], max_rows: int = 100) -> None:
        conn = self._connect()
        try:
            archived_at = time.time()
            try:
                raw_archived = history_row.get("archived_at")
                if raw_archived:
                    archived_at = datetime.fromisoformat(str(raw_archived)).timestamp()
            except Exception:
                pass
            conn.execute(
                """
                INSERT INTO completed_monitor_history(strategy_name, payload, archived_at)
                VALUES (?, ?, ?)
                """,
                (
                    str(history_row.get("strategy_name") or ""),
                    json.dumps(history_row, default=str),
                    float(archived_at),
                ),
            )
            if int(max_rows) > 0:
                conn.execute(
                    """
                    DELETE FROM completed_monitor_history
                    WHERE id NOT IN (
                        SELECT id
                        FROM completed_monitor_history
                        ORDER BY archived_at DESC, id DESC
                        LIMIT ?
                    )
                    """,
                    (int(max_rows),),
                )
            conn.commit()
        finally:
            conn.close()

    def get_completed_monitor_history(self, limit: int = 20) -> List[Dict[str, Any]]:
        conn = self._connect()
        try:
            normalized_limit = max(1, int(limit or 20))
            rows = conn.execute(
                """
                SELECT payload
                FROM completed_monitor_history
                ORDER BY archived_at DESC, id DESC
                LIMIT ?
                """,
                (normalized_limit,),
            ).fetchall()
            out: List[Dict[str, Any]] = []
            for row in rows:
                try:
                    out.append(json.loads(row["payload"]))
                except Exception:
                    continue
            return out
        finally:
            conn.close()

    def delete_completed_monitor_history(self, strategy_name: Optional[str] = None, archived_at: Optional[str] = None) -> int:
        """Delete completed monitor history entries. If strategy_name and archived_at given, delete one entry. If only strategy_name, delete all for that strategy. If neither, delete all."""
        conn = self._connect()
        try:
            if strategy_name and archived_at:
                # The archived_at DB column is stored as a Unix timestamp float.
                # The frontend passes back an ISO-string from the payload JSON
                # (e.g. "2026-03-09T15:33:36.123456"). SQLite's type affinity
                # rules mean a REAL column value never equals a TEXT literal,
                # so the WHERE clause matched 0 rows every time.
                # Convert the incoming value to float before comparing.
                archived_at_float: Optional[float] = None
                try:
                    archived_at_float = float(archived_at)  # already a float string
                except (ValueError, TypeError):
                    pass
                if archived_at_float is None:
                    try:
                        archived_at_float = datetime.fromisoformat(str(archived_at)).timestamp()
                    except Exception:
                        pass
                if archived_at_float is not None:
                    cursor = conn.execute(
                        "DELETE FROM completed_monitor_history WHERE strategy_name = ? AND ABS(archived_at - ?) < 1.0",
                        (strategy_name, archived_at_float),
                    )
                else:
                    # Cannot parse archived_at; delete all entries for this strategy
                    cursor = conn.execute(
                        "DELETE FROM completed_monitor_history WHERE strategy_name = ?",
                        (strategy_name,),
                    )
            elif strategy_name:
                cursor = conn.execute(
                    "DELETE FROM completed_monitor_history WHERE strategy_name = ?",
                    (strategy_name,),
                )
            else:
                cursor = conn.execute("DELETE FROM completed_monitor_history")
            conn.commit()
            return cursor.rowcount
        finally:
            conn.close()

class StrategyExecutorService:
    """
    Service that manages multiple strategies using the Universal Engine.
    """

    def __init__(self, bot, state_db_path: str):
        self.bot = bot
        self.state_db_path = state_db_path
        self.state_mgr = StateManager(state_db_path)
        self._strategies: Dict[str, Dict] = {}          # name -> config
        self._executors: Dict[str, 'PerStrategyExecutor'] = {}
        self._exec_states: Dict[str, StrategyState] = {}  # name -> live state
        self._lock = threading.RLock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._mode_change_dict_lock = threading.Lock()
        self._mode_change_lock: Dict[str, threading.Lock] = {}
        self._monitor_cache: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._completed_monitor_history: List[Dict[str, Any]] = []
        self._max_completed_monitor_history = 100
        self._timed_out_strategies: Dict[str, int] = {}  # name -> consecutive timeout count
        try:
            self._completed_monitor_history = self.state_mgr.get_completed_monitor_history(
                limit=self._max_completed_monitor_history
            )
        except Exception as e:
            logger.debug("Could not load completed monitor history from DB: %s", e)

    def _ensure_strategy_chain(self, name: str, config: dict):
        """Auto-load the option chain required by a strategy if not already active."""
        sup = getattr(self.bot, "option_supervisor", None)
        if sup is None:
            logger.warning("No option_supervisor on bot — cannot auto-load chain for %s", name)
            return

        identity = config.get("identity", {}) or {}
        exchange = str(identity.get("exchange", "NFO")).strip().upper()
        symbol = str(identity.get("underlying", "")).strip().upper()
        if not symbol:
            return

        # Determine the expiry the strategy needs
        expiry = None
        schedule_mode = str(
            (config.get("schedule", {}) or {}).get("expiry_mode", "weekly_current")
        ).strip() or "weekly_current"
        if schedule_mode == "custom":
            logger.warning(
                "Strategy %s: schedule.expiry_mode=custom is deprecated; falling back to weekly_current",
                name,
            )
            schedule_mode = "weekly_current"

        try:
            reader = MarketReader(exchange=exchange, symbol=symbol, max_stale_seconds=30)
            expiry = reader.resolve_expiry_mode(schedule_mode)
            logger.info(
                "Strategy %s: resolved expiry %s from MarketReader (mode=%s) for %s:%s",
                name,
                expiry,
                schedule_mode,
                exchange,
                symbol,
            )
        except Exception as e:
            logger.warning(
                "Strategy %s: MarketReader expiry lookup failed for %s:%s: %s",
                name,
                exchange,
                symbol,
                e,
            )

        if not expiry:
            # If no DB exists yet, fall back to ScriptMaster expiry lookup
            try:
                from scripts.unified_scriptmaster import options_expiry as sm_options_expiry
                expiries = sm_options_expiry(symbol, exchange) or []
                if expiries:
                    expiry = expiries[0]  # nearest expiry
                    logger.info(
                        "Strategy %s: resolved expiry %s from ScriptMaster for %s:%s",
                        name,
                        expiry,
                        exchange,
                        symbol,
                    )
            except Exception as e:
                logger.warning(
                    "Strategy %s: ScriptMaster expiry lookup failed for %s:%s: %s",
                    name,
                    exchange,
                    symbol,
                    e,
                )

        if not expiry:
            logger.warning("Strategy %s: could not determine expiry, skipping auto-load", name)
            return

        key = f"{exchange}:{symbol}:{expiry}"
        # Check if already active
        with sup._lock:
            if key in sup._chains:
                logger.debug("Strategy %s: chain %s already active", name, key)
                return

        logger.info("Strategy %s: auto-loading option chain %s", name, key)
        ok = sup.ensure_chain(exchange=exchange, symbol=symbol, expiry=expiry, source="strategy")
        if ok:
            logger.info("Strategy %s: ✅ chain %s loaded successfully", name, key)
        else:
            logger.error("Strategy %s: ❌ failed to auto-load chain %s", name, key)

    def register_strategy(self, name: str, config_path: str):
        """Register a strategy with the service."""
        with self._lock:
            if name in self._strategies:
                logger.warning(f"Strategy {name} already registered, overwriting")
            # Load config – assume it's valid (validation should be done by caller)
            with open(config_path, 'r') as f:
                config = json.load(f)

            # Auto-load required option chain if not already active
            self._ensure_strategy_chain(name, config)

            self._strategies[name] = config
            # Create per‑strategy executor
            executor = PerStrategyExecutor(
                name=name,
                config=config,
                bot=self.bot,
                state_db_path=self.state_db_path
            )
            self._executors[name] = executor
            self._exec_states[name] = executor.state
            persisted_cache = {}
            try:
                persisted_cache = self.state_mgr.load_monitor_snapshot(name)
            except Exception as e:
                logger.debug("Could not load monitor cache for %s: %s", name, e)
            # BUG-FIX: Drop legs from previous days when loading monitor cache.
            # If a strategy was never unregistered (e.g. it hit SL and stayed
            # registered waiting for EOD) its old closed legs from yesterday
            # persist in monitor_leg_rows and would mix with today's new run.
            # Keep only legs opened today (or still ACTIVE from today).
            today_prefix = datetime.now().date().isoformat()  # "YYYY-MM-DD"
            if isinstance(persisted_cache, dict):
                persisted_cache = {
                    k: v for k, v in persisted_cache.items()
                    if str(v.get("opened_at", ""))[:10] >= today_prefix
                }
                # Also clear stale rows from DB so they don't reload next restart
                try:
                    self.state_mgr.save_monitor_snapshot(name, persisted_cache)
                except Exception as _e:
                    logger.debug("Could not prune stale monitor rows for %s: %s", name, _e)
            self._monitor_cache[name] = persisted_cache if isinstance(persisted_cache, dict) else {}
            logger.info(f"Registered strategy: {name}")

    def unregister_strategy(self, name: str):
        """Remove a strategy from the service."""
        with self._lock:
            self._executors.pop(name, None)
            self._strategies.pop(name, None)
            self._exec_states.pop(name, None)
            self._monitor_cache.pop(name, None)
            try:
                self.state_mgr.clear_monitor_snapshot(name)
            except Exception as e:
                logger.debug("Could not clear persisted monitor snapshot for %s: %s", name, e)
            # Clear execution guard positions so stale entries don't cause
            # cross-strategy conflicts on the next run of this strategy.
            try:
                guard = getattr(self.bot, "execution_guard", None)
                if guard is not None:
                    guard.force_close_strategy(name)
            except Exception as e:
                logger.warning("Could not clear execution guard for %s: %s", name, e)
            logger.info(f"Strategy unregistered: {name}")

    # ------------------------------------------------------------------
    # CRASH-RECOVERY: AUTO-RESUME STRATEGIES
    # ------------------------------------------------------------------

    def auto_resume_strategies(self) -> List[str]:
        """
        Scan saved_configs/ for strategies that were RUNNING before a
        crash/restart and re-register them so monitoring, adjustments,
        and exits continue from the persisted state.

        A strategy is eligible for auto-resume when ALL of:
          1. Its config JSON has ``"status": "RUNNING"``
          2. A state file exists with at least one active (FILLED) leg
          3. The state was persisted today (not stale from a previous day)

        After re-registration the executor picks up the persisted
        StrategyState (with legs, entered_today, adjustment history, etc.)
        and the normal tick loop resumes exit/adjustment monitoring.

        Returns:
            List of strategy names that were successfully resumed.
        """
        config_dir = (
            Path(__file__).resolve().parent / "saved_configs"
        )
        if not config_dir.exists():
            return []

        state_base = Path(self.state_db_path).parent
        today = datetime.now().date()
        resumed: List[str] = []

        for cfg_path in sorted(config_dir.glob("*.json")):
            if cfg_path.name.endswith(".schema.json"):
                continue
            try:
                config = json.loads(cfg_path.read_text(encoding="utf-8"))
            except Exception:
                continue

            status = str(config.get("status", "")).strip().upper()
            if status != "RUNNING":
                continue

            strategy_key = cfg_path.stem

            # Already running — skip
            if strategy_key in self._strategies:
                continue

            # Check for valid state file with active legs
            state_file = state_base / f"{strategy_key}_state.pkl"
            if not state_file.exists():
                logger.debug("auto_resume: no state file for %s", strategy_key)
                continue

            try:
                state_data = json.loads(state_file.read_text(encoding="utf-8"))
            except Exception as e:
                logger.warning("auto_resume: could not read state for %s: %s", strategy_key, e)
                continue

            # Must have at least one active, filled leg
            legs_raw = state_data.get("legs") or {}
            has_active = False
            for leg in legs_raw.values():
                if not isinstance(leg, dict):
                    continue
                if bool(leg.get("is_active")) and str(leg.get("order_status", "")).upper() == "FILLED":
                    has_active = True
                    break

            if not has_active:
                logger.debug("auto_resume: no active legs for %s — skipping", strategy_key)
                continue

            # Staleness check: state must be from today
            entry_time_raw = state_data.get("entry_time")
            if entry_time_raw:
                try:
                    entry_date = datetime.fromisoformat(str(entry_time_raw)).date()
                    if entry_date != today:
                        logger.info(
                            "auto_resume: %s entry_time=%s is not today (%s) — skipping",
                            strategy_key, entry_date, today,
                        )
                        continue
                except (ValueError, TypeError):
                    pass

            # Re-register strategy
            try:
                logger.warning(
                    "♻️ AUTO-RESUME | Recovering strategy '%s' with active legs from state file",
                    strategy_key,
                )
                self.register_strategy(name=strategy_key, config_path=str(cfg_path))
                resumed.append(strategy_key)
            except Exception as e:
                logger.error("auto_resume: failed to register %s: %s", strategy_key, e)

        # Force immediate broker reconciliation for all resumed strategies
        if resumed:
            broker_view = getattr(self.bot, "broker_view", None)
            for name in resumed:
                executor = self._executors.get(name)
                if executor and broker_view and not executor._resolve_test_mode():
                    try:
                        warnings = executor.reconciliation.reconcile_from_broker(broker_view)
                        if warnings:
                            logger.warning(
                                "♻️ AUTO-RESUME RECONCILE [%s]: %d mismatch(es) — %s",
                                name, len(warnings), "; ".join(warnings),
                            )
                        else:
                            logger.info("♻️ AUTO-RESUME RECONCILE [%s]: broker in sync ✅", name)
                        # Persist reconciled state immediately
                        executor.persistence.save(executor.state, str(executor.state_file))
                    except Exception as e:
                        logger.error("♻️ AUTO-RESUME RECONCILE [%s] failed: %s", name, e)

            # Send telegram notification
            telegram = getattr(self.bot, "telegram", None)
            if telegram:
                try:
                    msg = (
                        f"<b>♻️ STRATEGY AUTO-RESUME</b>\n"
                        f"Strategies recovered: {len(resumed)}\n"
                        f"Names: {', '.join(resumed)}\n"
                        f"Time: {datetime.now().strftime('%H:%M:%S')}\n\n"
                        f"Monitoring, adjustments, and exits will continue "
                        f"from last persisted state."
                    )
                    telegram.send_message(msg, parse_mode="HTML")
                except Exception:
                    pass

            logger.warning(
                "♻️ AUTO-RESUME COMPLETE | %d strategy(ies) recovered: %s",
                len(resumed), resumed,
            )

        return resumed

    @staticmethod
    def _mark_config_status(strategy_key: str, status: str) -> None:
        """Update the ``status`` field in a strategy's saved config JSON."""
        config_dir = Path(__file__).resolve().parent / "saved_configs"
        cfg_path = config_dir / f"{strategy_key}.json"
        if not cfg_path.exists():
            return
        try:
            cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
            cfg["status"] = status
            cfg["status_updated_at"] = datetime.now().isoformat()
            tmp = str(cfg_path) + ".tmp"
            with open(tmp, "w", encoding="utf-8", newline="\n") as f:
                json.dump(cfg, f, indent=2)
                f.write("\n")
                f.flush()
            import os
            os.replace(tmp, str(cfg_path))
        except Exception as e:
            logger.debug("Could not update config status for %s: %s", strategy_key, e)

    def start(self):
        """Start the background processing thread."""
        if self._running:
            return
        # Expire stale CREATED orders from previous days to prevent
        # historical symbols from mixing with today's positions.
        self._expire_stale_orders()
        # Remove ACTIVE monitor rows from previous days for strategies that are
        # not currently registered. These are orphan entries left behind when
        # the service crashed or the strategy was never cleanly unregistered.
        self._cleanup_stale_monitor_rows()
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info("StrategyExecutorService started")

    def _expire_stale_orders(self):
        """Mark stale strategy-generated CREATED orders from previous days as EXPIRED.

        Only affects orders with source='STRATEGY' so that system/positional
        orders created by other subsystems are left untouched.
        """
        try:
            from db.trading_db import get_trading_conn
            today = datetime.now().strftime("%Y-%m-%d")
            conn = get_trading_conn()
            cur = conn.cursor()
            cur.execute(
                "UPDATE orders SET status = 'EXPIRED', updated_at = NOW() "
                "WHERE status = 'CREATED' AND created_at < %s "
                "AND source = 'STRATEGY'",
                (today,),
            )
            count = cur.rowcount
            conn.commit()
            conn.close()
            if count > 0:
                logger.warning("Expired %d stale CREATED strategy orders from previous days", count)
        except Exception as e:
            logger.debug("Could not expire stale orders: %s", e)

    def _cleanup_stale_monitor_rows(self):
        """Delete ACTIVE monitor_leg_rows from previous days for unregistered strategies.

        Called at service start. Fixes two classes of orphan data:
        1. Strategies that hit stop-loss and stayed registered without being
           unregistered — their old CLOSED legs from yesterday mix with today's run.
        2. Strategies that were unregistered but `clear_monitor_snapshot` was
           never reached (e.g. crash mid-unregister).

        Only removes rows where opened_at < today so same-day CLOSED legs
        (legitimate lifecycle records) are preserved.
        """
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            conn = self.state_mgr._connect()
            try:
                # Extract opened_at date from JSON payload.
                # json.dumps uses ": " (colon + space), so the value starts
                # at offset +14 after the key: '"opened_at"' (11) + ': "' (3) = 14
                cursor = conn.execute(
                    """
                    DELETE FROM monitor_leg_rows
                    WHERE substr(payload, instr(payload, '"opened_at"') + 14, 10) < ?
                    """,
                    (today,),
                )
                count = cursor.rowcount
                conn.commit()
                if count > 0:
                    logger.warning(
                        "STARTUP_CLEANUP | Deleted %d stale monitor leg rows from previous days",
                        count,
                    )
            finally:
                conn.close()
        except Exception as e:
            logger.debug("Could not clean up stale monitor rows: %s", e)

    def _cancel_all_pending_orders(self):
        """Cancel / expire ALL lingering CREATED and SENT_TO_BROKER orders.

        Called:
        - In ``stop()`` before the service shuts down.
        - Automatically at 23:45 IST via ``_run_loop`` nightly cleanup.

        This is the final safety net: any orders that were not dispatched
        or got stuck are cleaned up so they don't leak into the next day.
        """
        try:
            from db.trading_db import get_trading_conn
            now = datetime.now()
            conn = get_trading_conn()
            cur = conn.cursor()
            cur.execute(
                "UPDATE orders SET status = 'EXPIRED', "
                "  updated_at = NOW(), "
                "  tag = COALESCE(tag, '') || '|EOD_CLEANUP' "
                "WHERE status IN ('CREATED', 'SENT_TO_BROKER')",
            )
            count = cur.rowcount
            conn.commit()
            conn.close()
            if count > 0:
                logger.warning(
                    "EOD_CLEANUP | expired %d pending orders (CREATED/SENT_TO_BROKER) at %s",
                    count, now.strftime("%H:%M:%S"),
                )
            else:
                logger.info("EOD_CLEANUP | no pending orders to expire at %s", now.strftime("%H:%M:%S"))
        except Exception as e:
            logger.warning("EOD_CLEANUP failed: %s", e)

    def stop(self):
        """Stop the background thread.  Expires leftover orders first."""
        # ── EOD cleanup: Cancel/expire all lingering CREATED / SENT_TO_BROKER ──
        try:
            self._cancel_all_pending_orders()
        except Exception as e:
            logger.warning("EOD order cleanup during stop() failed: %s", e)
        self._running = False
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("StrategyExecutorService stopped")

    # Seconds to wait for a single strategy tick before declaring it timed-out
    PROCESS_TICK_TIMEOUT = 60

    def _run_loop(self):
        """Main loop: iterate over all strategies and process each concurrently."""
        import concurrent.futures

        # Track nightly cleanup so it fires at most once per day.
        _eod_cleanup_done_date = None
        
        # Use a thread pool with a reasonable max workers
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as thread_pool:
            while self._running and not self._stop_event.is_set():

                # ── Nightly EOD order cleanup at 23:45 ──
                now = datetime.now()
                if now.hour == 23 and now.minute >= 45 and _eod_cleanup_done_date != now.date():
                    try:
                        self._cancel_all_pending_orders()
                        _eod_cleanup_done_date = now.date()
                    except Exception as e:
                        logger.warning("Nightly EOD cleanup failed: %s", e)

                with self._lock:
                    names = list(self._executors.keys())
                
                completed_names: List[str] = []
                
                def _process_one(name: str):
                    with self._lock:
                        executor = self._executors.get(name)
                    if executor:
                        try:
                            executor.process_tick()
                            with executor._tick_lock:
                                if getattr(executor, "cycle_completed", False) and not executor.state.any_leg_active:
                                    return name
                        except Exception as e:
                            logger.exception(f"Error processing strategy {name}: {e}")
                    return None
                
                futures = {thread_pool.submit(_process_one, name): name for name in names}
                for future in concurrent.futures.as_completed(futures):
                    if self._stop_event.is_set():
                        break
                    strategy_name = futures[future]
                    try:
                        res = future.result(timeout=self.PROCESS_TICK_TIMEOUT)
                    except concurrent.futures.TimeoutError:
                        logger.warning("Strategy '%s' timed out after %s seconds during process_tick",
                                       strategy_name, self.PROCESS_TICK_TIMEOUT)
                        # cancel() only prevents queued tasks; it cannot interrupt a running thread.
                        self._timed_out_strategies[strategy_name] = self._timed_out_strategies.get(strategy_name, 0) + 1
                        continue
                    except Exception as e:
                        logger.exception("Unexpected error collecting result for strategy '%s': %s",
                                         strategy_name, e)
                        continue
                    if res:
                        completed_names.append(res)
                        
                for name in completed_names:
                    try:
                        # BUG FIX: Do NOT unregister strategy while it has pending
                        # EXIT orders (CREATED / SENT_TO_BROKER).  Unregistering
                        # removes the strategy from _strategies, so get_strategy_mode()
                        # can no longer return "MOCK".  If the OrderWatcher dispatches
                        # remaining EXIT legs after unregistration, they would fall
                        # through to the LIVE broker — catastrophic for mock strategies.
                        if self._has_pending_exit_orders(name):
                            logger.info(
                                "UNREGISTER_DEFERRED | strategy=%s | reason=pending_exit_orders",
                                name,
                            )
                            continue
                        self._archive_completed_strategy(name)
                        self._mark_config_status(name, "COMPLETED")
                        self.unregister_strategy(name)
                        logger.info(f"Strategy cycle completed and auto-stopped: {name}")
                    except Exception as e:
                        logger.error(f"Failed to auto-stop completed strategy {name}: {e}")
                        
                time.sleep(2)  # same as old service

    def acquire_mode_change_lock(self, strategy_name: str) -> threading.Lock:
        with self._mode_change_dict_lock:
            if strategy_name not in self._mode_change_lock:
                self._mode_change_lock[strategy_name] = threading.Lock()
            return self._mode_change_lock[strategy_name]

    def _has_pending_exit_orders(self, strategy_name: str) -> bool:
        """Check whether any EXIT orders for this strategy are still pending dispatch.

        Returns True if there are CREATED or SENT_TO_BROKER EXIT orders
        *from today*, meaning the OrderWatcher has not finished processing
        all exit legs.  Orders from previous days are ignored — they will
        be expired by the daily reset.
        """
        try:
            repo = getattr(self.bot, "order_repo", None)
            if repo is None:
                return False
            today_str = datetime.now().strftime("%Y-%m-%d")
            open_orders = repo.get_open_orders_by_strategy(strategy_name) or []
            for order in open_orders:
                exec_type = str(getattr(order, "execution_type", "") or "").upper()
                status = str(getattr(order, "status", "") or "").upper()
                if exec_type != "EXIT" or status not in ("CREATED", "SENT_TO_BROKER"):
                    continue
                # Only count today's orders — stale orders from previous days
                # should not prevent unregistration.
                created = str(getattr(order, "created_at", "") or "")
                if created[:10] < today_str:
                    continue
                return True
            return False
        except Exception as e:
            logger.warning("_has_pending_exit_orders check failed for %s: %s", strategy_name, e)
            # If we can't check, err on the side of caution — keep registered
            return True

    def notify_fill(self, strategy_name: str, **kwargs):
        """Delegate fill notification to the appropriate executor."""
        executor = self._executors.get(strategy_name)
        if executor:
            executor.notify_fill(**kwargs)
        else:
            logger.warning(f"Fill notification for unknown strategy: {strategy_name}")

    def _validate_mode_change_allowed(self, strategy_name: str) -> Tuple[bool, str]:
        """Return (allowed, reason)."""
        if self.has_position(strategy_name):
            return False, "Strategy has active positions"
        return True, ""

    def has_position(self, strategy_name: str) -> bool:
        state = self._exec_states.get(strategy_name)
        if not state:
            return False
        return state.any_leg_active

    def _is_paper_mode(self, config: Dict[str, Any]) -> bool:
        identity = config.get("identity", {}) or {}
        return bool(
            identity.get("paper_mode")
            or identity.get("test_mode")
        )

    # BUG-014 FIX: get_strategy_mode() was called by router but never existed in service.
    # The router used hasattr guard so it silently fell back to "LIVE" for all strategies —
    # paper-mode strategies appeared as LIVE in the dashboard.
    def get_strategy_mode(self, strategy_name: str) -> str:
        """Return 'MOCK' or 'LIVE' based on strategy config paper_mode / test_mode flags."""
        config = self._strategies.get(strategy_name, {})
        return "MOCK" if self._is_paper_mode(config) else "LIVE"

    def get_strategy_leg_monitor_snapshot(self) -> Dict[str, Dict[str, Any]]:
        """
        Build monitor-ready leg snapshot from current executor state.
        """
        snapshot: Dict[str, Dict[str, Any]] = {}
        now_iso = datetime.now().isoformat()
        with self._lock:
            names = list(self._exec_states.keys())
            for strategy_name in names:
                state = self._exec_states.get(strategy_name)
                if state is None:
                    continue

                config = self._strategies.get(strategy_name, {}) or {}
                identity = config.get("identity", {}) or {}
                mode = "MOCK" if self._is_paper_mode(config) else "LIVE"
                cache = self._monitor_cache.setdefault(strategy_name, {})
                executor = self._executors.get(strategy_name)
                seen_keys: set[str] = set()

                for leg in list((getattr(state, "legs", {}) or {}).values()):
                    side_val = getattr(getattr(leg, "side", None), "value", getattr(leg, "side", ""))
                    side = str(side_val or "").upper()
                    qty = int(getattr(leg, "qty", 0) or 0)
                    order_qty = qty
                    try:
                        if executor is not None and hasattr(executor, "_lots_to_order_qty"):
                            order_qty = int(executor._lots_to_order_qty(int(qty), leg))
                    except Exception:
                        order_qty = qty
                    is_active = bool(getattr(leg, "is_active", False)) and qty > 0
                    status = "ACTIVE" if is_active else "CLOSED"
                    leg_pnl = float(getattr(leg, "pnl", 0) or 0)
                    unrealized = leg_pnl if is_active else 0.0
                    symbol = str(
                        getattr(leg, "trading_symbol", None)
                        or getattr(leg, "symbol", "")
                        or ""
                    ).strip()
                    if not symbol:
                        continue

                    tag = str(getattr(leg, "tag", "") or symbol)
                    leg_key = f"{tag}::{symbol}"
                    seen_keys.add(leg_key)

                    row = cache.get(leg_key)
                    if row is None:
                        # Determine event reason for the opening event
                        if tag.startswith("LEG@"):
                            open_reason = getattr(executor, '_last_event_reason', '') if executor else ''
                            open_reason = open_reason if open_reason else 'entry_conditions_met'
                            event_src = "ENTRY"
                        else:
                            adj_rule = getattr(
                                getattr(executor, 'adjustment_engine', None),
                                '_last_triggered_rule_name', ''
                            ) if executor else ''
                            open_reason = adj_rule if adj_rule else 'adjustment'
                            event_src = "ADJUSTMENT"
                        row = {
                            "strategy_name": strategy_name,
                            "opened_at": now_iso,
                            "closed_at": None,
                            "source": event_src,
                            "event_reason": open_reason,
                            "realized_pnl": 0.0,
                        }
                        cache[leg_key] = row

                    was_active = str(row.get("status", "")).upper() == "ACTIVE"
                    if was_active and not is_active and row.get("closed_at") is None:
                        row["closed_at"] = now_iso
                        row["realized_pnl"] = float(row.get("realized_pnl", 0) or 0) + float(leg_pnl)
                        # Capture exit reason at transition time
                        exit_rsn = getattr(executor, '_last_event_reason', '') if executor else ''
                        if exit_rsn:
                            row["event_reason"] = exit_rsn
                            row["source"] = "EXIT"

                    display_qty = max(0, int(order_qty))
                    if not is_active:
                        try:
                            prior_qty = int(row.get("qty", display_qty) or display_qty)
                            display_qty = max(display_qty, prior_qty)
                        except Exception:
                            pass

                    row.update({
                        "strategy_name": strategy_name,
                        "exchange": identity.get("exchange", ""),
                        "symbol": symbol,
                        "side": side,
                        "qty": max(0, display_qty),
                        "entry_price": float(getattr(leg, "entry_price", 0) or 0),
                        "exit_price": None,
                        "ltp": float(getattr(leg, "ltp", 0) or 0),
                        "status": status,
                        "unrealized_pnl": (unrealized if is_active else 0.0),
                        "delta": float(getattr(leg, "delta", 0) or 0),
                        "gamma": float(getattr(leg, "gamma", 0) or 0),
                        "theta": float(getattr(leg, "theta", 0) or 0),
                        "vega": float(getattr(leg, "vega", 0) or 0),
                        "opened_at": row.get("opened_at") or (
                            (state.entry_time.isoformat() if state.entry_time else now_iso)
                        ),
                        "closed_at": None if is_active else (row.get("closed_at") or now_iso),
                        "updated_at": now_iso,
                        "mode": mode,
                        "event_type": row.get("source", "ENTRY"),
                        "reason": row.get("event_reason", ""),
                    })
                    row["total_pnl"] = float(row.get("realized_pnl", 0) or 0) + float(row.get("unrealized_pnl", 0) or 0)
                    if is_active:
                        row["closed_at"] = None

                # Keep cached closed legs for lifecycle visibility; drop stale never-seen rows.
                for k in list(cache.keys()):
                    if k in seen_keys:
                        continue
                    stale = cache.get(k) or {}
                    if str(stale.get("status", "")).upper() != "CLOSED":
                        cache.pop(k, None)

                # BUG-FIX: Drop CLOSED legs from previous days so they don't appear
                # as orphan positions on the strategy page. Legs from previous runs
                # that are CLOSED should only appear in completed_monitor_history,
                # not in the live strategy view.
                today_str = datetime.now().date().isoformat()
                for k in list(cache.keys()):
                    row_data = cache.get(k) or {}
                    if str(row_data.get("status", "")).upper() != "CLOSED":
                        continue
                    opened = str(row_data.get("opened_at", ""))[:10]
                    if opened and opened < today_str:
                        cache.pop(k, None)

                legs_payload: List[Dict[str, Any]] = list(cache.values())
                active_legs = len([r for r in legs_payload if str(r.get("status", "")).upper() == "ACTIVE"])
                closed_legs = len([r for r in legs_payload if str(r.get("status", "")).upper() == "CLOSED"])
                unrealized_pnl = sum(float(r.get("unrealized_pnl", 0) or 0) for r in legs_payload if str(r.get("status", "")).upper() == "ACTIVE")
                realized_leg_pnl = sum(float(r.get("realized_pnl", 0) or 0) for r in legs_payload if str(r.get("status", "")).upper() == "CLOSED")
                snapshot[strategy_name] = {
                    "mode": mode,
                    "active_legs": active_legs,
                    "closed_legs": closed_legs,
                    "realized_pnl": float(realized_leg_pnl),
                    "unrealized_pnl": float(unrealized_pnl),
                    "adjustments_today": int(getattr(state, "adjustments_today", 0) or 0),
                    "lifetime_adjustments": int(getattr(state, "lifetime_adjustments", 0) or 0),
                    "last_adjustment_time": (
                        state.last_adjustment_time.isoformat() if state.last_adjustment_time else None
                    ),
                    "legs": legs_payload,
                }
                try:
                    self.state_mgr.save_monitor_snapshot(strategy_name, cache)
                except Exception as e:
                    logger.debug("Could not persist monitor cache for %s: %s", strategy_name, e)
        return snapshot

    def _archive_completed_strategy(self, strategy_name: str, runtime_state: str = "COMPLETED") -> None:
        """
        Persist a completed strategy monitor snapshot before unregistering it.
        Always saves a record, even for short/no-trade runs, so history is never lost.
        """
        # Force-refresh monitor cache from current state so that any
        # legs that were deactivated (e.g. by exit) since the last
        # polling snapshot are properly captured as CLOSED with their
        # final PnL values.
        try:
            self.get_strategy_leg_monitor_snapshot()
        except Exception:
            pass

        with self._lock:
            cache = dict((self._monitor_cache.get(strategy_name) or {}))
            state = self._exec_states.get(strategy_name)
            config = self._strategies.get(strategy_name, {}) or {}

            mode = "MOCK" if self._is_paper_mode(config) else "LIVE"
            legs_payload: List[Dict[str, Any]] = list(cache.values())
            legs_payload.sort(
                key=lambda row: str(row.get("updated_at") or row.get("opened_at") or ""),
                reverse=True,
            )

            active_rows = [r for r in legs_payload if str(r.get("status", "")).upper() == "ACTIVE"]
            closed_rows = [r for r in legs_payload if str(r.get("status", "")).upper() == "CLOSED"]
            unrealized_pnl = sum(float(r.get("unrealized_pnl", 0) or 0) for r in active_rows)
            realized_leg_pnl = sum(float(r.get("realized_pnl", 0) or 0) for r in closed_rows)
            realized_pnl = float(realized_leg_pnl)

            opened_ts_all: List[float] = []
            closed_ts_all: List[float] = []
            for row in legs_payload:
                raw_opened = row.get("opened_at")
                raw_closed = row.get("closed_at")
                try:
                    if raw_opened:
                        opened_ts_all.append(datetime.fromisoformat(str(raw_opened)).timestamp())
                except Exception:
                    pass
                try:
                    if raw_closed:
                        closed_ts_all.append(datetime.fromisoformat(str(raw_closed)).timestamp())
                except Exception:
                    pass

            if opened_ts_all and closed_ts_all:
                runtime_seconds = int(max(0, max(closed_ts_all) - min(opened_ts_all)))
            else:
                runtime_seconds = 0

            history_row = {
                "strategy_name": strategy_name,
                "mode": mode,
                "active": False,
                "runtime_state": runtime_state,
                "runtime_seconds": runtime_seconds,
                "realized_pnl": realized_pnl,
                "unrealized_pnl": float(unrealized_pnl),
                "total_pnl": float(realized_pnl + unrealized_pnl),
                "combined_delta": sum(float(r.get("delta", 0) or 0) for r in active_rows),
                "combined_gamma": sum(float(r.get("gamma", 0) or 0) for r in active_rows),
                "combined_theta": sum(float(r.get("theta", 0) or 0) for r in active_rows),
                "combined_vega": sum(float(r.get("vega", 0) or 0) for r in active_rows),
                "adjustments_today": int(getattr(state, "adjustments_today", 0) or 0) if state else 0,
                "lifetime_adjustments": int(getattr(state, "lifetime_adjustments", 0) or 0) if state else 0,
                "last_adjustment_time": (
                    state.last_adjustment_time.isoformat() if state and state.last_adjustment_time else None
                ),
                "leg_count": len(legs_payload),
                "active_legs": len(active_rows),
                "closed_legs": len(closed_rows),
                "positions": [],
                "all_legs": legs_payload,
                "active_leg_rows": active_rows,
                "closed_leg_rows": closed_rows,
                "archived_at": datetime.now().isoformat(),
            }

            self._completed_monitor_history.insert(0, history_row)
            if len(self._completed_monitor_history) > self._max_completed_monitor_history:
                self._completed_monitor_history = self._completed_monitor_history[: self._max_completed_monitor_history]
            try:
                self.state_mgr.append_completed_monitor_history(
                    history_row,
                    max_rows=self._max_completed_monitor_history,
                )
                self.state_mgr.clear_monitor_snapshot(strategy_name)
            except Exception as e:
                logger.debug("Could not persist completed monitor history for %s: %s", strategy_name, e)

            # Reset cumulative counters in the persisted state file so that
            # the next run of this strategy starts with a clean slate.
            if state is not None:
                state.cumulative_daily_pnl = 0.0
                executor = self._executors.get(strategy_name)
                if executor and hasattr(executor, "persistence") and hasattr(executor, "state_file"):
                    try:
                        executor.persistence.save(state, str(executor.state_file))
                    except Exception:
                        pass

    def get_completed_strategy_monitor_history(self, limit: int = 20) -> List[Dict[str, Any]]:
        normalized_limit = max(1, int(limit or 20))
        with self._lock:
            try:
                rows = self.state_mgr.get_completed_monitor_history(limit=normalized_limit)
                if rows:
                    self._completed_monitor_history = rows[: self._max_completed_monitor_history]
                    return copy.deepcopy(rows)
            except Exception as e:
                logger.debug("Could not load completed monitor history from DB: %s", e)
            rows = self._completed_monitor_history[:normalized_limit]
            return copy.deepcopy(rows)

    def delete_completed_strategy_monitor_history(self, strategy_name: Optional[str] = None, archived_at: Optional[str] = None) -> int:
        """Delete completed strategy monitor history. Supports deleting specific entry, all for a strategy, or all."""
        with self._lock:
            count = self.state_mgr.delete_completed_monitor_history(
                strategy_name=strategy_name, archived_at=archived_at
            )
            if strategy_name and archived_at:
                self._completed_monitor_history = [
                    r for r in self._completed_monitor_history
                    if not (r.get("strategy_name") == strategy_name and r.get("archived_at") == archived_at)
                ]
            elif strategy_name:
                self._completed_monitor_history = [
                    r for r in self._completed_monitor_history
                    if r.get("strategy_name") != strategy_name
                ]
            else:
                self._completed_monitor_history.clear()
            return count

    # BUG-026 FIX: STRATEGY_RECOVER_RESUME intent was submitted by the dashboard but
    # had NO consumer anywhere. The intent was stored in DB forever; recovery never happened.
    def handle_recover_resume(self, payload: dict) -> dict:
        """
        Handle a STRATEGY_RECOVER_RESUME intent submitted by the dashboard.

        Sets the strategy state to 'already entered' so the executor monitors
        exits/adjustments rather than waiting for a fresh entry signal.

        Args:
            payload: {
                "strategy_name": str,      # strategy to recover
                "symbol": str,             # broker symbol with open position
                "resume_monitoring": bool  # True = skip re-entry, monitor only
            }
        """
        strategy_name = payload.get("strategy_name", "").strip()
        symbol = payload.get("symbol", "").strip()
        resume_monitoring = payload.get("resume_monitoring", True)

        if not strategy_name:
            logger.error("RECOVER_RESUME: strategy_name is required")
            return {"status": "error", "reason": "strategy_name required"}

        with self._lock:
            executor = self._executors.get(strategy_name)
            if executor is None:
                logger.warning(f"RECOVER_RESUME: strategy '{strategy_name}' not registered; "
                               f"cannot apply until strategy is loaded via /runner/start")
                return {"status": "not_registered", "strategy_name": strategy_name}

            state = executor.state
            if resume_monitoring:
                # Mark as entered so executor skips fresh ENTRY and monitors exits/adjustments
                state.entered_today = True
                if state.entry_time is None:
                    state.entry_time = datetime.now()

                # If symbol provided and not already tracked, create a placeholder leg
                # so the exit engine can monitor it until full reconciliation happens
                if symbol and symbol not in [leg.symbol for leg in state.legs.values()]:
                    from .state import LegState
                    from .models import InstrumentType, Side
                    recovery_tag = f"RECOVERED_{symbol}"
                    recovery_leg = LegState(
                        tag=recovery_tag,
                        symbol=symbol,
                        trading_symbol=symbol,
                        instrument=InstrumentType.OPT,
                        option_type=None,
                        strike=None,
                        expiry="UNKNOWN",
                        side=Side.SELL,   # default; reconciliation will correct this
                        qty=0,            # qty=0 signals "needs reconciliation"
                        entry_price=0.0,
                        ltp=0.0,
                        is_active=True,
                    )
                    state.legs[recovery_tag] = recovery_leg
                    logger.info(f"RECOVER_RESUME: placeholder leg created for '{symbol}' "
                                f"in strategy '{strategy_name}'")

                # Persist the recovery state immediately
                try:
                    executor.persistence.save(state, str(executor.state_file))
                    logger.info(f"RECOVER_RESUME: state persisted for '{strategy_name}'")
                except Exception as e:
                    logger.error(f"RECOVER_RESUME: state persist failed: {e}")

            logger.warning(f"♻️ RECOVER_RESUME APPLIED | strategy={strategy_name} | "
                           f"symbol={symbol} | resume_monitoring={resume_monitoring}")
            return {
                "status": "resumed",
                "strategy_name": strategy_name,
                "symbol": symbol,
                "resume_monitoring": resume_monitoring,
            }

    @staticmethod
    def _adjustment_roll_leg(
        svc,
        name: str,
        exec_state: ExecutionState,
        engine_state: Any,
        leg: str,
        config: Dict[str, Any],
        reader: Any,
        qty: int,
    ) -> bool:
        """
        Roll helper used by hardening tests and adjustment flow.
        """
        leg_key = leg.strip().upper()
        if leg_key not in ("CE", "PE"):
            return False

        prefix = leg_key.lower()
        opposite = "pe" if prefix == "ce" else "ce"
        cur_symbol = getattr(exec_state, f"{prefix}_symbol", None)
        side = getattr(exec_state, f"{prefix}_side", None) or "SELL"
        raw_delta = getattr(engine_state, f"{opposite}_delta", 0.3)
        try:
            target_abs_delta = abs(float(raw_delta))
        except (TypeError, ValueError):
            target_abs_delta = 0.3

        candidate = reader.find_option_by_delta(
            option_type=leg_key,
            target_delta=target_abs_delta,
            tolerance=0.1,
        )
        if not candidate:
            logger.error("Roll failed for %s: no candidate by delta %.4f", name, target_abs_delta)
            return False

        new_symbol = candidate.get("trading_symbol") or candidate.get("symbol")
        if not new_symbol:
            return False
        if cur_symbol and new_symbol == cur_symbol:
            return True

        exchange = ((config or {}).get("basic", {}) or {}).get("exchange", "NFO")
        try:
            lot_size = int(reader.get_lot_size())
            if lot_size <= 0:
                lot_size = 1
        except Exception:
            lot_size = 1
        order_qty = int(qty) * lot_size
        alert = {
            "execution_type": "ADJUSTMENT",
            "strategy_name": name,
            "exchange": exchange,
            "legs": [
                {
                    "tradingsymbol": cur_symbol,
                    "direction": "BUY" if str(side).upper() == "SELL" else "SELL",
                    "qty": order_qty,
                    "order_type": "MARKET",
                    "price": 0.0,
                    "product_type": "NRML",
                },
                {
                    "tradingsymbol": new_symbol,
                    "direction": str(side).upper(),
                    "qty": order_qty,
                    "order_type": "MARKET",
                    "price": 0.0,
                    "product_type": "NRML",
                },
            ],
        }
        result = svc.bot.process_alert(alert)
        if isinstance(result, dict) and str(result.get("status", "")).strip().upper() in ("FAILED", "BLOCKED", "ERROR"):
            return False

        setattr(exec_state, f"{prefix}_symbol", new_symbol)
        setattr(exec_state, f"{prefix}_strike", float(candidate.get("strike", 0.0) or 0.0))
        setattr(exec_state, f"{prefix}_qty", int(qty))
        if candidate.get("ltp") is not None:
            setattr(exec_state, f"{prefix}_entry_price", float(candidate.get("ltp") or 0.0))

        try:
            svc.state_mgr.save(exec_state)
        except Exception as e:
            logger.warning("Roll state persist failed for %s: %s", name, e)
        return True


class PerStrategyExecutor:
    """
    Encapsulates the engine components for a single strategy.
    """

    def __init__(self, name: str, config: Dict[str, Any], bot, state_db_path: str):
        self.name = name
        self.config = config
        self.bot = bot
        # BUG-H4 FIX: Reentrant lock for thread-safe access to state between
        # process_tick (main loop thread) and notify_fill (OrderWatcher thread).
        # Must be RLock (reentrant) because MOCK fills fire synchronously:
        # process_tick → _execute_entry → bot.process_alert → execute_command
        # → notify_fill, all on the same thread while _tick_lock is held.
        import threading
        self._tick_lock = threading.RLock()

        # State persistence (use name‑based file)
        state_file = Path(state_db_path).parent / f"{name}_state.pkl"
        self.persistence = StatePersistence()
        self.state = self.persistence.load(str(state_file)) or StrategyState()

        # Market reader – db_path not needed, auto-resolves
        identity = config.get("identity", {})
        exchange = identity.get("exchange", "NFO")
        symbol = identity.get("underlying", "NIFTY")
        self.market = MarketReader(exchange, symbol, max_stale_seconds=30)
        # Store the raw expiry mode for per-tick re-evaluation (weekly_auto support).
        self._cycle_expiry_mode = str(
            (config.get("schedule", {}) or {}).get("expiry_mode", "weekly_current")
        ).strip() or "weekly_current"
        if self._cycle_expiry_mode == "custom":
            self._cycle_expiry_mode = "weekly_current"
        self._cycle_expiry_date = self._resolve_cycle_expiry(config)

        # Engines
        self.condition_engine = ConditionEngine(self.state)
        self.entry_engine = EntryEngine(self.state, self.market)
        self.adjustment_engine = AdjustmentEngine(self.state, self.market)
        self.exit_engine = ExitEngine(self.state)
        self.exit_engine.load_config(config.get("exit", {}))
        self.adjustment_engine.load_rules(config.get("adjustment", {}).get("rules", []))
        self.reconciliation = BrokerReconciliation(
            self.state,
            lot_size_resolver=self.market.get_lot_size,
        )

        self.state_file = state_file

        # Resolve lot_size once and stamp on any existing legs (recovery path).
        try:
            self._lot_size = max(1, int(self.market.get_lot_size(self._cycle_expiry_date)))
        except Exception:
            self._lot_size = 1
        for leg in self.state.legs.values():
            if getattr(leg, "lot_size", 1) <= 1 and self._lot_size > 1:
                leg.lot_size = self._lot_size

        # Daily reset tracking
        self._last_date = datetime.now().date()
        self.cycle_completed = False
        adjustment_cfg = self.config.get("adjustment", {})
        self._adjustment_validation_backoff_sec = max(
            5,
            int(adjustment_cfg.get("validation_retry_cooldown_sec", 60) or 60),
        )
        self._adjustment_validation_alert_cooldown_sec = max(
            5,
            int(adjustment_cfg.get("validation_alert_cooldown_sec", 60) or 60),
        )
        self._adjustment_block_until: Optional[datetime] = None
        self._adjustment_block_reason: str = ""
        self._adjustment_block_announced: bool = False
        self._last_persist_at: Optional[datetime] = None
        self._last_adjustment_validation_alert_at: Optional[datetime] = None
        self._last_entry_skip_reason: str = ""
        self._last_entry_skip_log_at: Optional[datetime] = None

        # Event tracking for monitor snapshot reason/event_type columns
        self._last_event_type: str = ""
        self._last_event_reason: str = ""

        # ✅ CONFIG AUDIT LOG: print every configured rule at startup so every run
        # is fully auditable from logs alone — no need to open JSON files.
        self._log_strategy_config_at_startup()

    def _log_strategy_config_at_startup(self) -> None:
        """Log the complete effective strategy configuration at startup.
        Every run starts with a full human-readable rule dump in the log.
        """
        identity = self.config.get("identity", {})
        timing = self.config.get("timing", {})
        schedule = self.config.get("schedule", {})
        exit_cfg = self.config.get("exit", {})
        adj_cfg = self.config.get("adjustment", {})
        entry_cfg = self.config.get("entry", {})

        sl_cfg = exit_cfg.get("stop_loss", {})
        pt_cfg = exit_cfg.get("profit_target", {})
        trail_cfg = exit_cfg.get("trailing", {})
        risk_cfg = exit_cfg.get("risk", {})
        time_cfg = exit_cfg.get("time", {})

        legs_info = ", ".join(
            f"{l.get('tag')}({l.get('side')}/{l.get('option_type')}/delta={l.get('strike_value')})"
            for l in entry_cfg.get("legs", [])
        )

        adj_rules = adj_cfg.get("rules", [])
        rules_summary = []
        for r in adj_rules:
            retrigger_raw = r.get("retrigger", r.get("retriger", True))
            retrigger_note = "" if retrigger_raw else " [ONCE-PER-DAY]"
            has_typo = "retriger" in r and "retrigger" not in r
            typo_note = " ⚠️TYPO:retriger" if has_typo else ""
            rules_summary.append(
                f"  Rule '{r.get('name')}' priority={r.get('priority',999)}"
                f" cooldown={r.get('cooldown_sec',0)}s"
                f" max_per_day={r.get('max_per_day')}"
                f" retrigger={retrigger_raw}{retrigger_note}{typo_note}"
                f" action={r.get('action', {}).get('type')}"
            )
        rules_block = "\n".join(rules_summary) if rules_summary else "  (none)"

        logger.info(
            "STRATEGY_CONFIG_LOADED | strategy=%s\n"
            "  underlying=%s  exchange=%s  paper_mode=%s  test_mode=%s\n"
            "  entry_window=%s–%s  exit_time=%s  active_days=%s\n"
            "  expiry_mode=%s  entry_on_expiry_day=%s\n"
            "  entry_legs=[%s]\n"
            "  stop_loss=amount:%s pct:%s action:%s allow_reentry:%s\n"
            "  profit_target=amount:%s pct:%s action:%s\n"
            "  trailing=trail:%s lock_in:%s\n"
            "  max_delta=%s  max_loss_per_day=%s  max_lots=%s\n"
            "  adjustments_enabled=%s  adj_rules_count=%d\n%s",
            self.name,
            identity.get("underlying"), identity.get("exchange"),
            identity.get("paper_mode"), identity.get("test_mode"),
            timing.get("entry_window_start"), timing.get("entry_window_end"),
            time_cfg.get("strategy_exit_time"), schedule.get("active_days"),
            schedule.get("expiry_mode"), schedule.get("entry_on_expiry_day"),
            legs_info,
            sl_cfg.get("amount"), sl_cfg.get("pct"), sl_cfg.get("action"), sl_cfg.get("allow_reentry"),
            pt_cfg.get("amount"), pt_cfg.get("pct"), pt_cfg.get("action"),
            trail_cfg.get("trail_amount"), trail_cfg.get("lock_in_at"),
            risk_cfg.get("max_delta"), risk_cfg.get("max_loss_per_day"), risk_cfg.get("max_lots"),
            adj_cfg.get("enabled"), len(adj_rules),
            rules_block,
        )

    def process_tick(self):
        """Called by the service loop each tick."""
        with self._tick_lock:
            self._process_tick_inner()

    def _process_tick_inner(self):
        """Core tick logic (runs under _tick_lock)."""
        now = datetime.now()

        # Daily reset
        if self._last_date != now.date():
            self.state.adjustments_today = 0
            self.state.total_trades_today = 0
            self.state.entered_today = False
            # BUG-H3 FIX: Reset trailing stop and profit step state on new day.
            self.state.trailing_stop_active = False
            self.state.trailing_stop_level = 0.0
            self.state.peak_pnl = 0.0
            self.state.current_profit_step = -1
            self.state.cumulative_daily_pnl = 0.0
            # Purge stale inactive legs from previous days to prevent
            # historical run data mixing with today's positions.
            stale_keys = [
                k for k, leg in self.state.legs.items()
                if not leg.is_active
            ]
            if stale_keys:
                for k in stale_keys:
                    del self.state.legs[k]
                logger.info(
                    "Purged %d stale inactive legs from previous runs for %s",
                    len(stale_keys), self.name,
                )

            # ── DAILY BOUNDARY FIX: Clear execution guard for this strategy ──
            # If the bot runs across midnight, yesterday's guard positions
            # would block today's ENTRY.  Force-clear so today starts clean.
            try:
                guard = getattr(self.bot, "execution_guard", None)
                if guard is not None and guard.has_strategy(self.name):
                    guard.force_close_strategy(self.name)
                    logger.info(
                        "DAILY_RESET_GUARD_CLEAR | strategy=%s | cleared stale guard positions from previous day",
                        self.name,
                    )
            except Exception as e:
                logger.warning("Daily guard clear failed for %s: %s", self.name, e)

            # ── DAILY BOUNDARY FIX: Reset cycle_completed ──
            # If yesterday's exit set cycle_completed=True but the strategy
            # was not yet unregistered (e.g. deferred due to pending orders),
            # reset it so the strategy can re-enter today.
            if self.cycle_completed:
                logger.info(
                    "DAILY_RESET_CYCLE | strategy=%s | resetting cycle_completed from previous day",
                    self.name,
                )
                self.cycle_completed = False

            # ── DAILY BOUNDARY FIX: Expire stale CREATED orders for this strategy ──
            # _expire_stale_orders() in the service only runs at startup.
            # If the bot runs across midnight, stale CREATED EXIT orders from
            # yesterday would be picked up by OrderWatcher and dispatched today.
            try:
                self._expire_strategy_stale_orders(now)
            except Exception as e:
                logger.warning("Daily order expiry failed for %s: %s", self.name, e)

            self._last_date = now.date()
            logger.info(f"Daily counters reset for {self.name}")

        self.state.current_time = now
        self._update_time_context(now)

        # --- NEW: Reconcile pending orders ---
        self._reconcile_pending_orders()

        # ── Chain availability guard ─────────────────────────────────────
        # Resolve the live expiry ONCE per tick so ensure_chain_data and
        # begin_tick both see the same value and every downstream read
        # (market data, entry, adjustment) works on a single consistent
        # snapshot without hitting SQLite multiple times per tick.
        tick_expiry = self._get_current_expiry()
        snap_expiry = tick_expiry if tick_expiry != "NO_EXPIRY" else None
        if not self.market.ensure_chain_data(snap_expiry):
            logger.warning(
                "CHAIN_UNAVAILABLE | strategy=%s expiry=%s — skipping tick (will retry next interval)",
                self.name, tick_expiry,
            )
            return
        self.market.begin_tick(snap_expiry)
        try:
            self._process_tick_with_chain(now)
        finally:
            self.market.end_tick()

    def _process_tick_with_chain(self, now: datetime):
        """Inner tick body — runs with chain snapshot already locked."""
        # Update market data (including leg data)
        self._update_market_data()

        # NEW: Expiry day actions
        self._check_expiry_day_action(now)

        # Check exits first
        exit_action = self.exit_engine.check_exits(now)
        if exit_action and exit_action != "profit_step_adj":
            exit_reason = self.exit_engine.last_exit_reason or "unspecified"
            active_legs_at_exit = [l for l in self.state.legs.values() if l.is_active]
            leg_detail = "; ".join(
                f"{l.tag}={l.option_type.value if l.option_type else '?'}"
                f"@{l.strike} exp={l.expiry}"
                f" pnl={round(l.pnl,2) if hasattr(l,'pnl') else 0}"
                f" ltp={round(l.ltp,2) if l.ltp else 0}"
                f" delta={round(l.delta,3) if l.delta else 0}"
                f" side={l.side.value} lots={l.qty}"
                for l in active_legs_at_exit
            )
            self._last_event_type = "EXIT"
            self._last_event_reason = exit_reason
            logger.info(
                "EXIT_TRIGGERED | strategy=%s | action=%s | reason=%s | active_legs=%d\n  Legs: %s",
                self.name,
                exit_action,
                exit_reason,
                len(active_legs_at_exit),
                leg_detail,
            )
            self._execute_exit(exit_action, source=exit_action)
            # ✅ BUG FIX(9): Persist state after exit so legs are marked
            # inactive on disk. Without this the early `return` skips the
            # periodic save at the bottom of process_tick, leaving a stale
            # state file with active legs that confuses the next session.
            try:
                self.persistence.save(self.state, str(self.state_file))
            except Exception as _e:
                logger.warning("State persist after exit failed for %s: %s", self.name, _e)
            return

        # Check entry (if not already entered today)
        if not self.state.entered_today:
            should_enter, reason = self._should_enter_with_reason(now)
            if should_enter:
                self._execute_entry()
            else:
                self._log_entry_skip(reason, now)

        # Check adjustments (unchanged)
        if self._adjustment_block_until and now < self._adjustment_block_until:
            if not self._adjustment_block_announced:
                logger.warning(
                    "Adjustment dispatch temporarily blocked for %s until %s due to validation guard: %s",
                    self.name,
                    self._adjustment_block_until.strftime("%H:%M:%S"),
                    self._adjustment_block_reason,
                )
                self._adjustment_block_announced = True
        else:
            if self._adjustment_block_until and now >= self._adjustment_block_until:
                self._adjustment_block_until = None
                self._adjustment_block_reason = ""
                self._adjustment_block_announced = False
            pre_adjustment_legs = copy.deepcopy(self.state.legs)
            pre_adjustments_today = self.state.adjustments_today
            pre_lifetime_adjustments = self.state.lifetime_adjustments
            pre_last_adjustment_time = self.state.last_adjustment_time
            # ✅ BUG-PARTIAL-FILL FIX: Do not run adjustments while any entry or
            # adjustment leg is still PENDING (awaiting broker fill confirmation).
            # Firing adjustment logic against a partially-filled position causes
            # 'orphan adjustment' storms (20 NOOPs on Mar 11) and corrupts the
            # adjustments_today counter.
            pending_legs = [
                leg for leg in self.state.legs.values()
                if leg.order_status == "PENDING" and leg.is_active is False
                and leg.order_placed_at is not None
                and (now - leg.order_placed_at).total_seconds() < 60
            ]
            # ✅ BUG FIX: Do not run adjustments if no legs are active yet.
            # Conditions like 'adj_count_today >= 0' are always true, so without
            # this guard the adjustment engine fires before entry and crashes when
            # dynamic leg selectors (LOWER_DELTA_LEG etc.) resolve to None.
            if not self.state.any_leg_active:
                actions = []
            elif pending_legs:
                logger.info(
                    "ADJUSTMENT_HOLD | strategy=%s | waiting for %d pending leg(s) to fill: %s",
                    self.name,
                    len(pending_legs),
                    [l.tag for l in pending_legs],
                )
                actions = []
            else:
                actions = self.adjustment_engine.check_and_apply(now)
            for action in actions:
                logger.info(f"Adjustment: {action}")
            if actions:
                if not self._dispatch_adjustment_orders(pre_adjustment_legs):
                    # Roll back local state if broker rejected adjustment orders.
                    self.state.legs = pre_adjustment_legs
                    self.state.adjustments_today = pre_adjustments_today
                    self.state.lifetime_adjustments = pre_lifetime_adjustments
                    self.state.last_adjustment_time = pre_last_adjustment_time
                    # Set cooldown to prevent spamming rejected adjustments every tick
                    self._adjustment_block_until = now + timedelta(seconds=self._adjustment_validation_backoff_sec)
                    self._adjustment_block_reason = "dispatch_rejected"
                    self._adjustment_block_announced = False
                    logger.warning(f"Adjustment rollback applied for {self.name}, blocking for {self._adjustment_validation_backoff_sec}s")
                else:
                    # Stamp lot_size on any newly created adjustment legs
                    for leg in self.state.legs.values():
                        if getattr(leg, "lot_size", 1) <= 1 and self._lot_size > 1:
                            leg.lot_size = self._lot_size
                    self.persistence.save(self.state, str(self.state_file))

        # Broker reconciliation (every 5 minutes)
        # BUG-14 FIX: Widened from second==0 (1s window) to second<5 (5s window)
        # to avoid missing the reconciliation window due to tick processing delays.
        # Added _last_reconcile_minute guard to ensure it still only fires once per window.
        if now.second < 5 and now.minute % 5 == 0 and not self._resolve_test_mode():
            last_rec = getattr(self, "_last_reconcile_minute", -1)
            current_window = now.hour * 60 + now.minute
            if current_window != last_rec:
                self._last_reconcile_minute = current_window
                try:
                    broker_view = getattr(self.bot, "broker_view", None)
                    if broker_view is not None:
                        warnings = self.reconciliation.reconcile_from_broker(broker_view)
                        if warnings:
                            logger.warning(f"RECONCILE [{self.name}]: {len(warnings)} mismatch(es) corrected")
                except Exception as e:
                    logger.error(f"Broker reconciliation failed for {self.name}: {e}")

        # Persist state periodically (every ~30 seconds)
        # NOTE: previous `now.second == 0` condition was fragile — the 2-second
        # tick loop could stay on odd seconds and never hit 0.
        elapsed = (now - self._last_persist_at).total_seconds() if self._last_persist_at else 999
        if elapsed >= 30:
            self._last_persist_at = now
            self.persistence.save(self.state, str(self.state_file))

    # ==================== NEW METHODS ====================

    def _expire_strategy_stale_orders(self, now: datetime):
        """Expire stale CREATED orders for THIS strategy from previous days.

        Called during daily reset to prevent the OrderWatcher from dispatching
        yesterday's unfinished EXIT orders today (e.g. MIS positions that were
        auto-squared by the broker at market close).
        """
        try:
            from db.trading_db import get_trading_conn
            today_str = now.strftime("%Y-%m-%d")
            conn = get_trading_conn()
            cur = conn.cursor()
            cur.execute(
                "UPDATE orders SET status = 'EXPIRED', updated_at = NOW() "
                "WHERE status IN ('CREATED', 'SENT_TO_BROKER') "
                "AND created_at < %s "
                "AND strategy_name = %s",
                (today_str, self.name),
            )
            count = cur.rowcount
            conn.commit()
            conn.close()
            if count > 0:
                logger.warning(
                    "DAILY_EXPIRE_STALE_ORDERS | strategy=%s | expired %d stale orders from previous days",
                    self.name, count,
                )
        except Exception as e:
            logger.warning("_expire_strategy_stale_orders failed for %s: %s", self.name, e)

    def _reconcile_pending_orders(self):
        """Query repository for orders and update pending legs."""
        if not hasattr(self.bot, "order_repo"):
            return

        # Get all orders for this strategy with non-final status.
        # NOTE: this excludes EXECUTED/FAILED rows by contract, so pending legs
        # must also reconcile through command_id / broker_order_id lookups.
        orders = self.bot.order_repo.get_open_orders_by_strategy(self.name)
        open_order_map = {(str(o.symbol or "").strip(), str(o.side or "").upper()): o for o in orders}

        broker_positions = None
        if not self._resolve_test_mode():
            try:
                broker_view = getattr(self.bot, "broker_view", None)
                if broker_view is not None:
                    broker_view.invalidate_cache("positions")
                    broker_positions = broker_view.get_positions(force_refresh=True) or []
            except Exception:
                broker_positions = None

        for leg in list(self.state.legs.values()):
            if leg.order_status != "PENDING":
                continue

            leg_symbols = self._leg_symbols(leg)
            # Prefer symbol+side for open orders, then exact command/broker IDs for terminal statuses.
            matching_order = None
            for sym in leg_symbols:
                matching_order = open_order_map.get((sym, leg.side.value))
                if matching_order is not None:
                    break
            if matching_order is None and getattr(leg, "command_id", None):
                matching_order = self.bot.order_repo.get_by_id(leg.command_id)
            if matching_order is None and getattr(leg, "order_id", None):
                matching_order = self.bot.order_repo.get_by_broker_id(leg.order_id)

            if matching_order:
                leg.command_id = matching_order.command_id
                leg.order_id = matching_order.broker_order_id

                if matching_order.status == "EXECUTED":
                    leg.order_status = "FILLED"
                    leg.is_active = True
                    leg.filled_qty = int(matching_order.quantity or 0)
                    logger.info(f"FILLED via reconciliation | {leg.tag}")
                elif matching_order.status in ("FAILED", "CANCELLED", "REJECTED"):
                    leg.order_status = "FAILED"
                    leg.close()
                    logger.warning(f"FAILED via reconciliation | {leg.tag}")
                # else SENT_TO_BROKER or CREATED – remain pending
            else:
                # Final fallback from broker truth: if position exists for leg symbol+direction,
                # treat pending leg as filled to keep monitor state aligned with live account.
                if broker_positions:
                    for p in broker_positions:
                        sym = str(p.get("tsym") or "").strip()
                        if sym not in leg_symbols:
                            continue
                        netqty = int(p.get("netqty", 0) or 0)
                        if netqty == 0:
                            continue
                        if leg.side.value == "SELL" and netqty < 0:
                            leg.order_status = "FILLED"
                            leg.is_active = True
                            leg.filled_qty = abs(netqty)
                            leg.entry_price = float(p.get("avgprc", leg.entry_price) or leg.entry_price)
                            leg.ltp = float(p.get("lp", leg.ltp) or leg.ltp)
                            logger.warning(
                                "FILLED via broker-position fallback | %s | symbol=%s qty=%s",
                                leg.tag,
                                sym,
                                abs(netqty),
                            )
                            break
                        if leg.side.value == "BUY" and netqty > 0:
                            leg.order_status = "FILLED"
                            leg.is_active = True
                            leg.filled_qty = abs(netqty)
                            leg.entry_price = float(p.get("avgprc", leg.entry_price) or leg.entry_price)
                            leg.ltp = float(p.get("lp", leg.ltp) or leg.ltp)
                            logger.warning(
                                "FILLED via broker-position fallback | %s | symbol=%s qty=%s",
                                leg.tag,
                                sym,
                                abs(netqty),
                            )
                            break
                if leg.order_status != "PENDING":
                    continue

                # MOCK safety net: before timeout, search DB for any EXECUTED
                # orders matching this leg's symbol+side (MOCK fills may have been
                # committed to DB even if notify_fill was missed).
                if self._resolve_test_mode():
                    try:
                        all_orders = self.bot.order_repo.get_open_orders_by_strategy(self.name)
                        # get_open_orders_by_strategy only returns CREATED/SENT_TO_BROKER;
                        # also try get_by_id for any command_id we may have.
                        cid = getattr(leg, "command_id", None)
                        if cid:
                            db_order = self.bot.order_repo.get_by_id(cid)
                            if db_order and db_order.status == "EXECUTED":
                                leg.order_status = "FILLED"
                                leg.is_active = True
                                leg.filled_qty = int(db_order.quantity or 0)
                                if db_order.executed_price:
                                    leg.entry_price = float(db_order.executed_price)
                                    leg.ltp = float(db_order.executed_price)
                                logger.info("MOCK FILLED via DB reconciliation | %s", leg.tag)
                                continue
                    except Exception as mock_recon_err:
                        logger.debug("MOCK reconciliation fallback failed: %s", mock_recon_err)

                # No matching order – check timeout
                if leg.order_placed_at and (datetime.now() - leg.order_placed_at).total_seconds() > 60:
                    logger.warning(f"PENDING TIMEOUT | {leg.tag} – marking FAILED")
                    leg.order_status = "FAILED"
                    leg.close()

        # Handle CLOSE_PENDING timeout (adjustment close orders)
        for leg in list(self.state.legs.values()):
            if leg.order_status != "CLOSE_PENDING":
                continue
            if leg.order_placed_at and (datetime.now() - leg.order_placed_at).total_seconds() > 60:
                logger.warning(f"CLOSE_PENDING TIMEOUT | {leg.tag} – marking CLOSED")
                leg.order_status = "CLOSED"
                self.state.cumulative_daily_pnl += leg.pnl
                leg.close()

    def notify_fill(self, symbol: str, side: str, qty: int, price: float,
                    delta: Optional[float], broker_order_id: str, command_id: Optional[str] = None):
        """Immediate fill notification from OrderWatcher (runs on watcher thread)."""
        with self._tick_lock:
            self._notify_fill_inner(symbol, side, qty, price, delta, broker_order_id, command_id)

    def _notify_fill_inner(self, symbol: str, side: str, qty: int, price: float,
                           delta: Optional[float], broker_order_id: str, command_id: Optional[str] = None):
        """Core fill logic (runs under _tick_lock)."""
        fill_symbol = str(symbol or "").strip()
        fill_side = str(side or "").upper()
        # Pass 1: match PENDING legs (entry / adjustment open orders)
        for leg in self.state.legs.values():
            if leg.order_status != "PENDING":
                continue
            if leg.side.value != fill_side:
                continue
            if fill_symbol not in self._leg_symbols(leg):
                continue
            leg.order_status = "FILLED"
            leg.is_active = True
            leg.order_id = broker_order_id
            leg.command_id = command_id
            leg.filled_qty = qty  # qty in contracts; may need conversion to lots
            leg.entry_price = price
            leg.ltp = price
            leg.delta = delta if delta is not None else leg.delta
            logger.info(f"FILL NOTIFIED | {self.name} | {symbol} {side} {qty} @ {price}")
            return
        # Pass 2: match CLOSE_PENDING legs (adjustment close orders – opposite side)
        for leg in self.state.legs.values():
            if leg.order_status != "CLOSE_PENDING":
                continue
            expected_close_side = "BUY" if leg.side.value == "SELL" else "SELL"
            if fill_side != expected_close_side:
                continue
            if fill_symbol not in self._leg_symbols(leg):
                continue
            leg.order_status = "CLOSED"
            self.state.cumulative_daily_pnl += leg.pnl
            leg.close(price)
            leg.order_id = broker_order_id
            logger.info(f"CLOSE FILL NOTIFIED | {self.name} | {symbol} {side} {qty} @ {price}")
            return
        logger.warning(f"FILL NOTIFICATION: No matching pending leg for {symbol} {side}")

    @staticmethod
    def _leg_symbols(leg: LegState) -> set:
        out = set()
        if getattr(leg, "symbol", None):
            out.add(str(leg.symbol).strip())
        if getattr(leg, "trading_symbol", None):
            out.add(str(leg.trading_symbol).strip())
        return {s for s in out if s}

    # ==================== MODIFIED EXISTING METHODS ====================

    def _update_market_data(self):
        """Refresh spot, ATM, and per‑leg data, but only for filled legs."""
        _expiry = self._get_current_expiry()
        self.state.spot_price = self.market.get_spot_price(_expiry)
        if not self.state.spot_open and self.state.spot_price:
            self.state.spot_open = self.state.spot_price
        self.state.atm_strike = self.market.get_atm_strike(_expiry)
        self.state.fut_ltp = self.market.get_fut_ltp(_expiry)
        try:
            chain_metrics = self.market.get_chain_metrics(_expiry)
        except Exception as e:
            logger.warning(
                "MARKET_METRICS_UNAVAILABLE | strategy=%s | expiry=%s | error=%s",
                self.name,
                _expiry,
                e,
            )
            chain_metrics = {}
        self.state.pcr = float(chain_metrics.get("pcr", 0.0) or 0.0)
        self.state.pcr_volume = float(chain_metrics.get("pcr_volume", 0.0) or 0.0)
        self.state.max_pain_strike = float(chain_metrics.get("max_pain_strike", 0.0) or 0.0)
        self.state.total_oi_ce = float(chain_metrics.get("total_oi_ce", 0.0) or 0.0)
        self.state.total_oi_pe = float(chain_metrics.get("total_oi_pe", 0.0) or 0.0)
        self.state.oi_buildup_ce = float(chain_metrics.get("oi_buildup_ce", 0.0) or 0.0)
        self.state.oi_buildup_pe = float(chain_metrics.get("oi_buildup_pe", 0.0) or 0.0)

        # Update each active leg – only if it is FILLED
        for leg in self.state.legs.values():
            if not leg.is_active or leg.order_status != "FILLED":
                continue
            if leg.instrument == InstrumentType.OPT:
                if leg.strike is None or leg.option_type is None:
                    logger.warning(f"Leg {leg.tag} is active but missing strike or option_type")
                    continue
                opt_data = self.market.get_option_at_strike(leg.strike, leg.option_type, leg.expiry)
                if opt_data:
                    # Update LTP only when the new value is a valid positive price.
                    # SQLite stores NULL for zero prices (_opt_float returns None for 0.0);
                    # overwriting a valid LTP with None/0 would cause leg.pnl to fall back
                    # to entry_price (PnL = 0), creating a false "spike" on the next update.
                    _sq_ltp = opt_data.get("ltp")
                    if _sq_ltp is not None and float(_sq_ltp) > 0:
                        leg.ltp = float(_sq_ltp)
                    if "delta" in opt_data and opt_data["delta"] is not None:
                        leg.delta = opt_data["delta"]
                    if "gamma" in opt_data and opt_data["gamma"] is not None:
                        leg.gamma = opt_data["gamma"]
                    if "theta" in opt_data and opt_data["theta"] is not None:
                        leg.theta = opt_data["theta"]
                    if "vega" in opt_data and opt_data["vega"] is not None:
                        leg.vega = opt_data["vega"]
                    if "iv" in opt_data and opt_data["iv"] is not None:
                        leg.iv = opt_data["iv"]
                    if "oi" in opt_data:
                        leg.oi = opt_data["oi"]
                    if "volume" in opt_data:
                        leg.volume = opt_data["volume"]
            # For futures legs, we could update via a different method, but not implemented here

        # ✅ BUG-001 FIX: Record PnL snapshots for all active filled legs
        for leg in self.state.legs.values():
            if leg.is_active and leg.order_status == "FILLED":
                try:
                    leg.record_pnl_snapshot(self.state.spot_price)
                except Exception:
                    pass  # Non-critical - don't let snapshot recording break the loop

    # NEW: RMS limit check (copied from executor.py)
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
        """Run entry engine and send orders via bot."""
        # NEW: Check RMS limits before entry
        if not self._check_rms_limits():
            logger.warning("Entry blocked by RMS limits")
            return

        symbol = self.config["identity"]["underlying"]
        default_expiry = self._get_current_expiry()
        new_legs = self.entry_engine.process_entry(
            self.config["entry"], symbol, default_expiry
        )
        if not new_legs:
            logger.info(
                "ENTRY_NOT_PLACED | strategy=%s | reason=entry_engine_returned_zero_legs",
                self.name,
            )
            return

        identity = self.config.get("identity", {})
        product_type = identity.get("product_type", "NRML")

        # Resolve lot_size and stamp on each leg for accurate PnL.
        try:
            lot_size = max(1, int(self.market.get_lot_size(self._get_current_expiry())))
        except Exception:
            lot_size = 1

        # --- Register legs in state FIRST so notify_fill() can find them ---
        # MOCK fills fire synchronously during process_alert(); if legs are
        # not yet in self.state.legs the fill notification is lost and legs
        # time-out to FAILED after 60 s.
        now = datetime.now()
        for leg in new_legs:
            leg.lot_size = lot_size
            leg.order_status = "PENDING"
            leg.is_active = False
            leg.order_placed_at = now
            self.state.legs[leg.tag] = leg

        # Convert each LegState to an alert leg. Leg qty is tracked in lots internally;
        # alert payload qty must be broker contract quantity.
        alert_legs = []
        for leg in new_legs:
            payload = self._build_alert_leg(
                leg=leg,
                direction=leg.side.value,
                qty=int(leg.qty),
            )
            payload["product_type"] = product_type
            alert_legs.append(payload)

        alert = {
            "secret_key": self._resolve_webhook_secret(),
            "execution_type": "ENTRY",
            "strategy_name": self.name,
            "exchange": identity.get("exchange", "NFO"),
            "legs": alert_legs,
            "test_mode": self._resolve_test_mode(),
        }

        result = self.bot.process_alert(alert)
        if self._is_failure_status((result or {}).get("status")):
            # Rollback: remove legs we pre-registered
            for leg in new_legs:
                self.state.legs.pop(leg.tag, None)
            logger.error(
                "ENTRY_REJECTED | strategy=%s | reason=alert_rejected | result=%s",
                self.name,
                result,
            )
            return

        self.state.entered_today = True
        self.state.entry_time = now
        self.cycle_completed = False

        # Log final status of each leg (may already be FILLED via notify_fill)
        filled = sum(1 for l in new_legs if l.order_status == "FILLED")
        pending = sum(1 for l in new_legs if l.order_status == "PENDING")
        leg_detail = "; ".join(
            f"{l.tag}={l.option_type.value if l.option_type else '?'}"
            f"@{l.strike} exp={l.expiry}"
            f" delta={round(l.delta,3) if l.delta else 0}"
            f" iv={round(l.iv,2) if l.iv else 0}"
            f" ltp={round(l.ltp,2) if l.ltp else 0}"
            f" side={l.side.value} lots={l.qty}"
            f" status={l.order_status}"
            for l in new_legs
        )
        self._last_event_type = "ENTRY"
        self._last_event_reason = "entry_conditions_met"
        logger.info(
            "ENTRY_PLACED | strategy=%s | legs=%s | filled=%s | pending=%s\n  Legs: %s",
            self.name,
            len(new_legs),
            filled,
            pending,
            leg_detail,
        )

        # Persist state immediately after entry so crash-recovery can find it
        try:
            self.persistence.save(self.state, str(self.state_file))
        except Exception as _e:
            logger.warning("State persist after entry failed for %s: %s", self.name, _e)

    def _resolve_cycle_expiry(self, config: Dict[str, Any]) -> str:
        """
        Resolve one concrete expiry for this strategy run and keep it fixed.
        Rules:
          1) Resolve schedule.expiry_mode dynamically from available DBs
             and lock that resolved date for the full cycle.
        """
        schedule_mode = str(
            (config.get("schedule", {}) or {}).get("expiry_mode", "weekly_current")
        ).strip() or "weekly_current"

        if schedule_mode == "custom":
            logger.warning(
                "schedule.expiry_mode=custom is deprecated for %s; falling back to weekly_current",
                self.name,
            )
            schedule_mode = "weekly_current"

        try:
            resolved = self.market.resolve_expiry_mode(schedule_mode)
            logger.info(
                "Resolved cycle expiry for %s: %s (mode=%s)",
                self.name,
                resolved,
                schedule_mode,
            )
            return resolved
        except Exception as e:
            logger.error(
                "Failed to resolve cycle expiry for %s with mode=%s: %s; using weekly_current",
                self.name,
                schedule_mode,
                e,
            )
            return self.market.resolve_expiry_mode("weekly_current")

    def _get_current_expiry(self) -> str:
        """
        Re-resolve the active expiry on every call.

        Uses the raw mode string (e.g. ``weekly_auto``) so that dynamic
        expiry logic (skip to next week on expiry day) re-evaluates each
        tick instead of using the frozen startup value.
        """
        try:
            return self.market.resolve_expiry_mode(self._cycle_expiry_mode)
        except Exception:
            return self._cycle_expiry_date  # fallback to startup-resolved value

    def _update_time_context(self, now: datetime) -> None:
        """Refresh runtime time fields consumed by condition parameters."""
        exit_time_str = (
            (self.config.get("exit", {}) or {})
            .get("time", {})
            .get("strategy_exit_time")
        )
        if not exit_time_str:
            self.state.minutes_to_exit = 0
            return
        try:
            exit_t = datetime.strptime(str(exit_time_str), "%H:%M").time()
            exit_dt = datetime.combine(now.date(), exit_t)
            if now >= exit_dt:
                self.state.minutes_to_exit = 0
            else:
                self.state.minutes_to_exit = max(
                    0, int((exit_dt - now).total_seconds() / 60)
                )
        except Exception:
            self.state.minutes_to_exit = 0

    def _should_enter_with_reason(self, now: datetime) -> Tuple[bool, str]:
        """Check if entry is allowed and return explicit reason."""
        if self.state.entered_today:
            return False, "already_entered_today"

        # Wait for option chain data to become available (spot & ATM must be non-zero).
        spot = getattr(self.state, "spot_price", 0) or 0
        atm = getattr(self.state, "atm_strike", 0) or 0
        if spot <= 0 or atm <= 0:
            return False, "waiting_for_chain_data"

        timing = self.config.get("timing", {})
        entry_start = timing.get("entry_window_start", "09:15")
        entry_end = timing.get("entry_window_end", "14:00")
        try:
            start_t = datetime.strptime(entry_start, "%H:%M").time()
            end_t = datetime.strptime(entry_end, "%H:%M").time()
        except ValueError:
            return False, f"invalid_entry_window:{entry_start}-{entry_end}"
        if not (start_t <= now.time() <= end_t):
            return False, f"outside_entry_window:{entry_start}-{entry_end}"

        # Respect configured trading days from schedule.
        active_days = self.config.get("schedule", {}).get("active_days", [])
        if active_days:
            day_name = now.strftime("%a").lower()[:3]
            if day_name not in [str(d).lower()[:3] for d in active_days]:
                return False, f"inactive_day:{day_name}"

        # Optional: square-off-before-min check
        sq_off = self.config.get("schedule", {}).get("square_off_before_min")
        if sq_off is not None and sq_off > 0:
            # Determine market close time based on exchange
            exchange = self.config.get("identity", {}).get("exchange", "NFO")
            if exchange == "MCX":
                close_time_str = "23:30"
            else:
                close_time_str = "15:30"
            try:
                close_t = datetime.strptime(close_time_str, "%H:%M").time()
                close_dt = datetime.combine(now.date(), close_t)
                if now >= close_dt:
                    # Already past close – no entry anyway
                    pass
                elif (close_dt - now).total_seconds() / 60 < sq_off:
                    return False, f"within_square_off_window:{sq_off}min"
            except Exception:
                pass

        return True, "ok"

    def _should_enter(self, now: datetime) -> bool:
        allowed, _ = self._should_enter_with_reason(now)
        return allowed

    def _log_entry_skip(self, reason: str, now: datetime) -> None:
        same_reason = reason == self._last_entry_skip_reason
        if (
            same_reason
            and self._last_entry_skip_log_at is not None
            and (now - self._last_entry_skip_log_at).total_seconds() < 60
        ):
            return
        self._last_entry_skip_reason = reason
        self._last_entry_skip_log_at = now
        logger.info(
            "ENTRY_SKIPPED | strategy=%s | reason=%s | time=%s",
            self.name,
            reason,
            now.strftime("%H:%M:%S"),
        )

    def _execute_exit(self, action: str, source: str = "unknown"):
        """Execute exit via bot."""
        if action == "combined_conditions":
            action = "exit_all"

        if action.startswith("exit_all"):
            # Reconcile with broker truth before placing exit orders.
            # This prevents placing fresh BUY orders when positions have
            # already been closed from the broker terminal.
            try:
                guard = getattr(self.bot, "execution_guard", None)
                broker_view = getattr(self.bot, "broker_view", None)
                if guard and broker_view and not self._resolve_test_mode():
                    if guard.has_strategy(self.name):
                        guard.reconcile_with_broker(self.name, broker_view)
                        # After reconciliation, if strategy has no positions left
                        # in the guard, positions were closed externally — skip exit.
                        if not guard.has_strategy(self.name):
                            logger.warning(
                                "EXIT_SKIPPED | strategy=%s | reason=positions_already_closed_at_broker",
                                self.name,
                            )
                            pnl_snapshot = self.state.combined_pnl
                            for leg in self.state.legs.values():
                                if leg.is_active:
                                    leg.close()
                            self.state.cumulative_daily_pnl += pnl_snapshot
                            self.state.entered_today = True
                            self.cycle_completed = True
                            logger.info(f"Exit completed (broker-closed) for {self.name}")
                            return
            except Exception as e:
                logger.warning("Pre-exit reconciliation failed for %s: %s", self.name, e)

            # In paper mode, route through process_alert(EXIT) so mock runs
            # exercise the same alert pipeline (including test_mode handling).
            if self._resolve_test_mode():
                active_legs = [leg for leg in self.state.legs.values() if leg.is_active and int(leg.qty) > 0]
                if active_legs:
                    alert_legs = [
                        self._build_alert_leg(
                            leg=leg,
                            direction="BUY" if leg.side.value == "SELL" else "SELL",
                            qty=int(leg.qty),
                        )
                        for leg in active_legs
                    ]
                    result = self._submit_alert(execution_type="EXIT", legs=alert_legs)
                    status = str((result or {}).get("status", "")).upper()
                    # When broker already has no open qty for a leg, executor should
                    # treat that as effectively closed to avoid infinite exit retries.
                    if self._is_failure_status(status) and not self._is_no_position_exit_result(result):
                        logger.error(f"Exit rejected for {self.name}: {result}")
                        return
            else:
                # Pass today's active leg symbols to avoid using stale order
                # records from previous days in position_exit_service.
                active_symbols = [
                    leg.symbol for leg in self.state.legs.values()
                    if leg.is_active and int(getattr(leg, "qty", 0) or 0) > 0
                ]
                self.bot.request_exit(
                    scope="strategy",
                    strategy_name=self.name,
                    symbols=active_symbols or None,
                    product_type="ALL",
                    reason=source,
                    source="STRATEGY_EXECUTOR"
                )
            # ✅ BUG FIX: Capture PnL BEFORE deactivating legs.
            # combined_pnl only sums active legs; reading after deactivation returns 0.
            pnl_snapshot = self.state.combined_pnl
            # Mark only active legs inactive (preserve exit_price of already-closed legs)
            for leg in self.state.legs.values():
                if leg.is_active:
                    leg.close()
            self.state.cumulative_daily_pnl += pnl_snapshot
            # BUG-H3 FIX: Reset trailing stop and profit step state for clean re-entry.
            self.state.trailing_stop_active = False
            self.state.trailing_stop_level = 0.0
            self.state.peak_pnl = 0.0
            self.state.current_profit_step = -1
            # ✅ BUG FIX: Detect stop-loss exit via exit_engine.last_exit_reason
            # (the 'source' param is the action string like 'exit_all', not 'stop_loss').
            exit_reason = getattr(self.exit_engine, 'last_exit_reason', '') or ''
            is_stop_loss_exit = exit_reason.startswith('stop_loss')
            if is_stop_loss_exit:
                sl_cfg = self.config.get("exit", {}).get("stop_loss", {})
                if sl_cfg.get("allow_reentry"):
                    # allow_reentry=True → stay registered and wait for next entry window
                    self.state.entered_today = False
                else:
                    # ✅ ISOLATION FIX: allow_reentry=False → this cycle is permanently done.
                    # Must set cycle_completed=True so _run_loop triggers clean unregistration.
                    # Previously missing this caused the strategy to stay registered indefinitely
                    # after SL exit, accumulating stale monitor rows that mixed into the next run.
                    self.state.entered_today = True
                    self.cycle_completed = True
            else:
                self.state.entered_today = True
                self.cycle_completed = True
            logger.info(f"Exit executed for {self.name}")

        # NEW: Handle partial_lots exit action
        elif action.startswith("partial_lots"):
            # Extract number of lots to close from config
            lots_to_close = self.exit_engine.exit_config.get("profit_target", {}).get("lots", 1)
            # Simplify: close from first active leg
            active = [leg for leg in self.state.legs.values() if leg.is_active]
            if active:
                leg = active[0]
                close_qty = min(lots_to_close, leg.qty)
                # In this base executor we don't actually send orders, just update state
                logger.info(f"Partial close: closing {close_qty} lots of {leg.tag}")
                leg.qty -= close_qty
                if leg.qty == 0:
                    leg.close()
                self.state.cumulative_daily_pnl += leg.pnl  # Approximate PnL from closed portion
            else:
                logger.warning("partial_lots: no active legs to close")

        # NEW: Handle profit step actions
        elif action.startswith("profit_step_"):
            step_action = action.replace("profit_step_", "")
            if step_action == "adj":
                # Trigger an adjustment rule – we simply log and let next tick handle it.
                logger.info("Profit step triggered adjustment (will be handled in next adjustment cycle)")
            elif step_action == "trail":
                # Tighten the trailing stop (reduce the trail distance by 25% as an example)
                current_trail = self.exit_engine.exit_config.get("trailing", {}).get("trail_amount", 0)
                if current_trail > 0:
                    self.state.trailing_stop_level = self.state.peak_pnl - current_trail * 0.75
                logger.info("Profit step tightened trailing stop")
            elif step_action == "partial":
                # Close 25% of the position (simplified: close 25% from each leg)
                for leg in self.state.legs.values():
                    if leg.is_active and leg.qty > 0:
                        close_qty = max(1, int(leg.qty * 0.25))
                        leg.qty -= close_qty
                        if leg.qty <= 0:
                            leg.close()
                logger.info("Profit step closed 25% of position")

        # NEW: Handle trail/lock_trail profit target actions
        elif action == "profit_target_trail":
            self.state.trailing_stop_active = True
            trail_amt = self.exit_engine.exit_config.get("trailing", {}).get("trail_amount", 0)
            self.state.trailing_stop_level = self.state.peak_pnl - trail_amt
            logger.info("Trailing stop activated by profit target")

        elif action.startswith("leg_rule_"):
            self._execute_leg_rule_exit(action)
        elif action == "partial_50":
            # BUG-C3 FIX: Submit broker orders for the closed half, then update local state.
            alert_legs = []
            reductions: List[Tuple[LegState, int]] = []
            for leg in self.state.legs.values():
                if leg.is_active and leg.qty > 0:
                    close_qty = max(1, int(leg.qty // 2))
                    alert_legs.append(
                        self._build_alert_leg(
                            leg=leg,
                            direction="BUY" if leg.side.value == "SELL" else "SELL",
                            qty=close_qty,
                        )
                    )
                    reductions.append((leg, close_qty))
            if alert_legs:
                result = self._submit_alert(execution_type="EXIT", legs=alert_legs)
                if not self._is_failure_status((result or {}).get("status")):
                    for leg, qty in reductions:
                        leg.qty = max(0, int(leg.qty) - qty)
                        if leg.qty == 0:
                            leg.close()
            logger.info(f"Partial 50% exit for {self.name} — {len(alert_legs)} legs submitted")
        else:
            logger.warning(f"Unhandled exit action: {action}")

    @staticmethod
    def _is_no_position_exit_result(result: Dict[str, Any]) -> bool:
        """Return True when all failed exits are benign 'no position' outcomes."""
        if not isinstance(result, dict):
            return False
        legs = result.get("legs")
        if not isinstance(legs, list) or not legs:
            return False
        saw_failed = False
        for leg in legs:
            if not isinstance(leg, dict):
                return False
            leg_status = str(leg.get("status", "")).upper()
            if leg_status not in ("FAILED", "BLOCKED"):
                continue
            saw_failed = True
            msg = str(leg.get("message", "") or "").upper()
            if "NO POSITION" not in msg and "POSITION NOT FOUND" not in msg:
                return False
        return saw_failed

    def _execute_leg_rule_exit(self, action: str):
        """Execute per-leg exit actions configured in exit.leg_rules."""
        rule = self.exit_engine.last_triggered_leg_rule or {}
        rule_action = str(rule.get("action") or action.replace("leg_rule_", "")).strip().lower()
        targets = self._resolve_leg_rule_targets(rule)
        if not targets:
            logger.warning(f"Leg-rule exit had no active targets: {self.name} action={rule_action}")
            return

        if rule_action == "close_all":
            self._execute_exit("exit_all", source="leg_rule_close_all")
            # ✅ BUG FIX(9): Persist after leg-rule exit so state is clean on disk.
            try:
                self.persistence.save(self.state, str(self.state_file))
            except Exception as _e:
                logger.warning("State persist after leg-rule exit failed for %s: %s", self.name, _e)
            return

        close_qty_map: Dict[str, int] = {}
        if rule_action == "close_leg":
            close_qty_map = {leg.tag: int(leg.qty) for leg in targets}
        elif rule_action == "reduce_50pct":
            close_qty_map = {leg.tag: max(0, int(leg.qty // 2)) for leg in targets}
        elif rule_action == "partial_lots":
            lots = int(rule.get("lots") or rule.get("qty") or 1)
            close_qty_map = {leg.tag: max(0, min(int(leg.qty), lots)) for leg in targets}
        elif rule_action == "roll_next":
            # Roll selected legs to next expiry at same strike/side.
            alert_legs: List[Dict[str, Any]] = []
            updates: List[Tuple[LegState, str, float, float, str]] = []
            for leg in targets:
                if leg.qty <= 0:
                    continue
                close_payload = self._build_alert_leg(
                    leg=leg,
                    direction="BUY" if leg.side.value == "SELL" else "SELL",
                    qty=int(leg.qty),
                )
                alert_legs.append(close_payload)

                if leg.option_type is None or leg.strike is None:
                    continue
                try:
                    new_expiry = self.market.get_next_expiry(leg.expiry, "weekly_next")
                    opt_data = self.market.get_option_at_strike(leg.strike, leg.option_type, new_expiry) or {}
                except Exception as e:
                    logger.error(f"Roll-next lookup failed for {leg.tag}: {e}")
                    continue

                open_payload = self._build_alert_leg(
                    leg=leg,
                    direction=leg.side.value,
                    qty=int(leg.qty),
                    tradingsymbol_override=(
                        opt_data.get("trading_symbol")
                        or opt_data.get("tsym")
                        or opt_data.get("symbol")
                        or leg.trading_symbol
                        or leg.symbol
                    ),
                    price_override=float(opt_data.get("ltp", leg.ltp) or leg.ltp),
                )
                alert_legs.append(open_payload)

                updates.append((
                    leg,
                    new_expiry,
                    float(opt_data.get("ltp", leg.entry_price) or leg.entry_price),
                    float(opt_data.get("ltp", leg.ltp) or leg.ltp),
                    str(open_payload.get("tradingsymbol") or leg.trading_symbol),
                ))

            if alert_legs:
                result = self._submit_alert(execution_type="ADJUSTMENT", legs=alert_legs)
                if not self._is_failure_status((result or {}).get("status")):
                    for leg, new_expiry, new_entry, new_ltp, tsym in updates:
                        leg.expiry = new_expiry
                        leg.entry_price = new_entry
                        leg.ltp = new_ltp
                        if tsym:
                            leg.trading_symbol = tsym
            return
        else:
            logger.warning(f"Unsupported leg rule action '{rule_action}' in strategy {self.name}")
            return

        alert_legs = []
        reductions: List[Tuple[LegState, int]] = []
        for leg in targets:
            qty = int(close_qty_map.get(leg.tag, 0))
            if qty <= 0:
                continue
            alert_legs.append(
                self._build_alert_leg(
                    leg=leg,
                    direction="BUY" if leg.side.value == "SELL" else "SELL",
                    qty=qty,
                )
            )
            reductions.append((leg, qty))

        if alert_legs:
            result = self._submit_alert(execution_type="EXIT", legs=alert_legs)
            if not self._is_failure_status((result or {}).get("status")):
                for leg, qty in reductions:
                    leg.qty = max(0, int(leg.qty) - qty)
                    if leg.qty == 0:
                        leg.close()
        if ref == "all":
            return [leg for leg in self.state.legs.values() if leg.is_active]
        if ref == "group" and group:
            return [leg for leg in self.state.legs.values() if leg.is_active and leg.group == group]
        leg = self.state.legs.get(ref)
        if leg and leg.is_active:
            return [leg]
        return []

    def _dispatch_adjustment_orders(self, before_legs: Dict[str, LegState]) -> bool:
        """
        Translate state transitions from adjustment engine into broker orders.
        Returns False when broker rejects the adjustment payload.
        """
        # NEW: Check RMS limits before sending adjustment orders
        # Calculate additional lots from new legs (delta > 0)
        additional_lots = 0
        for tag in set(self.state.legs.keys()) - set(before_legs.keys()):
            leg = self.state.legs.get(tag)
            if leg and (leg.is_active or leg.order_status == "PENDING"):
                additional_lots += leg.qty
        if not self._check_rms_limits(additional_lots=additional_lots):
            logger.warning("Adjustment blocked by RMS limits")
            return False

        tags = sorted(set(before_legs.keys()) | set(self.state.legs.keys()))
        legs_payload: List[Dict[str, Any]] = []
        close_by_symbol: Dict[str, int] = {}
        open_by_symbol: Dict[str, int] = {}

        for tag in tags:
            before = before_legs.get(tag)
            after = self.state.legs.get(tag)
            before_qty = int(before.qty) if (before and before.is_active) else 0
            # Include PENDING legs that are NEW (not in before_legs) — these are
            # adjustment-created legs awaiting fill dispatch.
            after_qty = int(after.qty) if after and (
                after.is_active or (after.order_status == "PENDING" and before is None)
            ) else 0
            delta = after_qty - before_qty

            if delta < 0:
                ref = before or after
                if ref is None:
                    continue
                payload = self._build_alert_leg(
                    leg=ref,
                    direction="BUY" if ref.side.value == "SELL" else "SELL",
                    qty=abs(delta),
                )
                legs_payload.append(payload)
                sym = str(payload.get("tradingsymbol") or "").strip()
                if sym:
                    close_by_symbol[sym] = close_by_symbol.get(sym, 0) + int(payload.get("qty", 0) or 0)
                # Mark the closing leg for fill tracking so notify_fill can match it
                if after and after.order_status not in ("PENDING",):
                    after.order_status = "CLOSE_PENDING"
                    after.order_placed_at = datetime.now()
            elif delta > 0:
                if after is None:
                    continue
                payload = self._build_alert_leg(
                    leg=after,
                    direction=after.side.value,
                    qty=delta,
                )
                legs_payload.append(payload)
                sym = str(payload.get("tradingsymbol") or "").strip()
                if sym:
                    open_by_symbol[sym] = open_by_symbol.get(sym, 0) + int(payload.get("qty", 0) or 0)

        if not legs_payload:
            return True

        # Smart overlap netting: when close and open resolve to the same
        # symbol (e.g. same-strike no-op from adjustment engine), net the
        # quantities.  Equal qty → remove both (no order needed).
        # Unequal qty → keep only the net difference.
        overlap = sorted(set(close_by_symbol.keys()) & set(open_by_symbol.keys()))
        if overlap:
            for sym in overlap:
                cq = close_by_symbol.get(sym, 0)
                oq = open_by_symbol.get(sym, 0)
                net = oq - cq  # positive = net open, negative = net close
                if net == 0:
                    # Perfect cancel — remove both entries from payload
                    logger.info(
                        "ADJUSTMENT_NOOP_NETTED | strategy=%s | symbol=%s | close=%s open=%s → netted to 0, removing both",
                        self.name, sym, cq, oq,
                    )
                    legs_payload = [
                        lp for lp in legs_payload
                        if str(lp.get("tradingsymbol") or "").strip() != sym
                    ]
                else:
                    # Partial overlap — keep net difference
                    logger.info(
                        "ADJUSTMENT_PARTIAL_NET | strategy=%s | symbol=%s | close=%s open=%s → net=%s",
                        self.name, sym, cq, oq, net,
                    )
                    # Remove all legs for this symbol, then add back the net
                    # Find a representative leg to build from
                    rep_leg = None
                    for lp in legs_payload:
                        if str(lp.get("tradingsymbol") or "").strip() == sym:
                            rep_leg = lp
                            break
                    legs_payload = [
                        lp for lp in legs_payload
                        if str(lp.get("tradingsymbol") or "").strip() != sym
                    ]
                    if rep_leg is not None:
                        net_leg = dict(rep_leg)
                        net_leg["qty"] = abs(net)
                        if net > 0:
                            # Net open: same direction as the opening leg
                            for lp in [l for l in legs_payload]:
                                pass  # already removed
                            # Use the open direction — find original open direction
                            net_leg["direction"] = rep_leg.get("direction", "SELL")
                        else:
                            # Net close: opposite of position side
                            # Use the close direction
                            net_leg["direction"] = rep_leg.get("direction", "BUY")
                        legs_payload.append(net_leg)

            if not legs_payload:
                logger.info(
                    "ADJUSTMENT_ALL_NETTED | strategy=%s | all overlapping legs cancelled out, no orders to send",
                    self.name,
                )
                return True

        # Log the full adjustment order being dispatched
        adj_rule_name = getattr(self.adjustment_engine, '_last_triggered_rule_name', None) or 'unknown'
        close_legs = [lp for lp in legs_payload if str(lp.get("direction","")).upper() in ("BUY","SELL") and str(lp.get("direction","")).upper() != str(self.config.get("identity",{}).get("side","SELL")).upper()]
        adj_detail = "; ".join(
            f"{lp.get('direction')} {lp.get('tradingsymbol')} qty={lp.get('qty')}"
            f" delta={lp.get('delta')} iv={lp.get('iv')}"
            for lp in legs_payload
        )
        adj_reason = getattr(self.adjustment_engine, '_last_triggered_reason', 'conditions_met')
        self._last_event_type = "ADJUSTMENT"
        self._last_event_reason = adj_rule_name
        logger.info(
            "ADJUSTMENT_DISPATCH | strategy=%s | rule=%s | reason=%s | legs=%d\n  Orders: %s",
            self.name, adj_rule_name, adj_reason, len(legs_payload), adj_detail,
        )

        result = self._submit_alert(execution_type="ADJUSTMENT", legs=legs_payload)
        return not self._is_failure_status((result or {}).get("status"))

    # NEW: Expiry day actions (copied from executor.py)
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
                leg.close()
            except Exception as e:
                logger.error(f"Roll failed for {leg.tag}: {e}")
        logger.info(f"Rolled {len(new_legs)} legs to next expiry")

    def _build_alert_leg(
        self,
        leg: LegState,
        direction: str,
        qty: int,
        tradingsymbol_override: Optional[str] = None,
        price_override: Optional[float] = None,
    ) -> Dict[str, Any]:
        identity = self.config.get("identity", {})
        default_order_type = str(identity.get("order_type", "MARKET")).upper()
        order_type = "LIMIT" if default_order_type == "LIMIT" else "MARKET"
        exchange = str(identity.get("exchange", "NFO")).upper()

        tradingsymbol = tradingsymbol_override or leg.trading_symbol or leg.symbol
        # ScriptMaster rule: certain instruments (OPTSTK/OPTFUT) must use LIMIT.
        try:
            if requires_limit_order(exchange=exchange, tradingsymbol=tradingsymbol):
                order_type = "LIMIT"
        except Exception:
            pass

        if price_override is not None:
            price = float(price_override)
        else:
            price = float(leg.ltp) if order_type == "LIMIT" else 0.0

        if tradingsymbol == leg.symbol and leg.instrument == InstrumentType.OPT and leg.strike is not None and leg.option_type is not None:
            try:
                opt_data = self.market.get_option_at_strike(leg.strike, leg.option_type, leg.expiry) or {}
                resolved = opt_data.get("trading_symbol") or opt_data.get("tsym") or opt_data.get("symbol")
                if resolved:
                    tradingsymbol = str(resolved)
                    leg.trading_symbol = tradingsymbol
            except Exception:
                pass
        # ✅ BUG FIX: Resolve futures trading_symbol via ScriptMaster when it was not
        # set during entry (e.g. "NIFTY" -> "NIFTY26MARFUT").  Without this, the
        # broker order is placed with the bare underlying name and gets rejected.
        if tradingsymbol == leg.symbol and leg.instrument == InstrumentType.FUT:
            try:
                from scripts.unified_scriptmaster import get_future
                fut_info = get_future(leg.symbol, exchange, result=0)
                if isinstance(fut_info, dict):
                    resolved = str(fut_info.get("TradingSymbol") or fut_info.get("tsym") or "")
                    if resolved:
                        tradingsymbol = resolved
                        leg.trading_symbol = tradingsymbol
            except Exception:
                pass

        return {
            "tradingsymbol": tradingsymbol,
            "direction": str(direction).upper(),
            "qty": self._lots_to_order_qty(int(qty), leg),
            "order_type": order_type,
            "price": price,
            "product_type": identity.get("product_type", "NRML"),
            # ── Extended leg info for Telegram / audit ──
            "tag": leg.tag,
            "option_type": leg.option_type.value if leg.option_type else None,
            "strike": leg.strike,
            "expiry": leg.expiry,
            "delta": round(leg.delta, 4) if leg.delta else None,
            "iv": round(leg.iv, 2) if leg.iv else None,
            "lots": int(qty),
        }

    def _lots_to_order_qty(self, lots: int, leg: Optional[LegState] = None) -> int:
        """
        Convert lots (internal state unit) to broker order quantity (contract units).
        """
        try:
            normalized_lots = max(0, int(lots))
        except (TypeError, ValueError):
            normalized_lots = 0
        if normalized_lots == 0:
            return 0

        lot_size = 1
        expiry = getattr(leg, "expiry", None) if leg is not None else None
        try:
            resolved = int(self.market.get_lot_size(expiry))
            if resolved > 0:
                lot_size = resolved
        except Exception as e:
            logger.warning("Failed to resolve lot_size for expiry %s: %s, defaulting to 1", expiry, e)
            lot_size = 1
        return normalized_lots * lot_size

    def _submit_alert(self, execution_type: str, legs: List[Dict[str, Any]]) -> Dict[str, Any]:
        identity = self.config.get("identity", {})
        alert = {
            "secret_key": self._resolve_webhook_secret(),
            "execution_type": execution_type,
            "strategy_name": self.name,
            "exchange": identity.get("exchange", "NFO"),
            "legs": legs,
            "test_mode": self._resolve_test_mode(),
        }
        result = self.bot.process_alert(alert)
        if isinstance(result, dict) and self._is_failure_status(result.get("status")):
            logger.error(f"{execution_type} order rejected for {self.name}: {result}")
        return result if isinstance(result, dict) else {}

    @staticmethod
    def _is_failure_status(status: Any) -> bool:
        normalized = str(status or "").strip().upper()
        return normalized in ("FAILED", "BLOCKED", "ERROR")

    def _resolve_webhook_secret(self) -> str:
        """Get webhook secret from bot config or env."""
        bot_cfg = getattr(self.bot, "config", None)
        if bot_cfg and hasattr(bot_cfg, "webhook_secret"):
            return str(bot_cfg.webhook_secret)
        import os
        return os.getenv("WEBHOOK_SECRET_KEY", os.getenv("WEBHOOK_SECRET", ""))

    def _resolve_test_mode(self) -> Optional[str]:
        """Return test_mode if paper mode or test mode is enabled."""
        identity = self.config.get("identity", {})
        if identity.get("paper_mode") or identity.get("test_mode"):
            return "SUCCESS"
        return None
