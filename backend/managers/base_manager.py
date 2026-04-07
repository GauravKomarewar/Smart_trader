"""
BaseManager — Abstract base for all domain managers.

Each manager:
  1. Runs a background thread with a periodic refresh loop
  2. Fetches data from broker sessions via multi_broker registry
  3. Upserts into its PostgreSQL table
  4. Reports health to mgr_health table
  5. Can be started/stopped/queried by SupremeManager
"""

from __future__ import annotations

import logging
import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from db.trading_db import get_trading_conn
import psycopg2.extras

logger = logging.getLogger("smart_trader.managers.base")

# Shared thread pool for all managers (bounded to avoid thread explosion)
_MANAGER_POOL = ThreadPoolExecutor(max_workers=8, thread_name_prefix="mgr-pool")


class BaseManager(ABC):
    """Abstract base class for all domain managers."""

    # Subclasses must set these
    MANAGER_NAME: str = "base"
    REFRESH_INTERVAL: float = 2.0  # seconds between refresh cycles
    SESSION_TIMEOUT: float = 10.0  # max seconds to wait for one broker session

    def __init__(self):
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._running = False
        self._cycle_count = 0
        self._error_count = 0
        self._last_error: Optional[str] = None
        self._last_run_ms = 0
        self._log = logging.getLogger(f"smart_trader.mgr.{self.MANAGER_NAME}")
        # Track per-session data counts to guard against transient empty results
        # Key: "user_id:config_id" → last known row count
        self._prev_data_counts: Dict[str, int] = {}

    # ── Lifecycle ──────────────────────────────────────────────────────────

    def start(self):
        """Start the manager background thread."""
        if self._running:
            self._log.warning("Already running")
            return
        self._stop_event.clear()
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"mgr-{self.MANAGER_NAME}",
            daemon=True,
        )
        self._thread.start()
        self._log.info("Started (interval=%.1fs)", self.REFRESH_INTERVAL)
        self._update_health("running")

    def stop(self):
        """Signal the manager to stop."""
        if not self._running:
            return
        self._stop_event.set()
        self._running = False
        if self._thread:
            self._thread.join(timeout=5.0)
            self._thread = None
        self._log.info("Stopped")
        self._update_health("stopped")

    @property
    def is_running(self) -> bool:
        return self._running and not self._stop_event.is_set()

    # ── Main loop ──────────────────────────────────────────────────────────

    def _run_loop(self):
        """Background loop: refresh → write DB → report health → sleep."""
        while not self._stop_event.is_set():
            t0 = time.monotonic()
            try:
                self._do_refresh()
                self._cycle_count += 1
            except Exception as e:
                self._error_count += 1
                self._last_error = str(e)
                if self._error_count <= 3 or self._error_count % 50 == 0:
                    self._log.error("Refresh error (#%d): %s", self._error_count, e)
            finally:
                self._last_run_ms = int((time.monotonic() - t0) * 1000)
                # Update health every 10 cycles or on error
                if self._cycle_count % 10 == 0 or self._error_count % 10 == 0:
                    self._update_health("running")

            # Sleep in small increments so stop_event is responsive
            elapsed = time.monotonic() - t0
            remaining = max(0, self.REFRESH_INTERVAL - elapsed)
            self._stop_event.wait(timeout=remaining)

    def _do_refresh(self):
        """Fetch from all live broker sessions and write to DB (concurrent per user)."""
        from broker.multi_broker import registry as mb

        all_sessions = mb.get_all_sessions()
        # Group by user_id
        user_sessions: dict[str, list] = {}
        for sess in all_sessions:
            if sess.is_demo:
                continue
            user_sessions.setdefault(sess.user_id, []).append(sess)

        if not user_sessions:
            return

        # Process all users concurrently
        futures = {
            _MANAGER_POOL.submit(self.refresh_user, user_id, sessions): user_id
            for user_id, sessions in user_sessions.items()
        }
        for f in as_completed(futures, timeout=self.SESSION_TIMEOUT + 5):
            try:
                f.result()
            except Exception as e:
                uid = futures[f]
                self._log.debug("Refresh failed user=%s: %s", uid[:8], e)

    @abstractmethod
    def refresh_user(self, user_id: str, sessions: list):
        """Fetch data for one user from all their broker sessions and upsert to DB.
        Subclasses must implement this."""
        ...

    # ── Concurrent session helpers ─────────────────────────────────────────

    def _refresh_sessions_concurrent(self, user_id: str, sessions: list,
                                      per_session_fn):
        """Run per_session_fn(user_id, sess) concurrently for all live sessions."""
        live = [s for s in sessions if s.is_live]
        if not live:
            return
        if len(live) == 1:
            # Single session — no need for threads
            try:
                per_session_fn(user_id, live[0])
            except Exception as e:
                self._log.debug("Session refresh failed %s/%s: %s",
                                live[0].broker_id, live[0].client_id, e)
            return
        futures = {
            _MANAGER_POOL.submit(per_session_fn, user_id, sess): sess
            for sess in live
        }
        for f in as_completed(futures, timeout=self.SESSION_TIMEOUT):
            try:
                f.result()
            except Exception as e:
                sess = futures[f]
                self._log.debug("Session refresh failed %s/%s: %s",
                                sess.broker_id, sess.client_id, e)

    def _should_delete_stale(self, user_id: str, config_id: str,
                              current_count: int) -> bool:
        """Return True if it's safe to delete stale rows for this session.
        
        Guards against transient empty results: if the previous cycle had data
        but this cycle has zero rows, skip deletion this cycle. Two consecutive
        empties are treated as genuine (positions really closed / orders done).
        """
        key = f"{user_id}:{config_id}"
        prev = self._prev_data_counts.get(key, -1)  # -1 = first cycle
        self._prev_data_counts[key] = current_count
        if current_count == 0 and prev > 0:
            self._log.debug("Skipping stale-delete for %s:%s (transient empty, prev=%d)",
                            user_id[:8], config_id[:8], prev)
            return False
        return True

    # ── Health reporting ───────────────────────────────────────────────────

    def _update_health(self, status: str):
        """Write current health state to mgr_health table."""
        try:
            conn = get_trading_conn()
            try:
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO mgr_health (manager_name, status, last_run_at, last_run_ms,
                                            cycle_count, error_count, last_error, updated_at)
                    VALUES (%s, %s, NOW(), %s, %s, %s, %s, NOW())
                    ON CONFLICT (manager_name)
                    DO UPDATE SET status = EXCLUDED.status,
                                  last_run_at = EXCLUDED.last_run_at,
                                  last_run_ms = EXCLUDED.last_run_ms,
                                  cycle_count = EXCLUDED.cycle_count,
                                  error_count = EXCLUDED.error_count,
                                  last_error = EXCLUDED.last_error,
                                  updated_at = EXCLUDED.updated_at
                """, (self.MANAGER_NAME, status, self._last_run_ms,
                      self._cycle_count, self._error_count, self._last_error))
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            self._log.debug("Health update failed: %s", e)

    def get_health(self) -> dict:
        """Return current health snapshot."""
        return {
            "name": self.MANAGER_NAME,
            "running": self.is_running,
            "cycle_count": self._cycle_count,
            "error_count": self._error_count,
            "last_run_ms": self._last_run_ms,
            "last_error": self._last_error,
        }

    # ── DB helpers ─────────────────────────────────────────────────────────

    @staticmethod
    def _upsert_jsonb_rows(table: str, unique_cols: list[str],
                           rows: list[dict], extra_cols: list[str] | None = None):
        """
        Bulk upsert rows into a JSONB table.
        Each row dict must have all unique_cols + 'data' (JSONB) + extra_cols.
        Uses INSERT ... ON CONFLICT DO UPDATE.
        """
        if not rows:
            return
        all_cols = unique_cols + (extra_cols or []) + ["data", "fetched_at"]
        placeholders = ", ".join(["%s"] * len(all_cols))
        col_names = ", ".join(all_cols)
        conflict = ", ".join(unique_cols)
        update_set = ", ".join(
            f"{c} = EXCLUDED.{c}" for c in all_cols if c not in unique_cols
        )

        conn = get_trading_conn()
        try:
            cur = conn.cursor()
            now = datetime.now(timezone.utc)
            for row in rows:
                values = []
                for c in all_cols:
                    if c == "fetched_at":
                        values.append(now)
                    elif c == "data":
                        values.append(psycopg2.extras.Json(row.get("data", {})))
                    else:
                        values.append(row.get(c))
                cur.execute(
                    f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) "
                    f"ON CONFLICT ({conflict}) DO UPDATE SET {update_set}",
                    values,
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _delete_stale(table: str, user_id: str, config_id: str,
                      current_keys: list[tuple], key_cols: list[str]):
        """
        Remove rows for this user+config that are NOT in current_keys.
        This handles positions/orders that disappeared (closed/cancelled).
        """
        if not key_cols:
            return
        conn = get_trading_conn()
        try:
            cur = conn.cursor()
            if not current_keys:
                # No data at all → delete all for this config
                cur.execute(
                    f"DELETE FROM {table} WHERE user_id = %s AND config_id = %s",
                    (user_id, config_id),
                )
            else:
                # Build exclusion condition
                conditions = " AND ".join(
                    f"{col} = %s" for col in key_cols
                )
                placeholders = " OR ".join(
                    f"({conditions})" for _ in current_keys
                )
                flat_values = []
                for key_tuple in current_keys:
                    flat_values.extend(key_tuple)

                cur.execute(
                    f"DELETE FROM {table} "
                    f"WHERE user_id = %s AND config_id = %s "
                    f"AND NOT ({placeholders})",
                    [user_id, config_id] + flat_values,
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
