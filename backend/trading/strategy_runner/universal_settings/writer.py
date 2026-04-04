import json
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional


class StrategyRunWriter:
    """
    Lightweight run/event/metrics writer used by tests and dashboard analytics.
    """

    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=5)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self):
        conn = self._connect()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS strategy_runs (
                    run_id TEXT PRIMARY KEY,
                    strategy_name TEXT,
                    strategy_version TEXT,
                    exchange TEXT,
                    symbol TEXT,
                    market_type TEXT,
                    resolved_config TEXT,
                    started_at TEXT,
                    stopped_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS strategy_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS strategy_metrics (
                    run_id TEXT PRIMARY KEY,
                    max_mtm REAL,
                    max_drawdown REAL,
                    adjustments INTEGER,
                    entry_time TEXT,
                    exit_time TEXT,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.commit()
        finally:
            conn.close()

    def start_run(self, run_id: str, resolved_config: Dict[str, Any], market_type: str = "database_market"):
        cfg = resolved_config or {}
        now = datetime.now().isoformat()
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO strategy_runs(
                    run_id, strategy_name, strategy_version, exchange, symbol,
                    market_type, resolved_config, started_at, stopped_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT stopped_at FROM strategy_runs WHERE run_id=?), NULL))
                """,
                (
                    run_id,
                    cfg.get("strategy_name") or cfg.get("name"),
                    cfg.get("strategy_version"),
                    cfg.get("exchange"),
                    cfg.get("symbol"),
                    market_type,
                    json.dumps(cfg, default=str),
                    now,
                    run_id,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def stop_run(self, run_id: str):
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE strategy_runs SET stopped_at = ? WHERE run_id = ?",
                (datetime.now().isoformat(), run_id),
            )
            conn.commit()
        finally:
            conn.close()

    def log_event(self, run_id: str, event_type: str, payload: Optional[Dict[str, Any]] = None):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO strategy_events(run_id, event_type, payload, created_at)
                VALUES(?, ?, ?, ?)
                """,
                (
                    run_id,
                    event_type,
                    json.dumps(payload or {}, default=str),
                    datetime.now().isoformat(),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def update_metrics(
        self,
        run_id: str,
        max_mtm: float,
        max_drawdown: float,
        adjustments: int,
        entry_time: Optional[str] = None,
        exit_time: Optional[str] = None,
    ):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO strategy_metrics(
                    run_id, max_mtm, max_drawdown, adjustments, entry_time, exit_time, updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    max_mtm=excluded.max_mtm,
                    max_drawdown=excluded.max_drawdown,
                    adjustments=excluded.adjustments,
                    entry_time=excluded.entry_time,
                    exit_time=excluded.exit_time,
                    updated_at=excluded.updated_at
                """,
                (
                    run_id,
                    float(max_mtm),
                    float(max_drawdown),
                    int(adjustments),
                    entry_time,
                    exit_time,
                    datetime.now().isoformat(),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def _row_to_dict(self, row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
        return dict(row) if row is not None else None

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        conn = self._connect()
        try:
            row = conn.execute("SELECT * FROM strategy_runs WHERE run_id = ?", (run_id,)).fetchone()
            return self._row_to_dict(row)
        finally:
            conn.close()

    def get_run_events(self, run_id: str) -> List[Dict[str, Any]]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM strategy_events WHERE run_id = ? ORDER BY id",
                (run_id,),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_run_metrics(self, run_id: str) -> Optional[Dict[str, Any]]:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM strategy_metrics WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            return self._row_to_dict(row)
        finally:
            conn.close()
