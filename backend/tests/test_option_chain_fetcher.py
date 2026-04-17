"""
Tests for the option chain SQLite fetcher — stale row deletion and DB integrity.

Covers:
  - After upsert, rows NOT in the fresh payload are deleted (stale row fix)
  - If chain_rows is empty, no delete is performed (safety guard)
  - DB has correct row count after update
  - strikecount constant is 20 (covers ATM±1000pts for 50-point step indices)
"""
from __future__ import annotations

import sqlite3
import tempfile
import os
import pytest

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── Import the module under test ──────────────────────────────────────────────
from trading.utils.option_chain_fetcher import create_or_update_chain_db
import trading.utils.option_chain_fetcher as _fetcher_mod


# ── Helpers ───────────────────────────────────────────────────────────────────

def _row(strike: float, option_type: str, ltp: float = 100.0) -> dict:
    return {
        "strike": strike,
        "option_type": option_type,
        "ltp": ltp,
        "oi": 10000,
        "volume": 500,
        "delta": -0.3,
        "gamma": 0.01,
        "theta": -0.5,
        "vega": 0.2,
        "iv": 15.0,
        "bid": ltp - 0.5,
        "ask": ltp + 0.5,
        "trading_symbol": f"NIFTY{int(strike)}{'CE' if option_type == 'CE' else 'PE'}",
        "expiry": "24-04-2026",
    }


def _create_db(db_path: str, rows: list) -> None:
    """Call create_or_update_chain_db with the path monkey-patched to db_path."""
    from unittest.mock import patch
    from pathlib import Path
    with patch.object(_fetcher_mod, "_get_db_path", return_value=Path(db_path)):
        create_or_update_chain_db(
            exchange="NFO",
            symbol="NIFTY",
            expiry="24-04-2026",
            spot_price=24000.0,
            atm_strike=24000.0,
            fut_ltp=24000.0,
            chain_rows=rows,
        )


def _count(path: str) -> int:
    conn = sqlite3.connect(path)
    try:
        cur = conn.execute("SELECT COUNT(*) FROM option_chain")
        return cur.fetchone()[0]
    finally:
        conn.close()


def _strikes(path: str) -> set:
    conn = sqlite3.connect(path)
    try:
        cur = conn.execute("SELECT strike, option_type FROM option_chain")
        return {(row[0], row[1]) for row in cur.fetchall()}
    finally:
        conn.close()


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestStaleRowDeletion:
    def test_stale_rows_removed_on_update(self, tmp_path):
        """Rows not in fresh payload must be deleted."""
        db_path = str(tmp_path / "chain.sqlite")
        # First write: 3 strikes
        initial = [
            _row(24000, "CE"), _row(24000, "PE"),
            _row(24050, "CE"), _row(24050, "PE"),
            _row(24100, "CE"), _row(24100, "PE"),
        ]
        _create_db(db_path, initial)
        assert _count(db_path) == 6

        # Second write: only 2 strikes (24050 removed)
        updated = [
            _row(24000, "CE"), _row(24000, "PE"),
            _row(24100, "CE"), _row(24100, "PE"),
        ]
        _create_db(db_path, updated)
        remaining = _strikes(db_path)
        assert (24050.0, "CE") not in remaining
        assert (24050.0, "PE") not in remaining
        assert _count(db_path) == 4

    def test_empty_payload_does_not_delete_existing_rows(self, tmp_path):
        """Empty chain_rows must not wipe the DB (safety guard)."""
        db_path = str(tmp_path / "chain.sqlite")
        initial = [_row(24000, "CE"), _row(24000, "PE")]
        _create_db(db_path, initial)
        assert _count(db_path) == 2

        _create_db(db_path, [])  # empty update
        assert _count(db_path) == 2  # rows must survive

    def test_fresh_rows_upserted_correctly(self, tmp_path):
        """Existing row LTP must be updated on second write."""
        db_path = str(tmp_path / "chain.sqlite")
        _create_db(db_path, [_row(24000, "CE", ltp=100.0)])
        _create_db(db_path, [_row(24000, "CE", ltp=150.0)])

        conn = sqlite3.connect(db_path)
        try:
            cur = conn.execute("SELECT ltp FROM option_chain WHERE strike=24000 AND option_type='CE'")
            ltp = cur.fetchone()[0]
        finally:
            conn.close()
        assert ltp == pytest.approx(150.0)

    def test_atm_strike_written(self, tmp_path):
        db_path = str(tmp_path / "chain.sqlite")
        rows = [_row(24000, "CE"), _row(24000, "PE")]
        _create_db(db_path, rows)
        conn = sqlite3.connect(db_path)
        try:
            cur = conn.execute("SELECT COUNT(*) FROM option_chain WHERE strike=24000")
            count = cur.fetchone()[0]
        finally:
            conn.close()
        assert count == 2

    def test_multiple_option_types_preserved(self, tmp_path):
        db_path = str(tmp_path / "chain.sqlite")
        rows = [_row(s, ot) for s in [23900, 24000, 24100] for ot in ["CE", "PE"]]
        _create_db(db_path, rows)
        assert _count(db_path) == 6
        # Partial refresh: only CE strikes remain
        ce_only = [_row(s, "CE") for s in [23900, 24000, 24100]]
        _create_db(db_path, ce_only)
        remaining = _strikes(db_path)
        # PE rows should be gone
        for s in [23900.0, 24000.0, 24100.0]:
            assert (s, "PE") not in remaining
        assert _count(db_path) == 3


class TestFyersClientStrikecount:
    """Verify the Fyers client uses strikecount=20 (covers ATM±1000pts)."""
    def test_strikecount_is_20(self):
        # Directly inspect the source constant without importing the full module
        import ast
        src_path = os.path.join(
            os.path.dirname(__file__), "..", "broker", "fyers_client.py"
        )
        with open(src_path) as f:
            source = f.read()
        # Find "strikecount": 20 in the source
        assert '"strikecount": 20' in source or "'strikecount': 20" in source, (
            "fyers_client.py must use strikecount=20 for sufficient strike coverage"
        )
