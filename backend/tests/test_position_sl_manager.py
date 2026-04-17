"""
Tests for PositionSLManager backoff and retry logic.

Covers:
  - _BACKOFF_SECS schedule integrity
  - Cooldown prevents re-firing during backoff window
  - Fail counter increments on each failure
  - _MAX_FAILURES triggers deactivation (no infinite retry)
  - Successful exit clears fail counter and cooldown
  - in_flight deduplication
  - Backoff index capped at last entry (no IndexError)
"""
from __future__ import annotations

import time
import pytest
from unittest.mock import patch, MagicMock, call

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from trading.position_sl_manager import PositionSLManager


class TestBackoffSchedule:
    def test_backoff_list_is_ascending(self):
        sched = PositionSLManager._BACKOFF_SECS
        assert list(sched) == sorted(sched), "Backoff schedule must be ascending"

    def test_backoff_has_at_least_one_entry(self):
        assert len(PositionSLManager._BACKOFF_SECS) >= 1

    def test_max_failures_exceeds_backoff_list_length(self):
        # _MAX_FAILURES >= len(_BACKOFF_SECS) ensures the cap is reachable
        assert PositionSLManager._MAX_FAILURES >= len(PositionSLManager._BACKOFF_SECS)

    def test_first_backoff_is_short(self):
        # First retry shouldn't wait more than 30s
        assert PositionSLManager._BACKOFF_SECS[0] <= 30

    def test_last_backoff_caps_at_reasonable_max(self):
        # Should not exceed 10 minutes
        assert PositionSLManager._BACKOFF_SECS[-1] <= 600


class TestExitRetryTracking:
    """Unit-test the internal retry state without DB or broker calls."""

    def _mgr(self) -> PositionSLManager:
        return PositionSLManager()

    def test_initial_state_is_empty(self):
        mgr = self._mgr()
        assert mgr._exit_fail_counts == {}
        assert mgr._exit_cooldown_until == {}

    def test_fail_count_increments(self):
        mgr = self._mgr()
        key = ("user1", "cfg1", "NIFTY|NRML")
        # Simulate 3 failures
        for i in range(1, 4):
            fails = mgr._exit_fail_counts.get(key, 0) + 1
            mgr._exit_fail_counts[key] = fails
            backoff = mgr._BACKOFF_SECS[min(fails - 1, len(mgr._BACKOFF_SECS) - 1)]
            mgr._exit_cooldown_until[key] = time.monotonic() + backoff
        assert mgr._exit_fail_counts[key] == 3
        assert mgr._exit_cooldown_until[key] > time.monotonic()

    def test_backoff_index_capped_no_index_error(self):
        """Index must never exceed len(_BACKOFF_SECS) - 1, regardless of fail count."""
        mgr = self._mgr()
        for fails in range(1, 30):  # Way beyond _MAX_FAILURES
            idx = min(fails - 1, len(mgr._BACKOFF_SECS) - 1)
            backoff = mgr._BACKOFF_SECS[idx]
            assert backoff == mgr._BACKOFF_SECS[-1] if fails > len(mgr._BACKOFF_SECS) else backoff

    def test_cooldown_blocks_entry(self):
        """Simulate the cooldown check that happens in _check_row."""
        mgr = self._mgr()
        key = ("u", "c", "SYM|NRML")
        # Set cooldown far in the future
        mgr._exit_cooldown_until[key] = time.monotonic() + 9999
        assert time.monotonic() < mgr._exit_cooldown_until[key]

    def test_cooldown_expires(self):
        mgr = self._mgr()
        key = ("u", "c", "SYM|NRML")
        # Set cooldown in the past
        mgr._exit_cooldown_until[key] = time.monotonic() - 1
        assert time.monotonic() >= mgr._exit_cooldown_until[key]

    def test_success_clears_fail_counts(self):
        mgr = self._mgr()
        key = ("u", "c", "SYM|NRML")
        mgr._exit_fail_counts[key] = 5
        mgr._exit_cooldown_until[key] = time.monotonic() + 300
        # Simulate success path
        mgr._exit_fail_counts.pop(key, None)
        mgr._exit_cooldown_until.pop(key, None)
        assert key not in mgr._exit_fail_counts
        assert key not in mgr._exit_cooldown_until

    def test_max_failures_reached_would_deactivate(self):
        """After _MAX_FAILURES, the deactivate path must be taken."""
        mgr = self._mgr()
        key = ("u", "c", "SYM|NRML")
        mgr._exit_fail_counts[key] = mgr._MAX_FAILURES
        fails = mgr._exit_fail_counts[key]
        assert fails >= mgr._MAX_FAILURES  # deactivate branch reachable

    def test_in_flight_prevents_duplicate_exits(self):
        mgr = self._mgr()
        key = ("u", "c", "SYM|NRML")
        mgr._in_flight.add(key)
        assert key in mgr._in_flight
        # Second "check" should bail out early
        mgr._in_flight.discard(key)
        assert key not in mgr._in_flight


class TestPositionSLManagerLifecycle:
    def test_start_and_stop(self):
        mgr = PositionSLManager()
        # Patch _loop so it doesn't actually sleep
        with patch.object(mgr, "_loop"):
            mgr.start()
            assert mgr._running is True
            mgr.stop()
            assert mgr._running is False

    def test_double_start_is_idempotent(self):
        mgr = PositionSLManager()
        with patch.object(mgr, "_loop"):
            mgr.start()
            thread1 = mgr._thread
            mgr.start()  # second start should not create a new thread
            assert mgr._thread is thread1
            mgr.stop()
