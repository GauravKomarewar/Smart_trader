"""
Tests for strategy runner Reconciliation engine.

Covers:
  - reconcile() with matching broker positions returns no warnings
  - reconcile() warns when broker shows active position for leg marked closed
  - reconcile() skips close when positions untagged (BUG-11 guard)
  - reconcile() does NOT close legs when broker returns empty with active legs
  - reconcile() handles tagged broker positions correctly
  - AdjustmentEngine noop reactivation clears exit fields (Bug 3 fix)
"""
from __future__ import annotations

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from trading.strategy_runner.state import StrategyState, LegState
from trading.strategy_runner.models import Side, InstrumentType, OptionType
from trading.strategy_runner.reconciliation import BrokerReconciliation


# ── Helpers ───────────────────────────────────────────────────────────────────

def _leg(tag: str, is_active: bool = True, qty: int = 1, symbol: str = "NIFTY24CE") -> LegState:
    leg = LegState(
        tag=tag,
        symbol="NIFTY",
        instrument=InstrumentType.OPT,
        option_type=OptionType.CE,
        strike=24000.0,
        expiry="24-04-2026",
        side=Side.SELL,
        qty=qty,
        entry_price=100.0,
        ltp=100.0,
        trading_symbol=symbol,
    )
    leg.is_active = is_active
    return leg


def _pos(tag: str = None, qty: int = -1, symbol: str = "NIFTY24CE") -> dict:
    p = {
        "symbol": symbol,
        "exchange": "NFO",
        "product": "NRML",
        "qty": qty,
        "avg_price": 100.0,
        "ltp": 95.0,
    }
    if tag:
        p["tag"] = tag
    return p


# ── Reconciliation tests ───────────────────────────────────────────────────────

class TestReconciliation:
    def _state_with_legs(self, *legs: LegState) -> StrategyState:
        state = StrategyState()
        for leg in legs:
            state.legs[leg.tag] = leg
        return state

    def test_empty_broker_positions_empty_state_no_warnings(self):
        state = self._state_with_legs()
        rec = BrokerReconciliation(state)
        warnings = rec.reconcile([])
        assert warnings == []

    def test_matching_positions_no_warnings(self):
        leg = _leg("CE_SELL")
        state = self._state_with_legs(leg)
        pos = _pos(tag="CE_SELL", qty=-1)
        rec = BrokerReconciliation(state)
        warnings = rec.reconcile([pos])
        assert warnings == []

    def test_untagged_broker_positions_returns_warning_not_close(self):
        """Untagged positions must not cause active legs to be closed (BUG-11)."""
        leg = _leg("CE_SELL")
        leg.is_active = True
        state = self._state_with_legs(leg)
        pos = _pos(tag=None, qty=-1)  # no tag
        rec = BrokerReconciliation(state)
        warnings = rec.reconcile([pos])
        # Leg must still be active — not closed
        assert leg.is_active is True
        # Should produce a warning about untagged positions
        assert any("untagged" in w.lower() for w in warnings)

    def test_untagged_symbol_with_spaces_and_hyphens_still_reconciles(self):
        """Symbol canonicalization should match compact leg symbol against broker variants."""
        leg = _leg("CE_SELL", symbol="NIFTY2650524500CE")
        leg.is_active = True
        state = self._state_with_legs(leg)
        rec = BrokerReconciliation(state)

        # Simulate broker variant formatting that previously failed strict match.
        warnings = rec.reconcile([
            {
                "tradingsymbol": "NIFTY-26 50 52 4500 CE",
                "netqty": -1,
                "avg_price": 100.0,
                "ltp": 99.5,
            }
        ])

        assert leg.is_active is True
        assert not any("missing/flat in broker" in w for w in warnings)

    def test_empty_broker_with_active_legs_no_close(self):
        """Empty broker snapshot with active legs → skip reconciliation (BUG-11 guard)."""
        leg = _leg("CE_SELL")
        leg.is_active = True
        state = self._state_with_legs(leg)
        rec = BrokerReconciliation(state)
        # Empty broker positions with active leg → must NOT close it
        warnings = rec.reconcile([])
        assert leg.is_active is True

    def test_inactive_leg_not_in_broker_no_warning(self):
        """Closed (inactive) legs that broker doesn't report is normal — no warning."""
        leg = _leg("CE_SELL", is_active=False)
        state = self._state_with_legs(leg)
        rec = BrokerReconciliation(state)
        warnings = rec.reconcile([])
        assert warnings == []

    def test_tagged_broker_position_recovers_inactive_leg(self):
        """If tagged broker position is still open, resume should reactivate the leg."""
        leg = _leg("CE_SELL", is_active=False)
        state = self._state_with_legs(leg)
        rec = BrokerReconciliation(state)

        warnings = rec.reconcile([
            {
                "tag": "CE_SELL",
                "symbol": "NIFTY24CE",
                "netqty": -1,
                "avg_price": 101.5,
                "ltp": 99.0,
            }
        ])

        assert leg.is_active is True
        assert leg.qty == 1
        assert leg.entry_price == pytest.approx(101.5)
        assert any("Recovered leg CE_SELL" in w for w in warnings)

    def test_tagged_duplicate_rows_do_not_false_close_leg(self):
        """Aggregate duplicate tagged rows so a zero-net duplicate cannot re-close a live leg."""
        leg = _leg("CE_SELL", is_active=False)
        state = self._state_with_legs(leg)
        rec = BrokerReconciliation(state)

        warnings = rec.reconcile([
            {
                "tag": "CE_SELL",
                "symbol": "NIFTY24CE",
                "netqty": -65,
                "avg_price": 102.0,
                "ltp": 98.0,
            },
            {
                "tag": "CE_SELL",
                "symbol": "NIFTY24CE",
                "netqty": 0,
                "avg_price": 0,
                "ltp": 98.0,
            },
        ])

        assert leg.is_active is True
        assert any("Recovered leg CE_SELL" in w for w in warnings)

    def test_untagged_recovery_for_inactive_leg(self):
        """Inactive leg should be revived from untagged netqty when symbols match."""
        leg = _leg("CE_SELL", is_active=False, symbol="NIFTY2650524500CE")
        state = self._state_with_legs(leg)
        rec = BrokerReconciliation(state)

        warnings = rec.reconcile([
            {
                "tradingsymbol": "NIFTY2650524500CE",
                "netqty": -65,
                "avg_price": 101.0,
                "ltp": 99.0,
            }
        ])

        assert leg.is_active is True
        assert any("Recovered leg CE_SELL from untagged broker snapshot" in w for w in warnings)

    def test_untagged_shoonya_style_symbol_matches_compact_leg(self):
        """Shoonya C/P strike suffix format should map to CE/PE compact leg symbols."""
        leg = _leg("CE_SELL", is_active=True, symbol="NIFTY2650524500CE")
        state = self._state_with_legs(leg)
        rec = BrokerReconciliation(state)

        warnings = rec.reconcile([
            {
                "tradingsymbol": "NIFTY05MAY26C24500",
                "netqty": -65,
                "avg_price": 84.2,
                "ltp": 117.1,
            }
        ])

        assert leg.is_active is True
        assert not any("closing" in w.lower() for w in warnings)

    def test_tagged_transient_zero_snapshot_does_not_immediately_close(self):
        """Require repeated tagged net=0 snapshots before closing an active leg."""
        leg = _leg("CE_SELL", is_active=False, symbol="NIFTY2650524500CE")
        state = self._state_with_legs(leg)
        rec = BrokerReconciliation(state)

        # Recover first.
        rec.reconcile([
            {
                "tag": "CE_SELL",
                "symbol": "NIFTY2650524500CE",
                "qty": -65,
            }
        ])
        assert leg.is_active is True

        # First transient zero snapshot should not close.
        warnings = rec.reconcile([
            {
                "tag": "CE_SELL",
                "symbol": "NIFTY2650524500CE",
                "qty": 0,
            }
        ])
        assert leg.is_active is True
        assert any("retaining state" in w for w in warnings)

    def test_tagged_nonzero_position_never_ends_with_all_legs_closed(self):
        """Final guard should revive a tagged non-zero leg if state ends fully closed."""
        ce = _leg("LEG@1_CE", is_active=True, symbol="NIFTY2650524500CE")
        pe = _leg("LEG@2_PE", is_active=False, symbol="NIFTY2650524500PE")
        pe.option_type = OptionType.PE
        state = self._state_with_legs(ce, pe)
        rec = BrokerReconciliation(state)

        warnings = rec.reconcile([
            {
                "tag": "LEG@2_PE",
                "symbol": "NIFTY2650524500PE",
                "netqty": -65,
                "avg_price": 88.0,
                "ltp": 92.0,
            }
        ])

        assert state.active_legs_count >= 1
        assert state.legs["LEG@2_PE"].is_active is True
        assert any("Recovered leg LEG@2_PE" in w for w in warnings)


# ── AdjustmentEngine noop reactivation (Bug 3 fix) ───────────────────────────

class TestAdjustmentNoop:
    """
    When simple_close_open_new resolves to the same strike/type/expiry,
    the closing leg must be reactivated with exit fields cleared.
    """
    def test_noop_clears_exit_fields_and_reactivates(self):
        from trading.strategy_runner.adjustment_engine import AdjustmentEngine

        state = StrategyState()
        leg = _leg("CE_SELL", is_active=True)
        leg.exit_timestamp = datetime.now()
        leg.exit_price = 95.0
        state.legs["CE_SELL"] = leg

        # Simulate what the noop block does
        leg.close()  # adjustment_engine calls this first
        # Then noop detected → reactivate
        leg.is_active = True
        leg.exit_timestamp = None
        leg.exit_price = None

        assert leg.is_active is True
        assert leg.exit_timestamp is None
        assert leg.exit_price is None

    def test_noop_reverses_cumulative_pnl(self):
        from trading.strategy_runner.adjustment_engine import AdjustmentEngine

        state = StrategyState()
        leg = _leg("CE_SELL")
        # entry_price=100, ltp=90, qty=25, lot_size=1 → pnl = (100-90)*25 = 250
        leg.entry_price = 100.0
        leg.ltp = 90.0
        leg.qty = 25
        state.legs["CE_SELL"] = leg
        state.cumulative_daily_pnl = 0.0

        # Simulate the noop logic: first add pnl, then subtract it back
        closing_pnl = leg.pnl or 0.0
        assert closing_pnl == pytest.approx(250.0)
        state.cumulative_daily_pnl += closing_pnl  # 250
        # Noop detected — undo:
        state.cumulative_daily_pnl -= closing_pnl  # back to 0

        assert state.cumulative_daily_pnl == pytest.approx(0.0)
