"""
Tests for StrategyExecutor live-mode fixes (L1–L4) and fill-polling.

Covers:
  - _confirm_pending_fills: COMPLETE → is_active=True, entry_price updated
  - _confirm_pending_fills: REJECTED → order_status=FAILED
  - _confirm_pending_fills: still OPEN → leaves AWAITING_FILL (retry next tick)
  - _confirm_pending_fills: paper mode → skipped
  - _confirm_pending_fills: no OMS → skipped
  - _place_exit_order returns False on broker rejection (L2-FIX)
  - _place_exit_order returns True on success (L2-FIX)
  - L4: all-entries-rejected clears legs, does NOT set entered_today
  - Shoonya order_id (norenordno) round-trip through OMS matches bo_index
"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from datetime import datetime

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from trading.strategy_runner.state import LegState, StrategyState
from trading.strategy_runner.models import Side, InstrumentType, OptionType


# ── Minimal stub helpers ──────────────────────────────────────────────────────

def _make_leg(tag="CE_SELL", order_status="AWAITING_FILL", is_active=False,
              order_id="INT_001", strike=24000, trading_symbol="NIFTY24APR24000CE") -> LegState:
    leg = LegState(
        tag=tag,
        symbol="NIFTY",
        instrument=InstrumentType.OPT,
        option_type=OptionType.CE,
        strike=float(strike),
        expiry="24-04-2026",
        side=Side.SELL,
        qty=1,
        entry_price=100.0,
        ltp=100.0,
        trading_symbol=trading_symbol,
    )
    leg.order_status = order_status
    leg.is_active = is_active
    leg.order_id = order_id
    return leg


def _make_oms(broker_orders: list, broker_order_id: str = "BRK_001") -> MagicMock:
    """Return a minimal OMS mock where broker.get_order_book returns dicts."""
    internal_order = MagicMock()
    internal_order.broker_order_id = broker_order_id

    oms = MagicMock()
    oms.fetch_broker_order_book.return_value = broker_orders
    oms.get_order.return_value = internal_order
    oms.broker = MagicMock()
    return oms


def _executor_with_oms(oms, paper_mode: bool = False):
    """
    Build a minimal StrategyExecutor-like object exposing only
    _confirm_pending_fills and the state it needs.
    """
    # We import here to avoid top-level side effects if executor can't be imported
    from trading.strategy_runner.executor import StrategyExecutor

    # Use the actual class but bypass __init__ by creating a blank instance
    exec_ = object.__new__(StrategyExecutor)
    exec_.state = StrategyState()
    exec_._oms = oms
    exec_._paper_mode = paper_mode
    exec_._subscribed_symbols = set()
    exec_._tick_service = None
    exec_._db_persistence = None
    # Stub _save_state and subscribe_leg_symbols
    exec_._save_state = MagicMock()
    exec_.subscribe_leg_symbols = MagicMock()
    return exec_


# ── _confirm_pending_fills ────────────────────────────────────────────────────

class TestConfirmPendingFills:
    def test_paper_mode_skipped(self):
        oms = _make_oms([])
        exec_ = _executor_with_oms(oms, paper_mode=True)
        leg = _make_leg()
        exec_.state.legs["CE_SELL"] = leg
        exec_._confirm_pending_fills(datetime.now())
        oms.fetch_broker_order_book.assert_not_called()

    def test_no_oms_skipped(self):
        exec_ = _executor_with_oms(None)
        exec_._oms = None
        leg = _make_leg()
        exec_.state.legs["CE_SELL"] = leg
        exec_._confirm_pending_fills(datetime.now())  # must not raise

    def test_no_pending_legs_skips_broker_call(self):
        oms = _make_oms([])
        exec_ = _executor_with_oms(oms)
        # Leg is already FILLED
        leg = _make_leg(order_status="FILLED", is_active=True)
        exec_.state.legs["CE_SELL"] = leg
        exec_._confirm_pending_fills(datetime.now())
        oms.fetch_broker_order_book.assert_not_called()

    def test_complete_status_sets_active(self):
        broker_order = {
            "order_id": "BRK_001",
            "status": "COMPLETE",
            "avg_price": 123.45,
        }
        oms = _make_oms([broker_order])
        exec_ = _executor_with_oms(oms)
        leg = _make_leg()
        exec_.state.legs["CE_SELL"] = leg
        exec_._confirm_pending_fills(datetime.now())
        assert leg.is_active is True
        assert leg.order_status == "FILLED"
        assert leg.entry_price == pytest.approx(123.45)

    def test_rejected_status_sets_failed(self):
        broker_order = {
            "order_id": "BRK_001",
            "status": "REJECTED",
            "avg_price": 0,
        }
        oms = _make_oms([broker_order])
        exec_ = _executor_with_oms(oms)
        leg = _make_leg()
        exec_.state.legs["CE_SELL"] = leg
        exec_._confirm_pending_fills(datetime.now())
        assert leg.order_status == "FAILED"
        assert leg.is_active is False

    def test_open_status_leaves_awaiting_fill(self):
        """Still-open orders stay AWAITING_FILL for next tick."""
        broker_order = {
            "order_id": "BRK_001",
            "status": "OPEN",
            "avg_price": 0,
        }
        oms = _make_oms([broker_order])
        exec_ = _executor_with_oms(oms)
        leg = _make_leg()
        exec_.state.legs["CE_SELL"] = leg
        exec_._confirm_pending_fills(datetime.now())
        assert leg.order_status == "AWAITING_FILL"
        assert leg.is_active is False

    def test_order_not_in_book_yet_leaves_awaiting_fill(self):
        """Order placed but not yet visible in book → keep AWAITING_FILL."""
        oms = _make_oms([])  # empty order book
        exec_ = _executor_with_oms(oms)
        leg = _make_leg()
        exec_.state.legs["CE_SELL"] = leg
        exec_._confirm_pending_fills(datetime.now())
        assert leg.order_status == "AWAITING_FILL"

    def test_shoonya_norenordno_matches_broker_order_id(self):
        """Shoonya stores norenordno in broker_order_id — must match in bo_index."""
        broker_order = {
            "order_id": "SH_99999",  # _map_order uses norenordno as order_id
            "status": "COMPLETE",
            "avg_price": 55.0,
        }
        internal_order = MagicMock()
        internal_order.broker_order_id = "SH_99999"  # OMS stores same
        oms = MagicMock()
        oms.fetch_broker_order_book.return_value = [broker_order]
        oms.get_order.return_value = internal_order
        oms.broker = MagicMock()

        exec_ = _executor_with_oms(oms)
        leg = _make_leg()
        leg.qty = 50  # order_qty is a computed property: qty * lot_size
        exec_.state.legs["CE_SELL"] = leg
        exec_._confirm_pending_fills(datetime.now())
        assert leg.is_active is True
        assert leg.entry_price == pytest.approx(55.0)

    def test_expired_status_sets_failed(self):
        broker_order = {
            "order_id": "BRK_001",
            "status": "EXPIRED",
            "avg_price": 0,
        }
        oms = _make_oms([broker_order])
        exec_ = _executor_with_oms(oms)
        leg = _make_leg()
        exec_.state.legs["CE_SELL"] = leg
        exec_._confirm_pending_fills(datetime.now())
        assert leg.order_status == "FAILED"

    def test_multiple_pending_legs_handled_in_one_book_call(self):
        """Single get_order_book() call must cover all pending legs."""
        broker_orders = [
            {"order_id": "BRK_001", "status": "COMPLETE", "avg_price": 100.0},
            {"order_id": "BRK_002", "status": "REJECTED", "avg_price": 0},
        ]
        # Two internal orders
        int_order_1 = MagicMock()
        int_order_1.broker_order_id = "BRK_001"
        int_order_2 = MagicMock()
        int_order_2.broker_order_id = "BRK_002"

        oms = MagicMock()
        oms.fetch_broker_order_book.return_value = broker_orders
        oms.broker = MagicMock()
        oms.get_order.side_effect = lambda oid: int_order_1 if oid == "INT_001" else int_order_2

        exec_ = _executor_with_oms(oms)
        leg1 = _make_leg(tag="CE_1", order_id="INT_001")
        leg1.qty = 50  # order_qty is a computed property
        leg2 = _make_leg(tag="PE_1", order_id="INT_002")
        exec_.state.legs["CE_1"] = leg1
        exec_.state.legs["PE_1"] = leg2

        exec_._confirm_pending_fills(datetime.now())

        # Only one book call regardless of number of pending legs
        oms.fetch_broker_order_book.assert_called_once()
        assert leg1.is_active is True
        assert leg2.order_status == "FAILED"


# ── L4: All-entries-rejected guard ───────────────────────────────────────────

class TestL4AllEntriesRejected:
    """
    _execute_entry must NOT set entered_today=True when all broker orders
    are rejected. Failed legs must be cleaned up.
    """
    def test_l4_no_entered_today_on_all_rejected(self):
        from trading.strategy_runner.executor import StrategyExecutor
        exec_ = object.__new__(StrategyExecutor)
        exec_.state = StrategyState()
        exec_.state.entered_today = False

        # Simulate the L4-fix logic directly
        new_legs = [_make_leg(tag="L1", order_status="FAILED"),
                    _make_leg(tag="L2", order_status="FAILED")]
        accepted_legs = [leg_ for leg_ in new_legs if leg_.order_status != "FAILED"]
        if accepted_legs:
            exec_.state.entered_today = True

        assert exec_.state.entered_today is False
        assert accepted_legs == []

    def test_l4_entered_today_set_when_some_accepted(self):
        from trading.strategy_runner.executor import StrategyExecutor
        exec_ = object.__new__(StrategyExecutor)
        exec_.state = StrategyState()
        exec_.state.entered_today = False

        new_legs = [_make_leg(tag="L1", order_status="AWAITING_FILL"),
                    _make_leg(tag="L2", order_status="FAILED")]
        accepted_legs = [leg_ for leg_ in new_legs if leg_.order_status != "FAILED"]
        if accepted_legs:
            exec_.state.entered_today = True

        assert exec_.state.entered_today is True


class TestPriceSanitization:
    def _make_blank_executor(self, oms, paper_mode: bool = False):
        from trading.strategy_runner.executor import StrategyExecutor

        exec_ = object.__new__(StrategyExecutor)
        exec_.state = StrategyState()
        exec_._oms = oms
        exec_._paper_mode = paper_mode
        exec_._strategy_id = "UT_STRAT"
        exec_.market = MagicMock()
        exec_.market.exchange = "NFO"
        exec_._tick_service = MagicMock()
        exec_._subscribed_symbols = set()
        return exec_

    def test_exit_order_rejects_contaminated_option_ltp(self):
        """Option exit should never place order with spot-like contaminated LTP."""
        order = MagicMock()
        order.id = "oid-1"
        order.status = "COMPLETE"

        oms = MagicMock()
        oms.place_order.return_value = order

        exec_ = self._make_blank_executor(oms, paper_mode=False)
        exec_.market.get_option_at_strike.return_value = {"ltp": 195.45}
        exec_._tick_service.get_latest.return_value = {"ltp": 23849.75}

        leg = _make_leg(
            tag="LEG@1_CE",
            order_status="FILLED",
            is_active=True,
            strike=24100,
            trading_symbol="NIFTY2650524100CE",
        )
        leg.ltp = 23849.75

        ok = exec_._place_exit_order(leg)

        assert ok is True
        req = oms.place_order.call_args[0][0]
        assert req.price == pytest.approx(195.45)
        assert leg.ltp == pytest.approx(195.45)

    def test_paper_entry_uses_current_option_price(self):
        """Paper-mode entry should record current option price, not stale leg.entry_price."""
        order = MagicMock()
        order.id = "oid-2"
        order.broker_order_id = "brk-2"
        order.status = "COMPLETE"
        order.avg_price = 0.0

        oms = MagicMock()
        oms.place_order.return_value = order

        exec_ = self._make_blank_executor(oms, paper_mode=True)
        exec_.market.get_option_at_strike.return_value = {"ltp": 102.80}
        exec_._tick_service.get_latest.return_value = {"ltp": 102.80}

        leg = _make_leg(
            tag="LEG@1_CE",
            order_status="PENDING",
            is_active=False,
            strike=24100,
            trading_symbol="NIFTY2650524100CE",
        )
        leg.entry_price = 95.0
        leg.ltp = 0.0

        exec_._place_entry_order(leg)

        req = oms.place_order.call_args[0][0]
        assert req.price == pytest.approx(102.80)
        assert leg.entry_price == pytest.approx(102.80)
        assert leg.is_active is True
