"""
Tests for OMS (Order Management System).

Covers:
  - Paper-mode order placement (instant fill simulation)
  - Live-mode broker rejection handling (L3-FIX: success=False guard)
  - Batch order placement
  - Position tracking after fills
  - OMS without broker (no-broker path)
  - fetch_broker_order_book helper
  - Order cancellation
  - PnL helpers
"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch
from typing import Any

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from trading.oms import (
    OrderManagementSystem,
    OrderRequest,
    OrderSide,
    OrderType,
    ProductType,
    OrderStatus,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _req(**kwargs) -> OrderRequest:
    defaults = dict(
        symbol="NIFTY24APR24000CE",
        exchange="NFO",
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        product=ProductType.NRML,
        quantity=50,
        price=0.0,
        strategy_id="test_strat",
        tag="ENTRY",
    )
    defaults.update(kwargs)
    return OrderRequest(**defaults)


# ── Paper-mode tests ───────────────────────────────────────────────────────────

class TestOMSPaperMode:
    def test_paper_order_fills_instantly(self):
        oms = OrderManagementSystem()
        order = oms.place_order(_req(test_mode=True, price=150.0))
        assert order.status == OrderStatus.COMPLETE
        assert order.filled_qty == 50
        assert order.avg_price == 150.0

    def test_no_broker_acts_as_paper(self):
        """When broker is None, any order is treated as test mode (instant fill)."""
        oms = OrderManagementSystem(broker_adapter=None)
        order = oms.place_order(_req(price=200.0))
        # no broker → fills as test
        assert order.status == OrderStatus.COMPLETE
        assert order.filled_qty == 50

    def test_paper_order_creates_position(self):
        oms = OrderManagementSystem()
        oms.place_order(_req(test_mode=True, price=100.0))
        positions = oms.get_positions()
        assert len(positions) == 1
        pos = positions[0]
        assert pos["quantity"] == -50   # SELL → negative
        assert pos["avg_price"] == 100.0

    def test_buy_then_sell_closes_position(self):
        oms = OrderManagementSystem()
        oms.place_order(_req(side=OrderSide.BUY, test_mode=True, price=100.0))
        oms.place_order(_req(side=OrderSide.SELL, test_mode=True, price=110.0))
        positions = oms.get_positions()
        assert positions[0]["quantity"] == 0
        assert positions[0]["realised_pnl"] == pytest.approx(500.0)  # 10 * 50


# ── Live-mode rejection handling (L3-FIX) ─────────────────────────────────────

class TestOMSLiveModeRejection:
    def _make_broker(self, resp: dict) -> MagicMock:
        broker = MagicMock()
        broker.place_order.return_value = resp
        return broker

    def test_broker_success_true_sets_open(self):
        broker = self._make_broker({"success": True, "order_id": "ORD123"})
        oms = OrderManagementSystem(broker_adapter=broker)
        order = oms.place_order(_req())
        assert order.status == OrderStatus.OPEN
        assert order.broker_order_id == "ORD123"

    def test_broker_success_false_sets_rejected(self):
        """L3-FIX: success=False from broker must mark order REJECTED."""
        broker = self._make_broker({
            "success": False,
            "message": "Insufficient margin",
        })
        oms = OrderManagementSystem(broker_adapter=broker)
        order = oms.place_order(_req())
        assert order.status == OrderStatus.REJECTED
        assert "Insufficient margin" in (order.error_msg or "")

    def test_broker_exception_sets_rejected(self):
        broker = MagicMock()
        broker.place_order.side_effect = RuntimeError("Network timeout")
        oms = OrderManagementSystem(broker_adapter=broker)
        order = oms.place_order(_req())
        assert order.status == OrderStatus.REJECTED
        assert "Network timeout" in (order.error_msg or "")

    def test_broker_norenordno_mapped_to_broker_order_id(self):
        """Shoonya returns norenordno — OMS must accept it as broker_order_id fallback."""
        broker = self._make_broker({
            "success": True,
            "order_id": "",         # empty
            "norenordno": "SH99999",
        })
        oms = OrderManagementSystem(broker_adapter=broker)
        order = oms.place_order(_req())
        assert order.broker_order_id == "SH99999"

    def test_broker_order_id_takes_precedence_over_norenordno(self):
        broker = self._make_broker({
            "success": True,
            "order_id": "FY12345",
            "norenordno": "SH99999",
        })
        oms = OrderManagementSystem(broker_adapter=broker)
        order = oms.place_order(_req())
        assert order.broker_order_id == "FY12345"


# ── fetch_broker_order_book ───────────────────────────────────────────────────

class TestFetchBrokerOrderBook:
    def test_no_broker_returns_empty_list(self):
        oms = OrderManagementSystem()
        assert oms.fetch_broker_order_book() == []

    def test_delegates_to_broker_get_order_book(self):
        broker = MagicMock()
        broker.get_order_book.return_value = [{"order_id": "X1"}]
        oms = OrderManagementSystem(broker_adapter=broker)
        result = oms.fetch_broker_order_book()
        assert result == [{"order_id": "X1"}]
        broker.get_order_book.assert_called_once()

    def test_broker_exception_returns_empty_list(self):
        broker = MagicMock()
        broker.get_order_book.side_effect = RuntimeError("API down")
        oms = OrderManagementSystem(broker_adapter=broker)
        result = oms.fetch_broker_order_book()
        assert result == []


# ── Order book / PnL ──────────────────────────────────────────────────────────

class TestOMSOrderBook:
    def test_get_all_orders_empty(self):
        oms = OrderManagementSystem()
        assert oms.get_all_orders() == []

    def test_get_order_returns_none_for_unknown_id(self):
        oms = OrderManagementSystem()
        assert oms.get_order("nonexistent") is None

    def test_get_order_returns_placed_order(self):
        oms = OrderManagementSystem()
        placed = oms.place_order(_req(test_mode=True))
        retrieved = oms.get_order(placed.id)
        assert retrieved is not None
        assert retrieved.id == placed.id

    def test_total_pnl_after_closed_position(self):
        oms = OrderManagementSystem()
        oms.place_order(_req(side=OrderSide.SELL, test_mode=True, price=200.0))
        oms.place_order(_req(side=OrderSide.BUY, test_mode=True, price=180.0))
        # Short sold @ 200, covered @ 180 → gain 20 * 50 = 1000
        assert oms.total_pnl() == pytest.approx(1000.0)

    def test_open_orders_only_returns_non_final(self):
        oms = OrderManagementSystem()
        # Paper fills immediately → no open orders
        oms.place_order(_req(test_mode=True))
        assert oms.get_open_orders() == []


# ── Batch placement ───────────────────────────────────────────────────────────

class TestOMSBatch:
    def test_place_batch_returns_all_orders(self):
        oms = OrderManagementSystem()
        reqs = [_req(test_mode=True), _req(tag="HEDGE", test_mode=True)]
        orders = oms.place_batch(reqs)
        assert len(orders) == 2
        assert all(o.status == OrderStatus.COMPLETE for o in orders)

    def test_place_batch_live_all_rejected_no_position(self):
        broker = MagicMock()
        broker.place_order.return_value = {"success": False, "message": "Rate limit"}
        oms = OrderManagementSystem(broker_adapter=broker)
        orders = oms.place_batch([_req(), _req()])
        assert all(o.status == OrderStatus.REJECTED for o in orders)
        assert oms.get_positions() == []


# ── Cancel ────────────────────────────────────────────────────────────────────

class TestOMSCancel:
    def test_cancel_open_order_succeeds(self):
        broker = MagicMock()
        broker.place_order.return_value = {"success": True, "order_id": "O1"}
        broker.cancel_order.return_value = True
        oms = OrderManagementSystem(broker_adapter=broker)
        order = oms.place_order(_req())
        assert order.status == OrderStatus.OPEN
        cancelled = oms.cancel_order(order.id)
        assert cancelled is True
        updated = oms.get_order(order.id)
        assert updated.status == OrderStatus.CANCELLED

    def test_cancel_nonexistent_order_returns_false(self):
        oms = OrderManagementSystem()
        assert oms.cancel_order("no-such-id") is False
