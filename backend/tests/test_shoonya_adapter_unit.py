"""
Tests for ShoonyaAdapter normalisation and place_order return format.

Covers:
  - place_order returns {"success": bool, "order_id": str} format
  - place_order maps norenordno → order_id
  - place_order returns success=False on emsg responses
  - place_order returns success=False when client is None
  - _map_order: Shoonya "OPEN" → canonical "PENDING"
  - _map_order: "COMPLETE" → "COMPLETE"
  - _map_order: "REJECTED" → "REJECTED"
  - _map_order: "CANCELLED" → "CANCELLED"
  - _map_order: norenordno → order_id field
  - get_order_book raises RuntimeError on expired session (stat=Not_Ok)
  - get_positions normalises netqty, lp/ltp LTP fields
"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from broker.adapters.shoonya_adapter import ShoonyaAdapter
from broker.adapters.base import Order


# ── Minimal stub session ──────────────────────────────────────────────────────

def _make_session(client=None) -> MagicMock:
    session = MagicMock()
    session._client = client or MagicMock()
    return session


def _adapter(client=None) -> ShoonyaAdapter:
    session = _make_session(client=client)
    return ShoonyaAdapter(session, account_id="CFG1", client_id="CL1")


# ── place_order return format ─────────────────────────────────────────────────

class TestShoonyaPlaceOrder:
    def _order(self) -> dict:
        return {
            "symbol": "NIFTY24APR24000CE",
            "exchange": "NFO",
            "side": "SELL",
            "product": "NRML",
            "order_type": "MARKET",
            "qty": 50,
            "price": 0.0,
        }

    def _patch_db(self):
        """Patch DB lookup to return None (symbol unchanged, no DB resolution)."""
        return patch("db.symbols_db.lookup_by_trading_symbol", return_value=None)

    def test_no_client_returns_success_false(self):
        session = MagicMock()
        session._client = None
        adapter = ShoonyaAdapter(session, account_id="CFG", client_id="CL")
        with self._patch_db():
            result = adapter.place_order(self._order())
        assert result["success"] is False
        assert "not connected" in result["message"].lower()

    def test_ok_response_returns_success_true_with_order_id(self):
        adapter = _adapter()
        with self._patch_db():
            with patch.object(adapter, "_raw_place_order", return_value={"stat": "Ok", "norenordno": "12345"}):
                result = adapter.place_order(self._order())
        assert result["success"] is True
        assert result["order_id"] == "12345"

    def test_emsg_returns_success_false(self):
        adapter = _adapter()
        with self._patch_db():
            with patch.object(adapter, "_raw_place_order", return_value={"stat": "Not_Ok", "emsg": "RMS:Blocked"}):
                result = adapter.place_order(self._order())
        assert result["success"] is False
        assert "RMS:Blocked" in result["message"]

    def test_none_response_returns_success_false(self):
        """_raw_place_order raises an exception → caught → success=False."""
        adapter = _adapter()
        with self._patch_db():
            with patch.object(adapter, "_raw_place_order", side_effect=RuntimeError("Connection error")):
                result = adapter.place_order(self._order())
        assert result["success"] is False

    def test_exception_returns_success_false(self):
        adapter = _adapter()
        with self._patch_db():
            with patch.object(adapter, "_raw_place_order", side_effect=RuntimeError("Timeout")):
                result = adapter.place_order(self._order())
        assert result["success"] is False
        assert "Timeout" in result["message"]

    def test_result_always_has_required_keys(self):
        """Every response must include success, order_id, message."""
        for raw_resp in [
            {"stat": "Ok", "norenordno": "ABC"},
            {"stat": "Not_Ok", "emsg": "err"},
        ]:
            adapter = _adapter()
            with self._patch_db():
                with patch.object(adapter, "_raw_place_order", return_value=raw_resp):
                    result = adapter.place_order(self._order())
            assert "success" in result
            assert "order_id" in result
            assert "message" in result

    def test_shoonya_tsym_resolved_from_db(self):
        """When DB returns a shoonya_tsym, it should override the raw symbol."""
        adapter = _adapter()
        db_rec = {
            "trading_symbol": "NIFTY-APR2026-24500-CE",
            "shoonya_tsym": "NIFTY21APR26C24500",
            "exchange": "NFO",
            "tick_size": 0.05,
            "expiry": "2026-04-21",
            "strike": 24500.0,
            "option_type": "CE",
        }
        with patch("db.symbols_db.lookup_by_trading_symbol", return_value=db_rec):
            with patch.object(adapter, "_raw_place_order", return_value={"stat": "Ok", "norenordno": "XYZ"}) as mock_raw:
                result = adapter.place_order(self._order())
        # Verify _raw_place_order was called with the Shoonya-resolved symbol
        call_params = mock_raw.call_args[0][1]  # second positional arg = params dict
        assert call_params["tradingsymbol"] == "NIFTY21APR26C24500"
        assert result["success"] is True

    def test_shoonya_tsym_partner_lookup_when_empty(self):
        """When shoonya_tsym empty on primary row, fall back to partner-row query."""
        adapter = _adapter()
        db_rec = {
            "trading_symbol": "NIFTY2642124500CE",
            "shoonya_tsym": "",   # empty — partner lookup required
            "exchange": "NFO",
            "tick_size": 0.05,
            "expiry": "2026-04-21",
            "strike": 24500.0,
            "option_type": "CE",
        }
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ("NIFTY21APR26C24500",)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("db.symbols_db.lookup_by_trading_symbol", return_value=db_rec):
            with patch("db.trading_db.get_trading_conn", return_value=mock_conn):
                with patch.object(adapter, "_raw_place_order", return_value={"stat": "Ok", "norenordno": "XYZ"}) as mock_raw:
                    result = adapter.place_order(self._order())

        call_params = mock_raw.call_args[0][1]
        assert call_params["tradingsymbol"] == "NIFTY21APR26C24500"
        assert result["success"] is True


# ── _map_order status normalisation ──────────────────────────────────────────

class TestShoonyaMapOrder:
    def _raw(self, status: str, norenordno: str = "ORD123") -> dict:
        return {
            "norenordno": norenordno,
            "tsym": "NIFTY24APR24000CE",
            "exch": "NFO",
            "trantype": "S",
            "prd": "M",
            "prctyp": "MKT",
            "qty": "50",
            "prc": "0",
            "Status": status,
            "fillshares": "0",
            "avgprc": "0",
        }

    def test_open_maps_to_pending(self):
        order = ShoonyaAdapter._map_order(self._raw("OPEN"))
        assert order.status == "PENDING"

    def test_complete_maps_to_complete(self):
        order = ShoonyaAdapter._map_order(self._raw("COMPLETE"))
        assert order.status == "COMPLETE"

    def test_rejected_maps_to_rejected(self):
        order = ShoonyaAdapter._map_order(self._raw("REJECTED"))
        assert order.status == "REJECTED"

    def test_cancelled_maps_to_cancelled(self):
        order = ShoonyaAdapter._map_order(self._raw("CANCELLED"))
        assert order.status == "CANCELLED"

    def test_trigger_pending_maps_to_pending(self):
        order = ShoonyaAdapter._map_order(self._raw("TRIGGER_PENDING"))
        assert order.status == "PENDING"

    def test_unknown_status_maps_to_pending(self):
        order = ShoonyaAdapter._map_order(self._raw("SOME_UNKNOWN"))
        assert order.status == "PENDING"

    def test_norenordno_becomes_order_id(self):
        order = ShoonyaAdapter._map_order(self._raw("COMPLETE", norenordno="SH_77777"))
        assert order.order_id == "SH_77777"

    def test_side_sell(self):
        order = ShoonyaAdapter._map_order(self._raw("COMPLETE"))
        assert order.side == "SELL"

    def test_side_buy(self):
        raw = self._raw("COMPLETE")
        raw["trantype"] = "B"
        order = ShoonyaAdapter._map_order(raw)
        assert order.side == "BUY"

    def test_map_order_returns_order_instance(self):
        order = ShoonyaAdapter._map_order(self._raw("COMPLETE"))
        assert isinstance(order, Order)


# ── get_order_book session expiry ─────────────────────────────────────────────

class TestShoonyaGetOrderBook:
    def test_returns_empty_list_on_ok(self):
        session = MagicMock()
        session.get_order_book.return_value = []
        adapter = ShoonyaAdapter(session, account_id="CFG", client_id="CL")
        result = adapter.get_order_book()
        assert result == []

    def test_raises_on_not_ok_dict(self):
        session = MagicMock()
        session.get_order_book.return_value = {"stat": "Not_Ok", "emsg": "Session Expired"}
        adapter = ShoonyaAdapter(session, account_id="CFG", client_id="CL")
        with pytest.raises(RuntimeError, match="Session Expired"):
            adapter.get_order_book()

    def test_raises_when_none_returned(self):
        session = MagicMock()
        session.get_order_book.return_value = None
        adapter = ShoonyaAdapter(session, account_id="CFG", client_id="CL")
        with pytest.raises(RuntimeError, match="Session Expired"):
            adapter.get_order_book()

    def test_maps_orders(self):
        session = MagicMock()
        session.get_order_book.return_value = [{
            "norenordno": "999",
            "tsym": "NIFTY",
            "exch": "NFO",
            "trantype": "B",
            "prd": "M",
            "prctyp": "MKT",
            "qty": "50",
            "prc": "0",
            "Status": "COMPLETE",
            "fillshares": "50",
            "avgprc": "100",
        }]
        adapter = ShoonyaAdapter(session, account_id="CFG", client_id="CL")
        orders = adapter.get_order_book()
        assert len(orders) == 1
        assert orders[0].order_id == "999"
        assert orders[0].status == "COMPLETE"


# ── get_positions normalisation ───────────────────────────────────────────────

class TestShoonyaGetPositions:
    def test_maps_netqty_and_ltp(self):
        from broker.adapters.shoonya_adapter import ShoonyaAdapter
        session = MagicMock()
        session.get_positions.return_value = [{
            "tsym": "NIFTY24APR24000CE",
            "exch": "NFO",
            "prd": "M",
            "netqty": "-50",
            "netavgprc": "100",
            "lp": "95",
            "rpnl": "250",
            "urmtom": "0",
        }]
        adapter = ShoonyaAdapter(session, account_id="CFG", client_id="CL")
        positions = adapter.get_positions()
        assert len(positions) == 1
        pos = positions[0]
        assert pos.qty == -50
        assert pos.ltp == 95.0
        assert pos.avg_price == 100.0
