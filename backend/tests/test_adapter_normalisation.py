"""
Smart Trader — Adapter Normalisation Unit Tests
=================================================

Tests that each broker adapter correctly maps raw broker-specific API responses
to normalised Smart Trader models (Position, Order, Holding, Funds, Trade).

These tests do NOT hit live APIs — they use mock HTTP responses to verify the
field mapping, product translation, status normalisation, and enrichment logic.

Run:
    cd /home/ubuntu/Smart_trader/backend
    python3 -m pytest tests/test_adapter_normalisation.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

# Ensure backend is on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from broker.adapters.base import BrokerAdapter, Funds, Holding, Order, Position, Trade
from broker.adapters.kite_adapter import KiteAdapter
from broker.adapters.upstox_adapter import UpstoxAdapter
from broker.adapters.angelone_adapter import AngelOneAdapter
from broker.adapters.dhan_adapter import DhanAdapter
from broker.adapters.groww_adapter import GrowwAdapter


# ═══════════════════════════════════════════════════════════════════════════════
#  Fixtures: sample raw broker API responses
# ═══════════════════════════════════════════════════════════════════════════════


# ── Kite ──────────────────────────────────────────────────────────────────────

KITE_POSITION_RAW = {
    "tradingsymbol": "NIFTY26APR24500CE",
    "exchange": "NFO",
    "instrument_token": 12345678,
    "product": "NRML",
    "quantity": 50,
    "overnight_quantity": 50,
    "multiplier": 1,
    "average_price": 150.50,
    "close_price": 145.00,
    "last_price": 155.75,
    "value": -7525.0,
    "pnl": 262.50,
    "m2m": 537.50,
    "unrealised": 262.50,
    "realised": 0.0,
    "buy_quantity": 50,
    "buy_price": 150.50,
    "buy_value": 7525.0,
    "sell_quantity": 0,
    "sell_price": 0,
    "sell_value": 0,
    "day_buy_quantity": 0,
    "day_sell_quantity": 0,
}

KITE_ORDER_RAW = {
    "order_id": "250405000123456",
    "tradingsymbol": "SBIN",
    "exchange": "NSE",
    "instrument_token": 779521,
    "transaction_type": "BUY",
    "order_type": "LIMIT",
    "product": "CNC",
    "quantity": 10,
    "price": 750.00,
    "trigger_price": 0,
    "status": "COMPLETE",
    "filled_quantity": 10,
    "average_price": 749.50,
    "pending_quantity": 0,
    "order_timestamp": "2026-04-05 09:15:01",
    "exchange_timestamp": "2026-04-05 09:15:01",
    "status_message": None,
    "variety": "regular",
    "tag": "smart_trader",
}

KITE_HOLDING_RAW = {
    "tradingsymbol": "RELIANCE",
    "exchange": "NSE",
    "instrument_token": 738561,
    "isin": "INE002A01018",
    "product": "CNC",
    "price": 0,
    "quantity": 5,
    "used_quantity": 0,
    "t1_quantity": 0,
    "realised_quantity": 5,
    "average_price": 2400.00,
    "last_price": 2550.50,
    "close_price": 2545.00,
    "pnl": 752.50,
    "day_change": 5.50,
    "day_change_percentage": 0.216,
}

KITE_TRADE_RAW = {
    "trade_id": "90000001",
    "order_id": "250405000123456",
    "exchange": "NSE",
    "tradingsymbol": "SBIN",
    "instrument_token": 779521,
    "product": "CNC",
    "average_price": 749.50,
    "quantity": 10,
    "exchange_order_id": "300000000000000",
    "transaction_type": "BUY",
    "fill_timestamp": "2026-04-05 09:15:01",
    "order_timestamp": "09:15:01",
    "exchange_timestamp": "2026-04-05 09:15:01",
}

KITE_MARGINS_RAW = {
    "status": "success",
    "data": {
        "equity": {
            "enabled": True,
            "net": 150000.00,
            "available": {
                "adhoc_margin": 0,
                "cash": 200000.00,
                "opening_balance": 200000.00,
                "live_balance": 150000.00,
                "collateral": 25000.00,
                "intraday_payin": 0,
            },
            "utilised": {
                "debits": 50000.00,
                "exposure": 10000.00,
                "m2m_realised": 500.00,
                "m2m_unrealised": -200.00,
                "option_premium": 0,
                "payout": 0,
                "span": 30000.00,
                "holding_sales": 0,
                "turnover": 0,
                "liquid_collateral": 0,
                "stock_collateral": 0,
                "delivery": 0,
            },
        },
        "commodity": {
            "enabled": True,
            "net": 80000.00,
            "available": {
                "adhoc_margin": 0,
                "cash": 80000.00,
                "opening_balance": 80000.00,
                "live_balance": 80000.00,
                "collateral": 0,
                "intraday_payin": 0,
            },
            "utilised": {
                "debits": 0,
                "exposure": 0,
                "m2m_realised": 0,
                "m2m_unrealised": 0,
                "option_premium": 0,
                "payout": 0,
                "span": 0,
                "holding_sales": 0,
                "turnover": 0,
                "liquid_collateral": 0,
                "stock_collateral": 0,
                "delivery": 0,
            },
        },
    },
}


# ── Upstox ────────────────────────────────────────────────────────────────────

UPSTOX_POSITION_RAW_FNO = {
    "exchange": "NFO",
    "multiplier": 1.0,
    "value": 39.75,
    "pnl": 26.25,
    "product": "D",
    "instrument_token": "NSE_FO|52618",
    "average_price": 2.65,
    "buy_value": 0.0,
    "overnight_quantity": 15,
    "day_buy_value": 0.0,
    "day_buy_price": 0.0,
    "overnight_buy_amount": 39.75,
    "overnight_buy_quantity": 15,
    "day_buy_quantity": 0,
    "day_sell_value": 0.0,
    "day_sell_price": 0.0,
    "overnight_sell_amount": 0.0,
    "overnight_sell_quantity": 0,
    "day_sell_quantity": 0,
    "quantity": 15,
    "last_price": 1.75,
    "unrealised": -13.5,
    "realised": 0.0,
    "sell_value": 0.0,
    "trading_symbol": "BANKNIFTY26APR38000PE",
    "tradingsymbol": "BANKNIFTY26APR38000PE",
    "close_price": 1.95,
    "buy_price": 2.65,
    "sell_price": 0.0,
}

UPSTOX_POSITION_RAW_EQ = {
    "exchange": "NSE",
    "multiplier": 1.0,
    "value": 571.4,
    "pnl": 0.45,
    "product": "D",
    "instrument_token": "NSE_EQ|INE062A01020",
    "average_price": None,
    "buy_value": 570.95,
    "overnight_quantity": 0,
    "quantity": 1,
    "last_price": 571.2,
    "unrealised": 0.0,
    "realised": 0.45,
    "sell_value": 571.4,
    "trading_symbol": "SBIN",
    "tradingsymbol": "SBIN",
    "close_price": 572.65,
    "buy_price": 570.95,
    "sell_price": 571.4,
    "day_buy_quantity": 1,
    "day_sell_quantity": 1,
}

UPSTOX_ORDER_RAW = {
    "order_id": "260405000555555",
    "instrument_token": "NSE_FO|43919",
    "exchange": "NFO",
    "trading_symbol": "NIFTY26APR24500CE",
    "tradingsymbol": "NIFTY26APR24500CE",
    "transaction_type": "SELL",
    "product": "I",
    "order_type": "LIMIT",
    "quantity": 50,
    "price": 200.00,
    "trigger_price": 0,
    "traded_quantity": 50,
    "average_price": 199.75,
    "status": "complete",
    "order_timestamp": "2026-04-05 10:30:00",
    "status_message": "",
    "tag": "smart_trader",
}

UPSTOX_HOLDING_RAW = {
    "isin": "INE528G01035",
    "cnc_used_quantity": 0,
    "collateral_type": "WC",
    "company_name": "YES BANK LTD.",
    "haircut": 0.2,
    "product": "D",
    "quantity": 36,
    "trading_symbol": "YESBANK",
    "tradingsymbol": "YESBANK",
    "last_price": 17.05,
    "close_price": 17.05,
    "pnl": -61.2,
    "day_change": 0,
    "day_change_percentage": 0,
    "instrument_token": "NSE_EQ|INE528G01035",
    "average_price": 18.75,
    "collateral_quantity": 0,
    "collateral_update_quantity": 0,
    "t1_quantity": 0,
    "exchange": "NSE",
}

UPSTOX_TRADE_RAW = {
    "trade_id": "T001",
    "order_id": "260405000555555",
    "exchange": "NFO",
    "trading_symbol": "NIFTY26APR24500CE",
    "tradingsymbol": "NIFTY26APR24500CE",
    "instrument_token": "NSE_FO|43919",
    "transaction_type": "SELL",
    "product": "I",
    "traded_quantity": 50,
    "average_price": 199.75,
    "trade_timestamp": "2026-04-05 10:30:01",
    "exchange_timestamp": "2026-04-05 10:30:01",
}


# ── Angel One ─────────────────────────────────────────────────────────────────

ANGEL_POSITION_RAW = {
    "tradingsymbol": "NIFTY26APR24500CE",
    "symboltoken": "12345",
    "symbolname": "NIFTY",
    "exchange": "NFO",
    "producttype": "CARRYFORWARD",
    "netqty": "25",
    "buyqty": "25",
    "sellqty": "0",
    "buyavgprice": "180.50",
    "sellavgprice": "0",
    "ltp": "195.25",
    "realised": "0",
    "unrealised": "368.75",
    "cfbuyqty": "25",
    "cfsellqty": "0",
}

ANGEL_ORDER_RAW = {
    "orderid": "AO20260405001",
    "tradingsymbol": "TATASTEEL",
    "symboltoken": "54321",
    "transactiontype": "SELL",
    "exchange": "NSE",
    "producttype": "INTRADAY",
    "ordertype": "STOPLOSS_LIMIT",
    "quantity": "100",
    "price": "140.00",
    "triggerprice": "141.00",
    "status": "trigger pending",
    "filledshares": "0",
    "averageprice": "0",
    "text": "",
    "updatetime": "2026-04-05 10:00:00",
    "variety": "NORMAL",
}

ANGEL_HOLDING_RAW = {
    "tradingsymbol": "INFY",
    "exchange": "NSE",
    "isin": "INE009A01021",
    "quantity": 10,
    "t1quantity": 0,
    "averageprice": 1450.0,
    "ltp": 1520.0,
    "profitandloss": 700.0,
    "pnlpercentage": 4.83,
    "symboltoken": "99999",
}

ANGEL_TRADE_RAW = {
    "tradeid": "T100",
    "orderid": "AO20260405002",
    "tradingsymbol": "INFY",
    "exchange": "NSE",
    "transactiontype": "BUY",
    "producttype": "DELIVERY",
    "fillshares": "10",
    "fillprice": "1450.00",
    "filltime": "2026-04-05 09:30:00",
}

ANGEL_FUNDS_RAW = {
    "status": True,
    "data": {
        "availablecash": "175000.50",
        "collateral": "50000.00",
        "m2mrealized": "1200.00",
        "m2munrealized": "-300.00",
        "utiliseddebits": "25000.00",
        "net": "200000.50",
    },
}


# ── Dhan ──────────────────────────────────────────────────────────────────────

DHAN_POSITION_RAW = {
    "tradingSymbol": "NIFTY-26APR2026-24500-CE",
    "securityId": "50001",
    "positionType": "OPEN",
    "exchangeSegment": "NSE_FNO",
    "productType": "INTRADAY",
    "buyAvg": 125.50,
    "buyQty": 75,
    "sellAvg": 0,
    "sellQty": 0,
    "netQty": 75,
    "costPrice": 125.50,
    "ltp": 130.00,
    "realizedProfit": 0,
    "unrealizedProfit": 337.50,
    "drvExpiryDate": "2026-04-26",
    "drvOptionType": "CALL",
    "drvStrikePrice": 24500.0,
}

DHAN_ORDER_RAW = {
    "orderId": "DH20260405001",
    "exchangeOrderId": "EX001",
    "correlationId": "smart_trader",
    "transactionType": "BUY",
    "exchangeSegment": "NSE_EQ",
    "productType": "CNC",
    "orderType": "MARKET",
    "validity": "DAY",
    "securityId": "1333",
    "tradingSymbol": "HDFCBANK",
    "quantity": 5,
    "price": 0,
    "triggerPrice": 0,
    "filledQty": 5,
    "averageTradedPrice": 1580.25,
    "orderStatus": "TRADED",
    "createTime": "2026-04-05 09:20:00",
    "updateTime": "2026-04-05 09:20:01",
}

DHAN_HOLDING_RAW = {
    "exchange": "NSE",
    "tradingSymbol": "ITC",
    "securityId": "1660",
    "isin": "INE154A01025",
    "totalQty": 100,
    "dpQty": 100,
    "t1Qty": 0,
    "availableQty": 100,
    "collateralQty": 0,
    "avgCostPrice": 420.50,
    "ltp": 435.00,
}

DHAN_TRADE_RAW = {
    "tradingId": "DT001",
    "orderId": "DH20260405001",
    "exchangeSegment": "NSE_EQ",
    "transactionType": "BUY",
    "productType": "CNC",
    "tradingSymbol": "HDFCBANK",
    "tradedQuantity": 5,
    "tradedPrice": 1580.25,
    "exchangeTime": "2026-04-05 09:20:01",
}

DHAN_FUNDS_RAW = {
    "availableBalance": 125000.00,
    "sodLimit": 150000.00,
    "collateralAmount": 10000.00,
    "receiveableAmount": 0,
    "utilisedMargin": 25000.00,
    "blockedPayoutAmount": 0,
    "withdrawableBalance": 100000.00,
}


# ── Groww ─────────────────────────────────────────────────────────────────────

GROWW_POSITION_RAW = {
    "trading_symbol": "RELIANCE",
    "segment": "CASH",
    "exchange": "NSE",
    "product": "CNC",
    "quantity": 10,
    "net_price": 2400.00,
    "credit_quantity": 10,
    "debit_quantity": 0,
    "realised_pnl": 0,
    "symbol_isin": "INE002A01018",
    "net_carry_forward_quantity": 10,
}

GROWW_ORDER_RAW = {
    "groww_order_id": "GW20260405001",
    "trading_symbol": "TCS",
    "exchange": "NSE",
    "transaction_type": "BUY",
    "order_type": "LIMIT",
    "product": "CNC",
    "quantity": 3,
    "price": 3800.00,
    "trigger_price": 0,
    "order_status": "OPEN",
    "filled_quantity": 0,
    "average_fill_price": 0,
    "created_at": "2026-04-05 09:16:00",
    "remark": "",
}

GROWW_HOLDING_RAW = {
    "trading_symbol": "WIPRO",
    "isin": "INE075A01022",
    "quantity": 20,
    "average_price": 450.00,
    "t1_quantity": 0,
    "demat_free_quantity": 20,
    "pledge_quantity": 0,
    "demat_locked_quantity": 0,
}

GROWW_FUNDS_RAW = {
    "status": "SUCCESS",
    "payload": {
        "available_margin": 95000.00,
        "used_margin": 15000.00,
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
#  Tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestKiteAdapter:
    """Test Zerodha Kite Connect adapter normalisation."""

    @pytest.fixture
    def adapter(self):
        return KiteAdapter(
            creds={}, api_key="test_key", access_token="test_token",
            account_id="kite-001", client_id="AB1234",
        )

    def test_is_connected(self, adapter):
        assert adapter.is_connected() is True

    def test_map_position(self, adapter):
        pos = KiteAdapter._map_position(KITE_POSITION_RAW)
        assert isinstance(pos, Position)
        assert pos.symbol == "NIFTY26APR24500CE"
        assert pos.exchange == "NFO"
        assert pos.product == "NRML"
        assert pos.qty == 50
        assert pos.avg_price == 150.50
        assert pos.ltp == 155.75
        assert pos.realised_pnl == 0.0
        assert pos.unrealised_pnl == 262.50
        assert pos.pnl == 262.50
        assert pos.buy_qty == 50
        assert pos.sell_qty == 0
        assert pos.instrument_type == "CE"

    def test_map_order(self, adapter):
        order = KiteAdapter._map_order(KITE_ORDER_RAW)
        assert isinstance(order, Order)
        assert order.order_id == "250405000123456"
        assert order.symbol == "SBIN"
        assert order.exchange == "NSE"
        assert order.side == "BUY"
        assert order.product == "CNC"
        assert order.order_type == "LIMIT"
        assert order.qty == 10
        assert order.price == 750.00
        assert order.status == "COMPLETE"
        assert order.filled_qty == 10
        assert order.avg_price == 749.50
        assert order.timestamp == "2026-04-05 09:15:01"

    def test_map_holding(self, adapter):
        h = KiteAdapter._map_holding(KITE_HOLDING_RAW)
        assert isinstance(h, Holding)
        assert h.symbol == "RELIANCE"
        assert h.exchange == "NSE"
        assert h.qty == 5
        assert h.avg_price == 2400.00
        assert h.ltp == 2550.50
        assert h.pnl == 752.50
        assert h.isin == "INE002A01018"
        assert h.dp_qty == 5
        assert h.t1_qty == 0
        assert abs(h.pnl_pct - 6.27) < 0.1

    def test_map_trade(self, adapter):
        t = KiteAdapter._map_trade(KITE_TRADE_RAW)
        assert isinstance(t, Trade)
        assert t.trade_id == "90000001"
        assert t.order_id == "250405000123456"
        assert t.symbol == "SBIN"
        assert t.side == "BUY"
        assert t.product == "CNC"
        assert t.qty == 10
        assert t.price == 749.50
        assert t.timestamp == "2026-04-05 09:15:01"

    def test_map_funds_combines_equity_and_commodity(self, adapter):
        """Kite funds should combine both equity and commodity segments."""
        with patch.object(adapter, "_get") as mock_get:
            mock_get.return_value = KITE_MARGINS_RAW
            funds = adapter.get_funds()
            assert isinstance(funds, Funds)
            # equity live_balance + commodity live_balance
            assert funds.available_cash == 230000.00  # 150000 + 80000
            assert funds.used_margin == 50000.00  # 50000 + 0
            assert funds.total_balance == 280000.00
            assert funds.collateral == 25000.00
            assert funds.realised_pnl == 500.00
            assert funds.unrealised_pnl == -200.00
            assert funds.broker == "zerodha"

    def test_enrichment(self, adapter):
        pos = KiteAdapter._map_position(KITE_POSITION_RAW)
        enriched = adapter._enrich(pos)
        assert enriched.broker == "zerodha"
        assert enriched.account_id == "kite-001"
        assert enriched.client_id == "AB1234"

    def test_order_status_mapping(self):
        """Test all Kite status values map correctly."""
        for raw_status, expected in [
            ("COMPLETE", "COMPLETE"),
            ("REJECTED", "REJECTED"),
            ("CANCELLED", "CANCELLED"),
            ("OPEN", "PENDING"),
            ("TRIGGER PENDING", "PENDING"),
            ("PUT ORDER REQ RECEIVED", "PENDING"),
            ("AMO REQ RECEIVED", "PENDING"),
        ]:
            order_raw = {**KITE_ORDER_RAW, "status": raw_status}
            order = KiteAdapter._map_order(order_raw)
            assert order.status == expected, f"Status {raw_status} → {order.status} (expected {expected})"


class TestUpstoxAdapter:
    """Test Upstox adapter normalisation — especially product D context mapping."""

    @pytest.fixture
    def adapter(self):
        return UpstoxAdapter(
            creds={}, access_token="test_token",
            account_id="upstox-001", client_id="UP1234",
        )

    def test_is_connected(self, adapter):
        assert adapter.is_connected() is True

    def test_map_position_fno_product_d_maps_to_nrml(self, adapter):
        """Upstox product 'D' for F&O should map to NRML, not CNC."""
        pos = UpstoxAdapter._map_position(UPSTOX_POSITION_RAW_FNO)
        assert pos.product == "NRML"
        assert pos.exchange == "NFO"
        assert pos.symbol == "BANKNIFTY26APR38000PE"
        assert pos.qty == 15
        assert pos.instrument_type == "PE"
        assert pos.ltp == 1.75

    def test_map_position_equity_product_d_maps_to_cnc(self, adapter):
        """Upstox product 'D' for equity should map to CNC."""
        pos = UpstoxAdapter._map_position(UPSTOX_POSITION_RAW_EQ)
        assert pos.product == "CNC"
        assert pos.exchange == "NSE"
        assert pos.symbol == "SBIN"

    def test_map_position_intraday(self, adapter):
        """Upstox product 'I' should map to MIS."""
        raw = {**UPSTOX_POSITION_RAW_EQ, "product": "I"}
        pos = UpstoxAdapter._map_position(raw)
        assert pos.product == "MIS"

    def test_map_order(self, adapter):
        order = UpstoxAdapter._map_order(UPSTOX_ORDER_RAW)
        assert order.order_id == "260405000555555"
        assert order.side == "SELL"
        assert order.product == "MIS"  # "I" → MIS
        assert order.order_type == "LIMIT"
        assert order.status == "COMPLETE"
        assert order.filled_qty == 50
        assert order.avg_price == 199.75

    def test_map_holding(self, adapter):
        h = UpstoxAdapter._map_holding(UPSTOX_HOLDING_RAW)
        assert h.symbol == "YESBANK"
        assert h.exchange == "NSE"
        assert h.qty == 36
        assert h.avg_price == 18.75
        assert h.ltp == 17.05
        assert h.pnl == -61.20
        assert h.isin == "INE528G01035"
        assert h.t1_qty == 0

    def test_map_trade_product_mapping(self, adapter):
        t = UpstoxAdapter._map_trade(UPSTOX_TRADE_RAW)
        assert t.trade_id == "T001"
        assert t.side == "SELL"
        assert t.product == "MIS"  # "I" → MIS
        assert t.qty == 50
        assert t.price == 199.75

    def test_exchange_from_instrument_token(self):
        from broker.adapters.upstox_adapter import _parse_exchange_from_token
        assert _parse_exchange_from_token("NSE_EQ|INE848E01016") == "NSE"
        assert _parse_exchange_from_token("NSE_FO|43919") == "NFO"
        assert _parse_exchange_from_token("MCX_FO|259711") == "MCX"
        assert _parse_exchange_from_token("NCD_FO|13177") == "CDS"
        assert _parse_exchange_from_token("BSE_FO|12345") == "BFO"


class TestAngelOneAdapter:
    """Test Angel One SmartAPI adapter normalisation."""

    @pytest.fixture
    def adapter(self):
        return AngelOneAdapter(
            creds={"API_KEY": "test_key"}, jwt_token="test_jwt",
            account_id="angel-001", client_id="A12345",
        )

    def test_is_connected(self, adapter):
        assert adapter.is_connected() is True

    def test_map_position(self, adapter):
        pos = AngelOneAdapter._map_position(ANGEL_POSITION_RAW)
        assert pos.symbol == "NIFTY26APR24500CE"
        assert pos.exchange == "NFO"
        assert pos.product == "NRML"  # CARRYFORWARD → NRML
        assert pos.qty == 25
        assert pos.avg_price == 180.50  # buyavgprice since qty > 0
        assert pos.ltp == 195.25
        assert pos.unrealised_pnl == 368.75
        assert pos.instrument_type == "CE"

    def test_map_order(self, adapter):
        order = AngelOneAdapter._map_order(ANGEL_ORDER_RAW)
        assert order.order_id == "AO20260405001"
        assert order.symbol == "TATASTEEL"
        assert order.side == "SELL"
        assert order.product == "MIS"  # INTRADAY → MIS
        assert order.order_type == "SL"  # STOPLOSS_LIMIT → SL
        assert order.qty == 100
        assert order.price == 140.00
        assert order.trigger_price == 141.00
        assert order.status == "PENDING"  # trigger pending → PENDING

    def test_map_holding(self, adapter):
        h = AngelOneAdapter._map_holding(ANGEL_HOLDING_RAW)
        assert h.symbol == "INFY"
        assert h.qty == 10
        assert h.avg_price == 1450.0
        assert h.ltp == 1520.0
        assert h.pnl == 700.0
        assert h.pnl_pct == 4.83
        assert h.isin == "INE009A01021"

    def test_map_trade(self, adapter):
        t = AngelOneAdapter._map_trade(ANGEL_TRADE_RAW)
        assert t.trade_id == "T100"
        assert t.order_id == "AO20260405002"
        assert t.side == "BUY"
        assert t.product == "CNC"  # DELIVERY → CNC
        assert t.qty == 10
        assert t.price == 1450.00

    def test_map_funds(self, adapter):
        with patch.object(adapter, "_get") as mock_get:
            mock_get.return_value = ANGEL_FUNDS_RAW
            funds = adapter.get_funds()
            assert isinstance(funds, Funds)
            assert funds.available_cash == 175000.50
            assert funds.used_margin == 25000.00
            assert funds.collateral == 50000.00
            assert funds.realised_pnl == 1200.00
            assert funds.unrealised_pnl == -300.00


class TestDhanAdapter:
    """Test Dhan HQ API adapter normalisation."""

    @pytest.fixture
    def adapter(self):
        return DhanAdapter(
            creds={"CLIENT_ID": "DH001"}, access_token="test_token",
            account_id="dhan-001", client_id="DH001",
        )

    def test_is_connected(self, adapter):
        assert adapter.is_connected() is True

    def test_map_position(self, adapter):
        pos = DhanAdapter._map_position(DHAN_POSITION_RAW)
        assert pos.symbol == "NIFTY-26APR2026-24500-CE"
        assert pos.exchange == "NFO"  # NSE_FNO → NFO
        assert pos.product == "MIS"  # INTRADAY → MIS
        assert pos.qty == 75
        assert pos.avg_price == 125.50
        assert pos.unrealised_pnl == 337.50
        assert pos.instrument_type == "CE"  # drvOptionType = CALL → CE
        assert pos.buy_qty == 75

    def test_map_order(self, adapter):
        order = DhanAdapter._map_order(DHAN_ORDER_RAW)
        assert order.order_id == "DH20260405001"
        assert order.symbol == "HDFCBANK"
        assert order.exchange == "NSE"  # NSE_EQ → NSE
        assert order.side == "BUY"
        assert order.product == "CNC"
        assert order.order_type == "MARKET"
        assert order.status == "COMPLETE"  # TRADED → COMPLETE
        assert order.filled_qty == 5
        assert order.avg_price == 1580.25

    def test_map_holding(self, adapter):
        h = DhanAdapter._map_holding(DHAN_HOLDING_RAW)
        assert h.symbol == "ITC"
        assert h.exchange == "NSE"
        assert h.qty == 100
        assert h.dp_qty == 100
        assert h.t1_qty == 0
        assert h.avg_price == 420.50
        assert h.ltp == 435.00
        assert h.isin == "INE154A01025"
        # PnL computed = (435 - 420.50) * 100
        assert abs(h.pnl - 1450.0) < 0.01

    def test_map_trade(self, adapter):
        t = DhanAdapter._map_trade(DHAN_TRADE_RAW)
        assert t.trade_id == "DT001"
        assert t.side == "BUY"
        assert t.product == "CNC"
        assert t.qty == 5
        assert t.price == 1580.25

    def test_map_funds(self, adapter):
        with patch.object(adapter, "_get") as mock_get:
            mock_get.return_value = DHAN_FUNDS_RAW
            funds = adapter.get_funds()
            assert funds.available_cash == 125000.00
            assert funds.used_margin == 25000.00
            assert funds.total_balance == 150000.00  # sodLimit
            assert funds.collateral == 10000.00

    def test_exchange_segment_mapping(self):
        """Dhan exchangeSegment maps correctly to canonical exchange."""
        for raw_seg, expected_exch in [
            ("NSE_EQ", "NSE"), ("NSE_FNO", "NFO"), ("BSE_EQ", "BSE"),
            ("BSE_FNO", "BFO"), ("MCX_COMM", "MCX"), ("NSE_CURRENCY", "CDS"),
        ]:
            raw = {**DHAN_POSITION_RAW, "exchangeSegment": raw_seg}
            pos = DhanAdapter._map_position(raw)
            assert pos.exchange == expected_exch, f"{raw_seg} → {pos.exchange} (expected {expected_exch})"


class TestGrowwAdapter:
    """Test Groww Trade API adapter normalisation."""

    @pytest.fixture
    def adapter(self):
        return GrowwAdapter(
            creds={}, access_token="test_token",
            account_id="groww-001", client_id="GW001",
        )

    def test_is_connected(self, adapter):
        assert adapter.is_connected() is True

    def test_map_position(self, adapter):
        pos = GrowwAdapter._map_position(GROWW_POSITION_RAW)
        assert pos.symbol == "RELIANCE"
        assert pos.exchange == "NSE"
        assert pos.product == "CNC"
        assert pos.qty == 10
        assert pos.avg_price == 2400.00
        assert pos.buy_qty == 10
        assert pos.sell_qty == 0

    def test_map_order(self, adapter):
        order = GrowwAdapter._map_order(GROWW_ORDER_RAW)
        assert order.order_id == "GW20260405001"
        assert order.symbol == "TCS"
        assert order.side == "BUY"
        assert order.product == "CNC"
        assert order.order_type == "LIMIT"
        assert order.qty == 3
        assert order.price == 3800.00
        assert order.status == "PENDING"  # OPEN → PENDING

    def test_map_holding(self, adapter):
        h = GrowwAdapter._map_holding(GROWW_HOLDING_RAW)
        assert h.symbol == "WIPRO"
        assert h.qty == 20
        assert h.avg_price == 450.00
        assert h.isin == "INE075A01022"
        assert h.dp_qty == 20
        assert h.t1_qty == 0

    def test_map_funds(self, adapter):
        with patch.object(adapter, "_get") as mock_get:
            mock_get.return_value = GROWW_FUNDS_RAW
            funds = adapter.get_funds()
            assert funds.available_cash == 95000.00
            assert funds.used_margin == 15000.00
            assert funds.total_balance == 110000.00


# ═══════════════════════════════════════════════════════════════════════════════
#  Cross-adapter consistency tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestCrossAdapterConsistency:
    """Verify all adapters produce consistent normalised output formats."""

    @pytest.fixture
    def all_positions(self) -> List[Position]:
        return [
            KiteAdapter._map_position(KITE_POSITION_RAW),
            UpstoxAdapter._map_position(UPSTOX_POSITION_RAW_FNO),
            AngelOneAdapter._map_position(ANGEL_POSITION_RAW),
            DhanAdapter._map_position(DHAN_POSITION_RAW),
            GrowwAdapter._map_position(GROWW_POSITION_RAW),
        ]

    @pytest.fixture
    def all_orders(self) -> List[Order]:
        return [
            KiteAdapter._map_order(KITE_ORDER_RAW),
            UpstoxAdapter._map_order(UPSTOX_ORDER_RAW),
            AngelOneAdapter._map_order(ANGEL_ORDER_RAW),
            DhanAdapter._map_order(DHAN_ORDER_RAW),
            GrowwAdapter._map_order(GROWW_ORDER_RAW),
        ]

    def test_all_positions_are_position_type(self, all_positions):
        for pos in all_positions:
            assert isinstance(pos, Position)

    def test_all_positions_have_required_fields(self, all_positions):
        for pos in all_positions:
            d = pos.to_dict()
            for key in ("symbol", "exchange", "product", "qty", "avg_price", "ltp",
                        "pnl", "realised_pnl", "unrealised_pnl", "buy_qty", "sell_qty"):
                assert key in d, f"Position missing field: {key}"

    def test_all_products_are_canonical(self, all_positions):
        valid_products = {"MIS", "NRML", "CNC"}
        for pos in all_positions:
            assert pos.product in valid_products, \
                f"Non-canonical product '{pos.product}' from {pos.symbol}"

    def test_all_exchanges_are_canonical(self, all_positions):
        valid_exchanges = {"NSE", "BSE", "NFO", "BFO", "MCX", "CDS", "BCD"}
        for pos in all_positions:
            assert pos.exchange in valid_exchanges, \
                f"Non-canonical exchange '{pos.exchange}' from {pos.symbol}"

    def test_all_orders_are_order_type(self, all_orders):
        for order in all_orders:
            assert isinstance(order, Order)

    def test_all_order_sides_canonical(self, all_orders):
        for order in all_orders:
            assert order.side in ("BUY", "SELL"), f"Non-canonical side: {order.side}"

    def test_all_order_statuses_canonical(self, all_orders):
        valid_statuses = {"PENDING", "COMPLETE", "CANCELLED", "REJECTED"}
        for order in all_orders:
            assert order.status in valid_statuses, \
                f"Non-canonical status '{order.status}' for order {order.order_id}"

    def test_all_order_types_canonical(self, all_orders):
        valid_types = {"MARKET", "LIMIT", "SL", "SL-M"}
        for order in all_orders:
            assert order.order_type in valid_types, \
                f"Non-canonical order_type '{order.order_type}'"

    def test_all_instrument_types_canonical(self, all_positions):
        valid_types = {"EQ", "CE", "PE", "FUT", "IDX", ""}
        for pos in all_positions:
            assert pos.instrument_type in valid_types, \
                f"Non-canonical instrument_type '{pos.instrument_type}'"

    def test_to_dict_roundtrip(self, all_positions, all_orders):
        """Verify to_dict() produces JSON-serializable dicts."""
        for obj in all_positions + all_orders:
            d = obj.to_dict()
            assert isinstance(d, dict)
            # Should be JSON-serializable
            json.dumps(d)


# ═══════════════════════════════════════════════════════════════════════════════
#  Edge case tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestEdgeCases:
    """Test edge cases: missing fields, zero values, unexpected data."""

    def test_kite_empty_position(self):
        pos = KiteAdapter._map_position({})
        assert pos.symbol == ""
        assert pos.qty == 0
        assert pos.avg_price == 0.0

    def test_upstox_null_average_price(self):
        """Upstox can return null for average_price on closed positions."""
        raw = {**UPSTOX_POSITION_RAW_EQ, "average_price": None}
        pos = UpstoxAdapter._map_position(raw)
        assert pos.avg_price == 0.0

    def test_angel_string_number_fields(self):
        """Angel One returns numeric fields as strings."""
        pos = AngelOneAdapter._map_position(ANGEL_POSITION_RAW)
        assert isinstance(pos.qty, int)
        assert isinstance(pos.avg_price, float)
        assert isinstance(pos.ltp, float)

    def test_dhan_missing_option_type(self):
        """Dhan position without drvOptionType should infer from symbol suffix."""
        raw = {**DHAN_POSITION_RAW, "drvOptionType": None, "tradingSymbol": "NIFTY26APR24500PE"}
        pos = DhanAdapter._map_position(raw)
        assert pos.instrument_type == "PE"

    def test_groww_missing_fields(self):
        """Groww position with minimal fields."""
        pos = GrowwAdapter._map_position({"trading_symbol": "HDFC", "exchange": "NSE"})
        assert pos.symbol == "HDFC"
        assert pos.qty == 0
        assert pos.product == "MIS"  # default

    def test_not_connected_returns_empty(self):
        """All adapters return empty lists when not connected."""
        kite = KiteAdapter(creds={}, api_key="", access_token="",
                           account_id="", client_id="")
        assert kite.is_connected() is False
        assert kite.get_positions() == []
        assert kite.get_order_book() == []
        assert kite.get_holdings() == []
        assert kite.get_tradebook() == []

        upstox = UpstoxAdapter(creds={}, access_token="",
                               account_id="", client_id="")
        assert upstox.is_connected() is False
        assert upstox.get_positions() == []

    def test_order_rejected_status_with_message(self):
        """Rejected orders should carry the rejection reason in remarks."""
        raw = {
            **KITE_ORDER_RAW,
            "status": "REJECTED",
            "status_message": "Insufficient funds. Required margin is 95417.84",
        }
        order = KiteAdapter._map_order(raw)
        assert order.status == "REJECTED"
        assert "Insufficient funds" in order.remarks
