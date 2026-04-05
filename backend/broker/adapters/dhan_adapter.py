"""
Smart Trader — Dhan Broker Adapter
=====================================
Wraps Dhan HQ API v2 (REST) and translates responses
to normalised Smart Trader models.

Dhan field reference
--------------------
Positions: dhanClientId, tradingSymbol, securityId, positionType, exchangeSegment,
           productType, buyAvg, buyQty, sellAvg, sellQty, netQty, costPrice,
           realizedProfit, unrealizedProfit, drvExpiryDate, drvOptionType, drvStrikePrice

Orders:    orderId, exchangeOrderId, correlationId, transactionType, exchangeSegment,
           productType, orderType, validity, securityId, tradingSymbol, quantity,
           price, triggerPrice, filledQty, averageTradedPrice, orderStatus,
           createTime, updateTime, exchangeTime, drvExpiryDate, drvOptionType

Holdings:  exchange, tradingSymbol, securityId, isin, totalQty, dpQty, t1Qty,
           availableQty, collateralQty, avgCostPrice

Funds:     availableBalance, sodLimit, collateralAmount, receiveableAmount,
           utilisedMargin, blockedPayoutAmount, withdrawableBalance
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import requests

from broker.adapters.base import BrokerAdapter, Funds, Holding, Order, Position, Trade

logger = logging.getLogger("smart_trader.broker.dhan")

_BASE_URL = "https://api.dhan.co/v2"

_DHAN_STATUS_MAP = {
    "TRADED": "COMPLETE",
    "TRANSIT": "PENDING",
    "PENDING": "PENDING",
    "REJECTED": "REJECTED",
    "CANCELLED": "CANCELLED",
    "EXPIRED": "CANCELLED",
    "PART_TRADED": "PENDING",
}

# Dhan exchangeSegment ↔ Smart Trader exchange mapping
_EXCH_TO_DHAN = {
    "NSE": "NSE_EQ", "NFO": "NSE_FNO", "BSE": "BSE_EQ", "BFO": "BSE_FNO",
    "MCX": "MCX_COMM", "CDS": "NSE_CURRENCY",
}
_DHAN_TO_EXCH = {v: k for k, v in _EXCH_TO_DHAN.items()}


class DhanAdapter(BrokerAdapter):
    """
    Adapter for Dhan HQ API v2 trading accounts.

    Lifecycle:
        1. Instantiate with credentials dict containing DHAN_CLIENT_ID and access token.
        2. Use get_positions(), place_order(), etc.
    """

    def __init__(
        self,
        creds: Dict[str, Any],
        access_token: str,
        account_id: str,
        client_id: str,
    ) -> None:
        super().__init__("dhan", account_id, client_id)
        self._creds = creds
        self._access_token = access_token
        self._dhan_client_id = creds.get("CLIENT_ID", client_id)
        self._connected = bool(access_token)

    # ── Internal: build request headers ──────────────────────────────────────

    def _headers(self) -> Dict[str, str]:
        return {
            "access-token": self._access_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _get(self, path: str) -> Any:
        try:
            resp = requests.get(f"{_BASE_URL}{path}", headers=self._headers(), timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error("Dhan GET %s failed: %s", path, e)
            return None

    def _post(self, path: str, payload: dict) -> Optional[dict]:
        try:
            resp = requests.post(
                f"{_BASE_URL}{path}", json=payload,
                headers=self._headers(), timeout=15,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            # Try to extract error body
            body = {}
            try:
                body = e.response.json()
            except Exception:
                pass
            logger.error("Dhan POST %s HTTP error: %s body=%s", path, e, body)
            return body or {"errorCode": str(e)}
        except Exception as e:
            logger.error("Dhan POST %s failed: %s", path, e)
            return None

    def _put(self, path: str, payload: dict) -> Optional[dict]:
        try:
            resp = requests.put(
                f"{_BASE_URL}{path}", json=payload,
                headers=self._headers(), timeout=15,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error("Dhan PUT %s failed: %s", path, e)
            return None

    def _delete(self, path: str) -> Optional[dict]:
        try:
            resp = requests.delete(f"{_BASE_URL}{path}", headers=self._headers(), timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error("Dhan DELETE %s failed: %s", path, e)
            return None

    # ── Connection ─────────────────────────────────────────────────────────────

    def is_connected(self) -> bool:
        return self._connected

    # ── Positions ──────────────────────────────────────────────────────────────

    def get_positions(self) -> List[Position]:
        if not self.is_connected():
            return []
        raw_list = self._get("/positions")
        if not isinstance(raw_list, list):
            return []
        return [self._enrich(self._map_position(p)) for p in raw_list if p]

    @staticmethod
    def _map_position(raw: dict) -> Position:
        symbol = raw.get("tradingSymbol", "")
        dhan_seg = raw.get("exchangeSegment", "NSE_EQ")
        exchange = _DHAN_TO_EXCH.get(dhan_seg, "NSE")
        prd_raw = (raw.get("productType") or "").upper()
        prod_map = {"INTRADAY": "MIS", "CNC": "CNC", "MARGIN": "NRML", "MTF": "NRML",
                    "MIS": "MIS", "NRML": "NRML"}
        product = prod_map.get(prd_raw, "MIS")
        qty = int(raw.get("netQty") or 0)
        buy_avg = float(raw.get("buyAvg") or 0)
        sell_avg = float(raw.get("sellAvg") or 0)
        avg_prc = float(raw.get("costPrice") or (buy_avg if qty >= 0 else sell_avg))
        ltp = float(raw.get("ltp") or 0)
        rpnl = float(raw.get("realizedProfit") or 0)
        urmtom = float(raw.get("unrealizedProfit") or 0)
        buy_qty = int(raw.get("buyQty") or 0)
        sell_qty = int(raw.get("sellQty") or 0)

        itype = ""
        opt_type = raw.get("drvOptionType")
        if opt_type == "CALL":       itype = "CE"
        elif opt_type == "PUT":      itype = "PE"
        elif symbol.endswith("CE"):  itype = "CE"
        elif symbol.endswith("PE"):  itype = "PE"
        elif "FUT" in symbol:        itype = "FUT"
        else:                        itype = "EQ"

        return Position(
            symbol=symbol, exchange=exchange, product=product,
            qty=qty, avg_price=avg_prc, ltp=ltp,
            pnl=rpnl + urmtom, realised_pnl=rpnl, unrealised_pnl=urmtom,
            buy_qty=buy_qty, sell_qty=sell_qty, instrument_type=itype,
        )

    # ── Order book ─────────────────────────────────────────────────────────────

    def get_order_book(self) -> List[Order]:
        if not self.is_connected():
            return []
        raw_list = self._get("/orders")
        if not isinstance(raw_list, list):
            return []
        return [self._enrich(self._map_order(o)) for o in raw_list if o]

    @staticmethod
    def _map_order(raw: dict) -> Order:
        order_id = str(raw.get("orderId") or "")
        symbol = raw.get("tradingSymbol", "")
        dhan_seg = raw.get("exchangeSegment", "NSE_EQ")
        exchange = _DHAN_TO_EXCH.get(dhan_seg, "NSE")
        side = "BUY" if str(raw.get("transactionType", "")).upper() == "BUY" else "SELL"
        prd_raw = (raw.get("productType") or "").upper()
        prod_map = {"INTRADAY": "MIS", "CNC": "CNC", "MARGIN": "NRML", "MTF": "NRML"}
        product = prod_map.get(prd_raw, "MIS")
        otype_raw = (raw.get("orderType") or "").upper()
        otype_map = {"MARKET": "MARKET", "LIMIT": "LIMIT",
                     "STOP_LOSS": "SL", "STOP_LOSS_MARKET": "SL-M"}
        order_type = otype_map.get(otype_raw, "MARKET")
        qty = int(raw.get("quantity") or 0)
        price = float(raw.get("price") or 0)
        trgprc = float(raw.get("triggerPrice") or 0)
        status_raw = str(raw.get("orderStatus") or "PENDING").upper()
        status = _DHAN_STATUS_MAP.get(status_raw, "PENDING")
        filled = int(raw.get("filledQty") or 0)
        avg_prc = float(raw.get("averageTradedPrice") or 0)
        ts = raw.get("updateTime") or raw.get("createTime") or ""
        remarks = raw.get("omsErrorDescription") or ""

        return Order(
            order_id=order_id, symbol=symbol, exchange=exchange,
            side=side, product=product, order_type=order_type,
            qty=qty, price=price, trigger_price=trgprc,
            status=status, filled_qty=filled, avg_price=avg_prc,
            timestamp=ts, remarks=remarks,
        )

    # ── Funds ──────────────────────────────────────────────────────────────────

    def get_funds(self) -> Funds:
        if not self.is_connected():
            return self._enrich(Funds(available_cash=0.0))
        resp = self._get("/fundlimit")
        if not isinstance(resp, dict):
            return self._enrich(Funds(available_cash=0.0))
        available = float(resp.get("availableBalance") or resp.get("withdrawableBalance") or 0)
        used = float(resp.get("utilisedMargin") or 0)
        collateral = float(resp.get("collateralAmount") or 0)
        sod = float(resp.get("sodLimit") or 0)
        return self._enrich(Funds(
            available_cash=available,
            used_margin=used,
            total_balance=sod or (available + used),
            collateral=collateral,
        ))

    # ── Holdings ───────────────────────────────────────────────────────────────

    def get_holdings(self) -> List[Holding]:
        if not self.is_connected():
            return []
        raw_list = self._get("/holdings")
        if not isinstance(raw_list, list):
            return []
        return [self._enrich(self._map_holding(h)) for h in raw_list if h]

    @staticmethod
    def _map_holding(raw: dict) -> Holding:
        symbol = raw.get("tradingSymbol", "")
        exchange = raw.get("exchange", "NSE")
        qty = int(raw.get("totalQty") or 0)
        dp_qty = int(raw.get("dpQty") or 0)
        t1 = int(raw.get("t1Qty") or 0)
        avg_prc = float(raw.get("avgCostPrice") or 0)
        ltp = float(raw.get("ltp") or 0)
        pnl = (ltp - avg_prc) * qty if avg_prc and qty else 0.0
        pnl_pct = (pnl / (avg_prc * qty) * 100) if avg_prc and qty else 0.0
        isin = raw.get("isin") or ""
        return Holding(
            symbol=symbol, exchange=exchange, qty=qty, avg_price=avg_prc,
            ltp=ltp, pnl=pnl, pnl_pct=pnl_pct, isin=isin,
            dp_qty=dp_qty, t1_qty=t1,
        )

    # ── Tradebook ──────────────────────────────────────────────────────────────

    def get_tradebook(self) -> List[Trade]:
        if not self.is_connected():
            return []
        raw_list = self._get("/trades")
        if not isinstance(raw_list, list):
            return []
        return [self._enrich(self._map_trade(t)) for t in raw_list if t]

    @staticmethod
    def _map_trade(raw: dict) -> Trade:
        side = "BUY" if str(raw.get("transactionType", "")).upper() == "BUY" else "SELL"
        dhan_seg = raw.get("exchangeSegment", "NSE_EQ")
        exchange = _DHAN_TO_EXCH.get(dhan_seg, "NSE")
        prd_raw = (raw.get("productType") or "").upper()
        prod_map = {"INTRADAY": "MIS", "CNC": "CNC", "MARGIN": "NRML"}
        return Trade(
            trade_id=str(raw.get("tradingId") or raw.get("tradeId") or ""),
            order_id=str(raw.get("orderId") or ""),
            symbol=raw.get("tradingSymbol", ""),
            exchange=exchange,
            side=side,
            product=prod_map.get(prd_raw, prd_raw),
            qty=int(raw.get("tradedQuantity") or raw.get("quantity") or 0),
            price=float(raw.get("tradedPrice") or 0),
            timestamp=raw.get("exchangeTime") or raw.get("createTime") or "",
        )

    # ── Order management ───────────────────────────────────────────────────────

    def place_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        if not self.is_connected():
            return {"success": False, "message": "Not connected"}
        try:
            sym = order.get("symbol", "")
            exch = order.get("exchange", "NSE")

            side_str = str(order.get("side", "BUY")).upper()
            txn_type = "BUY" if side_str in ("BUY", "B") else "SELL"

            prd_map = {"MIS": "INTRADAY", "NRML": "MARGIN", "CNC": "CNC"}
            otype_map = {"MARKET": "MARKET", "LIMIT": "LIMIT", "SL": "STOP_LOSS", "SL-M": "STOP_LOSS_MARKET"}

            prd_raw = str(order.get("product", "MIS")).upper()
            otype_raw = str(order.get("order_type", "MARKET")).upper()

            # Dhan uses securityId (exchange token) — use from order dict
            security_id = str(order.get("token") or order.get("securityId") or order.get("security_id") or "0")

            dhan_order = {
                "dhanClientId": self._dhan_client_id,
                "transactionType": txn_type,
                "exchangeSegment": _EXCH_TO_DHAN.get(exch, "NSE_EQ"),
                "productType": prd_map.get(prd_raw, "INTRADAY"),
                "orderType": otype_map.get(otype_raw, "MARKET"),
                "validity": "DAY",
                "securityId": security_id,
                "quantity": int(order.get("qty", 0)),
                "price": float(order.get("price", 0) or 0),
                "triggerPrice": float(order.get("trigger_price", 0) or 0),
                "afterMarketOrder": False,
                "correlationId": order.get("tag", "smart_trader"),
            }

            result = self._post("/orders", dhan_order)
            if result and result.get("orderId"):
                return {
                    "success": True,
                    "order_id": str(result["orderId"]),
                    "message": result.get("orderStatus", "Order placed"),
                }
            msg = (result or {}).get("errorMessage") or (result or {}).get("omsErrorDescription") or "Order failed"
            return {"success": False, "message": msg}
        except Exception as e:
            logger.exception("place_order error: %s", e)
            return {"success": False, "message": str(e)}

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        if not self.is_connected():
            return {"success": False, "message": "Not connected"}
        try:
            result = self._delete(f"/orders/{order_id}")
            if result and result.get("orderId"):
                return {"success": True, "message": "Order cancelled"}
            return {"success": False, "message": (result or {}).get("errorMessage", "Cancel failed")}
        except Exception as e:
            logger.error("cancel_order error: %s", e)
            return {"success": False, "message": str(e)}

    def modify_order(self, order_id: str, modifications: Dict[str, Any]) -> Dict[str, Any]:
        if not self.is_connected():
            return {"success": False, "message": "Not connected"}
        try:
            otype_map = {"MARKET": "MARKET", "LIMIT": "LIMIT", "SL": "STOP_LOSS", "SL-M": "STOP_LOSS_MARKET"}
            otype_raw = str(modifications.get("order_type", "LIMIT")).upper()

            modify_data = {
                "dhanClientId": self._dhan_client_id,
                "orderId": order_id,
                "orderType": otype_map.get(otype_raw, "LIMIT"),
                "validity": "DAY",
                "quantity": int(modifications.get("qty", 0)) if modifications.get("qty") else 0,
                "price": float(modifications.get("price", 0) or 0),
                "triggerPrice": float(modifications.get("trigger_price", 0) or 0),
            }
            result = self._put(f"/orders/{order_id}", modify_data)
            if result and result.get("orderId"):
                return {"success": True, "message": "Order modified"}
            return {"success": False, "message": (result or {}).get("errorMessage", "Modify failed")}
        except Exception as e:
            logger.error("modify_order error: %s", e)
            return {"success": False, "message": str(e)}
