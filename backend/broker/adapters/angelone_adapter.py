"""
Smart Trader — Angel One Broker Adapter
=========================================
Wraps Angel One SmartAPI (REST) and translates responses
to normalised Smart Trader models.

Angel One field reference
-------------------------
Positions: exchange, symboltoken, producttype, tradingsymbol, symbolname,
           buyqty, sellqty, buyavgprice, sellavgprice, netqty, ltp, pnl,
           realised, unrealised, cfbuyqty, cfsellqty

Orders:    orderid, tradingsymbol, symboltoken, transactiontype, exchange,
           producttype, ordertype, quantity, price, triggerprice, status,
           filledshares, averageprice, text, updatetime, variety

Holdings:  tradingsymbol, exchange, isin, quantity, t1quantity, averageprice,
           ltp, profitandloss, pnlpercentage, symboltoken

Funds:     availablecash, collateral, m2munrealized, m2mrealized, utiliseddebits
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import requests

from broker.adapters.base import BrokerAdapter, Funds, Holding, Order, Position, Trade

logger = logging.getLogger("smart_trader.broker.angelone")

_BASE_URL = "https://apiconnect.angelone.in"

_ANGEL_STATUS_MAP = {
    "complete": "COMPLETE",
    "rejected": "REJECTED",
    "cancelled": "CANCELLED",
    "open": "PENDING",
    "pending": "PENDING",
    "trigger pending": "PENDING",
    "open pending": "PENDING",
    "modify pending": "PENDING",
    "cancel pending": "PENDING",
    "after market order req received": "PENDING",
    "validation pending": "PENDING",
    "put order req received": "PENDING",
}


class AngelOneAdapter(BrokerAdapter):
    """
    Adapter for Angel One SmartAPI trading accounts.

    Lifecycle:
        1. Instantiate with credentials dict and a valid JWT token.
        2. Use get_positions(), place_order(), etc.
    """

    def __init__(
        self,
        creds: Dict[str, Any],
        jwt_token: str,
        account_id: str,
        client_id: str,
    ) -> None:
        super().__init__("angel", account_id, client_id)
        self._creds = creds
        self._jwt_token = jwt_token
        self._api_key = creds.get("API_KEY", "")
        self._connected = bool(jwt_token and self._api_key)

    # ── Internal: build request headers ──────────────────────────────────────

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._jwt_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-UserType": "USER",
            "X-SourceID": "WEB",
            "X-ClientLocalIP": "127.0.0.1",
            "X-ClientPublicIP": "127.0.0.1",
            "X-MACAddress": "00:00:00:00:00:00",
            "X-PrivateKey": self._api_key,
        }

    def _get(self, path: str) -> Optional[dict]:
        try:
            resp = requests.get(f"{_BASE_URL}{path}", headers=self._headers(), timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") is False:
                logger.warning("Angel API error %s: %s", path, data.get("message"))
                return None
            return data
        except Exception as e:
            logger.error("Angel GET %s failed: %s", path, e)
            return None

    def _post(self, path: str, payload: dict) -> Optional[dict]:
        try:
            resp = requests.post(
                f"{_BASE_URL}{path}", json=payload,
                headers=self._headers(), timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            return data
        except Exception as e:
            logger.error("Angel POST %s failed: %s", path, e)
            return None

    # ── Connection ─────────────────────────────────────────────────────────────

    def is_connected(self) -> bool:
        return self._connected

    # ── Positions ──────────────────────────────────────────────────────────────

    def get_positions(self) -> List[Position]:
        if not self.is_connected():
            return []
        resp = self._get("/rest/secure/angelbroking/order/v1/getPosition")
        if not resp or not resp.get("data"):
            return []
        raw_list = resp["data"]
        if isinstance(raw_list, dict):
            # Angel returns {net: [...], day: [...]} — use net
            raw_list = raw_list.get("net") or raw_list.get("day") or []
        return [self._enrich(self._map_position(p)) for p in raw_list if p]

    @staticmethod
    def _map_position(raw: dict) -> Position:
        symbol = raw.get("tradingsymbol", "")
        exchange = raw.get("exchange", "NSE")
        prd_raw = (raw.get("producttype") or "").upper()
        prod_map = {
            "INTRADAY": "MIS", "DELIVERY": "CNC", "CARRYFORWARD": "NRML",
            "MARGIN": "NRML", "BO": "MIS", "MIS": "MIS", "CNC": "CNC", "NRML": "NRML",
        }
        product = prod_map.get(prd_raw, "MIS")
        qty = int(raw.get("netqty") or 0)
        buy_avg = float(raw.get("buyavgprice") or 0)
        sell_avg = float(raw.get("sellavgprice") or 0)
        avg_prc = buy_avg if qty >= 0 else sell_avg
        ltp = float(raw.get("ltp") or 0)
        rpnl = float(raw.get("realised") or 0)
        urmtom = float(raw.get("unrealised") or 0)
        buy_qty = int(raw.get("buyqty") or 0)
        sell_qty = int(raw.get("sellqty") or 0)

        itype = ""
        if symbol.endswith("CE"):       itype = "CE"
        elif symbol.endswith("PE"):     itype = "PE"
        elif "FUT" in symbol:           itype = "FUT"
        elif symbol.endswith("-EQ"):    itype = "EQ"
        else:                           itype = "EQ"

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
        resp = self._get("/rest/secure/angelbroking/order/v1/getOrderBook")
        if not resp or not resp.get("data"):
            return []
        return [self._enrich(self._map_order(o)) for o in resp["data"] if o]

    @staticmethod
    def _map_order(raw: dict) -> Order:
        order_id = str(raw.get("orderid") or "")
        symbol = raw.get("tradingsymbol", "")
        exchange = raw.get("exchange", "")
        side = "BUY" if str(raw.get("transactiontype", "")).upper() == "BUY" else "SELL"
        prd_raw = (raw.get("producttype") or "").upper()
        prod_map = {
            "INTRADAY": "MIS", "DELIVERY": "CNC", "CARRYFORWARD": "NRML",
            "MARGIN": "NRML", "BO": "MIS",
        }
        product = prod_map.get(prd_raw, "MIS")
        otype_raw = (raw.get("ordertype") or "").upper()
        otype_map = {
            "MARKET": "MARKET", "LIMIT": "LIMIT",
            "STOPLOSS_LIMIT": "SL", "STOPLOSS_MARKET": "SL-M",
        }
        order_type = otype_map.get(otype_raw, "MARKET")
        qty = int(raw.get("quantity") or 0)
        price = float(raw.get("price") or 0)
        trgprc = float(raw.get("triggerprice") or 0)
        status_raw = str(raw.get("status") or "pending").lower()
        status = _ANGEL_STATUS_MAP.get(status_raw, "PENDING")
        filled = int(raw.get("filledshares") or 0)
        avg_prc = float(raw.get("averageprice") or 0)
        ts = raw.get("updatetime") or raw.get("orderupdatetime") or ""
        remarks = raw.get("text") or ""

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
        resp = self._get("/rest/secure/angelbroking/user/v1/getRMS")
        if not resp or not resp.get("data"):
            return self._enrich(Funds(available_cash=0.0))
        d = resp["data"]
        available = float(d.get("availablecash") or d.get("net") or 0)
        used = float(d.get("utiliseddebits") or 0)
        collateral = float(d.get("collateral") or 0)
        m2m_r = float(d.get("m2mrealized") or 0)
        m2m_u = float(d.get("m2munrealized") or 0)
        return self._enrich(Funds(
            available_cash=available,
            used_margin=used,
            total_balance=available + used,
            realised_pnl=m2m_r,
            unrealised_pnl=m2m_u,
            collateral=collateral,
        ))

    # ── Holdings ───────────────────────────────────────────────────────────────

    def get_holdings(self) -> List[Holding]:
        if not self.is_connected():
            return []
        resp = self._get("/rest/secure/angelbroking/portfolio/v1/getHolding")
        if not resp or not resp.get("data"):
            return []
        return [self._enrich(self._map_holding(h)) for h in resp["data"] if h]

    @staticmethod
    def _map_holding(raw: dict) -> Holding:
        symbol = raw.get("tradingsymbol", "")
        exchange = raw.get("exchange", "NSE")
        qty = int(raw.get("quantity") or 0)
        t1 = int(raw.get("t1quantity") or 0)
        avg_prc = float(raw.get("averageprice") or 0)
        ltp = float(raw.get("ltp") or 0)
        pnl = float(raw.get("profitandloss") or 0)
        pnl_pct = float(raw.get("pnlpercentage") or 0)
        isin = raw.get("isin") or ""
        return Holding(
            symbol=symbol, exchange=exchange, qty=qty, avg_price=avg_prc,
            ltp=ltp, pnl=pnl, pnl_pct=pnl_pct, isin=isin,
            dp_qty=qty, t1_qty=t1,
        )

    # ── Tradebook ──────────────────────────────────────────────────────────────

    def get_tradebook(self) -> List[Trade]:
        if not self.is_connected():
            return []
        resp = self._get("/rest/secure/angelbroking/order/v1/getTradeBook")
        if not resp or not resp.get("data"):
            return []
        return [self._enrich(self._map_trade(t)) for t in resp["data"] if t]

    @staticmethod
    def _map_trade(raw: dict) -> Trade:
        side = "BUY" if str(raw.get("transactiontype", "")).upper() == "BUY" else "SELL"
        prd_raw = (raw.get("producttype") or "").upper()
        prod_map = {"INTRADAY": "MIS", "DELIVERY": "CNC", "CARRYFORWARD": "NRML"}
        return Trade(
            trade_id=str(raw.get("tradeid") or raw.get("fillid") or ""),
            order_id=str(raw.get("orderid") or ""),
            symbol=raw.get("tradingsymbol", ""),
            exchange=raw.get("exchange", ""),
            side=side,
            product=prod_map.get(prd_raw, prd_raw),
            qty=int(raw.get("fillshares") or raw.get("quantity") or 0),
            price=float(raw.get("fillprice") or raw.get("averageprice") or 0),
            timestamp=raw.get("filltime") or raw.get("updatetime") or "",
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

            prd_map = {"MIS": "INTRADAY", "NRML": "CARRYFORWARD", "CNC": "DELIVERY"}
            otype_map = {"MARKET": "MARKET", "LIMIT": "LIMIT", "SL": "STOPLOSS_LIMIT", "SL-M": "STOPLOSS_MARKET"}

            prd_raw = str(order.get("product", "MIS")).upper()
            otype_raw = str(order.get("order_type", "MARKET")).upper()

            # symboltoken: use from order dict or default to "0" (Angel may resolve)
            symbol_token = str(order.get("token") or order.get("symboltoken") or "0")

            angel_order = {
                "variety": "NORMAL",
                "tradingsymbol": sym,
                "symboltoken": symbol_token,
                "transactiontype": txn_type,
                "exchange": exch,
                "ordertype": otype_map.get(otype_raw, "MARKET"),
                "producttype": prd_map.get(prd_raw, "INTRADAY"),
                "duration": "DAY",
                "price": str(float(order.get("price", 0) or 0)),
                "quantity": str(int(order.get("qty", 0))),
                "triggerprice": str(float(order.get("trigger_price", 0) or 0)),
                "squareoff": "0",
                "stoploss": "0",
                "disclosedquantity": "0",
                "ordertag": order.get("tag", "smart_trader"),
            }

            result = self._post(
                "/rest/secure/angelbroking/order/v1/placeOrder",
                angel_order,
            )
            if result and result.get("status"):
                oid = (result.get("data") or {}).get("orderid", "")
                return {
                    "success": True,
                    "order_id": oid,
                    "message": result.get("message", "Order placed"),
                }
            msg = (result or {}).get("message", "Order failed")
            return {"success": False, "message": msg}
        except Exception as e:
            logger.exception("place_order error: %s", e)
            return {"success": False, "message": str(e)}

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        if not self.is_connected():
            return {"success": False, "message": "Not connected"}
        try:
            result = self._post(
                "/rest/secure/angelbroking/order/v1/cancelOrder",
                {"variety": "NORMAL", "orderid": order_id},
            )
            if result and result.get("status"):
                return {"success": True, "message": result.get("message", "Cancelled")}
            return {"success": False, "message": (result or {}).get("message", "Cancel failed")}
        except Exception as e:
            logger.error("cancel_order error: %s", e)
            return {"success": False, "message": str(e)}

    def modify_order(self, order_id: str, modifications: Dict[str, Any]) -> Dict[str, Any]:
        if not self.is_connected():
            return {"success": False, "message": "Not connected"}
        try:
            otype_map = {"MARKET": "MARKET", "LIMIT": "LIMIT", "SL": "STOPLOSS_LIMIT", "SL-M": "STOPLOSS_MARKET"}
            otype_raw = str(modifications.get("order_type", "LIMIT")).upper()

            modify_data = {
                "variety": "NORMAL",
                "orderid": order_id,
                "ordertype": otype_map.get(otype_raw, "LIMIT"),
                "producttype": modifications.get("producttype", "INTRADAY"),
                "duration": "DAY",
                "price": str(float(modifications.get("price", 0) or 0)),
                "quantity": str(int(modifications.get("qty", 0))) if modifications.get("qty") else "0",
                "triggerprice": str(float(modifications.get("trigger_price", 0) or 0)),
            }
            # Remove zero-value optional fields
            if modify_data["quantity"] == "0":
                del modify_data["quantity"]

            result = self._post(
                "/rest/secure/angelbroking/order/v1/modifyOrder",
                modify_data,
            )
            if result and result.get("status"):
                return {"success": True, "message": result.get("message", "Modified")}
            return {"success": False, "message": (result or {}).get("message", "Modify failed")}
        except Exception as e:
            logger.error("modify_order error: %s", e)
            return {"success": False, "message": str(e)}
