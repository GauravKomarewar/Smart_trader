"""
Smart Trader — Upstox Broker Adapter
=====================================
Wraps Upstox API v3 (REST) and translates responses
to normalised Smart Trader models.

Upstox field reference
----------------------
Place Order: instrument_token (format "NSE_FO|43919"), quantity, product (I/D/MTF),
             validity (DAY/IOC), price, order_type (MARKET/LIMIT/SL/SL-M),
             transaction_type (BUY/SELL), disclosed_quantity, trigger_price,
             is_amo, slice, market_protection, tag

Orders:     order_id, instrument_token, transaction_type, product, order_type,
            quantity, price, trigger_price, traded_quantity, average_price,
            status, tag, validity, exchange

Positions:  instrument_token, product, quantity, average_price, last_price,
            close_price, pnl, realised, unrealised, buy_quantity, sell_quantity,
            buy_price, sell_price, exchange

Holdings:   isin, company_name, product, quantity, average_price, last_price,
            close_price, pnl, t1_quantity, exchange

Auth:       Bearer ACCESS_TOKEN (OAuth 2.0)
Base URL:   https://api-hft.upstox.com/v2  (portfolio/positions/holdings)
Order URL:  https://api-hft.upstox.com/v3  (order placement)
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import requests

from broker.adapters.base import BrokerAdapter, Funds, Holding, Order, Position, Trade

logger = logging.getLogger("smart_trader.broker.upstox")

_BASE_V2 = "https://api-hft.upstox.com/v2"
_BASE_V3 = "https://api-hft.upstox.com/v3"

_UPSTOX_STATUS_MAP = {
    "complete": "COMPLETE",
    "traded": "COMPLETE",
    "rejected": "REJECTED",
    "cancelled": "CANCELLED",
    "open": "PENDING",
    "pending": "PENDING",
    "trigger pending": "PENDING",
    "not cancelled": "PENDING",
    "not modified": "PENDING",
    "after market order req received": "PENDING",
    "modify pending": "PENDING",
    "cancel pending": "PENDING",
    "validation pending": "PENDING",
    "put order req received": "PENDING",
}

# Upstox exchange segment ↔ Smart Trader mapping
_EXCH_TO_UPSTOX = {
    "NSE": "NSE_EQ", "NFO": "NSE_FO", "BSE": "BSE_EQ",
    "BFO": "BSE_FO", "MCX": "MCX_FO", "CDS": "NCD_FO",
}
_UPSTOX_TO_EXCH = {}
for _k, _v in _EXCH_TO_UPSTOX.items():
    _UPSTOX_TO_EXCH[_v] = _k
# Additional reverse mappings
_UPSTOX_TO_EXCH.update({
    "NSE_EQ": "NSE", "NSE_FO": "NFO", "BSE_EQ": "BSE",
    "BSE_FO": "BFO", "MCX_FO": "MCX", "NCD_FO": "CDS",
})


def _parse_exchange_from_token(instrument_token: str) -> str:
    """Extract exchange from Upstox instrument_token like 'NSE_EQ|INE848E01016'."""
    if "|" in instrument_token:
        seg = instrument_token.split("|")[0]
        return _UPSTOX_TO_EXCH.get(seg, "NSE")
    return "NSE"


class UpstoxAdapter(BrokerAdapter):
    """
    Adapter for Upstox API trading accounts.

    Lifecycle:
        1. Instantiate with credentials dict and a valid OAuth access token.
        2. Use get_positions(), place_order(), etc.
    """

    def __init__(
        self,
        creds: Dict[str, Any],
        access_token: str,
        account_id: str,
        client_id: str,
    ) -> None:
        super().__init__("upstox", account_id, client_id)
        self._creds = creds
        self._access_token = access_token
        self._connected = bool(access_token)

    # ── Internal: build request headers ──────────────────────────────────────

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _get(self, url: str) -> Any:
        try:
            resp = requests.get(url, headers=self._headers(), timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict) and data.get("status") == "error":
                logger.warning("Upstox API error: %s", data)
                return None
            return data
        except Exception as e:
            logger.error("Upstox GET %s failed: %s", url, e)
            return None

    def _post(self, url: str, payload: dict) -> Optional[dict]:
        try:
            resp = requests.post(
                url, json=payload,
                headers=self._headers(), timeout=15,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            body = {}
            try:
                body = e.response.json()
            except Exception:
                pass
            logger.error("Upstox POST %s HTTP error: %s body=%s", url, e, body)
            return body or {"status": "error"}
        except Exception as e:
            logger.error("Upstox POST %s failed: %s", url, e)
            return None

    def _put(self, url: str, payload: dict) -> Optional[dict]:
        try:
            resp = requests.put(
                url, json=payload,
                headers=self._headers(), timeout=15,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error("Upstox PUT %s failed: %s", url, e)
            return None

    def _delete(self, url: str, params: Optional[dict] = None) -> Optional[dict]:
        try:
            resp = requests.delete(
                url, headers=self._headers(), params=params, timeout=15,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error("Upstox DELETE %s failed: %s", url, e)
            return None

    # ── Connection ─────────────────────────────────────────────────────────────

    def is_connected(self) -> bool:
        return self._connected

    # ── Positions ──────────────────────────────────────────────────────────────

    def get_positions(self) -> List[Position]:
        if not self.is_connected():
            return []
        resp = self._get(f"{_BASE_V2}/portfolio/short-term-positions")
        if not isinstance(resp, dict) or resp.get("status") != "success":
            return []
        raw_list = (resp.get("data") or [])
        return [self._enrich(self._map_position(p)) for p in raw_list if p]

    @staticmethod
    def _map_position(raw: dict) -> Position:
        symbol = raw.get("trading_symbol") or raw.get("tradingsymbol", "")
        inst_token = raw.get("instrument_token", "")
        exchange = raw.get("exchange", "") or _parse_exchange_from_token(inst_token)
        prd_raw = (raw.get("product") or "").upper()
        # Upstox "D" = Delivery: CNC for equity, NRML for F&O/commodity
        if prd_raw == "D":
            product = "NRML" if exchange in ("NFO", "BFO", "MCX", "CDS") else "CNC"
        else:
            prod_map = {"I": "MIS", "MTF": "NRML",
                        "MIS": "MIS", "CNC": "CNC", "NRML": "NRML", "CO": "MIS"}
            product = prod_map.get(prd_raw, "MIS")
        qty = int(raw.get("quantity") or raw.get("net_quantity") or 0)
        avg_prc = float(raw.get("average_price") or 0)
        ltp = float(raw.get("last_price") or 0)
        rpnl = float(raw.get("realised") or 0)
        urmtom = float(raw.get("unrealised") or 0)
        buy_qty = int(raw.get("buy_quantity") or raw.get("day_buy_quantity") or 0)
        sell_qty = int(raw.get("sell_quantity") or raw.get("day_sell_quantity") or 0)

        itype = ""
        if symbol.endswith("CE"):       itype = "CE"
        elif symbol.endswith("PE"):     itype = "PE"
        elif "FUT" in symbol:           itype = "FUT"
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
        resp = self._get(f"{_BASE_V2}/order/retrieve-all")
        if not isinstance(resp, dict) or resp.get("status") != "success":
            return []
        raw_list = resp.get("data") or []
        return [self._enrich(self._map_order(o)) for o in raw_list if o]

    @staticmethod
    def _map_order(raw: dict) -> Order:
        order_id = str(raw.get("order_id") or "")
        symbol = raw.get("trading_symbol") or raw.get("tradingsymbol", "")
        inst_token = raw.get("instrument_token", "")
        exchange = raw.get("exchange", "") or _parse_exchange_from_token(inst_token)
        side = "BUY" if str(raw.get("transaction_type", "")).upper() == "BUY" else "SELL"
        prd_raw = (raw.get("product") or "").upper()
        # Upstox "D" = Delivery: CNC for equity, NRML for F&O/commodity
        if prd_raw == "D":
            product = "NRML" if exchange in ("NFO", "BFO", "MCX", "CDS") else "CNC"
        else:
            prod_map = {"I": "MIS", "MTF": "NRML", "CO": "MIS"}
            product = prod_map.get(prd_raw, "MIS")
        otype_raw = (raw.get("order_type") or "").upper()
        otype_map = {"MARKET": "MARKET", "LIMIT": "LIMIT", "SL": "SL", "SL-M": "SL-M"}
        order_type = otype_map.get(otype_raw, "MARKET")
        qty = int(raw.get("quantity") or 0)
        price = float(raw.get("price") or 0)
        trgprc = float(raw.get("trigger_price") or 0)
        status_raw = str(raw.get("status") or "pending").lower()
        status = _UPSTOX_STATUS_MAP.get(status_raw, "PENDING")
        filled = int(raw.get("traded_quantity") or raw.get("filled_quantity") or 0)
        avg_prc = float(raw.get("average_price") or 0)
        ts = raw.get("order_timestamp") or raw.get("exchange_timestamp") or ""
        remarks = raw.get("status_message") or ""

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
        resp = self._get(f"{_BASE_V2}/user/get-fund-and-margin")
        if not isinstance(resp, dict) or resp.get("status") != "success":
            return self._enrich(Funds(available_cash=0.0))
        data = resp.get("data") or {}
        # Upstox returns {equity: {...}, commodity: {...}}
        eq = data.get("equity") or {}
        available = float(eq.get("available_margin") or 0)
        used = float(eq.get("used_margin") or 0)
        return self._enrich(Funds(
            available_cash=available,
            used_margin=used,
            total_balance=available + used,
        ))

    # ── Holdings ───────────────────────────────────────────────────────────────

    def get_holdings(self) -> List[Holding]:
        if not self.is_connected():
            return []
        resp = self._get(f"{_BASE_V2}/portfolio/long-term-holdings")
        if not isinstance(resp, dict) or resp.get("status") != "success":
            return []
        raw_list = resp.get("data") or []
        return [self._enrich(self._map_holding(h)) for h in raw_list if h]

    @staticmethod
    def _map_holding(raw: dict) -> Holding:
        symbol = raw.get("trading_symbol") or raw.get("tradingsymbol", "")
        exchange = raw.get("exchange", "NSE")
        qty = int(raw.get("quantity") or 0)
        avg_prc = float(raw.get("average_price") or 0)
        ltp = float(raw.get("last_price") or 0)
        pnl = float(raw.get("pnl") or 0)
        pnl_pct = (pnl / (avg_prc * qty) * 100) if avg_prc and qty else 0.0
        t1 = int(raw.get("t1_quantity") or 0)
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
        resp = self._get(f"{_BASE_V2}/order/trades/get-trades-for-day")
        if not isinstance(resp, dict) or resp.get("status") != "success":
            return []
        raw_list = resp.get("data") or []
        return [self._enrich(self._map_trade(t)) for t in raw_list if t]

    @staticmethod
    def _map_trade(raw: dict) -> Trade:
        side = "BUY" if str(raw.get("transaction_type", "")).upper() == "BUY" else "SELL"
        exchange = raw.get("exchange", "")
        prd_raw = (raw.get("product") or "").upper()
        # Upstox "D" = Delivery: CNC for equity, NRML for F&O/commodity
        if prd_raw == "D":
            product = "NRML" if exchange in ("NFO", "BFO", "MCX", "CDS") else "CNC"
        else:
            prod_map = {"I": "MIS", "MTF": "NRML", "CO": "MIS"}
            product = prod_map.get(prd_raw, prd_raw)
        return Trade(
            trade_id=str(raw.get("trade_id") or ""),
            order_id=str(raw.get("order_id") or ""),
            symbol=raw.get("trading_symbol") or raw.get("tradingsymbol", ""),
            exchange=raw.get("exchange", ""),
            side=side,
            product=prod_map.get(prd_raw, prd_raw),
            qty=int(raw.get("traded_quantity") or raw.get("quantity") or 0),
            price=float(raw.get("average_price") or 0),
            timestamp=raw.get("trade_timestamp") or raw.get("exchange_timestamp") or "",
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

            prd_map = {"MIS": "I", "NRML": "D", "CNC": "D"}
            otype_map = {"MARKET": "MARKET", "LIMIT": "LIMIT", "SL": "SL", "SL-M": "SL-M"}

            prd_raw = str(order.get("product", "MIS")).upper()
            otype_raw = str(order.get("order_type", "MARKET")).upper()

            # Upstox uses instrument_token (format: "NSE_EQ|INE848E01016")
            inst_token = order.get("instrument_token") or order.get("token") or ""
            if not inst_token:
                # Construct from exchange and symbol as fallback
                seg = _EXCH_TO_UPSTOX.get(exch, "NSE_EQ")
                inst_token = f"{seg}|{sym}"

            upstox_order = {
                "quantity": int(order.get("qty", 0)),
                "product": prd_map.get(prd_raw, "I"),
                "validity": "DAY",
                "price": float(order.get("price", 0) or 0),
                "tag": order.get("tag", "smart_trader"),
                "instrument_token": inst_token,
                "order_type": otype_map.get(otype_raw, "MARKET"),
                "transaction_type": txn_type,
                "disclosed_quantity": 0,
                "trigger_price": float(order.get("trigger_price", 0) or 0),
                "is_amo": False,
                "slice": False,
            }

            result = self._post(f"{_BASE_V3}/order/place", upstox_order)
            if result and result.get("status") == "success":
                data = result.get("data") or {}
                order_ids = data.get("order_ids") or data.get("order_id")
                oid = order_ids[0] if isinstance(order_ids, list) else str(order_ids or "")
                return {
                    "success": True,
                    "order_id": oid,
                    "message": "Order placed",
                }
            msg = (result or {}).get("message") or (result or {}).get("errors", "Order failed")
            return {"success": False, "message": str(msg)}
        except Exception as e:
            logger.exception("place_order error: %s", e)
            return {"success": False, "message": str(e)}

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        if not self.is_connected():
            return {"success": False, "message": "Not connected"}
        try:
            result = self._delete(
                f"{_BASE_V3}/order/cancel",
                params={"order_id": order_id},
            )
            if result and result.get("status") == "success":
                return {"success": True, "message": "Order cancelled"}
            return {"success": False, "message": (result or {}).get("message", "Cancel failed")}
        except Exception as e:
            logger.error("cancel_order error: %s", e)
            return {"success": False, "message": str(e)}

    def modify_order(self, order_id: str, modifications: Dict[str, Any]) -> Dict[str, Any]:
        if not self.is_connected():
            return {"success": False, "message": "Not connected"}
        try:
            otype_map = {"MARKET": "MARKET", "LIMIT": "LIMIT", "SL": "SL", "SL-M": "SL-M"}
            otype_raw = str(modifications.get("order_type", "LIMIT")).upper()

            modify_data = {
                "order_id": order_id,
                "order_type": otype_map.get(otype_raw, "LIMIT"),
                "validity": "DAY",
                "quantity": int(modifications.get("qty", 0)) if modifications.get("qty") else 0,
                "price": float(modifications.get("price", 0) or 0),
                "trigger_price": float(modifications.get("trigger_price", 0) or 0),
                "disclosed_quantity": 0,
            }

            result = self._put(f"{_BASE_V3}/order/modify", modify_data)
            if result and result.get("status") == "success":
                return {"success": True, "message": "Order modified"}
            return {"success": False, "message": (result or {}).get("message", "Modify failed")}
        except Exception as e:
            logger.error("modify_order error: %s", e)
            return {"success": False, "message": str(e)}
