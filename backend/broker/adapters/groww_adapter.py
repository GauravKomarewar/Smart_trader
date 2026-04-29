"""
Smart Trader — Groww Broker Adapter
=====================================
Wraps Groww Trade API (REST) and translates responses
to normalised Smart Trader models.

Groww field reference
---------------------
Positions: trading_symbol, segment, credit_quantity, credit_price,
           debit_quantity, debit_price, carry_forward_credit_quantity,
           carry_forward_credit_price, carry_forward_debit_quantity,
           carry_forward_debit_price, exchange, symbol_isin, quantity,
           product, net_carry_forward_quantity, net_price,
           net_carry_forward_price, realised_pnl

Orders:    groww_order_id, trading_symbol, order_status, quantity, price,
           trigger_price, filled_quantity, remaining_quantity,
           average_fill_price, validity, exchange, order_type,
           transaction_type, segment, product, created_at

Holdings:  isin, trading_symbol, quantity, average_price, pledge_quantity,
           demat_locked_quantity, t1_quantity, demat_free_quantity

Auth:      Bearer ACCESS_TOKEN  (from profile page or API key + checksum/TOTP)
Base URL:  https://api.groww.in
Segments:  CASH, FNO, COMMODITY
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import requests

from broker.adapters.base import BrokerAdapter, Funds, Holding, Order, Position, Trade

logger = logging.getLogger("smart_trader.broker.groww")

_BASE_URL = "https://api.groww.in"

_GROWW_STATUS_MAP = {
    "COMPLETE": "COMPLETE",
    "EXECUTED": "COMPLETE",
    "OPEN": "PENDING",
    "PENDING": "PENDING",
    "TRIGGER_PENDING": "PENDING",
    "AMO_PENDING": "PENDING",
    "CANCELLED": "CANCELLED",
    "REJECTED": "REJECTED",
    "EXPIRED": "CANCELLED",
}


class GrowwAdapter(BrokerAdapter):
    """
    Adapter for Groww Trade API accounts.

    Lifecycle:
        1. Instantiate with credentials dict and a valid access token.
        2. Use get_positions(), place_order(), etc.
    """

    def __init__(
        self,
        creds: Dict[str, Any],
        access_token: str,
        account_id: str,
        client_id: str,
    ) -> None:
        super().__init__("groww", account_id, client_id)
        self._creds = creds
        self._access_token = access_token
        self._connected = bool(access_token)

    # ── Internal: build request headers ──────────────────────────────────────

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-API-VERSION": "1.0",
        }

    def _get(self, path: str, params: Optional[dict] = None) -> Any:
        try:
            resp = requests.get(
                f"{_BASE_URL}{path}", headers=self._headers(),
                params=params, timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict) and data.get("status") == "FAILURE":
                logger.warning("Groww API error %s: %s", path, data.get("error"))
                return None
            return data
        except Exception as e:
            logger.error("Groww GET %s failed: %s", path, e)
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
            body = {}
            try:
                body = e.response.json()
            except Exception:
                pass
            logger.error("Groww POST %s HTTP error: %s body=%s", path, e, body)
            return body or {"status": "FAILURE"}
        except Exception as e:
            logger.error("Groww POST %s failed: %s", path, e)
            return None

    # ── Connection ─────────────────────────────────────────────────────────────

    def is_connected(self) -> bool:
        return self._connected

    # ── Positions ──────────────────────────────────────────────────────────────

    def get_positions(self) -> List[Position]:
        if not self.is_connected():
            return []
        resp = self._get("/v1/positions/user")
        if not isinstance(resp, dict) or resp.get("status") != "SUCCESS":
            return []
        raw_list = (resp.get("payload") or {}).get("positions", [])
        return [self._enrich(self._map_position(p)) for p in raw_list if p]

    @staticmethod
    def _map_position(raw: dict) -> Position:
        symbol = raw.get("trading_symbol", "")
        exchange = raw.get("exchange", "NSE")
        prd_raw = (raw.get("product") or "").upper()
        prod_map = {"CNC": "CNC", "INTRADAY": "MIS", "MIS": "MIS",
                    "NRML": "NRML", "MARGIN": "NRML"}
        product = prod_map.get(prd_raw, "MIS")
        qty = int(raw.get("quantity") or 0)
        avg_prc = float(raw.get("net_price") or 0)
        credit_qty = int(raw.get("credit_quantity") or 0)
        debit_qty = int(raw.get("debit_quantity") or 0)
        rpnl = float(raw.get("realised_pnl") or 0)

        itype = ""
        if symbol.endswith("CE"):       itype = "CE"
        elif symbol.endswith("PE"):     itype = "PE"
        elif "FUT" in symbol:           itype = "FUT"
        else:                           itype = "EQ"

        return Position(
            symbol=symbol, exchange=exchange, product=product,
            qty=qty, avg_price=avg_prc, ltp=0.0,
            pnl=rpnl, realised_pnl=rpnl, unrealised_pnl=0.0,
            buy_qty=credit_qty, sell_qty=debit_qty, instrument_type=itype,
        )

    # ── Order book ─────────────────────────────────────────────────────────────

    def get_order_book(self) -> List[Order]:
        if not self.is_connected():
            return []
        resp = self._get("/v1/order/list", params={"page": 0, "page_size": 100})
        if not isinstance(resp, dict) or resp.get("status") != "SUCCESS":
            return []
        raw_list = (resp.get("payload") or {}).get("order_list", [])
        return [self._enrich(self._map_order(o)) for o in raw_list if o]

    @staticmethod
    def _map_order(raw: dict) -> Order:
        order_id = str(raw.get("groww_order_id") or "")
        symbol = raw.get("trading_symbol", "")
        exchange = raw.get("exchange", "NSE")
        side = "BUY" if str(raw.get("transaction_type", "")).upper() == "BUY" else "SELL"
        prd_raw = (raw.get("product") or "").upper()
        prod_map = {"CNC": "CNC", "INTRADAY": "MIS", "MIS": "MIS", "NRML": "NRML"}
        product = prod_map.get(prd_raw, "MIS")
        otype_raw = (raw.get("order_type") or "").upper()
        otype_map = {"MARKET": "MARKET", "LIMIT": "LIMIT", "SL": "SL", "SL-M": "SL-M"}
        order_type = otype_map.get(otype_raw, "MARKET")
        qty = int(raw.get("quantity") or 0)
        price = float(raw.get("price") or 0)
        trgprc = float(raw.get("trigger_price") or 0)
        status_raw = str(raw.get("order_status") or "PENDING").upper()
        status = _GROWW_STATUS_MAP.get(status_raw, "PENDING")
        filled = int(raw.get("filled_quantity") or 0)
        avg_prc = float(raw.get("average_fill_price") or 0)
        ts = raw.get("created_at") or raw.get("exchange_time") or ""
        remarks = raw.get("remark") or ""

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
        resp = self._get("/v1/margin/available")
        if not isinstance(resp, dict) or resp.get("status") != "SUCCESS":
            return self._enrich(Funds(available_cash=0.0))
        d = (resp.get("payload") or {})
        available = float(d.get("available_margin") or d.get("cash") or 0)
        used = float(d.get("used_margin") or 0)
        return self._enrich(Funds(
            available_cash=available,
            used_margin=used,
            total_balance=available + used,
        ))

    # ── Holdings ───────────────────────────────────────────────────────────────

    def get_holdings(self) -> List[Holding]:
        if not self.is_connected():
            return []
        resp = self._get("/v1/holdings/user")
        if not isinstance(resp, dict) or resp.get("status") != "SUCCESS":
            return []
        raw_list = (resp.get("payload") or {}).get("holdings", [])
        return [self._enrich(self._map_holding(h)) for h in raw_list if h]

    @staticmethod
    def _map_holding(raw: dict) -> Holding:
        symbol = raw.get("trading_symbol", "")
        qty = int(raw.get("quantity") or 0)
        avg_prc = float(raw.get("average_price") or 0)
        t1 = int(raw.get("t1_quantity") or 0)
        dp_qty = int(raw.get("demat_free_quantity") or qty)
        isin = raw.get("isin") or ""
        return Holding(
            symbol=symbol, exchange="NSE", qty=qty, avg_price=avg_prc,
            ltp=0.0, pnl=0.0, pnl_pct=0.0, isin=isin,
            dp_qty=dp_qty, t1_qty=t1,
        )

    # ── Tradebook ──────────────────────────────────────────────────────────────

    def get_tradebook(self) -> List[Trade]:
        # Groww requires groww_order_id per trade; collect from order book
        if not self.is_connected():
            return []
        return []  # Groww doesn't have a single tradebook endpoint

    # ── Order management ───────────────────────────────────────────────────────

    def place_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        if not self.is_connected():
            return {"success": False, "message": "Not connected"}
        try:
            sym = order.get("symbol", "")
            exch = order.get("exchange", "NSE")

            side_str = str(order.get("side", "BUY")).upper()
            txn_type = "BUY" if side_str in ("BUY", "B") else "SELL"

            prd_map = {"MIS": "INTRADAY", "NRML": "NRML", "CNC": "CNC"}
            otype_map = {"MARKET": "MARKET", "LIMIT": "LIMIT", "SL": "SL", "SL-M": "SL-M"}

            prd_raw = str(order.get("product", "MIS")).upper()
            otype_raw = str(order.get("order_type", "MARKET")).upper()

            # Determine segment from exchange
            seg_map = {"NSE": "CASH", "BSE": "CASH", "NFO": "FNO",
                       "BFO": "FNO", "MCX": "COMMODITY", "CDS": "FNO"}

            import uuid
            order_ref = f"ST-{uuid.uuid4().hex[:16]}"

            groww_order = {
                "trading_symbol": sym,
                "quantity": int(order.get("qty", 0)),
                "price": float(order.get("price", 0) or 0),
                "trigger_price": float(order.get("trigger_price", 0) or 0),
                "validity": "DAY",
                "exchange": exch if exch in ("NSE", "BSE") else "NSE",
                "segment": seg_map.get(exch, "CASH"),
                "product": prd_map.get(prd_raw, "CNC"),
                "order_type": otype_map.get(otype_raw, "MARKET"),
                "transaction_type": txn_type,
                "order_reference_id": order_ref,
            }

            result = self._post("/v1/order/create", groww_order)
            if result and result.get("status") == "SUCCESS":
                payload = result.get("payload") or {}
                oid = payload.get("groww_order_id", "")
                return {
                    "success": True,
                    "order_id": oid,
                    "message": payload.get("remark", "Order placed"),
                }
            err = (result or {}).get("error") or {}
            msg = err.get("message") if isinstance(err, dict) else str(result)
            return {"success": False, "message": msg or "Order failed"}
        except Exception as e:
            logger.exception("place_order error: %s", e)
            return {"success": False, "message": str(e)}

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        if not self.is_connected():
            return {"success": False, "message": "Not connected"}
        try:
            # Segment is required by Groww cancel API. Infer from order book.
            segment = "CASH"
            try:
                book = self.get_order_book()
                for o in book:
                    if str(getattr(o, "order_id", "")) == str(order_id):
                        exch = getattr(o, "exchange", "NSE")
                        seg_map = {"NSE": "CASH", "BSE": "CASH", "NFO": "FNO",
                                   "BFO": "FNO", "MCX": "COMMODITY", "CDS": "FNO"}
                        segment = seg_map.get(exch, "CASH")
                        break
            except Exception:
                pass
            result = self._post("/v1/order/cancel", {
                "groww_order_id": order_id,
                "segment": segment,
            })
            if result and result.get("status") == "SUCCESS":
                return {"success": True, "message": "Order cancelled"}
            msg = ((result or {}).get("error") or {}).get("message", "Cancel failed")
            return {"success": False, "message": msg}
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
                "groww_order_id": order_id,
                "segment": modifications.get("segment", "CASH"),
                "order_type": otype_map.get(otype_raw, "LIMIT"),
                "quantity": int(modifications.get("qty", 0)) if modifications.get("qty") else None,
                "price": float(modifications.get("price", 0) or 0),
                "trigger_price": float(modifications.get("trigger_price", 0) or 0),
            }
            # Remove None values
            modify_data = {k: v for k, v in modify_data.items() if v is not None}

            result = self._post("/v1/order/modify", modify_data)
            if result and result.get("status") == "SUCCESS":
                return {"success": True, "message": "Order modified"}
            msg = ((result or {}).get("error") or {}).get("message", "Modify failed")
            return {"success": False, "message": msg}
        except Exception as e:
            logger.error("modify_order error: %s", e)
            return {"success": False, "message": str(e)}
