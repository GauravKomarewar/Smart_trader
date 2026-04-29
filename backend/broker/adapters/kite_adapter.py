"""
Smart Trader — Zerodha Kite Connect v3 Broker Adapter
======================================================
Wraps Kite Connect REST API v3 and translates responses
to normalised Smart Trader models.

Kite Connect field reference
----------------------------
Orders:     order_id, tradingsymbol, exchange, transaction_type, order_type,
            product, quantity, price, trigger_price, status, filled_quantity,
            average_price, pending_quantity, status_message, variety, tag

Positions:  tradingsymbol, exchange, instrument_token, product, quantity,
            overnight_quantity, multiplier, average_price, close_price,
            last_price, value, pnl, m2m, unrealised, realised,
            buy_quantity, sell_quantity, buy_price, sell_price

Holdings:   tradingsymbol, exchange, instrument_token, isin, product,
            quantity, average_price, last_price, close_price, pnl,
            day_change, t1_quantity, realised_quantity, used_quantity

Auth:       Authorization: token api_key:access_token
            X-Kite-Version: 3
Base URL:   https://api.kite.trade
Exchanges:  NSE, BSE, NFO, CDS, BCD, MCX
Products:   CNC, NRML, MIS, MTF  (same as Smart Trader canonical!)
OrderTypes: MARKET, LIMIT, SL, SL-M  (same as Smart Trader canonical!)
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import requests

from broker.adapters.base import BrokerAdapter, Funds, Holding, Order, Position, Trade

logger = logging.getLogger("smart_trader.broker.kite")

_BASE_URL = "https://api.kite.trade"

_KITE_STATUS_MAP = {
    "COMPLETE": "COMPLETE",
    "REJECTED": "REJECTED",
    "CANCELLED": "CANCELLED",
    "OPEN": "PENDING",
    "PENDING": "PENDING",
    "TRIGGER PENDING": "PENDING",
    "OPEN PENDING": "PENDING",
    "VALIDATION PENDING": "PENDING",
    "PUT ORDER REQ RECEIVED": "PENDING",
    "MODIFY VALIDATION PENDING": "PENDING",
    "MODIFY PENDING": "PENDING",
    "CANCEL PENDING": "PENDING",
    "AMO REQ RECEIVED": "PENDING",
    "UPDATE VALIDATION PENDING": "PENDING",
}


class KiteAdapter(BrokerAdapter):
    """
    Adapter for Zerodha Kite Connect v3 trading accounts.

    Kite Connect products and order types map 1:1 with Smart Trader's
    canonical format (CNC/NRML/MIS, MARKET/LIMIT/SL/SL-M).

    Lifecycle:
        1. Instantiate with credentials dict, api_key and access_token.
        2. Use get_positions(), place_order(), etc.
    """

    def __init__(
        self,
        creds: Dict[str, Any],
        api_key: str,
        access_token: str,
        account_id: str,
        client_id: str,
    ) -> None:
        super().__init__("zerodha", account_id, client_id)
        self._creds = creds
        self._api_key = api_key
        self._access_token = access_token
        self._connected = bool(api_key and access_token)

    # ── Internal: build request headers ──────────────────────────────────────

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"token {self._api_key}:{self._access_token}",
            "X-Kite-Version": "3",
            "Content-Type": "application/x-www-form-urlencoded",
        }

    def _json_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"token {self._api_key}:{self._access_token}",
            "X-Kite-Version": "3",
            "Accept": "application/json",
        }

    def _get(self, path: str) -> Any:
        try:
            resp = requests.get(
                f"{_BASE_URL}{path}", headers=self._json_headers(), timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict) and data.get("status") == "error":
                logger.warning("Kite API error %s: %s", path, data.get("message"))
                return None
            return data
        except Exception as e:
            logger.error("Kite GET %s failed: %s", path, e)
            return None

    def _post(self, path: str, payload: dict) -> Optional[dict]:
        """POST with form-encoded data (Kite uses form encoding for orders)."""
        try:
            resp = requests.post(
                f"{_BASE_URL}{path}", data=payload,
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
            logger.error("Kite POST %s HTTP error: %s body=%s", path, e, body)
            return body or {"status": "error"}
        except Exception as e:
            logger.error("Kite POST %s failed: %s", path, e)
            return None

    def _put(self, path: str, payload: dict) -> Optional[dict]:
        try:
            resp = requests.put(
                f"{_BASE_URL}{path}", data=payload,
                headers=self._headers(), timeout=15,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error("Kite PUT %s failed: %s", path, e)
            return None

    def _delete(self, path: str) -> Optional[dict]:
        try:
            resp = requests.delete(
                f"{_BASE_URL}{path}", headers=self._json_headers(), timeout=15,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error("Kite DELETE %s failed: %s", path, e)
            return None

    # ── Connection ─────────────────────────────────────────────────────────────

    def is_connected(self) -> bool:
        return self._connected

    # ── Positions ──────────────────────────────────────────────────────────────

    def get_positions(self) -> List[Position]:
        if not self.is_connected():
            return []
        resp = self._get("/portfolio/positions")
        if not isinstance(resp, dict) or resp.get("status") != "success":
            return []
        data = resp.get("data") or {}
        # Kite returns {net: [...], day: [...]} — use net
        raw_list = data.get("net") or []
        return [self._enrich(self._map_position(p)) for p in raw_list if p]

    @staticmethod
    def _map_position(raw: dict) -> Position:
        symbol = raw.get("tradingsymbol", "")
        exchange = raw.get("exchange", "NSE")
        product = (raw.get("product") or "MIS").upper()
        qty = int(raw.get("quantity") or 0)
        avg_prc = float(raw.get("average_price") or 0)
        ltp = float(raw.get("last_price") or 0)
        rpnl = float(raw.get("realised") or 0)
        urmtom = float(raw.get("unrealised") or 0)
        buy_qty = int(raw.get("buy_quantity") or 0)
        sell_qty = int(raw.get("sell_quantity") or 0)

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
        resp = self._get("/orders")
        if not isinstance(resp, dict) or resp.get("status") != "success":
            return []
        raw_list = resp.get("data") or []
        return [self._enrich(self._map_order(o)) for o in raw_list if o]

    @staticmethod
    def _map_order(raw: dict) -> Order:
        order_id = str(raw.get("order_id") or "")
        symbol = raw.get("tradingsymbol", "")
        exchange = raw.get("exchange", "")
        side = "BUY" if str(raw.get("transaction_type", "")).upper() == "BUY" else "SELL"
        # Kite products and order types are already canonical
        product = (raw.get("product") or "MIS").upper()
        order_type = (raw.get("order_type") or "MARKET").upper()
        qty = int(raw.get("quantity") or 0)
        price = float(raw.get("price") or 0)
        trgprc = float(raw.get("trigger_price") or 0)
        status_raw = str(raw.get("status") or "PENDING").upper()
        status = _KITE_STATUS_MAP.get(status_raw, "PENDING")
        filled = int(raw.get("filled_quantity") or 0)
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
        resp = self._get("/user/margins")
        if not isinstance(resp, dict) or resp.get("status") != "success":
            return self._enrich(Funds(available_cash=0.0))
        data = resp.get("data") or {}
        # Kite returns {equity: {...}, commodity: {...}} — combine both
        available = used = collateral = m2m_r = m2m_u = 0.0
        for segment in ("equity", "commodity"):
            seg = data.get(segment) or {}
            if not seg.get("enabled", False):
                continue
            seg_avail = seg.get("available") or {}
            seg_used = seg.get("utilised") or {}
            available += float(seg_avail.get("live_balance") or seg_avail.get("cash") or 0)
            used += float(seg_used.get("debits") or 0)
            collateral += float(seg_avail.get("collateral") or 0)
            m2m_r += float(seg_used.get("m2m_realised") or 0)
            m2m_u += float(seg_used.get("m2m_unrealised") or 0)
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
        resp = self._get("/portfolio/holdings")
        if not isinstance(resp, dict) or resp.get("status") != "success":
            return []
        raw_list = resp.get("data") or []
        return [self._enrich(self._map_holding(h)) for h in raw_list if h]

    @staticmethod
    def _map_holding(raw: dict) -> Holding:
        symbol = raw.get("tradingsymbol", "")
        exchange = raw.get("exchange", "NSE")
        qty = int(raw.get("quantity") or 0)
        avg_prc = float(raw.get("average_price") or 0)
        ltp = float(raw.get("last_price") or 0)
        pnl = float(raw.get("pnl") or 0)
        pnl_pct = (pnl / (avg_prc * qty) * 100) if avg_prc and qty else 0.0
        t1 = int(raw.get("t1_quantity") or 0)
        dp_qty = int(raw.get("realised_quantity") or qty)
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
        resp = self._get("/trades")
        if not isinstance(resp, dict) or resp.get("status") != "success":
            return []
        raw_list = resp.get("data") or []
        return [self._enrich(self._map_trade(t)) for t in raw_list if t]

    @staticmethod
    def _map_trade(raw: dict) -> Trade:
        side = "BUY" if str(raw.get("transaction_type", "")).upper() == "BUY" else "SELL"
        product = (raw.get("product") or "").upper()
        return Trade(
            trade_id=str(raw.get("trade_id") or ""),
            order_id=str(raw.get("order_id") or ""),
            symbol=raw.get("tradingsymbol", ""),
            exchange=raw.get("exchange", ""),
            side=side,
            product=product,
            qty=int(raw.get("quantity") or raw.get("filled_quantity") or 0),
            price=float(raw.get("average_price") or 0),
            timestamp=raw.get("fill_timestamp") or raw.get("exchange_timestamp") or "",
        )

    # ── Order management ───────────────────────────────────────────────────────

    def place_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        if not self.is_connected():
            return {"success": False, "message": "Not connected"}
        try:
            sym = order.get("symbol", "")
            exch = order.get("exchange", "NSE")
            if ":" in sym:
                sym = sym.split(":", 1)[1]
            side_str = str(order.get("side", "BUY")).upper()
            txn_type = "BUY" if side_str in ("BUY", "B") else "SELL"

            # Kite products/order_types are already canonical — pass through
            product = str(order.get("product", "MIS")).upper()
            order_type = str(order.get("order_type", "MARKET")).upper()

            # Resolve Kite instrument token and correct exchange from symbols DB
            try:
                from db.symbols_db import resolve_broker_symbol as _gbs
                resolved = _gbs(sym, "kite", exch)
                if resolved["symbol"]:
                    logger.debug("Kite token resolved: %s → %s", sym, resolved["symbol"])
                if resolved["exchange"]:
                    exch = resolved["exchange"]
            except Exception as e:
                logger.warning("Kite symbol lookup failed for %s: %s", sym, e)

            variety = "regular"  # regular/amo/co/iceberg/auction

            kite_order = {
                "tradingsymbol": sym,
                "exchange": exch,
                "transaction_type": txn_type,
                "order_type": order_type,
                "quantity": int(order.get("qty", 0)),
                "product": product,
                "price": float(order.get("price", 0) or 0),
                "trigger_price": float(order.get("trigger_price", 0) or 0),
                "validity": "DAY",
                "disclosed_quantity": 0,
                "tag": order.get("tag", "smart_trader"),
            }

            result = self._post(f"/orders/{variety}", kite_order)
            if result and result.get("status") == "success":
                oid = (result.get("data") or {}).get("order_id", "")
                return {
                    "success": True,
                    "order_id": str(oid),
                    "message": "Order placed",
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
            variety = "regular"
            result = self._delete(f"/orders/{variety}/{order_id}")
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
            variety = "regular"
            order_type = str(modifications.get("order_type", "LIMIT")).upper()

            modify_data = {
                "order_type": order_type,
                "validity": "DAY",
            }
            if modifications.get("qty"):
                modify_data["quantity"] = int(modifications["qty"])
            if modifications.get("price") is not None:
                modify_data["price"] = float(modifications["price"])
            if modifications.get("trigger_price") is not None:
                modify_data["trigger_price"] = float(modifications["trigger_price"])

            result = self._put(f"/orders/{variety}/{order_id}", modify_data)
            if result and result.get("status") == "success":
                return {"success": True, "message": "Order modified"}
            return {"success": False, "message": (result or {}).get("message", "Modify failed")}
        except Exception as e:
            logger.error("modify_order error: %s", e)
            return {"success": False, "message": str(e)}
