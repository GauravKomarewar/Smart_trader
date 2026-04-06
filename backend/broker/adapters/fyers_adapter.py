"""
Smart Trader — Fyers Broker Adapter
=====================================
Wraps fyers_apiv3 FyersModel and translates responses
to normalised Smart Trader models.

Fyers field reference
---------------------
Positions: symbol (NSE:X), netQty, productType, buyAvg, sellAvg,
           buyQty, sellQty, realized_profit, unrealized_profit

Orders:    id, symbol, qty, limitPrice, stopPrice, side (1=BUY/−1=SELL),
           productType, status (int: 2=COMPLETE, 4=CANCEL, 7=REJECT, else PENDING)

Holdings:  symbol, isin, holdingType, quantity, remainingQuantity, pl,
           costPrice (avg buy price), ltp

Funds:     fund_limit list: [{title, equityAmount}]
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from broker.adapters.base import BrokerAdapter, Funds, Holding, Order, Position, Trade

logger = logging.getLogger("smart_trader.broker.fyers")


_FYERS_STATUS_MAP = {1: "PENDING", 2: "COMPLETE", 4: "CANCELLED",
                     5: "CANCELLED", 6: "PENDING", 7: "REJECTED"}


class FyersAdapter(BrokerAdapter):
    """
    Adapter for Fyers (API v3) trading accounts.

    Lifecycle:
        1. Instantiate with credentials dict and a valid access token.
        2. Call connect() to authenticate.
        3. Use get_positions(), place_order(), etc.
    """

    def __init__(
        self,
        creds: Dict[str, Any],
        token: str,
        account_id: str,
        client_id: str,
    ) -> None:
        """
        Args:
            creds:      Decrypted broker credentials dict with keys:
                        CLIENT_ID, APP_ID, SECRET_KEY, TOTP_KEY, PIN
            token:      Valid Fyers access token (already authenticated)
            account_id: BrokerConfig.id (UUID)
            client_id:  Fyers client ID (e.g. XY1234)
        """
        super().__init__("fyers", account_id, client_id)
        self._creds  = creds
        self._token  = token
        self._client: Optional[Any] = None   # FyersBrokerClient instance
        self._connected = False

        if token:
            self._build_client(token)

    # ── Internal: build the Fyers client ─────────────────────────────────────

    def _build_client(self, token: str) -> None:
        """Build a FyersModel client with a pre-acquired token."""
        try:
            from fyers_apiv3.fyersModel import FyersModel
        except ImportError as e:
            logger.error("fyers_apiv3 not installed: %s", e)
            return

        app_id    = self._creds.get("APP_ID", "")
        client_id = self._creds.get("CLIENT_ID", "")

        if not all([client_id, app_id]):
            logger.error("Missing required Fyers credentials (CLIENT_ID / APP_ID)")
            return

        # Token may arrive as "APP_ID:raw_token" or just "raw_token"
        if ":" in token:
            raw_access_token = token.split(":", 1)[1]
        else:
            raw_access_token = token

        try:
            self._client = FyersModel(
                client_id=app_id,
                token=raw_access_token,
                log_level="ERROR",
            )
            self._connected = True
            logger.info("Fyers client initialised for client=%s", client_id)
        except Exception as e:
            logger.error("FyersModel init failed: %s", e)

    # _inject_token removed — _build_client now creates FyersModel directly

    # ── Connection ─────────────────────────────────────────────────────────────

    def is_connected(self) -> bool:
        return self._connected and self._client is not None

    # ── Positions ──────────────────────────────────────────────────────────────

    def get_positions(self) -> List[Position]:
        if not self.is_connected():
            return []
        assert self._client is not None  # guaranteed by is_connected()
        try:
            resp = self._client.positions()
            if not isinstance(resp, dict) or resp.get("s") != "ok":
                return []
            raw_list = resp.get("netPositions") or []
            return [self._enrich(self._map_position(p)) for p in raw_list]
        except Exception as e:
            logger.error("get_positions error: %s", e)
            return []

    @staticmethod
    def _map_position(raw: dict) -> Position:
        # FyersBrokerClient._normalise_position already adds tsym/netqty/pnl aliases
        symbol    = raw.get("symbol", "")
        # Strip exchange prefix if present
        clean_sym = symbol.split(":", 1)[-1] if ":" in symbol else symbol
        exchange  = raw.get("exchange") or raw.get("exch") or (symbol.split(":")[0] if ":" in symbol else "NSE")
        prd_raw   = (raw.get("prd") or raw.get("productType") or "").upper()
        prod_map  = {"INTRADAY": "MIS", "MARGIN": "NRML", "CNC": "CNC", "MIS": "MIS", "NRML": "NRML"}
        product   = prod_map.get(prd_raw, "MIS")
        qty       = int(raw.get("qty") or raw.get("netqty") or raw.get("netQty") or 0)
        buy_avg   = float(raw.get("buy_avg") or raw.get("buyAvg") or 0)
        sell_avg  = float(raw.get("sellAvg") or 0)
        avg_prc   = buy_avg if qty >= 0 else sell_avg
        ltp       = float(raw.get("ltp") or raw.get("last_price") or 0)
        rpnl      = float(raw.get("realised_pnl") or raw.get("rpnl") or raw.get("realized_profit") or 0)
        urmtom    = float(raw.get("unrealised_pnl") or raw.get("urmtom") or raw.get("unrealized_profit") or 0)
        buy_qty   = int(raw.get("day_buy_qty") or raw.get("buyQty") or 0)
        sell_qty  = int(raw.get("day_sell_qty") or raw.get("sellQty") or 0)

        itype = ""
        if clean_sym.endswith("CE"):     itype = "CE"
        elif clean_sym.endswith("PE"):   itype = "PE"
        elif "FUT" in clean_sym:         itype = "FUT"
        elif "INDEX" in symbol.upper():  itype = "IDX"
        else:                            itype = "EQ"

        return Position(
            symbol=clean_sym, exchange=exchange, product=product,
            qty=qty, avg_price=avg_prc, ltp=ltp,
            pnl=rpnl + urmtom, realised_pnl=rpnl, unrealised_pnl=urmtom,
            buy_qty=buy_qty, sell_qty=sell_qty, instrument_type=itype,
        )

    # ── Order book ─────────────────────────────────────────────────────────────

    def get_order_book(self) -> List[Order]:
        if not self.is_connected():
            return []
        assert self._client is not None  # guaranteed by is_connected()
        try:
            resp = self._client.orderbook()
            if not isinstance(resp, dict) or resp.get("s") != "ok":
                return []
            raw_list = resp.get("orderBook") or []
            return [self._enrich(self._map_order(o)) for o in raw_list]
        except Exception as e:
            logger.error("get_order_book error: %s", e)
            return []

    @staticmethod
    def _map_order(raw: dict) -> Order:
        # FyersBrokerClient._normalise_order adds norenordno/status aliases
        order_id   = str(raw.get("id") or raw.get("norenordno") or raw.get("order_id") or "")
        symbol_raw = raw.get("tsym") or raw.get("symbol") or ""
        symbol     = symbol_raw.split(":", 1)[-1] if ":" in symbol_raw else symbol_raw
        exchange   = raw.get("exch") or raw.get("exchange") or ""
        trantype   = raw.get("transactiontype") or ("B" if raw.get("side", 1) == 1 else "S")
        side       = "BUY" if str(trantype).upper() in ("B", "BUY", "1") else "SELL"
        prd_raw    = (raw.get("prd") or raw.get("productType") or "").upper()
        prod_map   = {"INTRADAY": "MIS", "MARGIN": "NRML", "CNC": "CNC", "MIS": "MIS", "NRML": "NRML"}
        product    = prod_map.get(prd_raw, "MIS")
        # Fyers uses type field for order type: 1=LIMIT, 2=MARKET, 3=SL, 4=SL-M
        otype_map  = {1: "LIMIT", 2: "MARKET", 3: "SL", 4: "SL-M"}
        otype_raw  = raw.get("type") or 1
        order_type = otype_map.get(int(otype_raw), "LIMIT")
        qty        = int(raw.get("qty") or 0)
        price      = float(raw.get("prc") or raw.get("limitPrice") or 0)
        trgprc     = float(raw.get("trgprc") or raw.get("stopPrice") or 0)
        status_raw = raw.get("status", "PENDING")
        if isinstance(status_raw, int):
            status = _FYERS_STATUS_MAP.get(status_raw, "PENDING")
        else:
            status = str(status_raw).upper()
        filled     = int(raw.get("filledQty") or raw.get("filled_qty") or 0)
        avg_prc    = float(raw.get("tradedPrice") or raw.get("avg_price") or 0)
        ts         = raw.get("orderDateTime") or raw.get("timestamp") or ""
        remarks    = raw.get("message") or raw.get("rejreason") or ""

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
        assert self._client is not None  # guaranteed by is_connected()
        try:
            resp = self._client.funds()
            if not isinstance(resp, dict) or resp.get("s") != "ok":
                return self._enrich(Funds(available_cash=0.0))
            # fund_limit is a list of dicts with 'title' and 'equityAmount' keys
            fund_list = resp.get("fund_limit") or []
            fund_map = {item.get("title", ""): item for item in fund_list}
            available = float(fund_map.get("Available Balance", {}).get("equityAmount", 0))
            used = float(fund_map.get("Used Margin", {}).get("equityAmount", 0))
            total = float(fund_map.get("Total Balance", {}).get("equityAmount", 0))
            return self._enrich(Funds(
                available_cash=available,
                used_margin=used,
                total_balance=total,
            ))
        except Exception as e:
            logger.error("get_funds error: %s", e)
            return self._enrich(Funds(available_cash=0.0))

    # ── Holdings ───────────────────────────────────────────────────────────────

    def get_holdings(self) -> List[Holding]:
        if not self.is_connected():
            return []
        assert self._client is not None  # guaranteed by is_connected()
        try:
            resp = self._client.holdings()
            if not isinstance(resp, dict) or resp.get("s") != "ok":
                return []
            raw_list = resp.get("holdings") or []
            return [self._enrich(self._map_holding(h)) for h in raw_list]
        except Exception as e:
            logger.warning("get_holdings error: %s", e)
            return []

    @staticmethod
    def _map_holding(raw: dict) -> Holding:
        symbol_raw = raw.get("symbol", "")
        symbol     = symbol_raw.split(":", 1)[-1] if ":" in symbol_raw else symbol_raw
        exchange   = symbol_raw.split(":")[0] if ":" in symbol_raw else "NSE"
        qty        = int(raw.get("totalHolding") or raw.get("quantity") or 0)
        dp_qty     = int(raw.get("quantity") or 0)
        t1_qty     = int(raw.get("remainingQuantity") or 0)
        avg_prc    = float(raw.get("costPrice") or raw.get("avgPrice") or 0)
        ltp        = float(raw.get("ltp") or 0)
        pnl        = float(raw.get("pl") or 0) if raw.get("pl") is not None else (ltp - avg_prc) * qty
        pnl_pct    = (pnl / (avg_prc * qty) * 100) if avg_prc and qty else 0.0
        isin       = raw.get("isin") or ""
        return Holding(
            symbol=symbol, exchange=exchange, qty=qty, avg_price=avg_prc,
            ltp=ltp, pnl=pnl, pnl_pct=pnl_pct, isin=isin,
            dp_qty=dp_qty, t1_qty=t1_qty,
        )

    # ── Tradebook ──────────────────────────────────────────────────────────────

    def get_tradebook(self) -> List[Trade]:
        if not self.is_connected():
            return []
        assert self._client is not None  # guaranteed by is_connected()
        try:
            resp = self._client.tradebook()
            if not isinstance(resp, dict) or resp.get("s") != "ok":
                return []
            raw_list = resp.get("tradeBook") or []
            return [self._enrich(self._map_trade(t)) for t in raw_list]
        except Exception as e:
            logger.warning("get_tradebook error: %s", e)
            return []

    @staticmethod
    def _map_trade(raw: dict) -> Trade:
        symbol_raw = raw.get("symbol", "")
        symbol     = symbol_raw.split(":", 1)[-1] if ":" in symbol_raw else symbol_raw
        exchange   = symbol_raw.split(":")[0] if ":" in symbol_raw else ""
        side_int   = raw.get("side") or 1
        side       = "BUY" if int(side_int) == 1 else "SELL"
        prd_raw    = (raw.get("productType") or "").upper()
        prod_map   = {"INTRADAY": "MIS", "MARGIN": "NRML", "CNC": "CNC"}
        product    = prod_map.get(prd_raw, prd_raw)
        return Trade(
            trade_id=str(raw.get("id") or ""),
            order_id=str(raw.get("orderNumber") or raw.get("orderId") or ""),
            symbol=symbol, exchange=exchange,
            side=side, product=product,
            qty=int(raw.get("tradedQty") or raw.get("qty") or 0),
            price=float(raw.get("tradePrice") or raw.get("tradedPrice") or 0),
            timestamp=raw.get("orderDateTime") or "",
        )

    # ── Order management ───────────────────────────────────────────────────────

    def place_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """
        Place an order via Fyers.

        Smart Trader order keys → Fyers keys:
            symbol      → symbol (must be "EXCHANGE:SYMBOL" format)
            exchange    → prefixed to symbol
            side        → side (BUY=1, SELL=-1)
            product     → productType (MIS=INTRADAY, NRML=MARGIN, CNC=CNC)
            order_type  → type (MARKET=2, LIMIT=1, SL=3, SL-M=4)
            qty         → qty
            price       → limitPrice
            trigger_price → stopPrice
        """
        if not self.is_connected():
            return {"success": False, "message": "Not connected"}
        try:
            sym = order.get("symbol", "")
            exch = order.get("exchange", "NSE")
            # Fyers requires "EXCHANGE:SYMBOL" format
            fyers_sym = sym if ":" in sym else f"{exch}:{sym}"

            side_str = str(order.get("side", "BUY")).upper()
            side_int = 1 if side_str in ("BUY", "B") else -1

            prd_map = {"MIS": "INTRADAY", "NRML": "MARGIN", "CNC": "CNC"}
            otype_map = {"MARKET": 2, "LIMIT": 1, "SL": 3, "SL-M": 4}

            prd_raw   = str(order.get("product", "MIS")).upper()
            otype_raw = str(order.get("order_type", "MARKET")).upper()

            fyers_order = {
                "symbol":      fyers_sym,
                "qty":         int(order.get("qty", 0)),
                "type":        otype_map.get(otype_raw, 2),
                "side":        side_int,
                "productType": prd_map.get(prd_raw, "INTRADAY"),
                "limitPrice":  float(order.get("price", 0) or 0),
                "stopPrice":   float(order.get("trigger_price", 0) or 0),
                "validity":    "DAY",
                "disclosedQty":  0,
                "offlineOrder":  False,
            }

            result = self._client.place_order(data=fyers_order)
            # FyersModel.place_order returns dict: {"s": "ok", "id": "...", "code": 1101, "message": "..."}
            if isinstance(result, dict):
                ok = result.get("s") == "ok"
                return {
                    "success": ok,
                    "order_id": result.get("id", ""),
                    "message": result.get("message", "") or ("Order placed" if ok else "Order failed"),
                }
            return {"success": False, "message": "Unexpected response"}
        except Exception as e:
            logger.exception("place_order error: %s", e)
            return {"success": False, "message": str(e)}

    # ── Market data ──────────────────────────────────────────────────────────

    def get_ltp(self, exchange: str, symbol: str) -> Optional[float]:
        """Fetch last traded price via Fyers quotes API."""
        if not self.is_connected():
            return None
        try:
            fyers_sym = f"{exchange}:{symbol}" if ":" not in symbol else symbol
            resp = self._client.quotes({"symbols": fyers_sym})
            if isinstance(resp, dict) and resp.get("s") == "ok":
                data_list = resp.get("d", [])
                if data_list:
                    ltp = data_list[0].get("v", {}).get("lp")
                    if ltp is not None:
                        return float(ltp)
            return None
        except Exception as e:
            logger.warning("get_ltp error for %s:%s — %s", exchange, symbol, e)
            return None

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        if not self.is_connected():
            return {"success": False, "message": "Not connected"}
        assert self._client is not None  # guaranteed by is_connected()
        try:
            result = self._client.cancel_order(data={"id": order_id})
            if isinstance(result, dict):
                ok = result.get("s") == "ok"
                return {"success": ok, "message": result.get("message", "")}
            return {"success": False, "message": "Cancel failed"}
        except Exception as e:
            logger.error("cancel_order error: %s", e)
            return {"success": False, "message": str(e)}

    def modify_order(self, order_id: str, modifications: Dict[str, Any]) -> Dict[str, Any]:
        if not self.is_connected():
            return {"success": False, "message": "Not connected"}
        assert self._client is not None  # guaranteed by is_connected()
        try:
            modify_data = {"id": order_id, **modifications}
            result = self._client.modify_order(data=modify_data)
            if isinstance(result, dict):
                ok = result.get("s") == "ok"
                return {"success": ok, "message": result.get("message", "")}
            return {"success": bool(result), "message": "Modified" if result else "Modify failed"}
        except Exception as e:
            logger.error("modify_order error: %s", e)
            return {"success": False, "message": str(e)}
