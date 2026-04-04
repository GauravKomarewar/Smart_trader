"""
Smart Trader — Fyers Broker Adapter
=====================================
Wraps FyersBrokerClient (from shoonya_platform) and translates responses
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
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from broker.adapters.base import BrokerAdapter, Funds, Holding, Order, Position, Trade

logger = logging.getLogger("smart_trader.broker.fyers")


def _ensure_shoonya_path() -> None:
    """Add shoonya_platform to sys.path so FyersBrokerClient can be imported."""
    _root = Path("/home/ubuntu/shoonya_platform")
    if _root.exists() and str(_root) not in sys.path:
        sys.path.insert(0, str(_root))


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

    # ── Internal: build the FyersBrokerClient ─────────────────────────────────

    def _build_client(self, token: str) -> None:
        """Instantiate FyersBrokerClient and inject the token."""
        _ensure_shoonya_path()
        try:
            from shoonya_platform.brokers.fyers.client import FyersBrokerClient  # type: ignore[import]
            from shoonya_platform.brokers.fyers.config import FyersConfig  # type: ignore[import]
        except ImportError as e:
            logger.error("Cannot import FyersBrokerClient: %s", e)
            return

        app_id    = self._creds.get("APP_ID", "")
        client_id = self._creds.get("CLIENT_ID", "")
        secret_id = self._creds.get("SECRET_KEY", "")
        totp_key  = self._creds.get("TOTP_KEY", "")
        pin       = self._creds.get("PIN", "")
        redirect_url = self._creds.get("REDIRECT_URL", "https://trade.fyers.in/api-login/redirect-uri/index.html")

        # FyersConfig requires all fields; gracefully fall back if missing
        if not all([client_id, app_id, secret_id]):
            logger.error("Missing required Fyers credentials (CLIENT_ID / APP_ID / SECRET_KEY)")
            return

        try:
            fyers_cfg = FyersConfig(
                fyers_id=client_id,   # correct field name
                app_id=app_id,
                secret_id=secret_id,  # correct field name
                totp_key=totp_key or "AAAA",  # placeholder; token already acquired
                pin=pin or "0000",
                redirect_url=redirect_url or "https://trade.fyers.in/api-login/redirect-uri/index.html",
            )
            self._client = FyersBrokerClient(fyers_cfg)
            # Inject the pre-acquired token so no login flow is needed
            self._inject_token(token)
        except Exception as e:
            logger.error("FyersBrokerClient init failed: %s", e)

    def _inject_token(self, token: str) -> bool:
        """Directly set the access token on the underlying FyersV3Client."""
        if self._client is None:
            return False
        try:
            # FyersBrokerClient stores FyersV3Client in ._fyers_client
            fv3 = getattr(self._client, "_fyers_client", None)
            if fv3 is None:
                return False

            app_id = self._creds.get("APP_ID", "")
            # Token may arrive as "APP_ID:raw_token" or just "raw_token"
            if ":" in token:
                raw_access_token = token.split(":", 1)[1]
            else:
                raw_access_token = token

            # Inject onto FyersV3Client (it stores it as access_token)
            fv3.access_token = raw_access_token

            # FyersModel expects "app_id:access_token" as the authorization header
            fyers_model = getattr(fv3, "fyers", None)
            if fyers_model is None:
                # login() was never called — create FyersModel directly
                try:
                    from fyers_apiv3.fyersModel import FyersModel
                    fyers_model = FyersModel(
                        client_id=app_id,
                        token=raw_access_token,
                        log_level="ERROR",
                    )
                    fv3.fyers = fyers_model
                except Exception as _fe:
                    logger.warning("FyersModel direct creation failed: %s", _fe)

            if fyers_model is not None:
                fyers_model.token = raw_access_token
                # Rebuild header that FyersModel uses internally
                fyers_model.header = f"{app_id}:{raw_access_token}"

            self._client._logged_in = True
            self._connected = True
            logger.info("Fyers token injected for client=%s", self.client_id)
            return True
        except Exception as e:
            logger.error("_inject_token error: %s", e)
            return False

    # ── Connection ─────────────────────────────────────────────────────────────

    def is_connected(self) -> bool:
        return self._connected and self._client is not None

    # ── Positions ──────────────────────────────────────────────────────────────

    def get_positions(self) -> List[Position]:
        if not self.is_connected():
            return []
        assert self._client is not None  # guaranteed by is_connected()
        try:
            # FyersBrokerClient.get_positions() already returns partially normalised dicts
            raw_list = self._client.get_positions()
            return [self._enrich(self._map_position(p)) for p in (raw_list or [])]
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
            raw_list = self._client.get_order_book()
            return [self._enrich(self._map_order(o)) for o in (raw_list or [])]
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
            raw = self._client.get_limits()
            return self._enrich(Funds(
                available_cash=float(raw.get("available_cash", 0)),
                used_margin=float(raw.get("used_margin", 0)),
                total_balance=float(raw.get("total_balance", 0)),
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
            raw_list = self._client.get_holdings()
            return [self._enrich(self._map_holding(h)) for h in (raw_list or [])]
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
        try:
            fv3 = getattr(self._client, "_fyers_client", None)
            if fv3 is None or not hasattr(fv3, "fyers"):
                return []
            resp = fv3.fyers.tradebook()
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

            result = self._client.place_order(fyers_order)  # type: ignore[union-attr]
            # FyersBrokerClient.place_order returns OrderResult dataclass
            if hasattr(result, "success"):
                return {
                    "success": result.success,
                    "order_id": getattr(result, "order_id", ""),
                    "message": getattr(result, "error_message", "") or "Order placed",
                }
            return {"success": False, "message": "Unexpected response"}
        except Exception as e:
            logger.exception("place_order error: %s", e)
            return {"success": False, "message": str(e)}

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        if not self.is_connected():
            return {"success": False, "message": "Not connected"}
        assert self._client is not None  # guaranteed by is_connected()
        try:
            result = self._client.cancel_order(order_id)
            if result is None:
                return {"success": False, "message": "Cancel failed"}
            if isinstance(result, dict):
                ok = result.get("s") == "ok"
                return {"success": ok, "message": result.get("message", "")}
            return {"success": True, "message": "Cancelled"}
        except Exception as e:
            logger.error("cancel_order error: %s", e)
            return {"success": False, "message": str(e)}

    def modify_order(self, order_id: str, modifications: Dict[str, Any]) -> Dict[str, Any]:
        if not self.is_connected():
            return {"success": False, "message": "Not connected"}
        assert self._client is not None  # guaranteed by is_connected()
        try:
            result = self._client.modify_order(order_id, modifications)
            if isinstance(result, dict):
                ok = result.get("s") == "ok"
                return {"success": ok, "message": result.get("message", "")}
            return {"success": bool(result), "message": "Modified" if result else "Modify failed"}
        except Exception as e:
            logger.error("modify_order error: %s", e)
            return {"success": False, "message": str(e)}
