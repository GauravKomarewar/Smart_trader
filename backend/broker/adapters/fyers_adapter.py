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
import math
import threading
import time
from typing import Any, Dict, List, Optional

from broker.adapters.base import BrokerAdapter, Funds, Holding, Order, Position, Trade

logger = logging.getLogger("smart_trader.broker.fyers")


_FYERS_STATUS_MAP = {1: "PENDING", 2: "COMPLETE", 4: "CANCELLED",
                     5: "CANCELLED", 6: "PENDING", 7: "REJECTED"}

# Fyers returns exchange as integer: 10=NSE, 11=MCX, 12=BSE
_FYERS_EXCH_CODE = {10: "NSE", 11: "MCX", 12: "BSE"}


def _fyers_exchange(raw_exchange, symbol: str = "") -> str:
    """Convert Fyers numeric exchange code to canonical string.

    Also promotes NSE → NFO / BSE → BFO when the symbol is a derivative.
    """
    if isinstance(raw_exchange, int):
        exch = _FYERS_EXCH_CODE.get(raw_exchange, "NSE")
    elif isinstance(raw_exchange, str) and raw_exchange.isdigit():
        exch = _FYERS_EXCH_CODE.get(int(raw_exchange), raw_exchange)
    else:
        exch = str(raw_exchange) if raw_exchange else "NSE"
    # Detect derivatives from symbol suffix
    sym_up = symbol.upper()
    is_deriv = sym_up.endswith("CE") or sym_up.endswith("PE") or "FUT" in sym_up
    if is_deriv:
        if exch == "NSE":
            return "NFO"
        if exch == "BSE":
            return "BFO"
    return exch


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
        self._lock = threading.RLock()
        # ── Circuit breaker (prevents thundering herd on -429) ──
        self._cb_lock = threading.Lock()  # dedicated lock for circuit state
        self._cb_open_until: float = 0.0  # monotonic; 0 = circuit closed
        self._cb_half_open = False         # True = one probe call in flight
        self._cb_backoff: float = 120.0    # doubles on each failure, max 600s

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
            new_client = FyersModel(
                client_id=app_id,
                token=raw_access_token,
                log_level="ERROR",
            )
            with self._lock:
                self._client = new_client
                self._connected = True
            # Circuit is CLOSED after a fresh login — allow all calls through.
            # The half-open probe is only used when recovering from a rate-limit.
            with self._cb_lock:
                self._cb_open_until = 0.0    # circuit closed
                self._cb_half_open = False   # no probe in flight
                self._cb_backoff = 120.0
            logger.info("Fyers client initialised for client=%s", client_id)
            # Share the valid token with the global FyersDataClient (market data)
            try:
                from broker.fyers_client import get_fyers_client
                get_fyers_client().inject_token(app_id, raw_access_token)
            except Exception as _inj_err:
                logger.debug("Could not inject token into data client: %s", _inj_err)
        except Exception as e:
            logger.error("FyersModel init failed: %s", e)

    # _inject_token removed — _build_client now creates FyersModel directly

    # ── Connection ─────────────────────────────────────────────────────────────

    def is_connected(self) -> bool:
        with self._lock:
            return self._connected and self._client is not None

    def _circuit_allow(self) -> bool:
        """Return True if this call may proceed (CLOSED or HALF-OPEN probe).

        CLOSED     → allow all.
        HALF-OPEN  → block all while a probe is in flight.
        OPEN       → block until backoff expires, then let EXACTLY ONE probe through.
        """
        with self._cb_lock:
            # If a probe is already in flight, block all others regardless of state
            if self._cb_half_open:
                return False
            now = time.monotonic()
            if self._cb_open_until == 0.0:      # CLOSED → allow all
                return True
            if now >= self._cb_open_until:      # Backoff expired → single probe
                self._cb_half_open = True
                return True
            return False                        # OPEN → still waiting

    def _circuit_success(self) -> None:
        """Call after a successful Fyers API response to close the circuit."""
        with self._cb_lock:
            self._cb_open_until = 0.0
            self._cb_half_open = False
            self._cb_backoff = 30.0  # reset for next failure

    def _circuit_trip(self) -> None:
        """Call on -429 to open the circuit with exponential backoff."""
        with self._cb_lock:
            backoff = self._cb_backoff
            self._cb_open_until = time.monotonic() + backoff
            self._cb_half_open = False
            self._cb_backoff = min(backoff * 2, 120.0)  # cap at 2 min
        logger.warning("Fyers rate limit — circuit open for %.0fs (client=%s)",
                       backoff, self.client_id)

    # ── Positions ──────────────────────────────────────────────────────────────

    def get_positions(self) -> List[Position]:
        with self._lock:
            if not (self._connected and self._client is not None):
                return []
            client = self._client
        if not self._circuit_allow():
            return []
        try:
            resp = client.positions()
            if not isinstance(resp, dict) or resp.get("s") != "ok":
                if isinstance(resp, dict) and resp.get("code") == -429:
                    self._circuit_trip()
                return []
            self._circuit_success()
            raw_list = resp.get("netPositions") or []
            return [self._enrich(self._map_position(p)) for p in raw_list]
        except Exception as e:
            if "429" in str(e):
                self._circuit_trip()
            else:
                logger.error("get_positions error: %s", e)
            return []

    @staticmethod
    def _map_position(raw: dict) -> Position:
        # FyersBrokerClient._normalise_position already adds tsym/netqty/pnl aliases
        symbol    = raw.get("symbol", "")
        # Strip exchange prefix if present
        clean_sym = symbol.split(":", 1)[-1] if ":" in symbol else symbol
        raw_exch  = raw.get("exchange") or raw.get("exch") or (symbol.split(":")[0] if ":" in symbol else "NSE")
        exchange  = _fyers_exchange(raw_exch, clean_sym)
        prd_raw   = (raw.get("prd") or raw.get("productType") or "").upper()
        prod_map  = {"INTRADAY": "MIS", "MARGIN": "NRML", "CNC": "CNC", "MIS": "MIS", "NRML": "NRML"}
        product   = prod_map.get(prd_raw, "MIS")
        buy_avg   = float(raw.get("buy_avg") or raw.get("buyAvg") or 0)
        sell_avg  = float(raw.get("sellAvg") or 0)
        ltp       = float(raw.get("ltp") or raw.get("last_price") or 0)
        rpnl      = float(raw.get("realised_pnl") or raw.get("rpnl") or raw.get("realized_profit") or 0)
        urmtom    = float(raw.get("unrealised_pnl") or raw.get("urmtom") or raw.get("unrealized_profit") or 0)
        buy_qty   = int(raw.get("day_buy_qty") or raw.get("buyQty") or 0)
        sell_qty  = int(raw.get("day_sell_qty") or raw.get("sellQty") or 0)

        # Fyers qty is UNSIGNED — derive sign from buy_qty vs sell_qty
        raw_qty   = int(raw.get("qty") or raw.get("netqty") or raw.get("netQty") or 0)
        if raw_qty != 0 and sell_qty > buy_qty:
            qty = -abs(raw_qty)  # net short
        elif raw_qty != 0 and buy_qty > sell_qty:
            qty = abs(raw_qty)   # net long
        elif raw_qty != 0:
            # Equal buy/sell: use netQty sign if provided, else check avg prices
            net_qty_raw = raw.get("netQty")
            if net_qty_raw is not None and int(net_qty_raw) != 0:
                qty = int(net_qty_raw)
            else:
                qty = raw_qty  # fallback
        else:
            qty = 0

        avg_prc   = buy_avg if qty >= 0 else sell_avg

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
        with self._lock:
            if not (self._connected and self._client is not None):
                return []
            client = self._client
        if not self._circuit_allow():
            return []
        try:
            resp = client.orderbook()
            if not isinstance(resp, dict) or resp.get("s") != "ok":
                if isinstance(resp, dict) and resp.get("code") == -429:
                    self._circuit_trip()
                return []
            self._circuit_success()
            raw_list = resp.get("orderBook") or []
            return [self._enrich(self._map_order(o)) for o in raw_list]
        except Exception as e:
            if "429" in str(e):
                self._circuit_trip()
            else:
                logger.error("get_order_book error: %s", e)
            return []

    @staticmethod
    def _map_order(raw: dict) -> Order:
        # FyersBrokerClient._normalise_order adds norenordno/status aliases
        order_id   = str(raw.get("id") or raw.get("norenordno") or raw.get("order_id") or "")
        symbol_raw = raw.get("tsym") or raw.get("symbol") or ""
        symbol     = symbol_raw.split(":", 1)[-1] if ":" in symbol_raw else symbol_raw
        raw_exch   = raw.get("exch") or raw.get("exchange") or ""
        exchange   = _fyers_exchange(raw_exch, symbol)
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
        with self._lock:
            if not (self._connected and self._client is not None):
                return self._enrich(Funds(available_cash=0.0))
            client = self._client
        if not self._circuit_allow():
            return self._enrich(Funds(available_cash=0.0))
        try:
            resp = client.funds()
            if not isinstance(resp, dict) or resp.get("s") != "ok":
                if isinstance(resp, dict) and resp.get("code") == -429:
                    self._circuit_trip()
                return self._enrich(Funds(available_cash=0.0))
            self._circuit_success()
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
            if "429" in str(e):
                self._circuit_trip()
            else:
                logger.error("get_funds error: %s", e)
            return self._enrich(Funds(available_cash=0.0))

    # ── Holdings ───────────────────────────────────────────────────────────────

    def get_holdings(self) -> List[Holding]:
        with self._lock:
            if not (self._connected and self._client is not None):
                return []
            client = self._client
        if not self._circuit_allow():
            return []
        try:
            resp = client.holdings()
            if not isinstance(resp, dict) or resp.get("s") != "ok":
                if isinstance(resp, dict) and resp.get("code") == -429:
                    self._circuit_trip()
                return []
            self._circuit_success()
            raw_list = resp.get("holdings") or []
            return [self._enrich(self._map_holding(h)) for h in raw_list]
        except Exception as e:
            if "429" in str(e):
                self._circuit_trip()
            else:
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
        with self._lock:
            if not (self._connected and self._client is not None):
                return []
            client = self._client
        if not self._circuit_allow():
            return []
        try:
            resp = client.tradebook()
            if not isinstance(resp, dict) or resp.get("s") != "ok":
                code = resp.get("code") if isinstance(resp, dict) else None
                if code == -429:
                    self._circuit_trip()
                    return []
                logger.warning("get_tradebook non-ok response: %s", resp)
                return []
            self._circuit_success()
            raw_list = resp.get("tradeBook") or []
            return [self._enrich(self._map_trade(t)) for t in raw_list]
        except Exception as e:
            if "429" in str(e):
                self._circuit_trip()
            else:
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
            trade_id=str(raw.get("tradeNumber") or raw.get("id") or ""),
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
        with self._lock:
            if not (self._connected and self._client is not None):
                return {"success": False, "message": "Not connected"}
            client = self._client
        try:
            sym = order.get("symbol", "")
            exch = order.get("exchange", "NSE")
            # Resolve Fyers-specific symbol from instruments DB
            # e.g. GOLDPETAL30APR26 → MCX:GOLDPETAL26APRFUT
            fyers_sym = ""
            if ":" not in sym:
                try:
                    from broker.symbol_normalizer import lookup_by_trading_symbol
                    inst = lookup_by_trading_symbol(sym, exch)
                    if inst and getattr(inst, 'fyers_symbol', None):
                        fyers_sym = inst.fyers_symbol
                        logger.info("Fyers symbol resolved: %s → %s", sym, fyers_sym)
                except Exception as e:
                    logger.warning("Fyers symbol lookup failed for %s: %s", sym, e)
            if not fyers_sym:
                fyers_sym = sym if ":" in sym else f"{exch}:{sym}"

            side_str = str(order.get("side", "BUY")).upper()
            side_int = 1 if side_str in ("BUY", "B") else -1

            prd_map = {"MIS": "INTRADAY", "NRML": "MARGIN", "CNC": "CNC"}
            otype_map = {"MARKET": 2, "LIMIT": 1, "SL": 3, "SL-M": 4}

            prd_raw   = str(order.get("product", "MIS")).upper()
            otype_raw = str(order.get("order_type", "MARKET")).upper()

            fyers_order = {
                "symbol":      fyers_sym,
                "qty":         int(order.get("qty") or order.get("quantity") or 0),
                "type":        otype_map.get(otype_raw, 2),
                "side":        side_int,
                "productType": prd_map.get(prd_raw, "INTRADAY"),
                "limitPrice":  float(order.get("price", 0) or 0),
                "stopPrice":   float(order.get("trigger_price", 0) or 0),
                "validity":    "DAY",
                "disclosedQty":  0,
                "offlineOrder":  False,
            }

            # Fyers rejects limitPrice=0 for MCX MARKET orders (tick-size validation).
            # For MARKET orders with price=0, use LTP ± 1% buffer as protection price.
            # Round to whole number — MCX tick sizes are typically 1.0.
            if fyers_order["type"] == 2 and fyers_order["limitPrice"] == 0:
                ltp_fallback = float(order.get("ltp") or 0)
                if ltp_fallback > 0:
                    buffer = max(1.0, ltp_fallback * 0.01)  # 1% buffer
                    if side_int == 1:   # BUY — Use higher price (ceil)
                        fyers_order["limitPrice"] = float(math.ceil(ltp_fallback + buffer))
                    else:               # SELL — Use lower price (floor)
                        fyers_order["limitPrice"] = float(math.floor(max(1.0, ltp_fallback - buffer)))

            result = client.place_order(data=fyers_order)
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
        with self._lock:
            if not (self._connected and self._client is not None):
                return None
            client = self._client
        try:
            # Resolve Fyers symbol from DB first
            fyers_sym = ""
            if ":" not in symbol:
                try:
                    from broker.symbol_normalizer import lookup_by_trading_symbol
                    inst = lookup_by_trading_symbol(symbol, exchange)
                    if inst and getattr(inst, 'fyers_symbol', None):
                        fyers_sym = inst.fyers_symbol
                except Exception:
                    pass
            if not fyers_sym:
                fyers_sym = f"{exchange}:{symbol}" if ":" not in symbol else symbol
            resp = client.quotes({"symbols": fyers_sym})
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
        with self._lock:
            if not (self._connected and self._client is not None):
                return {"success": False, "message": "Not connected"}
            client = self._client
        try:
            result = client.cancel_order(data={"id": order_id})
            logger.info("Fyers cancel_order id=%s → %s", order_id,
                        {k: result.get(k) for k in ("s", "code", "message")} if isinstance(result, dict) else result)
            if isinstance(result, dict):
                ok = result.get("s") == "ok"
                return {"success": ok, "message": result.get("message", "")}
            return {"success": False, "message": "Cancel failed"}
        except Exception as e:
            logger.error("cancel_order error: %s", e)
            return {"success": False, "message": str(e)}

    def modify_order(self, order_id: str, modifications: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            if not (self._connected and self._client is not None):
                return {"success": False, "message": "Not connected"}
            client = self._client
        try:
            otype_map = {"MARKET": 2, "LIMIT": 1, "SL": 3, "SL-M": 4}
            modify_data: Dict[str, Any] = {"id": order_id}
            if "qty" in modifications:
                modify_data["qty"] = int(modifications["qty"])
            elif "quantity" in modifications:
                modify_data["qty"] = int(modifications["quantity"])
            if "price" in modifications:
                modify_data["limitPrice"] = float(modifications["price"])
            if "trigger_price" in modifications:
                modify_data["stopPrice"] = float(modifications["trigger_price"])
            if "order_type" in modifications:
                ot = str(modifications["order_type"]).upper()
                modify_data["type"] = otype_map.get(ot, 2)
            result = client.modify_order(data=modify_data)
            if isinstance(result, dict):
                ok = result.get("s") == "ok"
                return {"success": ok, "message": result.get("message", "")}
            return {"success": bool(result), "message": "Modified" if result else "Modify failed"}
        except Exception as e:
            logger.error("modify_order error: %s", e)
            return {"success": False, "message": str(e)}
