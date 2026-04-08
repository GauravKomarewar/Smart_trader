"""
Smart Trader — Shoonya Broker Adapter
======================================
Wraps Smart_trader's BrokerSession (shoonya_client.py) and translates
Shoonya-specific position/order dicts to normalised Smart Trader models.

Shoonya field reference
-----------------------
Positions: tsym, exch, prd, netqty, buyqty, sellqty, avgprc,
           rpnl (realised), urmtom (unrealised), lp (last price)

Orders: norenordno, tsym, exch, trantype (B/S), prd,
        prctyp (MKT/LMT/SL-LMT/SL-MKT), qty, prc, trgprc,
        Status (OPEN/COMPLETE/CANCELLED/REJECTED), fillshares,
        avgprc, exch_tm, rejreason

Holdings: tsym, exch, holdqty, dpqty, t1qty, avgprc, upldprc
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from broker.adapters.base import BrokerAdapter, Funds, Holding, Order, Position, Trade

logger = logging.getLogger("smart_trader.broker.shoonya")

# Shoonya price-type → canonical order_type
_PRCTYP_MAP = {
    "MKT":     "MARKET",
    "LMT":     "LIMIT",
    "SL-LMT":  "SL",
    "SL-MKT":  "SL-M",
}

# Shoonya product → canonical product
_PRD_MAP = {
    "I":   "MIS",
    "C":   "CNC",
    "M":   "NRML",
    "B":   "MIS",  # Bracket
    "H":   "MIS",  # Cover
    "MIS": "MIS",
    "CNC": "CNC",
    "NRML": "NRML",
}


class ShoonyaAdapter(BrokerAdapter):
    """
    Adapter for Shoonya / Finvasia accounts.

    Requires a live BrokerSession (from broker.shoonya_client) that has
    already had a token injected (inject_token called with OAuth token).
    """

    def __init__(self, session: Any, account_id: str, client_id: str) -> None:
        """
        Args:
            session: broker.shoonya_client.BrokerSession instance
            account_id: BrokerConfig.id (UUID)
            client_id:  Broker client ID string (e.g. FA14667)
        """
        super().__init__("shoonya", account_id, client_id)
        self._session = session

    # ── Connection ─────────────────────────────────────────────────────────────

    def is_connected(self) -> bool:
        return (
            self._session is not None
            and not getattr(self._session, "_demo_mode", True)
            and getattr(self._session, "_client", None) is not None
        )

    # ── Positions ──────────────────────────────────────────────────────────────

    def get_positions(self) -> List[Position]:
        raw = self._session.get_positions()
        if raw is None:
            raise RuntimeError("Session Expired: get_positions returned None")
        if isinstance(raw, dict) and raw.get("stat") == "Not_Ok":
            raise RuntimeError(f"Session Expired: {raw.get('emsg', 'unknown')}")
        return [self._enrich(self._map_position(p)) for p in (raw or [])]

    @staticmethod
    def _map_position(raw: dict) -> Position:
        # Support both Shoonya-native AND Fyers-normalised aliases
        symbol   = raw.get("tsym") or raw.get("tradingsymbol") or raw.get("symbol") or ""
        exchange = raw.get("exch") or raw.get("exchange") or ""
        prd_raw  = raw.get("prd") or raw.get("productType") or ""
        product  = _PRD_MAP.get(prd_raw.upper(), prd_raw)
        net_qty  = int(raw.get("netqty") or raw.get("qty") or raw.get("netQty") or 0)
        # Shoonya: avgprc can be 0 for sell-first positions.
        # Fallback chain: netavgprc → avgprc → totbuyavgprc/totsellavgprc → daybuyavgprc/daysellavgprc → upldprc
        avg_prc  = float(
            raw.get("netavgprc") or raw.get("avgprc") or raw.get("avg_price") or raw.get("buyAvg") or
            raw.get("totbuyavgprc") or raw.get("totsellavgprc") or
            raw.get("daybuyavgprc") or raw.get("daysellavgprc") or
            raw.get("upldprc") or 0
        )
        ltp      = float(raw.get("lp") or raw.get("ltp") or raw.get("last_price") or 0)
        rpnl     = float(raw.get("rpnl") or raw.get("realised_pnl") or raw.get("realized_profit") or 0)
        urmtom   = float(raw.get("urmtom") or raw.get("unrealised_pnl") or raw.get("unrealized_profit") or 0)
        buy_qty  = int(raw.get("buyqty") or raw.get("day_buy_qty") or raw.get("buyQty") or 0)
        sell_qty = int(raw.get("sellqty") or raw.get("day_sell_qty") or raw.get("sellQty") or 0)

        # Guess instrument type from symbol suffix
        itype = ""
        if any(s in symbol for s in ("CE", "PE")):
            itype = "CE" if symbol.endswith("CE") else "PE"
        elif "FUT" in symbol:
            itype = "FUT"
        elif "INDEX" in symbol.upper():
            itype = "IDX"
        else:
            itype = "EQ"

        return Position(
            symbol=symbol,
            exchange=exchange,
            product=product,
            qty=net_qty,
            avg_price=avg_prc,
            ltp=ltp,
            pnl=rpnl + urmtom,
            realised_pnl=rpnl,
            unrealised_pnl=urmtom,
            buy_qty=buy_qty,
            sell_qty=sell_qty,
            instrument_type=itype,
        )

    # ── Order book ─────────────────────────────────────────────────────────────

    def get_order_book(self) -> List[Order]:
        raw = self._session.get_order_book()
        if raw is None:
            raise RuntimeError("Session Expired: get_order_book returned None")
        if isinstance(raw, dict) and raw.get("stat") == "Not_Ok":
            raise RuntimeError(f"Session Expired: {raw.get('emsg', 'unknown')}")
        return [self._enrich(self._map_order(o)) for o in (raw or [])]

    @staticmethod
    def _map_order(raw: dict) -> Order:
        order_id   = str(raw.get("norenordno") or raw.get("order_id") or raw.get("id") or "")
        symbol     = raw.get("tsym") or raw.get("tradingsymbol") or raw.get("symbol") or ""
        exchange   = raw.get("exch") or raw.get("exchange") or ""
        trantype   = (raw.get("trantype") or raw.get("transactiontype") or "B").upper().strip()
        side       = "BUY" if trantype in ("B", "BUY") else "SELL"
        prd_raw    = (raw.get("prd") or raw.get("productType") or "").upper()
        product    = _PRD_MAP.get(prd_raw, prd_raw)
        prctyp     = (raw.get("prctyp") or raw.get("price_type") or "LMT").upper()
        order_type = _PRCTYP_MAP.get(prctyp, prctyp)
        qty        = int(raw.get("qty") or 0)
        price      = float(raw.get("prc") or raw.get("limitPrice") or 0)
        trgprc     = float(raw.get("trgprc") or raw.get("stopPrice") or 0)
        # Shoonya uses "OPEN" for pending; normalise
        status_raw = (raw.get("Status") or raw.get("status") or "PENDING").upper()
        status_map = {"OPEN": "PENDING", "COMPLETE": "COMPLETE", "CANCELLED": "CANCELLED",
                      "REJECTED": "REJECTED", "PENDING": "PENDING", "TRIGGER_PENDING": "PENDING"}
        status     = status_map.get(status_raw, "PENDING")
        filled     = int(raw.get("fillshares") or raw.get("filled_qty") or 0)
        avg_prc    = float(raw.get("avgprc") or raw.get("avg_price") or 0)
        ts         = raw.get("exch_tm") or raw.get("timestamp") or raw.get("orderTime") or ""
        remarks    = raw.get("rejreason") or raw.get("remarks") or raw.get("text") or ""

        return Order(
            order_id=order_id, symbol=symbol, exchange=exchange,
            side=side, product=product, order_type=order_type,
            qty=qty, price=price, trigger_price=trgprc,
            status=status, filled_qty=filled, avg_price=avg_prc,
            timestamp=ts, remarks=remarks,
        )

    # ── Funds ──────────────────────────────────────────────────────────────────

    def get_funds(self) -> Funds:
        raw = self._session.get_limits()
        if raw is None:
            raise RuntimeError("Session Expired: get_funds returned None")
        if isinstance(raw, dict) and raw.get("stat") == "Not_Ok":
            raise RuntimeError(f"Session Expired: {raw.get('emsg', 'unknown')}")
        if not raw:
            return self._enrich(Funds(available_cash=0.0))
        return self._enrich(Funds(
            available_cash=float(raw.get("marginAvailable") or raw.get("cash") or 0),
            used_margin=float(raw.get("marginUsed") or raw.get("marginused") or 0),
            total_balance=float(raw.get("totalBalance") or raw.get("cash") or 0),
            realised_pnl=float(raw.get("realizedPnl") or raw.get("rpnl") or 0),
            unrealised_pnl=float(raw.get("unrealizedPnl") or raw.get("urpnl") or 0),
        ))

    # ── Holdings ───────────────────────────────────────────────────────────────

    def get_holdings(self) -> List[Holding]:
        try:
            raw_list = self._session.get_holdings()
            return [self._enrich(self._map_holding(h)) for h in (raw_list or [])]
        except json.JSONDecodeError:
            raise  # Empty response = session expired — let auto-relogin handle
        except RuntimeError:
            raise  # Session expired — let auto-relogin handle
        except Exception as e:
            logger.warning("get_holdings error: %s", e)
            return []

    @staticmethod
    def _map_holding(raw: dict) -> Holding:
        exch_info = raw.get("exch_tsym") or []
        primary = exch_info[0] if isinstance(exch_info, list) and exch_info else {}
        symbol  = (
            raw.get("tsym")
            or raw.get("tradingsymbol")
            or primary.get("tsym")
            or primary.get("cname")
            or ""
        )
        exch    = (
            raw.get("exch")
            or raw.get("exchange")
            or primary.get("exch")
            or "NSE"
        )
        qty     = int(raw.get("holdqty") or raw.get("qty") or 0)
        dp_qty  = int(raw.get("dpqty") or raw.get("dp_qty") or qty)
        t1_qty  = int(raw.get("t1qty") or raw.get("t1_qty") or 0)
        avg_prc = float(raw.get("avgprc") or raw.get("avg_price") or raw.get("upldprc") or 0)
        ltp     = float(raw.get("lp") or raw.get("ltp") or raw.get("c") or 0)
        pnl     = (ltp - avg_prc) * qty if ltp and avg_prc else 0.0
        pnl_pct = (pnl / (avg_prc * qty) * 100) if avg_prc and qty else 0.0
        isin    = raw.get("isin") or primary.get("isin") or ""
        return Holding(
            symbol=symbol, exchange=exch, qty=qty, avg_price=avg_prc,
            ltp=ltp, pnl=pnl, pnl_pct=pnl_pct, isin=isin,
            dp_qty=dp_qty, t1_qty=t1_qty,
        )

    # ── Tradebook ──────────────────────────────────────────────────────────────

    def get_tradebook(self) -> List[Trade]:
        try:
            raw_list = self._session.get_tradebook()
            return [self._enrich(self._map_trade(t)) for t in (raw_list or [])]
        except json.JSONDecodeError:
            raise  # Empty response = session expired — let auto-relogin handle
        except RuntimeError:
            raise  # Session expired — let auto-relogin handle
        except Exception as e:
            logger.warning("get_tradebook error: %s", e)
            return []

    @staticmethod
    def _map_trade(raw: dict) -> Trade:
        return Trade(
            trade_id=str(raw.get("flid") or raw.get("fillid") or raw.get("ftm") or raw.get("trade_id") or ""),
            order_id=str(raw.get("norenordno") or raw.get("order_id") or ""),
            symbol=raw.get("tsym") or raw.get("symbol") or "",
            exchange=raw.get("exch") or "",
            side="BUY" if (raw.get("trantype") or "B").upper() in ("B", "BUY") else "SELL",
            product=_PRD_MAP.get((raw.get("prd") or "").upper(), raw.get("prd") or ""),
            qty=int(raw.get("fillshares") or raw.get("qty") or 0),
            price=float(raw.get("flprc") or raw.get("price") or 0),
            timestamp=raw.get("exch_tm") or raw.get("fill_time") or "",
        )

    # ── Order management ───────────────────────────────────────────────────────

    # Canonical side → Shoonya buy_or_sell
    _SIDE_MAP = {"BUY": "B", "SELL": "S", "B": "B", "S": "S"}
    # Canonical product → Shoonya product_type
    _PRD_TO_SHOONYA = {"MIS": "I", "CNC": "C", "NRML": "M", "I": "I", "C": "C", "M": "M"}
    # Canonical order_type → Shoonya price_type
    _OTYPE_TO_SHOONYA = {
        "MARKET": "MKT", "LIMIT": "LMT", "SL": "SL-LMT", "SL-M": "SL-MKT",
        "MKT": "MKT", "LMT": "LMT", "SL-LMT": "SL-LMT", "SL-MKT": "SL-MKT",
    }

    def place_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """Translate canonical order dict to Shoonya format, then place via session."""
        client = getattr(self._session, "_client", None)
        if client is None:
            return {"success": False, "message": "Shoonya not connected"}

        side_raw = str(order.get("side") or order.get("transaction_type") or "BUY").upper()
        prd_raw = str(order.get("product") or order.get("product_type") or "MIS").upper()
        otype_raw = str(order.get("order_type") or order.get("price_type") or "MARKET").upper()

        # Symbol: resolve via symbol DB for correct Shoonya-specific symbol
        sym = order.get("symbol") or order.get("tradingsymbol") or ""
        if ":" in sym:
            sym = sym.split(":", 1)[1]
        exch = order.get("exchange", "NSE")
        # Look up Shoonya-specific trading symbol from symbol DB
        try:
            from broker.symbol_normalizer import lookup_by_trading_symbol
            inst = lookup_by_trading_symbol(sym)
            if inst:
                shoonya_tsym = getattr(inst, 'trading_symbol', None) or ""
                if shoonya_tsym:
                    sym = shoonya_tsym
                    logger.debug("Shoonya symbol resolved: %s → %s", order.get("symbol"), sym)
                # Also use the DB exchange if available
                db_exch = getattr(inst, 'exchange', None) or ""
                if db_exch:
                    exch = db_exch
        except Exception as e:
            logger.warning("Shoonya symbol lookup failed for %s: %s", sym, e)

        shoonya_params = {
            "buy_or_sell":   self._SIDE_MAP.get(side_raw, "B"),
            "product_type":  self._PRD_TO_SHOONYA.get(prd_raw, "I"),
            "exchange":      exch,
            "tradingsymbol": sym,
            "quantity":      int(order.get("qty") or order.get("quantity") or 0),
            "discloseqty":   0,
            "price_type":    self._OTYPE_TO_SHOONYA.get(otype_raw, "MKT"),
            "price":         float(order.get("price", 0) or 0),
            "trigger_price": float(order.get("trigger_price", 0) or 0),
            "retention":     order.get("retention") or "DAY",
            "remarks":       order.get("tag") or order.get("remarks") or "smart_trader",
        }

        logger.info("Shoonya order: %s %s %s qty=%d px=%.2f type=%s",
                     shoonya_params["buy_or_sell"], shoonya_params["tradingsymbol"],
                     shoonya_params["exchange"], shoonya_params["quantity"],
                     shoonya_params["price"], shoonya_params["price_type"])
        try:
            result = client.place_order(**shoonya_params)
            if result is None:
                return {"success": False, "message": "Broker returned no response — market may be closed or session expired"}
            if isinstance(result, dict):
                oid = result.get("norenordno") or result.get("order_id") or ""
                ok = bool(oid) or result.get("stat") == "Ok"
                return {
                    "success": ok,
                    "order_id": oid,
                    "message": "Order placed" if ok else (result.get("emsg") or "Order failed"),
                    "raw": result,
                }
            return {"success": False, "message": str(result)}
        except Exception as e:
            logger.exception("Shoonya place_order error: %s", e)
            return {"success": False, "message": str(e)}

    # ── Market data ──────────────────────────────────────────────────────────

    def get_ltp(self, exchange: str, symbol: str) -> Optional[float]:
        """Fetch last traded price via Shoonya get_quotes API.

        Shoonya's get_quotes expects a numeric exchange token, NOT a trading symbol.
        We resolve the token from the symbols DB.
        """
        client = getattr(self._session, "_client", None)
        if client is None:
            return None
        try:
            # Resolve numeric exchange token from symbol DB
            token = symbol  # fallback: caller may have passed a token directly
            if not symbol.isdigit():
                try:
                    from broker.symbol_normalizer import lookup_by_trading_symbol
                    inst = lookup_by_trading_symbol(symbol, exchange)
                    if inst:
                        # Prefer shoonya-specific token, then generic exchange token
                        resolved = (getattr(inst, 'token', None) or "").strip()
                        if resolved:
                            token = resolved
                            logger.debug("Shoonya LTP: %s → token %s", symbol, token)
                        else:
                            logger.warning("Shoonya LTP: no token in DB for %s", symbol)
                except Exception as e:
                    logger.warning("Shoonya LTP: symbol lookup failed for %s: %s", symbol, e)
            resp = client.get_quotes(exchange=exchange, token=token)
            if resp and isinstance(resp, dict):
                lp = resp.get("lp")
                if lp is not None:
                    return float(lp)
            return None
        except Exception as e:
            logger.warning("get_ltp error for %s:%s — %s", exchange, symbol, e)
            return None

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        client = getattr(self._session, "_client", None)
        if client is None:
            return {"success": False, "message": "Not connected"}
        try:
            resp = client.cancel_order(order_id)
            if resp and isinstance(resp, dict):
                ok = resp.get("stat") == "Ok" or resp.get("result") == "Cancellation request submitted"
                return {"success": ok, "message": resp.get("emsg", "") or "Cancelled"}
            return {"success": bool(resp), "message": "Cancelled" if resp else "Cancel failed"}
        except Exception as e:
            logger.error("cancel_order error: %s", e)
            return {"success": False, "message": str(e)}

    def modify_order(self, order_id: str, modifications: Dict[str, Any]) -> Dict[str, Any]:
        client = getattr(self._session, "_client", None)
        if client is None:
            return {"success": False, "message": "Not connected"}
        try:
            # Shoonya's NorenApi.modify_order() requires positional args:
            #   orderno, exchange, tradingsymbol, newquantity, newprice_type
            # We need to look up the original order to get exchange + tradingsymbol + defaults.
            raw_book = self._session.get_order_book()
            if raw_book is None or (isinstance(raw_book, dict) and raw_book.get("stat") == "Not_Ok"):
                return {"success": False, "message": "Failed to fetch order book from broker — cannot modify"}
            if not isinstance(raw_book, list):
                raw_book = []
            original = None
            for o in raw_book:
                if str(o.get("norenordno", "")) == str(order_id):
                    original = o
                    break
            if not original:
                return {"success": False, "message": f"Order {order_id} not found in Shoonya order book"}

            exchange     = original.get("exch") or original.get("exchange") or "NSE"
            tradingsymbol = original.get("tsym") or original.get("tradingsymbol") or ""
            cur_qty      = int(original.get("qty") or 1)
            cur_prctyp   = (original.get("prctyp") or "LMT").upper()
            cur_price    = float(original.get("prc") or 0)
            cur_trgprc   = float(original.get("trgprc") or 0)

            # Apply modifications
            new_qty = int(modifications.get("quantity") or modifications.get("qty") or cur_qty)

            ot = modifications.get("order_type")
            if ot:
                new_prctyp = self._OTYPE_TO_SHOONYA.get(str(ot).upper(), cur_prctyp)
            else:
                new_prctyp = cur_prctyp

            new_price = float(modifications.get("price") if modifications.get("price") is not None else cur_price)
            new_trgprc = float(modifications.get("trigger_price") if modifications.get("trigger_price") is not None else cur_trgprc)

            kwargs: Dict[str, Any] = {}
            if new_price > 0:
                kwargs["newprice"] = new_price
            if new_trgprc > 0:
                kwargs["newtrigger_price"] = new_trgprc

            logger.info("Shoonya modify order %s: sym=%s exch=%s qty=%d prctyp=%s price=%.2f trgprc=%.2f",
                        order_id, tradingsymbol, exchange, new_qty, new_prctyp, new_price, new_trgprc)

            resp = client.modify_order(
                orderno=order_id,
                exchange=exchange,
                tradingsymbol=tradingsymbol,
                newquantity=new_qty,
                newprice_type=new_prctyp,
                **kwargs,
            )
            if resp and isinstance(resp, dict):
                ok = resp.get("stat") == "Ok"
                return {"success": ok, "message": resp.get("emsg", "") or "Modified", "order_id": order_id}
            return {"success": bool(resp), "message": "Modified" if resp else "Modify failed"}
        except Exception as e:
            logger.error("modify_order error: %s", e, exc_info=True)
            return {"success": False, "message": str(e)}
