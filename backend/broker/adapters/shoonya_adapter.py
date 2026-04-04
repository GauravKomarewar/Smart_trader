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
        return [self._enrich(self._map_position(p)) for p in (raw or [])]

    @staticmethod
    def _map_position(raw: dict) -> Position:
        # Support both Shoonya-native AND Fyers-normalised aliases
        symbol   = raw.get("tsym") or raw.get("tradingsymbol") or raw.get("symbol") or ""
        exchange = raw.get("exch") or raw.get("exchange") or ""
        prd_raw  = raw.get("prd") or raw.get("productType") or ""
        product  = _PRD_MAP.get(prd_raw.upper(), prd_raw)
        net_qty  = int(raw.get("netqty") or raw.get("qty") or raw.get("netQty") or 0)
        avg_prc  = float(raw.get("avgprc") or raw.get("avg_price") or raw.get("buyAvg") or 0)
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
        client = getattr(self._session, "_client", None)
        if client is None:
            return []
        try:
            raw_list = client.get_holdings()
            return [self._enrich(self._map_holding(h)) for h in (raw_list or [])]
        except Exception as e:
            logger.warning("get_holdings error: %s", e)
            return []

    @staticmethod
    def _map_holding(raw: dict) -> Holding:
        symbol  = raw.get("tsym") or raw.get("tradingsymbol") or ""
        exch    = raw.get("exch") or raw.get("exchange") or "NSE"
        qty     = int(raw.get("holdqty") or raw.get("qty") or 0)
        dp_qty  = int(raw.get("dpqty") or raw.get("dp_qty") or qty)
        t1_qty  = int(raw.get("t1qty") or raw.get("t1_qty") or 0)
        avg_prc = float(raw.get("avgprc") or raw.get("avg_price") or 0)
        ltp     = float(raw.get("lp") or raw.get("ltp") or 0)
        pnl     = (ltp - avg_prc) * qty if ltp and avg_prc else 0.0
        pnl_pct = (pnl / (avg_prc * qty) * 100) if avg_prc and qty else 0.0
        isin    = raw.get("isin") or ""
        return Holding(
            symbol=symbol, exchange=exch, qty=qty, avg_price=avg_prc,
            ltp=ltp, pnl=pnl, pnl_pct=pnl_pct, isin=isin,
            dp_qty=dp_qty, t1_qty=t1_qty,
        )

    # ── Tradebook ──────────────────────────────────────────────────────────────

    def get_tradebook(self) -> List[Trade]:
        client = getattr(self._session, "_client", None)
        if client is None:
            return []
        try:
            raw_fn = getattr(client, "get_trade_book", None) or getattr(client, "tradebook", None)
            if raw_fn is None:
                return []
            raw_list = raw_fn() or []
            return [self._enrich(self._map_trade(t)) for t in raw_list]
        except Exception as e:
            logger.warning("get_tradebook error: %s", e)
            return []

    @staticmethod
    def _map_trade(raw: dict) -> Trade:
        return Trade(
            trade_id=str(raw.get("ftm") or raw.get("trade_id") or ""),
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

    def place_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        return self._session.place_order(order)

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
            resp = client.modify_order(order_id, modifications)
            if resp and isinstance(resp, dict):
                ok = resp.get("stat") == "Ok"
                return {"success": ok, "message": resp.get("emsg", "") or "Modified"}
            return {"success": bool(resp), "message": "Modified" if resp else "Modify failed"}
        except Exception as e:
            logger.error("modify_order error: %s", e)
            return {"success": False, "message": str(e)}
