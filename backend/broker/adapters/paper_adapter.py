"""
Smart Trader — Paper Trade Adapter
=====================================
Simulates a live broker for paper/demo trading.

Maintains an in-memory order book and positions.  Orders are filled
immediately at the price given (or at a synthetic LTP for MARKET orders).
"""
from __future__ import annotations

import logging
import time as _time
from typing import Any, Dict, List
from uuid import uuid4

from broker.adapters.base import BrokerAdapter, Funds, Holding, Order, Position, Trade

logger = logging.getLogger("smart_trader.broker.paper")


class PaperAdapter(BrokerAdapter):
    """
    Paper trading adapter — in-memory simulated broker.

    Suitable for:
    * Testing order placement flows end-to-end
    * Demo users who have no live broker account
    * Regression testing without hitting real APIs
    """

    def __init__(
        self,
        account_id: str,
        client_id: str = "PAPER",
        starting_capital: float = 1_000_000.0,
    ) -> None:
        super().__init__("paper", account_id, client_id)
        self._capital   = starting_capital
        self._orders:    List[Dict[str, Any]] = []
        self._positions: Dict[str, Dict[str, Any]] = {}  # symbol → position dict
        self._trades:    List[Dict[str, Any]] = []

    # ── Connection ─────────────────────────────────────────────────────────────

    def is_connected(self) -> bool:
        return True

    # ── Positions ──────────────────────────────────────────────────────────────

    def get_positions(self) -> List[Position]:
        out = []
        for pos in self._positions.values():
            if pos["qty"] != 0:
                out.append(self._enrich(Position(
                    symbol=pos["symbol"],
                    exchange=pos.get("exchange", "NSE"),
                    product=pos.get("product", "MIS"),
                    qty=pos["qty"],
                    avg_price=pos["avg_price"],
                    ltp=pos.get("ltp", pos["avg_price"]),
                    pnl=pos.get("pnl", 0.0),
                    realised_pnl=pos.get("realised_pnl", 0.0),
                    unrealised_pnl=pos.get("unrealised_pnl", 0.0),
                    buy_qty=pos.get("buy_qty", 0),
                    sell_qty=pos.get("sell_qty", 0),
                )))
        return out

    # ── Order book ─────────────────────────────────────────────────────────────

    def get_order_book(self) -> List[Order]:
        return [
            self._enrich(Order(
                order_id=o["order_id"],
                symbol=o["symbol"],
                exchange=o.get("exchange", "NSE"),
                side=o["side"],
                product=o.get("product", "MIS"),
                order_type=o.get("order_type", "MARKET"),
                qty=o["qty"],
                price=o.get("price", 0.0),
                trigger_price=o.get("trigger_price", 0.0),
                status=o.get("status", "COMPLETE"),
                filled_qty=o.get("filled_qty", o["qty"]),
                avg_price=o.get("avg_price", o.get("price", 0.0)),
                timestamp=o.get("timestamp", ""),
            ))
            for o in self._orders
        ]

    # ── Funds ──────────────────────────────────────────────────────────────────

    def get_funds(self) -> Funds:
        total_pnl = sum(p.get("pnl", 0) for p in self._positions.values())
        return self._enrich(Funds(
            available_cash=max(0.0, self._capital + total_pnl),
            total_balance=self._capital,
        ))

    # ── Holdings ───────────────────────────────────────────────────────────────

    def get_holdings(self) -> List[Holding]:
        return []  # Paper trader has no DP holdings

    # ── Tradebook ──────────────────────────────────────────────────────────────

    def get_tradebook(self) -> List[Trade]:
        return [
            self._enrich(Trade(
                trade_id=t["trade_id"],
                order_id=t["order_id"],
                symbol=t["symbol"],
                exchange=t.get("exchange", "NSE"),
                side=t["side"],
                product=t.get("product", "MIS"),
                qty=t["qty"],
                price=t["price"],
                timestamp=t.get("timestamp", ""),
            ))
            for t in self._trades
        ]

    # ── Order management ───────────────────────────────────────────────────────

    def place_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """Simulate an immediate fill at the given price (or 0 for MARKET)."""
        order_id  = f"PAPER-{uuid4().hex[:8].upper()}"
        symbol    = order.get("symbol", "")
        exchange  = order.get("exchange", "NSE")
        side      = str(order.get("side", "BUY")).upper()
        product   = str(order.get("product", "MIS")).upper()
        qty       = int(order.get("qty", 0))
        price     = float(order.get("price") or 100.0)  # use 100 for MARKET simulation
        ts        = str(int(_time.time()))

        # Record the order
        self._orders.append({
            "order_id":  order_id,
            "symbol":    symbol,
            "exchange":  exchange,
            "side":      side,
            "product":   product,
            "order_type": order.get("order_type", "MARKET"),
            "qty":       qty,
            "price":     price,
            "trigger_price": float(order.get("trigger_price", 0) or 0),
            "status":    "COMPLETE",
            "filled_qty": qty,
            "avg_price": price,
            "timestamp": ts,
        })

        # Update positions (simple net qty model)
        key = f"{exchange}:{symbol}:{product}"
        pos = self._positions.get(key, {
            "symbol": symbol, "exchange": exchange, "product": product,
            "qty": 0, "avg_price": 0.0, "buy_qty": 0, "sell_qty": 0,
            "realised_pnl": 0.0, "unrealised_pnl": 0.0, "pnl": 0.0,
        })
        if side == "BUY":
            new_qty   = pos["qty"] + qty
            pos["avg_price"] = ((pos["avg_price"] * pos["qty"]) + (price * qty)) / new_qty if new_qty else price
            pos["qty"]       = new_qty
            pos["buy_qty"]   = pos.get("buy_qty", 0) + qty
        else:
            realised = (price - pos["avg_price"]) * min(qty, max(0, pos["qty"]))
            pos["realised_pnl"] = pos.get("realised_pnl", 0) + realised
            pos["qty"]          = pos["qty"] - qty
            pos["sell_qty"]     = pos.get("sell_qty", 0) + qty
        pos["pnl"] = pos["realised_pnl"] + pos.get("unrealised_pnl", 0)
        self._positions[key] = pos

        # Record trade fill
        self._trades.append({
            "trade_id": f"FILL-{uuid4().hex[:6].upper()}",
            "order_id": order_id,
            "symbol": symbol, "exchange": exchange,
            "side": side, "product": product,
            "qty": qty, "price": price, "timestamp": ts,
        })

        logger.info("PAPER order filled: %s %s %s %d @ %.2f  [id=%s]",
                    side, qty, symbol, qty, price, order_id)
        return {"success": True, "order_id": order_id, "message": f"Paper order filled @ {price}"}

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        for o in self._orders:
            if o["order_id"] == order_id and o["status"] == "PENDING":
                o["status"] = "CANCELLED"
                return {"success": True, "message": "Cancelled"}
        return {"success": False, "message": "Order not found or already filled"}

    def modify_order(self, order_id: str, modifications: Dict[str, Any]) -> Dict[str, Any]:
        for o in self._orders:
            if o["order_id"] == order_id and o["status"] == "PENDING":
                o.update(modifications)
                return {"success": True, "message": "Modified"}
        return {"success": False, "message": "Order not found or already filled"}
