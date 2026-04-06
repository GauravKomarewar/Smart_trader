"""
Smart Trader — Broker Adapter Base
=====================================
Defines the common interface every broker adapter must implement and the
normalised data models returned by all adapters.  Consumers (order routers,
position trackers, risk managers) use ONLY these models — never raw broker
responses.

Normalised models
-----------------
  Position   — open intraday / overnight position
  Order      — order book entry (pending / filled / cancelled)
  Holding    — long-term equity holding in DP
  Funds      — margin / cash limits snapshot
  Trade      — tradebook entry (fill record)

All monetary values are in ₹.  All quantities are lot-count-agnostic (i.e.
the raw quantity field, not multiplied by lot size).
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger("smart_trader.broker.registry")


# ── Normalised data models ─────────────────────────────────────────────────────

@dataclass
class Position:
    symbol:          str           # Trading symbol, e.g. NIFTY25APR24500CE
    exchange:        str           # NSE / BSE / NFO / MCX / CDS
    product:         str           # MIS / NRML / CNC
    qty:             int           # Net qty (negative = short)
    avg_price:       float         # Average entry price
    ltp:             float = 0.0   # Last traded price
    pnl:             float = 0.0   # Total P&L (realised + unrealised)
    realised_pnl:    float = 0.0
    unrealised_pnl:  float = 0.0
    buy_qty:         int   = 0
    sell_qty:        int   = 0
    instrument_type: str   = ""    # EQ / CE / PE / FUT / IDX
    # Enrichment fields set by the registry
    broker:          str   = ""    # "shoonya" | "fyers" | "paper"
    account_id:      str   = ""    # BrokerConfig.id (UUID)
    client_id:       str   = ""    # Human-readable broker client ID

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol":         self.symbol,
            "exchange":       self.exchange,
            "product":        self.product,
            "qty":            self.qty,
            "avg_price":      self.avg_price,
            "ltp":            self.ltp,
            "pnl":            self.pnl,
            "realised_pnl":   self.realised_pnl,
            "unrealised_pnl": self.unrealised_pnl,
            "buy_qty":        self.buy_qty,
            "sell_qty":       self.sell_qty,
            "instrument_type": self.instrument_type,
            "broker":         self.broker,
            "account_id":     self.account_id,
            "client_id":      self.client_id,
        }


@dataclass
class Order:
    order_id:      str
    symbol:        str
    exchange:      str
    side:          str           # BUY / SELL
    product:       str           # MIS / NRML / CNC
    order_type:    str           # MARKET / LIMIT / SL / SL-M
    qty:           int
    price:         float = 0.0
    trigger_price: float = 0.0
    status:        str   = "PENDING"  # PENDING / COMPLETE / CANCELLED / REJECTED
    filled_qty:    int   = 0
    avg_price:     float = 0.0
    timestamp:     str   = ""
    remarks:       str   = ""    # exchange message / rejection reason
    # Enrichment
    broker:        str   = ""
    account_id:    str   = ""
    client_id:     str   = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "order_id":      self.order_id,
            "symbol":        self.symbol,
            "exchange":      self.exchange,
            "side":          self.side,
            "product":       self.product,
            "order_type":    self.order_type,
            "qty":           self.qty,
            "price":         self.price,
            "trigger_price": self.trigger_price,
            "status":        self.status,
            "filled_qty":    self.filled_qty,
            "avg_price":     self.avg_price,
            "timestamp":     self.timestamp,
            "remarks":       self.remarks,
            "broker":        self.broker,
            "account_id":    self.account_id,
            "client_id":     self.client_id,
        }


@dataclass
class Holding:
    symbol:    str
    exchange:  str
    qty:       int           # Total DP qty
    avg_price: float
    ltp:       float = 0.0
    pnl:       float = 0.0   # Unrealised P&L
    pnl_pct:   float = 0.0
    isin:      str   = ""
    dp_qty:    int   = 0     # Demat (T+2) qty
    t1_qty:    int   = 0     # T+1 qty
    # Enrichment
    broker:    str   = ""
    account_id: str  = ""
    client_id: str   = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol":     self.symbol,
            "exchange":   self.exchange,
            "qty":        self.qty,
            "avg_price":  self.avg_price,
            "ltp":        self.ltp,
            "pnl":        self.pnl,
            "pnl_pct":    self.pnl_pct,
            "isin":       self.isin,
            "dp_qty":     self.dp_qty,
            "t1_qty":     self.t1_qty,
            "broker":     self.broker,
            "account_id": self.account_id,
            "client_id":  self.client_id,
        }


@dataclass
class Funds:
    available_cash:  float
    used_margin:     float = 0.0
    total_balance:   float = 0.0
    realised_pnl:    float = 0.0
    unrealised_pnl:  float = 0.0
    collateral:      float = 0.0
    # Enrichment
    broker:          str   = ""
    account_id:      str   = ""
    client_id:       str   = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "available_cash":  self.available_cash,
            "used_margin":     self.used_margin,
            "total_balance":   self.total_balance,
            "realised_pnl":    self.realised_pnl,
            "unrealised_pnl":  self.unrealised_pnl,
            "collateral":      self.collateral,
            "broker":          self.broker,
            "account_id":      self.account_id,
            "client_id":       self.client_id,
        }


@dataclass
class Trade:
    trade_id:  str
    order_id:  str
    symbol:    str
    exchange:  str
    side:      str   # BUY / SELL
    product:   str
    qty:       int
    price:     float
    timestamp: str
    # Enrichment
    broker:    str = ""
    account_id: str = ""
    client_id: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trade_id":  self.trade_id,
            "order_id":  self.order_id,
            "symbol":    self.symbol,
            "exchange":  self.exchange,
            "side":      self.side,
            "product":   self.product,
            "qty":       self.qty,
            "price":     self.price,
            "timestamp": self.timestamp,
            "broker":    self.broker,
            "account_id": self.account_id,
            "client_id": self.client_id,
        }


# ── Broker Adapter ABC ─────────────────────────────────────────────────────────

class BrokerAdapter(ABC):
    """
    Abstract base class for all broker adapters.

    Each adapter wraps one live broker session and translates Smart Trader's
    canonical API into broker-specific calls, returning normalised models.

    Subclasses must implement all abstract methods.  Optional methods
    (get_holdings, get_tradebook) have default no-op implementations.
    """

    def __init__(self, broker_id: str, account_id: str, client_id: str) -> None:
        self.broker_id  = broker_id    # "shoonya" | "fyers" | "paper"
        self.account_id = account_id   # BrokerConfig.id
        self.client_id  = client_id    # human-readable (FA14667, XY1234 …)
        self._log = logging.getLogger(f"smart_trader.broker.{broker_id}")

    # ── Required: must be implemented ──────────────────────────────────────────

    @abstractmethod
    def is_connected(self) -> bool:
        """Return True if a live session is active."""
        ...

    @abstractmethod
    def get_positions(self) -> List[Position]:
        """Return normalised open positions."""
        ...

    @abstractmethod
    def get_order_book(self) -> List[Order]:
        """Return today's normalised order book."""
        ...

    @abstractmethod
    def get_funds(self) -> Funds:
        """Return normalised margin / cash snapshot."""
        ...

    @abstractmethod
    def place_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """
        Place an order.

        Args:
            order: Smart Trader order dict with keys:
                     symbol, exchange, side (BUY/SELL), product, order_type,
                     qty, price (=0 for MARKET), trigger_price (=0 if not SL)

        Returns:
            {"success": bool, "order_id": str, "message": str}
        """
        ...

    @abstractmethod
    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """Cancel a pending order. Returns {"success": bool, "message": str}."""
        ...

    @abstractmethod
    def modify_order(self, order_id: str, modifications: Dict[str, Any]) -> Dict[str, Any]:
        """Modify a pending order. Returns {"success": bool, "message": str}."""
        ...

    # ── Optional: default no-op implementations ────────────────────────────────

    def get_holdings(self) -> List[Holding]:
        """Return long-term DP holdings. Default: empty list."""
        return []

    def get_tradebook(self) -> List[Trade]:
        """Return today's trade fills. Default: empty list."""
        return []

    def get_ltp(self, exchange: str, symbol: str) -> Optional[float]:
        """Return last traded price for a symbol. Default: None (not supported)."""
        return None

    # ── Helpers for subclasses ─────────────────────────────────────────────────

    def _enrich(self, obj: Any) -> Any:
        """Set broker/account_id/client_id enrichment fields on a model."""
        obj.broker     = self.broker_id
        obj.account_id = self.account_id
        obj.client_id  = self.client_id
        return obj
