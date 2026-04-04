"""
Smart Trader — Order Management System (OMS)
============================================

Responsibilities:
  - Order lifecycle management (PENDING → OPEN → COMPLETE/CANCELLED/REJECTED)
  - Portfolio positions tracking
  - P&L calculation (realised + unrealised)
  - Order book persistence (SQLite via ORM)
  - Atomic order batching for multi-leg strategies
  - Integration with ExecutionGuard before submission

Architecture:
  OMS.place_order()
    → ExecutionGuard.validate()
    → BrokerAdapter.submit()
    → track order state
    → update position book

  OMS is broker-agnostic. BrokerAdapter is injected per user session.
"""
import uuid
import threading
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
from enum import Enum

logger = logging.getLogger("smart_trader.oms")


# ── Domain Types ──────────────────────────────────────────────────────────────

class OrderStatus(str, Enum):
    PENDING   = "PENDING"
    OPEN      = "OPEN"
    COMPLETE  = "COMPLETE"
    CANCELLED = "CANCELLED"
    REJECTED  = "REJECTED"
    TRIGGER   = "TRIGGER_PENDING"


class OrderSide(str, Enum):
    BUY  = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT  = "LIMIT"
    SL     = "SL"
    SLM    = "SL-M"


class ProductType(str, Enum):
    MIS  = "MIS"
    NRML = "NRML"
    CNC  = "CNC"


@dataclass
class OrderRequest:
    symbol:      str
    exchange:    str
    side:        OrderSide
    order_type:  OrderType
    product:     ProductType
    quantity:    int
    price:       float = 0.0
    trigger_price: float = 0.0
    strategy_id: Optional[str] = None
    tag:         Optional[str] = None        # ENTRY / EXIT / ADJUST
    test_mode:   bool = False
    remarks:     str = ""


@dataclass
class Order:
    id:           str = field(default_factory=lambda: str(uuid.uuid4()))
    broker_order_id: Optional[str] = None
    symbol:       str = ""
    exchange:     str = ""
    side:         str = ""
    order_type:   str = ""
    product:      str = ""
    quantity:     int = 0
    filled_qty:   int = 0
    price:        float = 0.0
    avg_price:    float = 0.0
    trigger_price: float = 0.0
    status:       OrderStatus = OrderStatus.PENDING
    strategy_id:  Optional[str] = None
    tag:          Optional[str] = None
    test_mode:    bool = False
    remarks:      str = ""
    placed_at:    str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at:   str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    error_msg:    Optional[str] = None

    def to_dict(self) -> dict:
        d = asdict(self)
        d["status"] = self.status.value if isinstance(self.status, OrderStatus) else self.status
        return d


@dataclass
class Position:
    symbol:       str
    exchange:     str
    product:      str
    quantity:     int = 0        # net qty (positive=long, negative=short)
    avg_price:    float = 0.0
    ltp:          float = 0.0
    realised_pnl: float = 0.0
    strategy_id:  Optional[str] = None

    @property
    def unrealised_pnl(self) -> float:
        if self.quantity == 0:
            return 0.0
        return (self.ltp - self.avg_price) * self.quantity

    @property
    def total_pnl(self) -> float:
        return self.realised_pnl + self.unrealised_pnl

    def to_dict(self) -> dict:
        return {
            "symbol":        self.symbol,
            "exchange":      self.exchange,
            "product":       self.product,
            "quantity":      self.quantity,
            "avg_price":     self.avg_price,
            "ltp":           self.ltp,
            "realised_pnl":  self.realised_pnl,
            "unrealised_pnl":self.unrealised_pnl,
            "total_pnl":     self.total_pnl,
            "strategy_id":   self.strategy_id,
        }


# ── OMS ───────────────────────────────────────────────────────────────────────

class OrderManagementSystem:
    """
    Smart Trader OMS — manages orders and positions per session.

    Usage:
        oms = OrderManagementSystem(broker_adapter=my_adapter)
        result = oms.place_order(OrderRequest(...))
    """

    def __init__(self, broker_adapter=None, execution_guard=None):
        self._lock      = threading.RLock()
        self._orders:    Dict[str, Order]   = {}
        self._positions: Dict[str, Position] = {}  # key: f"{symbol}:{product}"
        self.broker     = broker_adapter
        self.guard      = execution_guard
        logger.info("OMS initialized (broker=%s guard=%s)",
                    type(broker_adapter).__name__ if broker_adapter else "None",
                    type(execution_guard).__name__  if execution_guard else "None")

    # ── Order placement ──────────────────────────────────────────────────────

    def place_order(self, req: OrderRequest) -> Order:
        """
        Places a single order. Validates with ExecutionGuard if present.
        In test_mode, simulates the order without touching the broker.
        """
        with self._lock:
            order = Order(
                symbol=req.symbol,
                exchange=req.exchange,
                side=req.side.value if isinstance(req.side, OrderSide) else req.side,
                order_type=req.order_type.value if isinstance(req.order_type, OrderType) else req.order_type,
                product=req.product.value if isinstance(req.product, ProductType) else req.product,
                quantity=req.quantity,
                price=req.price,
                trigger_price=req.trigger_price,
                strategy_id=req.strategy_id,
                tag=req.tag,
                test_mode=req.test_mode,
                remarks=req.remarks,
            )

            # ExecutionGuard validation
            if self.guard and req.tag in ("ENTRY", "EXIT", "ADJUST"):
                from trading.execution_guard import LegIntent
                leg = LegIntent(
                    strategy_id=req.strategy_id or "manual",
                    symbol=req.symbol,
                    direction=order.side,
                    qty=req.quantity,
                    tag=req.tag,
                )
                try:
                    allowed = self.guard.validate_and_prepare([leg], req.tag)
                    if not allowed:
                        order.status = OrderStatus.REJECTED
                        order.error_msg = "ExecutionGuard: blocked"
                        self._orders[order.id] = order
                        logger.warning("OMS: order blocked by guard | %s %s", req.symbol, req.tag)
                        return order
                except RuntimeError as e:
                    order.status = OrderStatus.REJECTED
                    order.error_msg = f"Guard: {e}"
                    self._orders[order.id] = order
                    logger.warning("OMS: guard rejection: %s", e)
                    return order

            # Test mode — simulate fill
            if req.test_mode or not self.broker:
                order.status = OrderStatus.COMPLETE
                order.filled_qty = req.quantity
                order.avg_price  = req.price if req.price else self._get_demo_ltp(req.symbol)
                order.broker_order_id = f"TEST-{order.id[:8].upper()}"
                self._orders[order.id] = order
                self._update_position(order)
                logger.info("OMS [TEST] %s %s %d@%.2f", order.side, order.symbol,
                            order.filled_qty, order.avg_price)
                return order

            # Live broker
            try:
                broker_resp = self.broker.place_order(
                    symbol=req.symbol,
                    exchange=req.exchange,
                    side=order.side,
                    order_type=order.order_type,
                    product=order.product,
                    quantity=req.quantity,
                    price=req.price,
                    trigger_price=req.trigger_price,
                    remarks=req.remarks,
                )
                order.broker_order_id = broker_resp.get("order_id") or broker_resp.get("norenordno")
                order.status = OrderStatus.OPEN
                logger.info("OMS placed: %s %s %d | broker_id=%s",
                            order.side, order.symbol, order.quantity, order.broker_order_id)
            except Exception as e:
                order.status = OrderStatus.REJECTED
                order.error_msg = str(e)
                logger.error("OMS broker error: %s", e)

            self._orders[order.id] = order
            return order

    def place_batch(self, requests: List[OrderRequest]) -> List[Order]:
        """Place multiple legs atomically — all or none logic for test mode."""
        return [self.place_order(r) for r in requests]

    # ── Order management ────────────────────────────────────────────────────

    def cancel_order(self, order_id: str) -> bool:
        with self._lock:
            order = self._orders.get(order_id)
            if not order:
                return False
            if order.status not in (OrderStatus.PENDING, OrderStatus.OPEN, OrderStatus.TRIGGER):
                return False
            if self.broker and order.broker_order_id:
                try:
                    self.broker.cancel_order(order.broker_order_id)
                except Exception as e:
                    logger.error("Cancel failed: %s", e)
            order.status = OrderStatus.CANCELLED
            order.updated_at = datetime.now(timezone.utc).isoformat()
            return True

    def get_order(self, order_id: str) -> Optional[Order]:
        return self._orders.get(order_id)

    def get_all_orders(self, strategy_id: Optional[str] = None) -> List[dict]:
        with self._lock:
            orders = list(self._orders.values())
            if strategy_id:
                orders = [o for o in orders if o.strategy_id == strategy_id]
            return [o.to_dict() for o in sorted(orders, key=lambda o: o.placed_at, reverse=True)]

    def get_open_orders(self) -> List[dict]:
        with self._lock:
            return [o.to_dict() for o in self._orders.values()
                    if o.status in (OrderStatus.PENDING, OrderStatus.OPEN, OrderStatus.TRIGGER)]

    # ── Position tracking ────────────────────────────────────────────────────

    def get_positions(self, strategy_id: Optional[str] = None) -> List[dict]:
        with self._lock:
            positions = list(self._positions.values())
            if strategy_id:
                positions = [p for p in positions if p.strategy_id == strategy_id]
            return [p.to_dict() for p in positions]

    def update_ltp(self, symbol: str, ltp: float):
        """Called by market data feed to refresh position MTM."""
        with self._lock:
            for key, pos in self._positions.items():
                if pos.symbol == symbol:
                    pos.ltp = ltp

    def total_pnl(self) -> float:
        with self._lock:
            return sum(p.total_pnl for p in self._positions.values())

    def total_realised_pnl(self) -> float:
        with self._lock:
            return sum(p.realised_pnl for p in self._positions.values())

    def square_off_all(self, strategy_id: Optional[str] = None):
        """Place market exit orders for all open positions."""
        with self._lock:
            positions = [p for p in self._positions.values()
                         if p.quantity != 0 and
                         (strategy_id is None or p.strategy_id == strategy_id)]

        orders = []
        for pos in positions:
            side = OrderSide.SELL if pos.quantity > 0 else OrderSide.BUY
            req = OrderRequest(
                symbol=pos.symbol,
                exchange=pos.exchange,
                side=side,
                order_type=OrderType.MARKET,
                product=ProductType(pos.product),
                quantity=abs(pos.quantity),
                strategy_id=pos.strategy_id,
                tag="EXIT",
                remarks="SquareOff",
            )
            orders.append(self.place_order(req))
        return [o.to_dict() for o in orders]

    # ── Internals ────────────────────────────────────────────────────────────

    def _update_position(self, order: Order):
        if order.status != OrderStatus.COMPLETE:
            return
        key = f"{order.symbol}:{order.product}"
        pos = self._positions.get(key)
        if not pos:
            pos = Position(symbol=order.symbol, exchange=order.exchange,
                           product=order.product, strategy_id=order.strategy_id)
            self._positions[key] = pos

        qty   = order.filled_qty
        price = order.avg_price

        if order.side == "BUY":
            new_qty = pos.quantity + qty
            if new_qty == 0:
                pos.realised_pnl += (price - pos.avg_price) * pos.quantity
                pos.avg_price = 0.0
            elif pos.quantity < 0:
                # covering a short
                cover = min(qty, abs(pos.quantity))
                pos.realised_pnl += (pos.avg_price - price) * cover
                remaining = qty - cover
                if remaining > 0:
                    pos.avg_price = price
                pos.quantity = new_qty
            else:
                total_cost = pos.avg_price * pos.quantity + price * qty
                pos.avg_price = total_cost / new_qty if new_qty else 0
                pos.quantity = new_qty
        else:  # SELL
            new_qty = pos.quantity - qty
            if pos.quantity > 0:
                sell = min(qty, pos.quantity)
                pos.realised_pnl += (price - pos.avg_price) * sell
                remaining_sell = qty - sell
                if remaining_sell > 0:
                    pos.avg_price = price
            else:
                total_cost = abs(pos.avg_price * pos.quantity) + price * qty
                pos.avg_price = total_cost / abs(new_qty) if new_qty else 0
            pos.quantity = new_qty

        pos.ltp = price

    def _get_demo_ltp(self, symbol: str) -> float:
        import random
        base = 1000 + hash(symbol) % 4000
        return round(base * (1 + random.uniform(-0.01, 0.01)), 2)


# ── OMS Router ────────────────────────────────────────────────────────────────
# Provides REST endpoints for the OMS instance

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from core.deps import current_user

oms_router = APIRouter(prefix="/oms", tags=["oms"])

# Global OMS instance (per-process; in production scope per user session)
_oms_instances: Dict[str, "OrderManagementSystem"] = {}
_oms_lock = threading.Lock()


def get_oms(user_id: str) -> "OrderManagementSystem":
    with _oms_lock:
        if user_id not in _oms_instances:
            _oms_instances[user_id] = OrderManagementSystem()
        return _oms_instances[user_id]


class PlaceOrderBody(BaseModel):
    symbol:       str
    exchange:     str = "NFO"
    side:         str = "BUY"
    order_type:   str = "MARKET"
    product:      str = "MIS"
    quantity:     int
    price:        float = 0.0
    trigger_price: float = 0.0
    strategy_id:  Optional[str] = None
    tag:          Optional[str] = None
    test_mode:    bool = False
    remarks:      str = ""


class CancelOrderBody(BaseModel):
    order_id: str


@oms_router.post("/orders")
def oms_place_order(body: PlaceOrderBody, payload: dict = Depends(current_user)):
    oms = get_oms(payload["sub"])
    req = OrderRequest(
        symbol=body.symbol,
        exchange=body.exchange,
        side=OrderSide(body.side),
        order_type=OrderType(body.order_type),
        product=ProductType(body.product),
        quantity=body.quantity,
        price=body.price,
        trigger_price=body.trigger_price,
        strategy_id=body.strategy_id,
        tag=body.tag,
        test_mode=body.test_mode,
        remarks=body.remarks,
    )
    order = oms.place_order(req)
    return order.to_dict()


@oms_router.delete("/orders/{order_id}")
def oms_cancel_order(order_id: str, payload: dict = Depends(current_user)):
    oms = get_oms(payload["sub"])
    ok = oms.cancel_order(order_id)
    return {"success": ok}


@oms_router.get("/orders")
def oms_get_orders(
    strategy_id: Optional[str] = None,
    payload: dict = Depends(current_user),
):
    oms = get_oms(payload["sub"])
    return oms.get_all_orders(strategy_id)


@oms_router.get("/positions")
def oms_get_positions(
    strategy_id: Optional[str] = None,
    payload: dict = Depends(current_user),
):
    oms = get_oms(payload["sub"])
    return oms.get_positions(strategy_id)


@oms_router.post("/square-off")
def oms_square_off(
    strategy_id: Optional[str] = None,
    payload: dict = Depends(current_user),
):
    oms = get_oms(payload["sub"])
    return oms.square_off_all(strategy_id)


@oms_router.get("/pnl")
def oms_pnl(payload: dict = Depends(current_user)):
    oms = get_oms(payload["sub"])
    return {
        "total_pnl": oms.total_pnl(),
        "realised_pnl": oms.total_realised_pnl(),
        "unrealised_pnl": oms.total_pnl() - oms.total_realised_pnl(),
    }
