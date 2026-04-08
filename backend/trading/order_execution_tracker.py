"""
OrderExecutionTracker — Background engine that ensures Smart-MKT orders get filled.

Problem:
  SEBI banned raw MARKET orders; we convert MKT→LIMIT with LTP ± 0.5% buffer.
  But if the market moves fast (especially in multi-leg orders), the limit price
  may become stale and the order remains OPEN/PENDING unfilled.

Solution:
  This engine watches recently-placed Smart-MKT orders.  If an order stays
  OPEN for more than a few seconds, it modifies the price to the current LTP
  (with a fresh buffer) so the fill actually happens — behaving like a true
  market order while remaining SEBI-compliant.

Design:
  • Place-order code tags smart-MKT orders with `_smart_mkt=True`.
  • After each smart-MKT placement, the order_id + metadata are registered here.
  • A daemon thread polls every 2 s; for every tracked order:
      - Fetch current status from broker order book.
      - If COMPLETE/FILLED → remove from tracking (nothing to do).
      - If REJECTED/CANCELLED → remove from tracking, log warning.
      - If still OPEN/PENDING → fetch fresh LTP, compute new price, modify order.
  • Orders are tracked for a maximum of 60 s (configurable).  After that they are
    assumed stuck and cancelled (safety net).

Thread-safety: protected via a threading.Lock.
"""
from __future__ import annotations

import logging
import math
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

logger = logging.getLogger("smart_trader.exec_tracker")

_POLL_INTERVAL = 2.0     # seconds between checks
_MAX_TRACK_SEC = 60.0    # stop tracking after this many seconds
_MODIFY_AFTER  = 3.0     # wait at least this many seconds before first modify
_MAX_MODIFIES  = 10       # max modify attempts per order (safety)


@dataclass
class TrackedOrder:
    """Metadata for a single smart-MKT order being tracked."""
    user_id:    str
    config_id:  str
    order_id:   str
    symbol:     str
    exchange:   str
    side:       str           # BUY / SELL
    qty:        int
    original_price: float     # price we initially placed
    placed_at:  float = field(default_factory=time.time)
    modifies:   int   = 0
    last_modify: float = 0.0
    tag:        str   = ""


class OrderExecutionTracker:
    """Singleton background engine for smart-MKT fill assurance."""

    def __init__(self):
        self._tracked: Dict[str, TrackedOrder] = {}  # order_id → TrackedOrder
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._running = False

    # ── Public API ────────────────────────────────────────────────────────────

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True, name="ExecTracker")
        self._thread.start()
        logger.info("OrderExecutionTracker started (poll=%.1fs, max_track=%.0fs)", _POLL_INTERVAL, _MAX_TRACK_SEC)

    def stop(self):
        self._running = False

    def track(
        self,
        user_id: str,
        config_id: str,
        order_id: str,
        symbol: str,
        exchange: str,
        side: str,
        qty: int,
        price: float,
        tag: str = "",
    ):
        """Register a smart-MKT order for fill tracking."""
        if not order_id:
            return
        entry = TrackedOrder(
            user_id=user_id,
            config_id=config_id,
            order_id=order_id,
            symbol=symbol,
            exchange=exchange,
            side=side,
            qty=qty,
            original_price=price,
            tag=tag,
        )
        with self._lock:
            self._tracked[order_id] = entry
        logger.info(
            "TRACK-ORDER %s %s %s %s qty=%d px=%.2f tag=%s",
            config_id[:8], symbol, side, order_id, qty, price, tag,
        )

    @property
    def tracked_count(self) -> int:
        with self._lock:
            return len(self._tracked)

    # ── Background loop ───────────────────────────────────────────────────────

    def _loop(self):
        while self._running:
            try:
                self._poll()
            except Exception as e:
                logger.error("OrderExecutionTracker loop error: %s", e, exc_info=True)
            time.sleep(_POLL_INTERVAL)

    def _poll(self):
        with self._lock:
            snapshot = dict(self._tracked)
        if not snapshot:
            return

        from broker.multi_broker import registry

        now = time.time()

        for order_id, entry in snapshot.items():
            age = now - entry.placed_at

            # Expired — cancel and remove
            if age > _MAX_TRACK_SEC:
                logger.warning(
                    "EXEC-TRACKER EXPIRED %s %s order_id=%s after %.0fs — cancelling",
                    entry.config_id[:8], entry.symbol, order_id, age,
                )
                try:
                    registry.cancel_order(entry.user_id, entry.config_id, order_id)
                except Exception as e:
                    logger.warning("Cancel expired order failed: %s", e)
                self._remove(order_id)
                continue

            # Check order status
            sess = registry.get_session(entry.user_id, entry.config_id)
            if not sess:
                continue

            try:
                orders = sess.get_order_book()
            except Exception:
                continue

            order_status = None
            for o in orders:
                oid = str(o.get("orderId") or o.get("order_id") or o.get("id") or "")
                if oid == order_id:
                    order_status = str(o.get("status") or "").upper()
                    break

            if order_status is None:
                # Order not found in book — might be too early or already done
                if age > 10:
                    logger.info("EXEC-TRACKER order_id=%s not found after %.0fs — removing", order_id, age)
                    self._remove(order_id)
                continue

            # Terminal states
            if order_status in ("COMPLETE", "FILLED", "FIL"):
                logger.info(
                    "EXEC-TRACKER FILLED %s %s order_id=%s in %.1fs",
                    entry.config_id[:8], entry.symbol, order_id, age,
                )
                self._remove(order_id)
                continue

            if order_status in ("REJECTED", "CANCELLED", "CANCELED"):
                logger.warning(
                    "EXEC-TRACKER %s %s %s order_id=%s after %.1fs",
                    order_status, entry.config_id[:8], entry.symbol, order_id, age,
                )
                self._remove(order_id)
                continue

            # Still OPEN/PENDING — modify price if enough time has passed
            if age < _MODIFY_AFTER:
                continue  # give it a few seconds to fill naturally

            if entry.modifies >= _MAX_MODIFIES:
                logger.warning(
                    "EXEC-TRACKER MAX_MODIFIES %s %s order_id=%s — cancelling",
                    entry.config_id[:8], entry.symbol, order_id,
                )
                try:
                    registry.cancel_order(entry.user_id, entry.config_id, order_id)
                except Exception:
                    pass
                self._remove(order_id)
                continue

            # Fetch fresh LTP
            try:
                adapter = getattr(sess, "_adapter", None)
                if adapter is None:
                    continue
                ltp = adapter.get_ltp(entry.exchange, entry.symbol)
                if not ltp or ltp <= 0:
                    continue
            except Exception:
                continue

            # Calculate new smart price
            exch = entry.exchange.upper()
            tick = 1.0 if exch == "MCX" else 0.05
            buffer = 1.005 if entry.side == "BUY" else 0.995
            new_price = round(ltp * buffer, 2)
            if entry.side == "BUY":
                new_price = math.ceil(new_price / tick) * tick
            else:
                new_price = math.floor(new_price / tick) * tick
            new_price = int(new_price) if exch == "MCX" else round(new_price, 2)

            # Only modify if price actually changed
            if new_price == entry.original_price:
                continue

            logger.info(
                "EXEC-TRACKER MODIFY %s %s order_id=%s old_px=%.2f new_px=%.2f ltp=%.2f attempt=%d",
                entry.config_id[:8], entry.symbol, order_id,
                entry.original_price, new_price, ltp, entry.modifies + 1,
            )

            try:
                result = registry.modify_order(
                    entry.user_id, entry.config_id, order_id,
                    {"price": new_price},
                )
                if result.get("success"):
                    with self._lock:
                        if order_id in self._tracked:
                            self._tracked[order_id].original_price = new_price
                            self._tracked[order_id].modifies += 1
                            self._tracked[order_id].last_modify = time.time()
                else:
                    logger.warning(
                        "EXEC-TRACKER MODIFY FAILED %s: %s",
                        order_id, result.get("message"),
                    )
                    # If modify fails due to already filled/cancelled, remove
                    msg = str(result.get("message", "")).upper()
                    if any(k in msg for k in ("COMPLETE", "REJECT", "CANCEL", "NOT FOUND", "INVALID")):
                        self._remove(order_id)
            except Exception as e:
                logger.warning("EXEC-TRACKER modify error %s: %s", order_id, e)

    def _remove(self, order_id: str):
        with self._lock:
            self._tracked.pop(order_id, None)


# ── Module-level singleton ────────────────────────────────────────────────────
execution_tracker = OrderExecutionTracker()
