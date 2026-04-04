"""
Smart Trader — UniversalOrderCommand (Intent)
=============================================

Ported from shoonya_platform/execution/intent.py

Single canonical data class for all trading commands.
Every order request flowing through CommandService must be
expressed as a UniversalOrderCommand.
"""

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from trading.persistence.order_record import OrderRecord


@dataclass
class UniversalOrderCommand:
    # ── Identity ────────────────────────────────────────────────────────────
    command_id:     str = field(default_factory=lambda: str(uuid.uuid4()))
    source:         str = "STRATEGY"    # STRATEGY | ENGINE | MANUAL | ORDER_WATCHER
    intent:         str = "ENTRY"       # ENTRY | EXIT | ADJUST
    user:           str = ""            # broker login id (for logging)
    strategy_name:  str = ""

    # ── Instrument ──────────────────────────────────────────────────────────
    exchange:   str = "NFO"
    symbol:     str = ""
    side:       str = "BUY"
    quantity:   int = 1
    product:    str = "MIS"

    # ── Order ───────────────────────────────────────────────────────────────
    order_type: str = "MARKET"
    price:      float = 0.0

    # ── Risk ────────────────────────────────────────────────────────────────
    stop_loss:      Optional[float] = None
    target:         Optional[float] = None
    trailing_type:  Optional[str]   = None    # POINTS | PERCENT | ABSOLUTE
    trailing_value: Optional[float] = None
    trail_step:     Optional[float] = None
    trail_when:     Optional[float] = None

    # ── Misc ────────────────────────────────────────────────────────────────
    comment:        str = ""
    test_mode:      bool = False

    # ── Multi-tenant ────────────────────────────────────────────────────────
    # Set by the CommandService layer after auth resolution
    user_id:    int = 0
    client_id:  str = ""

    def with_intent(self, intent: str) -> "UniversalOrderCommand":
        """Return a copy with a different intent."""
        import copy
        c = copy.copy(self)
        c.intent = intent
        return c

    @classmethod
    def from_record(cls, record: OrderRecord, *, order_type: str = "MARKET",
                    price: float = 0.0, source: str = "ORDER_WATCHER") -> "UniversalOrderCommand":
        """Reconstruct a command from a persisted OrderRecord (for re-dispatch)."""
        return cls(
            command_id      = record.command_id,
            source          = source,
            intent          = record.execution_type or "EXIT",
            user            = record.broker_user,
            strategy_name   = record.strategy_name,
            exchange        = record.exchange,
            symbol          = record.symbol,
            side            = record.side,
            quantity        = record.quantity,
            product         = record.product,
            order_type      = order_type or record.order_type or "MARKET",
            price           = price or record.price or 0.0,
            stop_loss       = record.stop_loss,
            target          = record.target,
            trailing_type   = record.trailing_type,
            trailing_value  = record.trailing_value,
            trail_when      = record.trail_when,
        )


def validate_order(cmd: UniversalOrderCommand):
    """
    Hard validation for an order command.
    Raises ValueError on constraint violation.
    """
    if not cmd.symbol:
        raise ValueError("symbol is required")
    if not cmd.exchange:
        raise ValueError("exchange is required")
    if cmd.quantity <= 0:
        raise ValueError(f"quantity must be > 0, got {cmd.quantity}")
    if cmd.side not in ("BUY", "SELL"):
        raise ValueError(f"side must be BUY or SELL, got {cmd.side}")
    if cmd.order_type not in ("MARKET", "LIMIT", "SL", "SL-M"):
        raise ValueError(f"order_type invalid: {cmd.order_type}")
    if cmd.order_type in ("LIMIT", "SL") and cmd.price <= 0:
        raise ValueError(f"price required for LIMIT/SL orders, got {cmd.price}")
