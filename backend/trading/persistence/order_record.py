"""
Smart Trader — OrderRecord
==========================

OMS STATUS CONTRACT:
  CREATED        : Intent persisted, not yet sent to broker
  SENT_TO_BROKER : Broker accepted, order_id assigned
  EXECUTED       : Filled at broker (from OrderWatcher reconciliation)
  CANCELLED      : Cancelled by user / broker
  FAILED         : Broker rejected / expired / unrecoverable failure

Multi-tenant: every record is scoped to (user_id, client_id).
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class OrderRecord:
    # ── Identity ────────────────────────────────────────────────────────────
    command_id:     str
    source:         str           # STRATEGY | ENGINE | MANUAL
    broker_user:    str           # broker login ID
    strategy_name:  str

    # ── Instrument ──────────────────────────────────────────────────────────
    exchange:   str
    symbol:     str
    side:       str               # BUY | SELL
    quantity:   int
    product:    str               # MIS | NRML | CNC

    # ── Order ───────────────────────────────────────────────────────────────
    order_type: str               # MARKET | LIMIT | SL
    price:      float

    # ── Risk ────────────────────────────────────────────────────────────────
    stop_loss:      Optional[float]
    target:         Optional[float]
    trailing_type:  Optional[str]   # POINTS | PERCENT | ABSOLUTE | None
    trailing_value: Optional[float]
    trail_when:     Optional[float] = None

    # ── Managed exit state ──────────────────────────────────────────────────
    managed_anchor_ltp:     Optional[float] = None
    managed_base_stop_loss: Optional[float] = None

    # ── Execution ───────────────────────────────────────────────────────────
    client_id: Optional[str] = None
    broker_order_id: Optional[str] = None
    execution_type:  str = "ENTRY"   # ENTRY | EXIT | ADJUST

    # ── State ───────────────────────────────────────────────────────────────
    status:     str = "CREATED"   # CREATED | SENT_TO_BROKER | EXECUTED | CANCELLED | FAILED
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    tag:        Optional[str] = None
