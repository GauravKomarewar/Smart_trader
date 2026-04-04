"""
Smart Trader — Positions Router
=================================

Endpoints:
  GET  /api/positions              — all open positions with SL/target data
  GET  /api/positions/orders       — recent order history from PostgreSQL
  POST /api/positions/{id}/cancel  — cancel a pending order
  PUT  /api/positions/{id}/sl      — update stop-loss on a managed exit
"""

from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from core.deps import current_user

positions_router = APIRouter(prefix="/positions", tags=["positions"])


def _get_repo(user_sub: str):
    from trading.bot_core import _bots
    for key, bot in _bots.items():
        if key.startswith(f"{user_sub}:"):
            return bot.order_repo
    return None


@positions_router.get("")
def get_positions(payload: dict = Depends(current_user)):
    """Open positions from PostgreSQL (EXECUTED, not yet closed)."""
    repo = _get_repo(payload["sub"])
    if repo is None:
        return []
    orders = repo.get_open_orders()
    return [vars(o) if not isinstance(o, dict) else o for o in orders]


@positions_router.get("/orders")
def get_orders(
    limit: int = 100,
    strategy: Optional[str] = None,
    payload: dict = Depends(current_user),
):
    """Recent order history."""
    repo = _get_repo(payload["sub"])
    if repo is None:
        return []
    orders = repo.get_all(limit=limit)
    if strategy:
        orders = [o for o in orders if (o.strategy_name or "") == strategy]
    return [vars(o) if not isinstance(o, dict) else o for o in orders]


class UpdateSLRequest(BaseModel):
    stop_loss: float
    target:    Optional[float] = None


@positions_router.put("/{command_id}/sl")
def update_stop_loss(
    command_id: str,
    body: UpdateSLRequest,
    payload: dict = Depends(current_user),
):
    """Update stop-loss (and optionally target) on an active position."""
    repo = _get_repo(payload["sub"])
    if repo is None:
        raise HTTPException(404, "No active bot session")
    record = repo.get_by_id(command_id)
    if not record:
        raise HTTPException(404, "Order not found")
    repo.update_stop_loss(command_id, body.stop_loss, body.target)
    return {"success": True, "command_id": command_id,
            "stop_loss": body.stop_loss, "target": body.target}


@positions_router.post("/{command_id}/cancel")
def cancel_order(
    command_id: str,
    payload: dict = Depends(current_user),
):
    """Cancel a pending/sent order via the broker adapter."""
    from trading.bot_core import _bots
    bot = None
    for key, b in _bots.items():
        if key.startswith(f"{payload['sub']}:"):
            bot = b
            break
    if bot is None:
        raise HTTPException(404, "No active bot session")

    record = bot.order_repo.get_by_id(command_id)
    if not record:
        raise HTTPException(404, "Order not found")

    broker_id = record.broker_order_id
    if not broker_id:
        raise HTTPException(400, "Order has no broker_order_id — cannot cancel")

    try:
        bot.api.cancel_order(broker_id)
        bot.order_repo.update_status(command_id, "FAILED")
        return {"success": True, "command_id": command_id, "broker_order_id": broker_id}
    except Exception as e:
        raise HTTPException(500, f"Cancel failed: {e}")
