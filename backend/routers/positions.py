"""
Smart Trader — Positions Router
=================================

Endpoints:
    GET  /api/positions              — all open positions with SL/target data
    GET  /api/positions/orders       — recent order history from PostgreSQL
    POST /api/positions/{id}/cancel  — cancel a pending order
    PUT  /api/positions/{id}/sl      — update stop-loss on a managed exit
"""

from collections import defaultdict
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from core.deps import current_user
from db.trading_db import get_trading_conn

positions_router = APIRouter(prefix="/positions", tags=["positions"])


def _get_repo(user_sub: str):
    from trading.bot_core import _bots
    for key, bot in _bots.items():
        if key.startswith(f"{user_sub}:"):
            return bot.order_repo
    return None


def _load_account_map(user_sub: str) -> dict[str, str]:
    conn = get_trading_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT client_id, config_id
            FROM mgr_sessions
            WHERE user_id = %s
            """,
            (user_sub,),
        )
        rows = cur.fetchall()
        return {
            str(client_id or "").strip(): str(config_id or "").strip()
            for client_id, config_id in rows
            if client_id and config_id
        }
    finally:
        conn.close()


def _position_qty(raw_position: dict) -> float:
    try:
        return float(
            raw_position.get("netqty")
            or raw_position.get("net_quantity")
            or raw_position.get("qty")
            or 0
        )
    except Exception:
        return 0.0


def _record_product(product: Optional[str]) -> str:
    mapping = {"M": "MIS", "C": "CNC", "I": "NRML"}
    value = str(product or "").upper()
    return mapping.get(value, value)


def _build_position_rows(user_sub: str) -> list[dict]:
    from managers.supreme_manager import supreme
    from routers.orders import _normalize_position
    from trading.persistence.repository import OrderRepository

    rows = supreme.get_positions(user_sub)
    if not rows:
        return []

    account_map = _load_account_map(user_sub)
    repo = OrderRepository(user_sub, "__all__")
    records = repo.get_all(limit=500)

    exact_matches: dict[tuple[str, str, str, str], list] = defaultdict(list)
    loose_matches: dict[tuple[str, str, str], list] = defaultdict(list)

    for record in records:
        if record.execution_type == "EXIT":
            continue
        if record.status in {"FAILED", "CANCELLED"}:
            continue

        symbol = str(record.symbol or "").strip()
        product = _record_product(record.product)
        side = str(record.side or "").upper()
        if not symbol or not product or not side:
            continue

        account_id = account_map.get(str(record.client_id or "").strip(), "")
        exact_matches[(account_id, symbol, product, side)].append(record)
        loose_matches[(symbol, product, side)].append(record)

    positions: list[dict] = []
    for idx, row in enumerate(rows):
        raw = dict(row.get("data", row)) if isinstance(row, dict) else {}
        if not raw:
            continue

        if isinstance(row, dict):
            raw.setdefault("account_id", row.get("config_id") or raw.get("account_id"))
            raw.setdefault("broker_id", row.get("broker_id") or raw.get("broker_id"))
            raw.setdefault("client_id", row.get("client_id") or raw.get("client_id"))

        if _position_qty(raw) == 0:
            continue

        normalized = _normalize_position(raw, idx)
        normalized["status"] = "OPEN"

        exact_key = (
            str(normalized.get("accountId") or ""),
            str(normalized.get("tradingsymbol") or normalized.get("symbol") or ""),
            str(normalized.get("product") or ""),
            str(normalized.get("side") or ""),
        )
        loose_key = exact_key[1:]

        record = None
        if exact_matches.get(exact_key):
            record = exact_matches[exact_key].pop(0)
        elif loose_matches.get(loose_key):
            record = loose_matches[loose_key].pop(0)

        normalized.update({
            "command_id": record.command_id if record else "",
            "strategy_name": record.strategy_name if record else "",
            "stop_loss": record.stop_loss if record else None,
            "target": record.target if record else None,
            "trailing_type": record.trailing_type if record else None,
            "trailing_value": record.trailing_value if record else None,
            "trail_when": record.trail_when if record else None,
            "broker_order_id": record.broker_order_id if record else None,
            "created_at": record.created_at if record else None,
        })
        positions.append(normalized)

    positions.sort(key=lambda position: (
        str(position.get("tradingsymbol") or position.get("symbol") or ""),
        str(position.get("accountId") or ""),
    ))
    return positions


@positions_router.get("")
def get_positions(payload: dict = Depends(current_user)):
    """Open broker positions merged with persisted Smart Trader risk fields."""
    return _build_position_rows(payload["sub"])


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
