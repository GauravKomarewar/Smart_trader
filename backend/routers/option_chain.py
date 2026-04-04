"""
Smart Trader — Option Chain Router
=====================================

Endpoints:
  GET  /api/option-chain/{underlying}/{expiry}   — full chain with Greeks
  GET  /api/option-chain/{underlying}/{expiry}/atm — ATM window
  POST /api/option-chain/subscribe               — add subscription
  POST /api/option-chain/refresh                 — force refresh
"""

from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from core.deps import current_user

oc_router = APIRouter(prefix="/option-chain", tags=["option-chain"])


def _get_oc_supervisor(user_sub: str):
    from trading.bot_core import _bots
    for key, bot in _bots.items():
        if key.startswith(f"{user_sub}:"):
            return bot.oc_supervisor
    return None


@oc_router.get("/{underlying}/{expiry}")
def get_chain(
    underlying: str,
    expiry: str,
    payload: dict = Depends(current_user),
):
    """Return the enriched option chain (with Greeks) for underlying+expiry."""
    sup = _get_oc_supervisor(payload["sub"])
    if sup is None:
        raise HTTPException(404, "No active bot session")
    data = sup.get_chain(underlying, expiry)
    if data is None:
        raise HTTPException(404, f"No chain data for {underlying}/{expiry}. Subscribe first.")
    return data


@oc_router.get("/{underlying}/{expiry}/atm")
def get_atm_window(
    underlying: str,
    expiry: str,
    count: int = 5,
    payload: dict = Depends(current_user),
):
    """Return ATM ± count strikes."""
    sup = _get_oc_supervisor(payload["sub"])
    if sup is None:
        raise HTTPException(404, "No active bot session")
    strikes = sup.get_atm_strikes(underlying, expiry, count)
    chain   = sup.get_chain(underlying, expiry)
    if chain is None:
        return {"strikes": strikes, "data": []}
    rows = [chain["chain"].get(s, {"strike": s}) for s in strikes]
    return {
        "strikes": strikes,
        "meta":    chain.get("meta", {}),
        "data":    rows,
    }


class SubscribeRequest(BaseModel):
    underlying: str
    expiry:     str
    strikes:    Optional[List[int]] = None


@oc_router.post("/subscribe")
def subscribe(req: SubscribeRequest, payload: dict = Depends(current_user)):
    sup = _get_oc_supervisor(payload["sub"])
    if sup is None:
        raise HTTPException(404, "No active bot session")
    sup.subscribe(req.underlying, req.expiry, req.strikes)
    return {"success": True, "subscribed": f"{req.underlying}/{req.expiry}"}


@oc_router.post("/refresh/{underlying}/{expiry}")
def refresh_chain(
    underlying: str,
    expiry: str,
    payload: dict = Depends(current_user),
):
    sup = _get_oc_supervisor(payload["sub"])
    if sup is None:
        raise HTTPException(404, "No active bot session")
    data = sup.force_refresh(underlying, expiry)
    if data is None:
        raise HTTPException(503, "Chain refresh failed — broker may not support get_option_chain()")
    return {"success": True, "meta": data.get("meta", {})}
