"""
Smart Trader — Risk Router
============================

Endpoints:
  GET  /api/risk/status         — per-account risk state for the logged-in user
  POST /api/risk/acknowledge-exit — clear force-exit flag after manual confirmation
"""

from fastapi import APIRouter, Depends, HTTPException
from core.deps import current_user

risk_router = APIRouter(prefix="/risk", tags=["risk"])


@risk_router.get("/status")
def risk_status(payload: dict = Depends(current_user)):
    """Return AccountRiskManager status for all accounts of this user."""
    from broker.account_risk import list_all_risk_statuses
    user_id = payload["sub"]
    statuses = list_all_risk_statuses(user_id)
    if not statuses:
        return {"active": False, "accounts": [], "message": "No risk managers active"}
    return {"active": True, "accounts": statuses}


@risk_router.post("/acknowledge-exit")
def acknowledge_exit(
    config_id: str = "",
    payload: dict = Depends(current_user),
):
    """Clear force_exit_triggered after manual confirmation."""
    from broker.account_risk import get_account_risk, list_all_risk_statuses
    user_id = payload["sub"]

    if config_id:
        rm = get_account_risk(user_id, config_id)
        rm.acknowledge_exit_complete()
        return {"success": True, "config_id": config_id}

    # Acknowledge all accounts
    statuses = list_all_risk_statuses(user_id)
    for s in statuses:
        rm = get_account_risk(user_id, s["account_id"])
        rm.acknowledge_exit_complete()
    return {"success": True, "acknowledged": len(statuses)}
