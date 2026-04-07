"""
Smart Trader — Risk Router
============================

Endpoints:
  GET  /api/risk/status                     — per-account risk state for the logged-in user
  POST /api/risk/acknowledge-exit           — clear force-exit flag after manual confirmation
  GET  /api/risk/history/{config_id}        — last 60 days of daily P&L / breach records
  GET  /api/risk/config/{config_id}         — active risk config params for one account
  POST /api/risk/unlock/{config_id}         — admin: clear lockout (consecutive-loss ban)
  GET  /api/risk/watcher                    — PositionWatcher snapshot & alerts
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


@risk_router.get("/history/{config_id}")
def risk_history(config_id: str, payload: dict = Depends(current_user)):
    """
    Return daily P&L history (last 60 trading days) for one broker account.
    Each record: date, final_pnl, daily_loss_hit, consecutive_loss_days, lockout_until.
    """
    from broker.account_risk import get_account_risk
    from database.db import get_db
    from models.broker_config import BrokerConfig
    from sqlalchemy.orm import Session
    from fastapi import Request
    user_id = payload["sub"]

    # Get the risk manager (may or may not be in-memory)
    rm = get_account_risk(user_id=user_id, config_id=config_id)
    history = rm.get_history()
    return {
        "config_id": config_id,
        "count":     len(history),
        "history":   history,
    }


@risk_router.get("/config/{config_id}")
def risk_config(config_id: str, payload: dict = Depends(current_user)):
    """Return the active risk configuration for one broker account."""
    from broker.account_risk import get_account_risk
    user_id = payload["sub"]
    rm = get_account_risk(user_id=user_id, config_id=config_id)
    return rm.get_config()


@risk_router.post("/unlock/{config_id}")
def force_unlock(config_id: str, payload: dict = Depends(current_user)):
    """
    Admin override: clear a consecutive-loss lockout for one account.
    Resets the lockout timer and consecutive-loss counter.
    """
    from broker.account_risk import get_account_risk
    user_id = payload["sub"]
    rm = get_account_risk(user_id=user_id, config_id=config_id)
    rm.force_unlock()
    return {"success": True, "config_id": config_id, "status": rm.get_status()}


@risk_router.get("/watcher")
def watcher_status(payload: dict = Depends(current_user)):
    """Return PositionWatcher snapshot and pending alerts for this user."""
    from trading.position_watcher import position_watcher as pw
    user_id = payload["sub"]
    return {
        "running": pw.is_running,
        "snapshot": pw.get_latest_snapshot(user_id),
        "alerts": pw.peek_alerts(user_id),
    }
