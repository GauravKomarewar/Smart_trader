"""
Smart Trader — Risk Router
============================

Endpoints:
  GET  /api/risk/status         — current RMS state for the logged-in user
  GET  /api/risk/pnl-ohlc       — PnL OHLC bars (1m/5m)
  POST /api/risk/reset-loss-flag — manually clear daily_loss_hit (admin)
"""

from fastapi import APIRouter, Depends, HTTPException
from core.deps import current_user

risk_router = APIRouter(prefix="/risk", tags=["risk"])

# Import bot registry lazily to avoid circular imports
def _get_bot(user_sub: str):
    from trading.bot_core import _bots
    # Find bot for user — iterate since key is "user_id:client_id"
    for key, bot in _bots.items():
        if key.startswith(f"{user_sub}:"):
            return bot
    return None


@risk_router.get("/status")
def risk_status(payload: dict = Depends(current_user)):
    """Return the SupremeRiskManager status for this user's active bot."""
    bot = _get_bot(payload["sub"])
    if bot is None:
        return {"active": False, "message": "No active bot session"}
    return {"active": True, **bot.risk.get_status()}


@risk_router.get("/pnl-ohlc")
def pnl_ohlc(
    tf: str = "1m",
    payload: dict = Depends(current_user),
):
    """Return PnL OHLC bars. tf: 1m | 5m | 1d"""
    if tf not in ("1m", "5m", "1d"):
        raise HTTPException(400, "tf must be 1m, 5m, or 1d")
    bot = _get_bot(payload["sub"])
    if bot is None:
        return []
    return bot.risk.get_pnl_ohlc(tf)


@risk_router.post("/acknowledge-exit")
def acknowledge_exit(payload: dict = Depends(current_user)):
    """Clear force_exit_in_progress after manual confirmation."""
    bot = _get_bot(payload["sub"])
    if bot is None:
        raise HTTPException(404, "No active bot session")
    bot.risk.acknowledge_exit_complete()
    return {"success": True}
