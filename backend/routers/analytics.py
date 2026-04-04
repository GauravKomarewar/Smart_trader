"""
Smart Trader — Historical Analytics Router
============================================

Endpoints:
  GET  /api/analytics/equity-curve       — daily PnL + cumulative equity
  GET  /api/analytics/statistics         — win-rate, drawdown, profit factor
  GET  /api/analytics/strategy-breakdown — per-strategy PnL summary
"""

from typing import Optional
from fastapi import APIRouter, Depends
from core.deps import current_user

analytics_router = APIRouter(prefix="/analytics", tags=["analytics"])


def _get_analytics(user_sub: str):
    from trading.bot_core import _bots
    for key, bot in _bots.items():
        if key.startswith(f"{user_sub}:"):
            return bot.analytics
    # Fallback: create a read-only instance using the user_id as both fields
    from trading.analytics.historical_service import HistoricalAnalyticsService
    return HistoricalAnalyticsService(int(user_sub), "")


@analytics_router.get("/equity-curve")
def equity_curve(
    days:     int = 90,
    strategy: Optional[str] = None,
    payload:  dict = Depends(current_user),
):
    svc = _get_analytics(payload["sub"])
    return svc.get_equity_curve(days=days, strategy_name=strategy or "")


@analytics_router.get("/statistics")
def statistics(
    days:     int = 30,
    strategy: Optional[str] = None,
    payload:  dict = Depends(current_user),
):
    svc = _get_analytics(payload["sub"])
    return svc.get_statistics(days=days, strategy_name=strategy or "")


@analytics_router.get("/strategy-breakdown")
def strategy_breakdown(
    days:    int = 30,
    payload: dict = Depends(current_user),
):
    svc = _get_analytics(payload["sub"])
    return svc.get_strategy_breakdown(days=days)
