"""
Smart Trader — Historical Analytics Service
=============================================

Stores daily PnL records in PostgreSQL `pnl_history` table.
Provides equity curve, PnL stats, and per-strategy breakdowns.

Port of shoonya_platform/analytics/historical_service.py + historical_store.py
"""

import logging
from datetime import date, timedelta
from typing import Dict, List

from db.trading_db import trading_cursor as get_trading_cursor

logger = logging.getLogger("smart_trader.analytics")


class HistoricalAnalyticsService:
    """Multi-tenant daily PnL recorder and equity curve provider."""

    def __init__(self, user_id: int, client_id: str):
        self.user_id   = user_id
        self.client_id = client_id

    # -- Recording ------------------------------------------------------------

    def record_daily_pnl(
        self,
        trade_date: date,
        pnl: float,
        gross_pnl: float = 0.0,
        charges: float = 0.0,
        trade_count: int = 0,
        win_count: int = 0,
        loss_count: int = 0,
        strategy_name: str = "",
        extra_json: str = "{}",
    ) -> None:
        """Upsert a daily PnL record for this user/client/date/strategy."""
        with get_trading_cursor() as cur:
            cur.execute("""
                INSERT INTO pnl_history
                    (user_id, client_id, trade_date, strategy_name,
                     pnl, gross_pnl, charges, trade_count, win_count, loss_count, extra)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (user_id, client_id, trade_date, strategy_name)
                DO UPDATE SET
                    pnl          = EXCLUDED.pnl,
                    gross_pnl    = EXCLUDED.gross_pnl,
                    charges      = EXCLUDED.charges,
                    trade_count  = EXCLUDED.trade_count,
                    win_count    = EXCLUDED.win_count,
                    loss_count   = EXCLUDED.loss_count,
                    extra        = EXCLUDED.extra,
                    updated_at   = NOW()
            """, (
                self.user_id, self.client_id,
                trade_date, strategy_name or "",
                pnl, gross_pnl, charges,
                trade_count, win_count, loss_count,
                extra_json,
            ))
        logger.debug("Recorded PnL %.2f for %s | strategy=%s", pnl, trade_date, strategy_name)

    # -- Equity curve ---------------------------------------------------------

    def get_equity_curve(self, days: int = 90, strategy_name: str = "") -> List[Dict]:
        """
        Returns daily PnL + cumulative equity for the last `days` calendar days.
        If strategy_name is empty, aggregates across all strategies.
        """
        since = date.today() - timedelta(days=days)
        with get_trading_cursor() as cur:
            if strategy_name:
                cur.execute("""
                    SELECT trade_date, pnl, trade_count, win_count, loss_count
                    FROM pnl_history
                    WHERE user_id = %s AND client_id = %s
                      AND trade_date >= %s AND strategy_name = %s
                    ORDER BY trade_date ASC
                """, (self.user_id, self.client_id, since, strategy_name))
            else:
                cur.execute("""
                    SELECT trade_date,
                           SUM(pnl)         AS pnl,
                           SUM(trade_count) AS trade_count,
                           SUM(win_count)   AS win_count,
                           SUM(loss_count)  AS loss_count
                    FROM pnl_history
                    WHERE user_id = %s AND client_id = %s
                      AND trade_date >= %s
                    GROUP BY trade_date
                    ORDER BY trade_date ASC
                """, (self.user_id, self.client_id, since))
            rows = cur.fetchall()

        result = []
        cumulative = 0.0
        for r in rows:
            cumulative += float(r["pnl"] or 0)
            result.append({
                "date":        str(r["trade_date"]),
                "pnl":         round(float(r["pnl"] or 0), 2),
                "equity":      round(cumulative, 2),
                "trade_count": int(r["trade_count"] or 0),
                "win_count":   int(r["win_count"] or 0),
                "loss_count":  int(r["loss_count"] or 0),
            })
        return result

    # -- Statistics -----------------------------------------------------------

    def get_statistics(self, days: int = 30, strategy_name: str = "") -> Dict:
        """Compute win-rate, avg win/loss, profit factor, max drawdown."""
        since = date.today() - timedelta(days=days)
        with get_trading_cursor() as cur:
            if strategy_name:
                cur.execute("""
                    SELECT pnl, win_count, loss_count, trade_count
                    FROM pnl_history
                    WHERE user_id = %s AND client_id = %s
                      AND trade_date >= %s AND strategy_name = %s
                    ORDER BY trade_date ASC
                """, (self.user_id, self.client_id, since, strategy_name))
            else:
                cur.execute("""
                    SELECT SUM(pnl) as pnl, SUM(win_count) as win_count,
                           SUM(loss_count) as loss_count, SUM(trade_count) as trade_count
                    FROM pnl_history
                    WHERE user_id = %s AND client_id = %s AND trade_date >= %s
                    GROUP BY trade_date
                    ORDER BY trade_date ASC
                """, (self.user_id, self.client_id, since))
            rows = cur.fetchall()

        if not rows:
            return {"total_days": 0, "total_pnl": 0.0, "win_rate": 0.0, "max_drawdown": 0.0}

        pnl_list = [float(r["pnl"] or 0) for r in rows]
        total_pnl    = sum(pnl_list)
        total_wins   = sum(int(r["win_count"] or 0) for r in rows)
        total_losses = sum(int(r["loss_count"] or 0) for r in rows)
        total_trades = sum(int(r["trade_count"] or 0) for r in rows)

        win_days  = sum(1 for p in pnl_list if p > 0)
        loss_days = sum(1 for p in pnl_list if p < 0)

        win_rate = win_days / len(pnl_list) * 100 if pnl_list else 0.0
        avg_win  = sum(p for p in pnl_list if p > 0) / win_days if win_days else 0.0
        avg_loss = sum(p for p in pnl_list if p < 0) / loss_days if loss_days else 0.0

        # Max drawdown (peak-to-trough in cumulative PnL)
        cum = 0.0
        peak = 0.0
        max_dd = 0.0
        for p in pnl_list:
            cum += p
            if cum > peak:
                peak = cum
            dd = peak - cum
            if dd > max_dd:
                max_dd = dd

        profit_factor = 0.0
        gross_wins  = sum(p for p in pnl_list if p > 0)
        gross_losses = abs(sum(p for p in pnl_list if p < 0))
        if gross_losses > 0:
            profit_factor = round(gross_wins / gross_losses, 2)

        return {
            "total_days":     len(pnl_list),
            "profitable_days": win_days,
            "loss_days":       loss_days,
            "total_pnl":       round(total_pnl, 2),
            "avg_daily_pnl":   round(total_pnl / len(pnl_list), 2),
            "avg_win_day":     round(avg_win, 2),
            "avg_loss_day":    round(avg_loss, 2),
            "win_rate":        round(win_rate, 2),
            "profit_factor":   profit_factor,
            "max_drawdown":    round(max_dd, 2),
            "total_trades":    total_trades,
            "total_wins":      total_wins,
            "total_losses":    total_losses,
        }

    # -- Strategy breakdown ---------------------------------------------------

    def get_strategy_breakdown(self, days: int = 30) -> List[Dict]:
        since = date.today() - timedelta(days=days)
        with get_trading_cursor() as cur:
            cur.execute("""
                SELECT strategy_name,
                       SUM(pnl)         AS total_pnl,
                       SUM(trade_count) AS trades,
                       SUM(win_count)   AS wins,
                       COUNT(*)         AS trading_days
                FROM pnl_history
                WHERE user_id = %s AND client_id = %s
                  AND trade_date >= %s AND strategy_name != ''
                GROUP BY strategy_name
                ORDER BY total_pnl DESC
            """, (self.user_id, self.client_id, since))
            return [
                {
                    "strategy":     r["strategy_name"],
                    "total_pnl":    round(float(r["total_pnl"] or 0), 2),
                    "trades":       int(r["trades"] or 0),
                    "wins":         int(r["wins"] or 0),
                    "trading_days": int(r["trading_days"] or 0),
                }
                for r in cur.fetchall()
            ]

    # -- EOD auto-record ------------------------------------------------------

    def record_eod(self, pnl: float, trade_count: int = 0, strategy_name: str = "") -> None:
        """Call at market close to record today's PnL."""
        self.record_daily_pnl(
            trade_date=date.today(),
            pnl=pnl,
            trade_count=trade_count,
            strategy_name=strategy_name,
        )
