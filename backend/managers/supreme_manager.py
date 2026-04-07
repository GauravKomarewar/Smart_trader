"""
SupremeManager — Orchestrator for all domain managers.

Responsibilities:
  1. Initialise & start all managers on app startup
  2. Monitor health of each manager
  3. Auto-restart crashed managers
  4. Provide aggregated health status
  5. Expose DB read functions for WebSocket/REST consumers
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Optional

from db.trading_db import get_trading_conn
import psycopg2.extras

from managers.schema import init_manager_schema
from managers.position_manager import PositionManager
from managers.order_manager import OrderManager
from managers.holdings_manager import HoldingsManager
from managers.tradebook_manager import TradebookManager
from managers.account_manager import AccountManager
from managers.session_manager import SessionManager
from managers.alert_manager import AlertManager

logger = logging.getLogger("smart_trader.mgr.supreme")


class SupremeManager:
    """
    Singleton orchestrator.  Starts all managers, monitors health,
    and provides the read-side API for WebSocket / REST endpoints.
    """

    def __init__(self):
        self._managers: dict[str, object] = {}
        self._monitor_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._started = False

    # ── Lifecycle ──────────────────────────────────────────────────────────

    def start(self):
        """Initialise schema, create managers, start them all."""
        if self._started:
            logger.warning("SupremeManager already started")
            return

        logger.info("Initialising manager schema...")
        init_manager_schema()

        # Create all managers
        self._managers = {
            "session":   SessionManager(),
            "account":   AccountManager(),
            "position":  PositionManager(),
            "order":     OrderManager(),
            "holdings":  HoldingsManager(),
            "tradebook": TradebookManager(),
            "alert":     AlertManager(),
        }

        # Start all
        for name, mgr in self._managers.items():
            try:
                mgr.start()
                logger.info("Started manager: %s", name)
            except Exception as e:
                logger.error("Failed to start manager %s: %s", name, e)

        # Start health monitor
        self._stop_event.clear()
        self._monitor_thread = threading.Thread(
            target=self._health_monitor_loop,
            name="mgr-supreme-monitor",
            daemon=True,
        )
        self._monitor_thread.start()
        self._started = True
        logger.info("SupremeManager started with %d managers", len(self._managers))

    def stop(self):
        """Stop all managers and the health monitor."""
        self._stop_event.set()
        for name, mgr in self._managers.items():
            try:
                mgr.stop()
            except Exception as e:
                logger.error("Error stopping %s: %s", name, e)
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5.0)
        self._started = False
        logger.info("SupremeManager stopped")

    # ── Health monitor ─────────────────────────────────────────────────────

    def _health_monitor_loop(self):
        """Check manager health every 30s, auto-restart crashed ones."""
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=30.0)
            if self._stop_event.is_set():
                break
            for name, mgr in self._managers.items():
                if not mgr.is_running and name != "alert":
                    logger.warning("Manager %s is not running — restarting", name)
                    try:
                        mgr.start()
                    except Exception as e:
                        logger.error("Failed to restart %s: %s", name, e)

    def get_all_health(self) -> list[dict]:
        """Return health snapshot for all managers."""
        return [mgr.get_health() for mgr in self._managers.values()]

    # ══════════════════════════════════════════════════════════════════════════
    #  READ-SIDE API — All data comes from PostgreSQL
    #  These are called by WebSocket feed and REST endpoints
    # ══════════════════════════════════════════════════════════════════════════

    # ── Positions ──────────────────────────────────────────────────────────

    @staticmethod
    def get_positions(user_id: str, config_id: str | None = None) -> list[dict]:
        """Get all positions for a user (optionally filtered by config_id)."""
        conn = get_trading_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if config_id:
                cur.execute(
                    "SELECT * FROM mgr_positions WHERE user_id = %s AND config_id = %s "
                    "ORDER BY symbol",
                    (user_id, config_id),
                )
            else:
                cur.execute(
                    "SELECT * FROM mgr_positions WHERE user_id = %s ORDER BY symbol",
                    (user_id,),
                )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    # ── Orders ─────────────────────────────────────────────────────────────

    @staticmethod
    def get_orders(user_id: str, config_id: str | None = None) -> list[dict]:
        conn = get_trading_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if config_id:
                cur.execute(
                    "SELECT * FROM mgr_orders WHERE user_id = %s AND config_id = %s "
                    "ORDER BY fetched_at DESC",
                    (user_id, config_id),
                )
            else:
                cur.execute(
                    "SELECT * FROM mgr_orders WHERE user_id = %s ORDER BY fetched_at DESC",
                    (user_id,),
                )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    # ── Holdings ───────────────────────────────────────────────────────────

    @staticmethod
    def get_holdings(user_id: str, config_id: str | None = None) -> list[dict]:
        conn = get_trading_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if config_id:
                cur.execute(
                    "SELECT * FROM mgr_holdings WHERE user_id = %s AND config_id = %s "
                    "ORDER BY symbol",
                    (user_id, config_id),
                )
            else:
                cur.execute(
                    "SELECT * FROM mgr_holdings WHERE user_id = %s ORDER BY symbol",
                    (user_id,),
                )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    # ── Tradebook ──────────────────────────────────────────────────────────

    @staticmethod
    def get_tradebook(user_id: str, config_id: str | None = None) -> list[dict]:
        conn = get_trading_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if config_id:
                cur.execute(
                    "SELECT * FROM mgr_tradebook WHERE user_id = %s AND config_id = %s "
                    "ORDER BY fetched_at DESC",
                    (user_id, config_id),
                )
            else:
                cur.execute(
                    "SELECT * FROM mgr_tradebook WHERE user_id = %s ORDER BY fetched_at DESC",
                    (user_id,),
                )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    # ── Funds ──────────────────────────────────────────────────────────────

    @staticmethod
    def get_funds(user_id: str, config_id: str | None = None) -> list[dict]:
        conn = get_trading_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if config_id:
                cur.execute(
                    "SELECT * FROM mgr_funds WHERE user_id = %s AND config_id = %s",
                    (user_id, config_id),
                )
            else:
                cur.execute(
                    "SELECT * FROM mgr_funds WHERE user_id = %s",
                    (user_id,),
                )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    # ── Sessions ───────────────────────────────────────────────────────────

    @staticmethod
    def get_sessions(user_id: str) -> list[dict]:
        conn = get_trading_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                "SELECT * FROM mgr_sessions WHERE user_id = %s ORDER BY broker_id",
                (user_id,),
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    # ── Broker Accounts (enriched cards combining funds + positions + orders) ──

    @staticmethod
    def get_broker_accounts(user_id: str) -> list[dict]:
        """
        Build enriched broker account cards by reading from multiple mgr_ tables.
        This is the DB-backed replacement for the old in-memory builder.
        """
        conn = get_trading_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # Get all sessions for user
            cur.execute(
                "SELECT * FROM mgr_sessions WHERE user_id = %s AND is_live = TRUE",
                (user_id,),
            )
            sessions = cur.fetchall()
            if not sessions:
                return []

            accounts = []
            for sess in sessions:
                cid = sess["config_id"]
                bid = sess["broker_id"]
                clid = sess["client_id"]

                # Funds
                cur.execute(
                    "SELECT data FROM mgr_funds WHERE user_id = %s AND config_id = %s",
                    (user_id, cid),
                )
                funds_row = cur.fetchone()
                limits = funds_row["data"] if funds_row else {}

                # Positions
                cur.execute(
                    "SELECT data FROM mgr_positions WHERE user_id = %s AND config_id = %s",
                    (user_id, cid),
                )
                positions = [r["data"] for r in cur.fetchall()]

                # Orders
                cur.execute(
                    "SELECT data FROM mgr_orders WHERE user_id = %s AND config_id = %s",
                    (user_id, cid),
                )
                orders = [r["data"] for r in cur.fetchall()]

                # Trades
                cur.execute(
                    "SELECT data FROM mgr_tradebook WHERE user_id = %s AND config_id = %s",
                    (user_id, cid),
                )
                trades = [r["data"] for r in cur.fetchall()]

                # Calculate P&L from positions
                day_pnl = unrealized = realized = 0.0
                for p in positions:
                    # Use key-presence instead of `or` chains (0.0 is falsy!)
                    if "unrealised_pnl" in p:
                        u = float(p["unrealised_pnl"])
                    elif "urmtom" in p:
                        u = float(p["urmtom"])
                    else:
                        u = 0.0
                    if "realised_pnl" in p:
                        r = float(p["realised_pnl"])
                    elif "rpnl" in p:
                        r = float(p["rpnl"])
                    else:
                        r = 0.0
                    unrealized += u
                    realized += r
                    day_pnl += u + r

                cash = float(limits.get("available_cash") or limits.get("cash") or
                             limits.get("marginAvailable") or 0)
                collateral = float(limits.get("collateral") or
                                   limits.get("collateralvalue") or 0)
                available = float(limits.get("available_cash") or
                                  limits.get("marginAvailable") or
                                  limits.get("cash") or 0)
                used = float(limits.get("used_margin") or
                             limits.get("marginUsed") or 0)
                total = float(limits.get("total_balance") or
                              limits.get("totalBalance") or (available + used) or 0)

                if day_pnl == 0 and unrealized == 0:
                    unrealized = float(limits.get("unrealised_pnl") or
                                       limits.get("unrealizedPnl") or 0)
                    realized = float(limits.get("realised_pnl") or
                                     limits.get("realizedPnl") or 0)
                    day_pnl = unrealized + realized

                def _ord_status(o):
                    return str(o.get("status", "") or o.get("stat", "")).upper()

                orders_count = len(orders)
                trades_count = len(trades)
                completed = sum(1 for o in orders if _ord_status(o) == "COMPLETE")
                open_cnt = sum(1 for o in orders if _ord_status(o) in
                               ("OPEN", "PENDING", "TRIGGER_PENDING"))

                # Risk
                risk_info = {"trading_allowed": True, "daily_pnl": 0,
                             "halt_reason": None, "force_exit_triggered": False}
                try:
                    from broker.account_risk import get_account_risk
                    rm = get_account_risk(
                        user_id=user_id, config_id=cid,
                        broker_id=bid, client_id=clid,
                    )
                    risk_info = rm.get_status()
                except Exception:
                    pass

                accounts.append({
                    "config_id": cid,
                    "broker_id": bid,
                    "broker_name": bid.title(),
                    "client_id": clid,
                    "is_live": True,
                    "mode": sess.get("mode", "live"),
                    "state": sess.get("state", "live"),
                    "data_stale": sess.get("state") == "stale",
                    "connected_at": sess.get("connected_at"),
                    "cash": cash,
                    "collateral": collateral,
                    "available_margin": available,
                    "used_margin": used,
                    "total_balance": total,
                    "payin": float(limits.get("payin") or 0),
                    "payout": float(limits.get("payout") or 0),
                    "day_pnl": round(day_pnl, 2),
                    "unrealized_pnl": round(unrealized, 2),
                    "realized_pnl": round(realized, 2),
                    "positions_count": len(positions),
                    "orders_count": orders_count,
                    "open_orders": open_cnt,
                    "completed_orders": completed,
                    "trades_count": trades_count,
                    "risk_status": risk_info.get("trading_allowed", True),
                    "risk_daily_pnl": risk_info.get("daily_pnl", 0),
                    "risk_halt_reason": risk_info.get("halt_reason"),
                    "risk_force_exit": risk_info.get("force_exit_triggered", False),
                    "error": sess.get("error"),
                    "raw_limits": {k: v for k, v in limits.items() if k != "raw"},
                })

            return accounts
        finally:
            conn.close()

    # ── Dashboard (combined view for WebSocket push) ───────────────────────

    @staticmethod
    def get_dashboard(user_id: str) -> dict:
        """
        Build the combined dashboard snapshot from PostgreSQL.
        This replaces _build_dashboard_cached — all data from DB, no broker calls.
        Includes BOTH active and closed positions (closed marked with status='CLOSED').
        Also merges Smart Trader DB orders so WS and REST return identical data.
        """
        from routers.orders import (
            _normalize_db_order,
            _normalize_holding,
            _normalize_order,
            _normalize_position,
            _normalize_trade,
        )

        conn = get_trading_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # Positions
            cur.execute(
                "SELECT data, config_id, broker_id, client_id "
                "FROM mgr_positions WHERE user_id = %s",
                (user_id,),
            )
            all_positions = []
            for r in cur.fetchall():
                p = dict(r["data"])
                p["account_id"] = r["config_id"]
                p["broker_id"] = r["broker_id"]
                p["client_id"] = r["client_id"]
                all_positions.append(p)

            # Orders
            cur.execute(
                "SELECT data, config_id, broker_id, client_id "
                "FROM mgr_orders WHERE user_id = %s",
                (user_id,),
            )
            all_orders = []
            for r in cur.fetchall():
                o = dict(r["data"])
                o["account_id"] = r["config_id"]
                o["broker_id"] = r["broker_id"]
                o["client_id"] = r["client_id"]
                all_orders.append(o)

            # Holdings
            cur.execute(
                "SELECT data, config_id, broker_id, client_id "
                "FROM mgr_holdings WHERE user_id = %s",
                (user_id,),
            )
            all_holdings = []
            for r in cur.fetchall():
                items = r["data"]
                # data can be a list of holdings or a single holding dict
                if isinstance(items, list):
                    for h in items:
                        hh = dict(h) if isinstance(h, dict) else {"raw": h}
                        hh["account_id"] = r["config_id"]
                        hh["broker_id"] = r["broker_id"]
                        hh["client_id"] = r["client_id"]
                        all_holdings.append(hh)
                elif isinstance(items, dict):
                    items["account_id"] = r["config_id"]
                    items["broker_id"] = r["broker_id"]
                    items["client_id"] = r["client_id"]
                    all_holdings.append(items)

            # Trades
            cur.execute(
                "SELECT data, config_id, broker_id, client_id "
                "FROM mgr_tradebook WHERE user_id = %s",
                (user_id,),
            )
            all_trades = []
            for r in cur.fetchall():
                items = r["data"]
                if isinstance(items, list):
                    for t in items:
                        tt = dict(t) if isinstance(t, dict) else {"raw": t}
                        tt["account_id"] = r["config_id"]
                        tt["broker_id"] = r["broker_id"]
                        tt["client_id"] = r["client_id"]
                        all_trades.append(tt)
                elif isinstance(items, dict):
                    items["account_id"] = r["config_id"]
                    items["broker_id"] = r["broker_id"]
                    items["client_id"] = r["client_id"]
                    all_trades.append(items)

            # Funds
            cur.execute(
                "SELECT data FROM mgr_funds WHERE user_id = %s",
                (user_id,),
            )
            funds = []
            for r in cur.fetchall():
                lim = r["data"]
                funds.append({
                    "available_cash": float(
                        lim.get("available_cash") or lim.get("marginAvailable") or
                        lim.get("cash") or 0),
                    "used_margin": float(
                        lim.get("used_margin") or lim.get("marginUsed") or 0),
                })

        finally:
            conn.close()

        # Normalize ALL positions — active + closed (with status flag)
        positions = []
        unrealized = realized = 0.0
        for i, p in enumerate(all_positions):
            qty = float(p.get("netqty") or p.get("net_quantity") or
                        p.get("qty", 0))
            norm = _normalize_position(p, i)
            if qty == 0:
                norm["status"] = "CLOSED"
                # Closed position PnL goes to realized
                if "realised_pnl" in p:
                    realized += float(p["realised_pnl"])
                elif "rpnl" in p:
                    realized += float(p["rpnl"])
                else:
                    realized += float(p.get("pnl", 0))
            else:
                norm["status"] = "OPEN"
                # Active position P&L
                unrealized += float(norm.get("dayPnl", 0))
                realized += float(norm.get("pnl", 0)) - float(norm.get("dayPnl", 0))
            positions.append(norm)

        # Sort positions: OPEN first, then by P&L descending
        positions.sort(key=lambda p: (0 if p.get("status") == "OPEN" else 1,
                                       -(p.get("pnl") or 0)))

        holdings = [_normalize_holding(h, i) for i, h in enumerate(all_holdings)]
        holdings.sort(key=lambda h: (h.get("symbol") or "", h.get("accountId") or ""))

        trades = [_normalize_trade(t, i) for i, t in enumerate(all_trades)]

        def _parse_trade_time(trade: dict) -> float:
            traded_at = trade.get("tradedAt", "")
            if not traded_at:
                return 0
            try:
                if "T" in str(traded_at):
                    return datetime.fromisoformat(str(traded_at).replace("Z", "+00:00")).timestamp()
                return datetime.strptime(str(traded_at), "%d-%b-%Y %H:%M:%S").timestamp()
            except Exception:
                return 0

        trades.sort(key=lambda t: (_parse_trade_time(t), t.get("tradeId") or t.get("id")), reverse=True)

        # Normalize broker orders
        orders = [_normalize_order(o, i) for i, o in enumerate(all_orders)]
        broker_order_accounts = {
            o.get("orderId"): o.get("accountId")
            for o in orders
            if o.get("orderId") and o.get("accountId")
        }

        # ── Merge Smart Trader DB orders (same for WS and REST) ──
        try:
            from trading.persistence.repository import OrderRepository
            db_repo = OrderRepository(user_id, "__all__")
            db_records = db_repo.get_all(limit=200)
            broker_ids = {o.get("orderId") for o in orders if o.get("orderId")}
            for i, rec in enumerate(db_records):
                if rec.broker_order_id and rec.broker_order_id in broker_ids:
                    continue
                normalized = _normalize_db_order(rec, len(orders) + i)
                if rec.broker_order_id and rec.broker_order_id in broker_order_accounts:
                    normalized["accountId"] = broker_order_accounts[rec.broker_order_id]
                orders.append(normalized)
        except Exception as e:
            logger.warning("get_dashboard: DB order merge failed: %s", e)

        # Sort orders by placedAt descending (newest first) — stable sort
        def _parse_order_time(o):
            """Extract sortable timestamp from placedAt string."""
            pa = o.get("placedAt", "")
            if not pa:
                return 0
            try:
                from datetime import datetime as _dt
                # ISO format
                if "T" in str(pa):
                    return _dt.fromisoformat(str(pa).replace("Z", "+00:00")).timestamp()
                # Fyers format: "06-Apr-2026 09:56:24"
                return _dt.strptime(str(pa), "%d-%b-%Y %H:%M:%S").timestamp()
            except Exception:
                return 0
        orders.sort(key=_parse_order_time, reverse=True)

        day_pnl = unrealized + realized

        # Calculate summary
        total_equity = sum(
            f.get("available_cash", 0) + f.get("used_margin", 0) for f in funds
        )
        used_margin = avail_margin = cash = 0.0
        for f in funds:
            used_margin += float(f.get("used_margin") or 0)
            avail_margin += float(f.get("available_cash") or 0)
            cash += float(f.get("available_cash") or 0)

        return {
            "positions": positions,
            "holdings": holdings,
            "orders": orders,
            "trades": trades,
            "riskMetrics": {
                "accountId": "",
                "dailyPnl": round(day_pnl, 2),
                "dailyPnlLimit": 0,
                "mtmPnl": round(unrealized, 2),
                "maxPositionValue": 0,
                "leverageUsed": 0,
                "maxLeverage": 0,
                "positionCount": len(positions),
                "maxPositions": 0,
                "riskStatus": "SAFE",
                "alerts": [],
            },
            "accountSummary": {
                "totalEquity": round(total_equity, 2),
                "dayPnl": round(day_pnl, 2),
                "dayPnlPct": 0,
                "unrealizedPnl": round(unrealized, 2),
                "realizedPnl": round(realized, 2),
                "usedMargin": round(used_margin, 2),
                "availableMargin": round(avail_margin, 2),
                "cash": round(cash, 2),
            },
        }

    # ── Per-broker data ────────────────────────────────────────────────────

    @staticmethod
    def get_broker_data(user_id: str, config_id: str) -> dict | None:
        """Build per-broker filtered data from DB."""
        from routers.orders import _normalize_holding, _normalize_order, _normalize_position, _normalize_trade

        conn = get_trading_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            cur.execute(
                "SELECT data FROM mgr_positions WHERE user_id = %s AND config_id = %s",
                (user_id, config_id),
            )
            raw_positions = [r["data"] for r in cur.fetchall()]

            cur.execute(
                "SELECT data FROM mgr_orders WHERE user_id = %s AND config_id = %s",
                (user_id, config_id),
            )
            raw_orders = [r["data"] for r in cur.fetchall()]

            cur.execute(
                "SELECT data FROM mgr_holdings WHERE user_id = %s AND config_id = %s",
                (user_id, config_id),
            )
            holdings = [r["data"] for r in cur.fetchall()]

            cur.execute(
                "SELECT data FROM mgr_tradebook WHERE user_id = %s AND config_id = %s",
                (user_id, config_id),
            )
            trades = [r["data"] for r in cur.fetchall()]

        finally:
            conn.close()

        # Normalize ALL positions with status flag (consistent with dashboard)
        positions = []
        for i, p in enumerate(raw_positions):
            qty = float(p.get("netqty") or p.get("net_quantity") or
                        p.get("qty", 0))
            norm = _normalize_position(p, i)
            norm["status"] = "CLOSED" if qty == 0 else "OPEN"
            positions.append(norm)
        positions.sort(key=lambda p: (0 if p.get("status") == "OPEN" else 1,
                                       -(p.get("pnl") or 0)))
        orders = [_normalize_order(o, i) for i, o in enumerate(raw_orders)]
        holdings = [_normalize_holding(h, i) for i, h in enumerate(holdings)]
        holdings.sort(key=lambda h: (h.get("symbol") or "", h.get("accountId") or ""))
        trades = [_normalize_trade(t, i) for i, t in enumerate(trades)]
        trades.sort(key=lambda t: t.get("tradedAt") or "", reverse=True)

        return {
            "positions": positions,
            "holdings": holdings,
            "orders": orders,
            "trades": trades,
            "config_id": config_id,
        }

    # ── Alerts ─────────────────────────────────────────────────────────────

    @staticmethod
    def get_alerts(user_id: str, limit: int = 50) -> list[dict]:
        return AlertManager.get_recent_alerts(user_id, limit)

    @staticmethod
    def push_alert(user_id: str, level: str, title: str, message: str,
                   source: str = "system", data: dict | None = None):
        AlertManager.push_alert(user_id, level, title, message, source, data)


# ── Module-level singleton ─────────────────────────────────────────────────────
supreme = SupremeManager()
