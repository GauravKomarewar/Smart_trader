"""
WebSocket router — real-time market data push to frontend clients.

Protocol:
  Client connects: ws://host:8001/ws/market
  Client sends:    {"action": "subscribe",   "symbols": ["NIFTY", "BANKNIFTY"]}
                   {"action": "unsubscribe", "symbols": ["NIFTY"]}
  Server sends:    {"type": "tick", "data": {symbol, ltp, change, changePct, ...}}
                   {"type": "heartbeat", "ts": <unix>}
                   {"type": "error", "message": "..."}

In DEMO mode, server generates synthetic ticks every ~1 second.
In LIVE mode, ticks are forwarded from Shoonya WebSocket via LiveFeed.
"""

import asyncio
import json
import logging
import time
from typing import Set

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from broker.shoonya_client import get_session

logger = logging.getLogger("smart_trader.websocket")
router = APIRouter(tags=["websocket"])

# ── Active connections registry ───────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self._connections: Set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        async with self._lock:
            self._connections.add(ws)
        logger.info("WS client connected | total=%d", len(self._connections))

    async def disconnect(self, ws: WebSocket) -> None:
        async with self._lock:
            self._connections.discard(ws)
        logger.info("WS client disconnected | total=%d", len(self._connections))

    async def broadcast(self, payload: dict) -> None:
        message = json.dumps(payload)
        dead = set()
        async with self._lock:
            conns = set(self._connections)
        for ws in conns:
            try:
                await ws.send_text(message)
            except Exception:
                dead.add(ws)
        if dead:
            async with self._lock:
                self._connections -= dead


manager = ConnectionManager()


# ── No demo tick generator — only real data is sent ────────────────────


@router.websocket("/ws/market")
async def market_websocket(websocket: WebSocket):
    """
    Live market data WebSocket.
    Clients subscribe to symbols and receive real-time ticks.
    """
    await manager.connect(websocket)
    session = get_session()
    subscribed: Set[str] = set()
    stop_event = asyncio.Event()
    demo_task = None

    try:
        # Send initial connection acknowledgement
        await websocket.send_text(json.dumps({
            "type": "connected",
            "mode": "demo" if session.is_demo else "live",
            "message": "Connected to Smart Trader market feed",
        }))

        while True:
            try:
                raw = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
            except asyncio.TimeoutError:
                # Send heartbeat if client is idle
                await websocket.send_text(json.dumps({"type": "heartbeat", "ts": int(time.time())}))
                continue

            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await websocket.send_text(json.dumps({"type": "error", "message": "Invalid JSON"}))
                continue

            action = msg.get("action", "")
            symbols = [s.upper() for s in msg.get("symbols", [])]

            if action == "subscribe":
                new_symbols = [s for s in symbols if s not in subscribed]
                subscribed.update(new_symbols)
                logger.info("WS subscribe: %s (total=%d)", new_symbols, len(subscribed))

                # No demo ticks — only real broker data is pushed

                await websocket.send_text(json.dumps({
                    "type": "subscribed",
                    "symbols": list(subscribed),
                }))

            elif action == "unsubscribe":
                subscribed -= set(symbols)
                logger.info("WS unsubscribe: %s (remaining=%d)", symbols, len(subscribed))
                await websocket.send_text(json.dumps({
                    "type": "unsubscribed",
                    "symbols": symbols,
                    "remaining": list(subscribed),
                }))

            elif action == "ping":
                await websocket.send_text(json.dumps({"type": "pong", "ts": int(time.time())}))

    except WebSocketDisconnect:
        logger.info("WS client disconnected normally")
    except Exception as e:
        logger.error("WS error: %s", e)
    finally:
        stop_event.set()
        if demo_task:
            demo_task.cancel()
        await manager.disconnect(websocket)


# ═══════════════════════════════════════════════════════════════════════════════
#  /ws/feed — Authenticated live dashboard feed
#  Pushes ALL broker data (positions, orders, holdings, trades, accounts)
#  every ~1 second per connected user.
# ═══════════════════════════════════════════════════════════════════════════════

from jose import JWTError as _JWTError

def _ws_authenticate(token: str) -> dict | None:
    """Verify JWT from WS query param. Returns payload or None."""
    if not token:
        return None
    try:
        from core.security import decode_token
        return decode_token(token)
    except Exception:
        return None


def _build_broker_accounts(user_id: str) -> list:
    """Build enriched broker account cards. Lightweight version for WS push."""
    from broker.multi_broker import registry as mb
    live_sessions = mb.list_live_sessions(user_id)
    accounts = []
    for sess in live_sessions:
        limits = {}
        positions = []
        orders = []
        trades = []
        try:
            limits = sess.get_limits() or {}
        except Exception:
            pass
        try:
            positions = sess.get_positions() or []
        except Exception:
            pass
        try:
            orders = sess.get_order_book() or []
        except Exception:
            pass
        try:
            trades = sess.get_tradebook() or []
        except Exception:
            pass

        # P&L from positions
        day_pnl = unrealized = realized = 0.0
        for p in positions:
            if isinstance(p, dict):
                u = float(p.get("unrealised_pnl") or p.get("pnl") or p.get("urmtom", 0) or 0)
                r = float(p.get("realised_pnl") or p.get("rpnl", 0) or 0)
            else:
                u = float(getattr(p, "unrealised_pnl", 0) or getattr(p, "pnl", 0) or 0)
                r = float(getattr(p, "realised_pnl", 0) or 0)
            unrealized += u
            realized += r
            day_pnl += u + r

        cash = float(limits.get("available_cash") or limits.get("cash") or limits.get("marginAvailable") or 0)
        collateral = float(limits.get("collateral") or limits.get("collateralvalue") or 0)
        available = float(limits.get("available_cash") or limits.get("marginAvailable") or limits.get("cash") or 0)
        used = float(limits.get("used_margin") or limits.get("marginUsed") or 0)
        total = float(limits.get("total_balance") or limits.get("totalBalance") or (available + used) or 0)

        if day_pnl == 0 and unrealized == 0:
            unrealized = float(limits.get("unrealised_pnl") or limits.get("unrealizedPnl") or 0)
            realized = float(limits.get("realised_pnl") or limits.get("realizedPnl") or 0)
            day_pnl = unrealized + realized

        def _ord_status(o):
            if isinstance(o, dict):
                return str(o.get("status", "") or o.get("stat", "")).upper()
            return str(getattr(o, "status", "")).upper()

        orders_count = len(orders)
        trades_count = len(trades)
        completed = sum(1 for o in orders if _ord_status(o) == "COMPLETE")
        open_cnt = sum(1 for o in orders if _ord_status(o) in ("OPEN", "PENDING", "TRIGGER_PENDING"))

        # Risk
        risk_info = {"trading_allowed": True, "daily_pnl": 0, "halt_reason": None, "force_exit_triggered": False}
        try:
            from broker.account_risk import get_account_risk
            rm = get_account_risk(user_id=user_id, config_id=sess.config_id,
                                  broker_id=sess.broker_id, client_id=sess.client_id)
            risk_info = rm.get_status()
        except Exception:
            pass

        accounts.append({
            "config_id": sess.config_id,
            "broker_id": sess.broker_id,
            "broker_name": sess.broker_id.title(),
            "client_id": sess.client_id,
            "is_live": True,
            "mode": sess.mode,
            "connected_at": sess.connected_at.isoformat() if sess.connected_at else None,
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
            "error": sess.error,
            "raw_limits": {k: v for k, v in limits.items() if k != "raw"},
        })
    return accounts


def _build_dashboard(user_id: str) -> dict:
    """Build combined dashboard snapshot across all brokers."""
    from broker.multi_broker import registry as mb
    positions = mb.get_positions(user_id)
    orders = mb.get_order_book(user_id)
    holdings = mb.get_holdings(user_id)
    trades = mb.get_tradebook(user_id)
    funds = mb.get_funds(user_id)

    total_equity = sum(f.get("available_cash", 0) + f.get("used_margin", 0) for f in funds)
    day_pnl = unrealized = realized = used_margin = avail_margin = cash = 0.0
    for p in positions:
        if isinstance(p, dict):
            unrealized += float(p.get("unrealised_pnl") or p.get("pnl") or 0)
            realized += float(p.get("realised_pnl") or p.get("rpnl") or 0)
    day_pnl = unrealized + realized
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


def _build_broker_data(user_id: str, config_id: str) -> dict | None:
    """Build per-broker filtered data."""
    from broker.multi_broker import registry as mb
    sess = mb.get_session(user_id, config_id)
    if not sess:
        return None
    positions = []
    holdings = []
    orders = []
    trades = []
    try:
        positions = sess.get_positions() or []
    except Exception:
        pass
    try:
        holdings = sess.get_holdings() or []
    except Exception:
        pass
    try:
        orders = sess.get_order_book() or []
    except Exception:
        pass
    try:
        trades = sess.get_tradebook() or []
    except Exception:
        pass
    return {
        "positions": positions,
        "holdings": holdings,
        "orders": orders,
        "trades": trades,
        "config_id": config_id,
    }


@router.websocket("/ws/feed")
async def live_feed_websocket(websocket: WebSocket):
    """
    Authenticated live dashboard feed.

    Client connects: ws://host/ws/feed?token=<jwt>
    Client sends:    {"action": "subscribe_broker", "config_id": "xyz"}
                     {"action": "unsubscribe_broker"}
                     {"action": "ping"}
    Server sends:    {"type": "dashboard", "data": {...}}      — combined dashboard (every 1s)
                     {"type": "broker_accounts", "data": [...]} — enriched accounts (every 2s)
                     {"type": "broker_data", "data": {...}}    — per-broker data (every 1s, if subscribed)
                     {"type": "heartbeat", "ts": <unix>}
                     {"type": "pong", "ts": <unix>}
    """
    # Authenticate via query parameter
    token = websocket.query_params.get("token", "")
    payload = _ws_authenticate(token)
    if not payload:
        await websocket.close(code=4001, reason="Unauthorized")
        return

    user_id = payload.get("sub", "")
    if not user_id:
        await websocket.close(code=4001, reason="Invalid token")
        return

    await websocket.accept()
    logger.info("WS feed connected for user=%s", user_id[:8])

    subscribed_broker: str | None = None  # config_id of broker to get filtered data for
    stop_event = asyncio.Event()

    async def _push_loop():
        """Background task: push data every second."""
        cycle = 0
        while not stop_event.is_set():
            try:
                cycle += 1
                ts = int(time.time())

                # Dashboard data — every cycle (1s)
                try:
                    dashboard = await asyncio.get_event_loop().run_in_executor(
                        None, _build_dashboard, user_id
                    )
                    await websocket.send_text(json.dumps({
                        "type": "dashboard",
                        "data": dashboard,
                        "ts": ts,
                    }))
                except Exception as e:
                    logger.debug("WS feed dashboard error: %s", e)

                # Broker accounts — every 2 cycles (2s)
                if cycle % 2 == 0:
                    try:
                        accounts = await asyncio.get_event_loop().run_in_executor(
                            None, _build_broker_accounts, user_id
                        )
                        await websocket.send_text(json.dumps({
                            "type": "broker_accounts",
                            "data": accounts,
                            "ts": ts,
                        }))
                    except Exception as e:
                        logger.debug("WS feed accounts error: %s", e)

                # Per-broker data — every cycle (1s), only if subscribed
                if subscribed_broker:
                    try:
                        bdata = await asyncio.get_event_loop().run_in_executor(
                            None, _build_broker_data, user_id, subscribed_broker
                        )
                        if bdata:
                            await websocket.send_text(json.dumps({
                                "type": "broker_data",
                                "data": bdata,
                                "ts": ts,
                            }))
                    except Exception as e:
                        logger.debug("WS feed broker_data error: %s", e)

                # Heartbeat
                await websocket.send_text(json.dumps({"type": "heartbeat", "ts": ts}))

            except Exception:
                stop_event.set()
                return

            await asyncio.sleep(1)

    push_task = asyncio.create_task(_push_loop())

    try:
        while True:
            try:
                raw = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
            except asyncio.TimeoutError:
                continue

            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            action = msg.get("action", "")

            if action == "subscribe_broker":
                subscribed_broker = msg.get("config_id")
                logger.info("WS feed: subscribe_broker config=%s user=%s",
                            subscribed_broker, user_id[:8])
                await websocket.send_text(json.dumps({
                    "type": "broker_subscribed",
                    "config_id": subscribed_broker,
                }))

            elif action == "unsubscribe_broker":
                subscribed_broker = None
                await websocket.send_text(json.dumps({
                    "type": "broker_unsubscribed",
                }))

            elif action == "ping":
                await websocket.send_text(json.dumps({"type": "pong", "ts": int(time.time())}))

    except WebSocketDisconnect:
        logger.info("WS feed disconnected user=%s", user_id[:8])
    except Exception as e:
        logger.error("WS feed error user=%s: %s", user_id[:8], e)
    finally:
        stop_event.set()
        push_task.cancel()
        try:
            await push_task
        except asyncio.CancelledError:
            pass
