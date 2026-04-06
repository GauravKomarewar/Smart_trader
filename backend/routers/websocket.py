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

    _last_good_dashboard: dict | None = None
    _last_good_accounts: list | None = None

    async def _push_loop():
        """Background task: read all views from PostgreSQL via SupremeManager."""
        nonlocal _last_good_dashboard, _last_good_accounts
        from managers.supreme_manager import supreme
        cycle = 0
        while not stop_event.is_set():
            try:
                cycle += 1
                ts = int(time.time())

                # ── Dashboard (every cycle, ~2s) — reads from DB ──────────
                try:
                    dashboard = await asyncio.get_event_loop().run_in_executor(
                        None, supreme.get_dashboard, user_id
                    )
                    has_data = (
                        dashboard.get("positions")
                        or dashboard.get("orders")
                        or any(v for v in dashboard.get("accountSummary", {}).values() if v)
                    )
                    if has_data:
                        _last_good_dashboard = dashboard
                    elif _last_good_dashboard:
                        dashboard = _last_good_dashboard
                    await websocket.send_text(json.dumps({
                        "type": "dashboard",
                        "data": dashboard,
                        "ts": ts,
                    }))
                except Exception as e:
                    logger.debug("WS feed dashboard error: %s", e)

                # ── Risk alerts (every cycle) ─────────────────────────────
                try:
                    from trading.position_watcher import position_watcher as pw
                    alerts = pw.drain_alerts(user_id)
                    if alerts:
                        await websocket.send_text(json.dumps({
                            "type": "risk_alerts",
                            "data": alerts,
                            "ts": ts,
                        }))
                    risk_snapshot = pw.get_latest_snapshot(user_id)
                    if risk_snapshot:
                        await websocket.send_text(json.dumps({
                            "type": "risk_snapshot",
                            "data": risk_snapshot,
                            "ts": ts,
                        }))
                except Exception as e:
                    logger.debug("WS feed risk_alerts error: %s", e)

                # ── Broker accounts (every 2 cycles) — from DB ────────────
                if cycle % 2 == 0:
                    try:
                        accounts = await asyncio.get_event_loop().run_in_executor(
                            None, supreme.get_broker_accounts, user_id
                        )
                        has_account_data = any(
                            a.get("cash") or a.get("day_pnl") or a.get("positions_count")
                            or a.get("orders_count")
                            for a in accounts
                        )
                        if has_account_data:
                            _last_good_accounts = accounts
                        elif _last_good_accounts:
                            accounts = _last_good_accounts
                        await websocket.send_text(json.dumps({
                            "type": "broker_accounts",
                            "data": accounts,
                            "ts": ts,
                        }))
                    except Exception as e:
                        logger.debug("WS feed accounts error: %s", e)

                # ── Per-broker data (if subscribed) — from DB ─────────────
                if subscribed_broker:
                    try:
                        bdata = await asyncio.get_event_loop().run_in_executor(
                            None, supreme.get_broker_data, user_id, subscribed_broker
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

            await asyncio.sleep(2)

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
