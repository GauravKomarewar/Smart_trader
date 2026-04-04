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
import random
import time
from typing import Set

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from broker.shoonya_client import get_session, _make_demo_quote

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


# ── Demo tick generator ───────────────────────────────────────────────────────
async def _demo_tick_loop(ws: WebSocket, symbols: Set[str], stop_event: asyncio.Event):
    """Push synthetic ticks every ~1 second in demo mode."""
    while not stop_event.is_set():
        for sym in list(symbols):
            quote = _make_demo_quote(sym)
            try:
                await ws.send_text(json.dumps({"type": "tick", "data": quote}))
            except Exception:
                stop_event.set()
                return
        # Heartbeat
        try:
            await ws.send_text(json.dumps({"type": "heartbeat", "ts": int(time.time())}))
        except Exception:
            stop_event.set()
            return
        await asyncio.sleep(1)


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

                # Start demo tick loop if in demo mode
                if session.is_demo and subscribed:
                    if demo_task and not demo_task.done():
                        stop_event.set()
                        await asyncio.sleep(0.1)
                        stop_event.clear()
                    demo_task = asyncio.create_task(
                        _demo_tick_loop(websocket, subscribed, stop_event)
                    )

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
