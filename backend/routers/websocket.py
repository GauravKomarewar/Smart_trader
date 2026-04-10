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


def _normalize_sym(s: str) -> str:
    """Normalize symbol for comparison: strip exchange prefix, suffixes, and spaces."""
    s = str(s).upper().split(":")[-1]
    for sfx in ("-INDEX", "-EQ", "-BE", "-SM", "-IL"):
        if s.endswith(sfx):
            s = s[: -len(sfx)]
    return s.replace(" ", "")

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


# ── Live tick service integration ──────────────────────────────────────────────

def _get_tick_service():
    try:
        from broker.live_tick_service import get_tick_service
        return get_tick_service()
    except Exception:
        return None


@router.websocket("/ws/market")
async def market_websocket(websocket: WebSocket):
    """
    Live market data WebSocket.
    Clients subscribe to symbols and receive real-time ticks from both
    Fyers (primary) and Shoonya (parallel) data pipelines.
    """
    await manager.connect(websocket)
    session = get_session()
    subscribed: Set[str] = set()
    subscribed_norm: Set[str] = set()   # pre-computed normalized set for O(1) lookup
    stop_event = asyncio.Event()

    # Register per-connection broadcast callback for this client
    tick_svc = _get_tick_service()
    if tick_svc is not None:
        loop = asyncio.get_event_loop()
        tick_svc.set_event_loop(loop)

    async def _send_tick(msg_or_str, is_preserialized: bool = False) -> None:
        """Forward a tick only to symbols this client has subscribed.
        Supports both pre-serialized string (zero-copy) and dict messages."""
        if is_preserialized:
            # Extract symbol from pre-serialized JSON for filtering
            # Quick JSON parse for symbol check
            try:
                msg = json.loads(msg_or_str)
            except Exception:
                return
            sym = msg.get("data", {}).get("symbol", "")
        else:
            sym = msg_or_str.get("data", {}).get("symbol", "")
        if not sym:
            return
        # O(1) normalized symbol matching
        sym_norm = _normalize_sym(sym)
        if sym.upper() not in subscribed and sym_norm not in subscribed_norm:
            return
        try:
            if is_preserialized:
                await websocket.send_text(msg_or_str)
            else:
                await websocket.send_text(json.dumps(msg_or_str))
        except Exception:
            pass

    if tick_svc is not None:
        tick_svc.add_broadcast_callback(_send_tick)

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
                subscribed_norm.update(_normalize_sym(s) for s in new_symbols)
                logger.info("WS subscribe: %s (total=%d)", new_symbols, len(subscribed))

                # Pass new subscriptions to the live tick service
                if tick_svc is not None and new_symbols:
                    tick_svc.subscribe(new_symbols)

                # Send back any already-cached ticks immediately
                if tick_svc is not None:
                    for sym in new_symbols:
                        cached = tick_svc.get_latest(sym)
                        if cached:
                            await websocket.send_text(json.dumps({
                                "type": "tick", "data": cached
                            }))

                await websocket.send_text(json.dumps({
                    "type": "subscribed",
                    "symbols": list(subscribed),
                }))

            elif action == "unsubscribe":
                subscribed -= set(symbols)
                subscribed_norm = {_normalize_sym(s) for s in subscribed}
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
        if tick_svc is not None:
            tick_svc.remove_broadcast_callback(_send_tick)
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
    subscribed_option_chain: dict | None = None  # {symbol, expiry, exchange}
    subscribed_market_depth: str | None = None   # symbol for depth feed
    stop_event = asyncio.Event()

    _last_dash_hash: str = ""
    _last_acct_hash: str = ""
    _last_pos_hash: str = ""
    _last_oc_hash: str = ""
    _last_depth_hash: str = ""
    _last_strat_hash: str = ""
    _last_broker_status_hash: str = ""

    def _quick_hash(obj) -> str:
        """Fast hash of a JSON-serializable object for dedup."""
        import hashlib
        return hashlib.md5(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()

    async def _push_loop():
        """Background task: read all views from PostgreSQL via SupremeManager.
        Wakes instantly when event_bus signals new data (order/position change)."""
        nonlocal _last_dash_hash, _last_acct_hash
        from managers.supreme_manager import supreme
        from core.event_bus import event_bus
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
                    # Always trust backend data — SupremeManager is authoritative
                    # Only push if data actually changed (anti-flicker)
                    dash_hash = _quick_hash(dashboard)
                    if dash_hash != _last_dash_hash:
                        _last_dash_hash = dash_hash
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
                        # Only push if accounts data actually changed (anti-flicker)
                        acct_hash = _quick_hash(accounts)
                        if acct_hash != _last_acct_hash:
                            _last_acct_hash = acct_hash
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

                # ── Strategy positions (every 2 cycles) ───────────────────
                if cycle % 2 == 0:
                    try:
                        pos_rows = await asyncio.get_event_loop().run_in_executor(
                            None, supreme.get_positions, user_id, None
                        )
                        pos_data = [r.get("data", r) if isinstance(r, dict) else r for r in pos_rows]
                        pos_hash = _quick_hash(pos_data)
                        if pos_hash != _last_pos_hash:
                            _last_pos_hash = pos_hash
                            await websocket.send_text(json.dumps({
                                "type": "positions_detail",
                                "data": pos_data,
                                "ts": ts,
                            }))
                    except Exception as e:
                        logger.debug("WS feed positions error: %s", e)

                # ── Strategy status (every 3 cycles) ──────────────────────
                if cycle % 3 == 0:
                    try:
                        from routers.strategy import _get_all_strategy_status
                        strat_data = _get_all_strategy_status()
                        strat_hash = _quick_hash(strat_data)
                        if strat_hash != _last_strat_hash:
                            _last_strat_hash = strat_hash
                            await websocket.send_text(json.dumps({
                                "type": "strategy_status",
                                "data": strat_data,
                                "ts": ts,
                            }))
                    except Exception as e:
                        logger.debug("WS feed strategy_status error: %s", e)

                # ── Broker status (every 5 cycles) ────────────────────────
                if cycle % 5 == 0:
                    try:
                        from broker.multi_broker import registry
                        live_sessions = registry.list_live_sessions(user_id)
                        bstatus = {
                            "isLive": bool(live_sessions),
                            "brokers": list({s.broker_id for s in live_sessions}) if live_sessions else [],
                            "count": len(live_sessions),
                        }
                        bs_hash = _quick_hash(bstatus)
                        if bs_hash != _last_broker_status_hash:
                            _last_broker_status_hash = bs_hash
                            await websocket.send_text(json.dumps({
                                "type": "broker_status",
                                "data": bstatus,
                                "ts": ts,
                            }))
                    except Exception as e:
                        logger.debug("WS feed broker_status error: %s", e)

                # ── Option chain (every 3 cycles, if subscribed) ──────────
                if subscribed_option_chain and cycle % 3 == 0:
                    try:
                        from routers.market import get_option_chain as _oc_endpoint
                        oc_data = await _oc_endpoint(
                            subscribed_option_chain["symbol"],
                            subscribed_option_chain.get("exchange", "NSE"),
                            15,
                            subscribed_option_chain.get("expiry") or None,
                        )
                        oc_hash = _quick_hash(oc_data)
                        if oc_hash != _last_oc_hash:
                            _last_oc_hash = oc_hash
                            await websocket.send_text(json.dumps({
                                "type": "option_chain",
                                "data": oc_data,
                                "ts": ts,
                            }))
                    except Exception as e:
                        logger.debug("WS feed option_chain error: %s", e)

                # ── Market depth (every 2 cycles, if subscribed) ──────────
                if subscribed_market_depth and cycle % 2 == 0:
                    try:
                        from routers.market import get_market_depth as _depth_endpoint
                        depth_data = await _depth_endpoint(subscribed_market_depth)
                        depth_hash = _quick_hash(depth_data)
                        if depth_hash != _last_depth_hash:
                            _last_depth_hash = depth_hash
                            await websocket.send_text(json.dumps({
                                "type": "market_depth",
                                "data": depth_data,
                                "ts": ts,
                            }))
                    except Exception as e:
                        logger.debug("WS feed market_depth error: %s", e)

                # Heartbeat
                await websocket.send_text(json.dumps({"type": "heartbeat", "ts": ts}))

            except Exception:
                stop_event.set()
                return

            # ── Instant events: drain & push any order/position events ────
            instant_events = event_bus.drain(user_id)
            for ev in instant_events:
                try:
                    await websocket.send_text(json.dumps(ev))
                except Exception:
                    break

            # Event-driven sleep: wake instantly when event_bus fires,
            # otherwise fall back to 1-second cycle
            await event_bus.wait(user_id, timeout=1.0)

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

            elif action == "subscribe_option_chain":
                sym = msg.get("symbol", "")
                if sym:
                    subscribed_option_chain = {
                        "symbol": sym.upper(),
                        "expiry": msg.get("expiry", ""),
                        "exchange": msg.get("exchange", "NSE"),
                    }
                    _last_oc_hash = ""  # force immediate push
                    logger.info("WS feed: subscribe_option_chain %s user=%s", sym, user_id[:8])
                    await websocket.send_text(json.dumps({
                        "type": "option_chain_subscribed",
                        "symbol": sym.upper(),
                    }))

            elif action == "unsubscribe_option_chain":
                subscribed_option_chain = None
                await websocket.send_text(json.dumps({"type": "option_chain_unsubscribed"}))

            elif action == "subscribe_market_depth":
                sym = msg.get("symbol", "")
                if sym:
                    subscribed_market_depth = sym.upper()
                    _last_depth_hash = ""  # force immediate push
                    logger.info("WS feed: subscribe_market_depth %s user=%s", sym, user_id[:8])
                    await websocket.send_text(json.dumps({
                        "type": "market_depth_subscribed",
                        "symbol": sym.upper(),
                    }))

            elif action == "unsubscribe_market_depth":
                subscribed_market_depth = None
                await websocket.send_text(json.dumps({"type": "market_depth_unsubscribed"}))

    except WebSocketDisconnect:
        logger.info("WS feed disconnected user=%s", user_id[:8])
    except Exception as e:
        logger.error("WS feed error user=%s: %s", user_id[:8], e)
    finally:
        stop_event.set()
        push_task.cancel()
        try:
            from core.event_bus import event_bus
            event_bus.cleanup(user_id)
        except Exception:
            pass
        try:
            await push_task
        except asyncio.CancelledError:
            pass
