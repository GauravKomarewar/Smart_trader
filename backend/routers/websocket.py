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


# ── Per-session data cache (prevents redundant API calls + anti-flicker) ──────

import threading as _threading

class _SessionDataCache:
    """Cache of last-good data per broker session.
    
    Fetched once per cycle; both _build_dashboard and _build_broker_accounts
    read from here instead of making separate API calls.
    """

    def __init__(self):
        self._lock = _threading.Lock()
        # config_id → {limits, positions, orders, trades, holdings}
        self._data: dict[str, dict] = {}

    def refresh(self, user_id: str):
        """Fetch fresh data from all live sessions. Cache last-good per session."""
        from broker.multi_broker import registry as mb
        sessions = mb.list_live_sessions(user_id)
        for sess in sessions:
            cid = sess.config_id
            old = self._data.get(cid, {})
            new_entry: dict = {}

            # Limits
            try:
                lim = sess.get_limits()
                new_entry["limits"] = lim if lim else old.get("limits", {})
            except Exception:
                new_entry["limits"] = old.get("limits", {})

            # Positions
            try:
                pos = sess.get_positions()
                new_entry["positions"] = pos if pos is not None else old.get("positions", [])
            except Exception:
                new_entry["positions"] = old.get("positions", [])

            # Orders
            try:
                ords = sess.get_order_book()
                new_entry["orders"] = ords if ords is not None else old.get("orders", [])
            except Exception:
                new_entry["orders"] = old.get("orders", [])

            # Trades
            try:
                trd = sess.get_tradebook()
                new_entry["trades"] = trd if trd is not None else old.get("trades", [])
            except Exception:
                new_entry["trades"] = old.get("trades", [])

            # Holdings
            try:
                hold = sess.get_holdings()
                new_entry["holdings"] = hold if hold is not None else old.get("holdings", [])
            except Exception:
                new_entry["holdings"] = old.get("holdings", [])

            # Session metadata
            new_entry["sess"] = sess

            with self._lock:
                self._data[cid] = new_entry

    def get_session_data(self, config_id: str) -> dict:
        with self._lock:
            return self._data.get(config_id, {})

    def get_all_sessions(self) -> list[dict]:
        with self._lock:
            return list(self._data.values())

    def clear(self):
        with self._lock:
            self._data.clear()


def _build_broker_accounts_cached(cache: _SessionDataCache, user_id: str) -> list:
    """Build enriched broker account cards from cached data (no extra API calls)."""
    entries = cache.get_all_sessions()
    accounts = []
    for entry in entries:
        sess = entry.get("sess")
        if not sess or not sess.is_live:
            continue

        limits = entry.get("limits", {})
        positions = entry.get("positions", [])
        orders = entry.get("orders", [])
        trades = entry.get("trades", [])

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


def _build_dashboard_cached(cache: _SessionDataCache, user_id: str) -> dict:
    """Build combined dashboard snapshot from cached data (no extra API calls)."""
    from routers.orders import _normalize_position, _normalize_order

    all_positions = []
    all_orders = []
    all_holdings = []
    all_trades = []
    funds = []

    for entry in cache.get_all_sessions():
        sess = entry.get("sess")
        if not sess or not sess.is_live:
            continue
        cid = sess.config_id
        bid = sess.broker_id
        clid = sess.client_id

        for p in entry.get("positions", []):
            all_positions.append(dict(p, account_id=cid, broker_id=bid, client_id=clid))
        for o in entry.get("orders", []):
            all_orders.append(dict(o, account_id=cid, broker_id=bid, client_id=clid))
        all_holdings.extend(entry.get("holdings", []))
        all_trades.extend(entry.get("trades", []))

        lim = entry.get("limits", {})
        if lim:
            funds.append({
                "available_cash": float(lim.get("available_cash") or lim.get("marginAvailable") or lim.get("cash") or 0),
                "used_margin": float(lim.get("used_margin") or lim.get("marginUsed") or 0),
            })

    # Filter out closed positions (qty=0) before normalizing
    active_raw = [
        p for p in all_positions
        if float(p.get("netqty") or p.get("net_quantity") or p.get("qty", 0)) != 0
    ]
    positions = [_normalize_position(p, i) for i, p in enumerate(active_raw)]
    orders = [_normalize_order(o, i) for i, o in enumerate(all_orders)]

    total_equity = sum(f.get("available_cash", 0) + f.get("used_margin", 0) for f in funds)
    day_pnl = unrealized = realized = used_margin = avail_margin = cash = 0.0
    for p in positions:
        if isinstance(p, dict):
            unrealized += float(p.get("dayPnl") or p.get("unrealised_pnl") or 0)
            realized += float(p.get("pnl", 0)) - float(p.get("dayPnl") or p.get("unrealised_pnl") or 0)
    day_pnl = unrealized + realized
    for f in funds:
        used_margin += float(f.get("used_margin") or 0)
        avail_margin += float(f.get("available_cash") or 0)
        cash += float(f.get("available_cash") or 0)

    return {
        "positions": positions,
        "holdings": all_holdings,
        "orders": orders,
        "trades": all_trades,
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


def _build_broker_data_cached(cache: _SessionDataCache, config_id: str) -> dict | None:
    """Build per-broker filtered data from cached data."""
    from routers.orders import _normalize_position, _normalize_order
    entry = cache.get_session_data(config_id)
    if not entry:
        return None
    raw_positions = entry.get("positions", [])
    holdings = entry.get("holdings", [])
    raw_orders = entry.get("orders", [])
    trades = entry.get("trades", [])

    active_raw = [
        p for p in raw_positions
        if float(p.get("netqty") or p.get("net_quantity") or p.get("qty", 0)) != 0
    ]
    positions = [_normalize_position(p, i) for i, p in enumerate(active_raw)]
    orders = [_normalize_order(o, i) for i, o in enumerate(raw_orders)]

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

    data_cache = _SessionDataCache()
    _last_good_dashboard: dict | None = None
    _last_good_accounts: list | None = None

    async def _push_loop():
        """Background task: refresh cache once, then build all views from it."""
        nonlocal _last_good_dashboard, _last_good_accounts
        cycle = 0
        while not stop_event.is_set():
            try:
                cycle += 1
                ts = int(time.time())

                # ── Step 1: Refresh data cache (ONE set of API calls) ─────
                try:
                    await asyncio.get_event_loop().run_in_executor(
                        None, data_cache.refresh, user_id
                    )
                except Exception as e:
                    logger.debug("WS feed cache refresh error: %s", e)

                # ── Step 2: Dashboard (every cycle, ~2s) ──────────────────
                try:
                    dashboard = _build_dashboard_cached(data_cache, user_id)
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

                # ── Step 3: Risk alerts (every cycle) ─────────────────────
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

                # ── Step 4: Broker accounts (every 2 cycles) ──────────────
                if cycle % 2 == 0:
                    try:
                        accounts = _build_broker_accounts_cached(data_cache, user_id)
                        # Anti-flicker: only push if accounts have data
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

                # ── Step 5: Per-broker data (if subscribed) ───────────────
                if subscribed_broker:
                    try:
                        bdata = _build_broker_data_cached(data_cache, subscribed_broker)
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
