"""Orders router — place, modify, cancel, book."""

import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import Optional
from sqlalchemy.orm import Session

from broker.shoonya_client import get_best_session, register_user_session
from core.deps import current_user
from db.database import get_db, BrokerSession as DBSession, BrokerConfig

logger = logging.getLogger("smart_trader.orders")
router = APIRouter(prefix="/orders", tags=["orders"])


def _resolve_session(user_id: str, db: Session):
    """
    Return the best live session for a user.
    If the in-memory registry is empty (e.g. after restart), restore from DB.
    """
    session = get_best_session(user_id)
    if not session.is_demo:
        return session
    # Try DB restore — only USER_ID + token needed for REST calls
    try:
        db_sess = (
            db.query(DBSession)
            .filter(DBSession.user_id == user_id, DBSession.is_logged_in == True)
            .order_by(DBSession.login_at.desc())
            .first()
        )
        if db_sess and db_sess.session_token is not None:
            cfg = db.query(BrokerConfig).filter(BrokerConfig.id == db_sess.config_id).first()
            if cfg:
                register_user_session(user_id, {"USER_ID": str(cfg.client_id)}, str(db_sess.session_token))
                logger.info("Auto-restored Shoonya session from DB for user=%s", user_id)
                return get_best_session(user_id)
    except Exception as exc:
        logger.debug("DB session restore failed: %s", exc)
    return session  # demo fallback


class PlaceOrderRequest(BaseModel):
    symbol: str
    exchange: str = "NSE"
    transactionType: str         # "B" (buy) or "S" (sell)
    productType: str = "MIS"     # MIS / NRML / CNC
    orderType: str = "LMT"       # LMT / MKT / SL-LMT / SL-MKT
    quantity: int = Field(ge=1)
    price: float = 0.0
    triggerPrice: Optional[float] = None
    tag: Optional[str] = "SmartTrader"
    validity: str = "DAY"
    # Multi-broker routing: which account to use (config_id from /broker/configs)
    accountId: Optional[str] = None   # None = use best available session


@router.post("/place")
async def place_order(
    req: PlaceOrderRequest,
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """
    Place a new order via the user's connected broker account.

    Multi-broker routing:
      • If `accountId` is provided → uses that specific broker account
      • Otherwise → uses the first active live session (or demo fallback)

    Pre-trade risk check is enforced per-account before sending to broker.
    """
    user_id = payload["sub"]

    # ── Multi-broker routing ──────────────────────────────────────────────────
    if req.accountId:
        from broker.multi_broker import registry as mb_registry
        mb_sess = mb_registry.get_session(user_id, req.accountId)
        if mb_sess and not mb_sess.is_demo:
            # ── Pre-trade risk check ──────────────────────────────────────────
            order_dict = {
                "transaction_type": req.transactionType,
                "symbol":    req.symbol,
                "exchange":  req.exchange,
                "quantity":  req.quantity,
                "price":     req.price,
                "tag":       req.tag or "",
            }
            try:
                from broker.account_risk import get_account_risk
                rm = get_account_risk(
                    user_id   = user_id,
                    config_id = req.accountId,
                    broker_id = mb_sess.broker_id,
                    client_id = mb_sess.client_id,
                )
                allowed, reason = rm.validate_order(order_dict)
                if not allowed:
                    logger.warning(
                        "Order blocked by risk guard [user=%s client=%s]: %s",
                        user_id[:8], mb_sess.client_id, reason,
                    )
                    raise HTTPException(status_code=400, detail=reason)
            except HTTPException:
                raise
            except Exception as risk_e:
                logger.warning("Risk check error (non-fatal): %s", risk_e)

            logger.info(
                "PlaceOrder (multi-broker) [user=%s broker=%s client=%s]: "
                "%s %s qty=%d price=%.2f via account=%s",
                user_id[:8], mb_sess.broker_id, mb_sess.client_id,
                req.transactionType, req.symbol, req.quantity, req.price, req.accountId[:8],
            )
            result = mb_sess.place_order({
                "transaction_type": req.transactionType,
                "product_type":     req.productType,
                "exchange":         req.exchange,
                "symbol":           req.symbol,
                "quantity":         req.quantity,
                "price_type":       req.orderType,
                "price":            req.price,
                "trigger_price":    req.triggerPrice or 0,
                "retention":        req.validity,
                "tag":              req.tag or "smart_trader",
            })
            if not result.get("success"):
                raise HTTPException(status_code=400, detail=result.get("message", "Order failed"))
            return result

    # ── Legacy single-session fallback ────────────────────────────────────────
    session = _resolve_session(user_id, db)
    if not session.is_logged_in:
        raise HTTPException(status_code=401, detail="Not authenticated — connect your broker first")

    logger.info(
        "PlaceOrder: %s %s %s qty=%d price=%.2f type=%s",
        req.transactionType, req.symbol, req.productType,
        req.quantity, req.price, req.orderType,
    )
    result = session.place_order(req.model_dump())
    if not result.get("success"):
        logger.error("Order failed: %s", result)
        raise HTTPException(status_code=400, detail=result.get("message", "Order failed"))

    logger.info("Order placed: orderId=%s", result.get("orderId"))
    return result


@router.get("/book")
async def get_order_book(
    account_id: Optional[str] = None,
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """
    Return today's order book.
    If account_id is provided → single account.
    Otherwise → aggregated across all live accounts.
    """
    user_id = payload["sub"]
    from broker.multi_broker import registry as mb
    live_sessions = mb.list_live_sessions(user_id)

    if live_sessions:
        orders = mb.get_order_book(user_id, config_id=account_id)
        if not orders and not account_id:
            # Fall back to legacy session
            session = _resolve_session(user_id, db)
            orders = session.get_order_book()
    else:
        session = _resolve_session(user_id, db)
        orders = session.get_order_book()

    return {"data": orders, "count": len(orders)}


@router.get("/positions")
async def get_positions(
    account_id: Optional[str] = None,
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """
    Return open positions.
    If account_id is provided → single account.
    Otherwise → aggregated across all live accounts.
    """
    user_id = payload["sub"]
    from broker.multi_broker import registry as mb
    live_sessions = mb.list_live_sessions(user_id)

    if live_sessions:
        positions = mb.get_positions(user_id, config_id=account_id)
        if not positions and not account_id:
            session = _resolve_session(user_id, db)
            positions = session.get_positions()
    else:
        session = _resolve_session(user_id, db)
        positions = session.get_positions()

    return {"data": positions, "count": len(positions)}


# ── Field normalizers ─────────────────────────────────────────────────────────

def _prd_map(prd: str) -> str:
    return {"M": "MIS", "C": "CNC", "I": "NRML"}.get(str(prd).upper(), str(prd).upper())

def _side_from_qty(netqty) -> str:
    try:
        return "SELL" if float(netqty) < 0 else "BUY"
    except Exception:
        return "BUY"

def _order_status(raw: str) -> str:
    mapping = {
        "COMPLETE": "COMPLETE", "OPEN": "OPEN", "PENDING": "PENDING",
        "CANCELLED": "CANCELLED", "REJECTED": "REJECTED",
        "TRANSIT": "PENDING", "TRIGGER_PENDING": "TRIGGER_PENDING",
    }
    return mapping.get(str(raw).upper(), "OPEN")

def _order_type_map(prctyp: str) -> str:
    return {
        "LMT": "LIMIT", "MKT": "MARKET",
        "SL-LMT": "SL", "SL-MKT": "SL-M",
    }.get(str(prctyp).upper(), "LIMIT")

def _normalize_position(p: dict, idx: int) -> dict:
    """Map raw Shoonya position fields to frontend Position schema."""
    tsym = p.get("tsym") or p.get("tradingsymbol") or p.get("symbol", f"UNKNOWN{idx}")
    netqty = float(p.get("netqty") or p.get("net_quantity") or p.get("qty", 0))
    lp     = float(p.get("lp")    or p.get("last_price") or p.get("ltp", 0))
    avgprc = float(p.get("avgprc") or p.get("average_price") or p.get("avg_price", 0))
    rpnl   = float(p.get("rpnl")  or p.get("realised_pnl") or p.get("realized_pnl", 0))
    urmtom = float(p.get("urmtom") or p.get("unrealised_pnl") or p.get("pnl", 0))
    exch   = p.get("exch") or p.get("exchange", "NSE")
    prd    = _prd_map(p.get("prd") or p.get("product", "MIS"))
    abs_qty = abs(int(netqty))
    pnl = rpnl + urmtom
    value = lp * abs_qty
    return {
        "id": p.get("token") or p.get("symboltoken") or f"pos-{idx}",
        "accountId": p.get("actid") or "live",
        "symbol": tsym,
        "tradingsymbol": tsym,
        "exchange": exch,
        "product": prd,
        "quantity": int(netqty),
        "avgPrice": avgprc,
        "ltp": lp,
        "pnl": pnl,
        "pnlPct": (pnl / (avgprc * abs_qty) * 100) if avgprc and abs_qty else 0.0,
        "dayPnl": urmtom,
        "value": value,
        "multiplier": 1,
        "side": _side_from_qty(netqty),
        "type": "OPT" if "CE" in tsym or "PE" in tsym else ("FUT" if "FUT" in tsym else "EQ"),
    }

def _normalize_order(o: dict, idx: int) -> dict:
    """Map raw Shoonya order fields to frontend Order schema."""
    tsym    = o.get("tsym") or o.get("tradingsymbol") or o.get("symbol", f"UNKNOWN{idx}")
    exch    = o.get("exch") or o.get("exchange", "NSE")
    trantype = o.get("trantype") or o.get("transaction_type", "B")
    txn = "BUY" if str(trantype).upper() in ("B", "BUY") else "SELL"
    qty     = int(float(o.get("qty") or o.get("quantity", 0)))
    filled  = int(float(o.get("fillshares") or o.get("filled_quantity", 0)))
    prc     = float(o.get("prc") or o.get("price", 0))
    avgprc  = float(o.get("avgprc") or o.get("average_price", 0))
    trgprc  = float(o.get("trgprc") or o.get("trigger_price") or 0)
    status  = _order_status(o.get("status") or o.get("order_status", "OPEN"))
    prctyp  = _order_type_map(o.get("prctyp") or o.get("price_type") or o.get("order_type", "LMT"))
    prd     = _prd_map(o.get("prd") or o.get("product", "MIS"))
    order_id = o.get("norenordno") or o.get("order_id") or o.get("orderId", f"ord-{idx}")
    placed_at = o.get("ordertime") or o.get("placed_at") or datetime.now().isoformat()
    return {
        "id": order_id,
        "accountId": o.get("actid") or "live",
        "orderId": order_id,
        "symbol": tsym,
        "tradingsymbol": tsym,
        "exchange": exch,
        "type": "OPT" if "CE" in tsym or "PE" in tsym else ("FUT" if "FUT" in tsym else "EQ"),
        "transactionType": txn,
        "orderType": prctyp,
        "product": prd,
        "quantity": qty,
        "filledQty": filled,
        "price": prc,
        "triggerPrice": trgprc if trgprc else None,
        "avgPrice": avgprc,
        "status": status,
        "statusMessage": o.get("rejreason") or o.get("status_message"),
        "validity": "DAY",
        "tag": o.get("remarks") or o.get("tag"),
        "placedAt": placed_at,
        "updatedAt": placed_at,
    }


@router.get("/dashboard")
async def get_dashboard(payload: dict = Depends(current_user), db: Session = Depends(get_db)):
    """
    Return normalized live dashboard data: positions, orders, holdings, trades, account summary.
    Aggregates across all connected broker accounts (multi-broker aware).
    Falls back to legacy single session if no multi-broker sessions exist.
    """
    user_id = payload["sub"]
    from broker.multi_broker import registry as mb

    live_sessions = mb.list_live_sessions(user_id)
    all_raw_positions = []
    all_raw_orders = []
    all_raw_holdings = []
    all_raw_trades = []
    all_limits = {}
    is_live = False

    if live_sessions:
        is_live = True
        for sess in live_sessions:
            try:
                raw_pos = sess.get_positions() or []
                all_raw_positions.extend(raw_pos)
            except Exception as e:
                logger.warning("Dashboard: get_positions failed for %s: %s", sess.client_id, e)
            try:
                raw_ord = sess.get_order_book() or []
                all_raw_orders.extend(raw_ord)
            except Exception as e:
                logger.warning("Dashboard: get_order_book failed for %s: %s", sess.client_id, e)
            try:
                raw_hold = sess.get_holdings() or []
                all_raw_holdings.extend(raw_hold)
            except Exception as e:
                logger.debug("Dashboard: get_holdings failed for %s: %s", sess.client_id, e)
            try:
                raw_trades = sess.get_tradebook() or []
                all_raw_trades.extend(raw_trades)
            except Exception as e:
                logger.debug("Dashboard: get_tradebook failed for %s: %s", sess.client_id, e)
        # Aggregate limits from first live session (primary account)
        try:
            all_limits = live_sessions[0].get_limits() or {}
        except Exception:
            pass
    else:
        # Legacy single-session fallback
        session = _resolve_session(user_id, db)
        is_live = not session.is_demo
        all_raw_positions = session.get_positions() or []
        all_raw_orders = session.get_order_book() or []
        if is_live:
            all_limits = session.get_limits() or {}

    positions = [_normalize_position(p, i) for i, p in enumerate(all_raw_positions)]
    orders = [_normalize_order(o, i) for i, o in enumerate(all_raw_orders)]

    # Normalize holdings
    holdings = []
    for i, h in enumerate(all_raw_holdings):
        if isinstance(h, dict):
            holdings.append(h)
        else:
            try:
                holdings.append(h.to_dict() if hasattr(h, 'to_dict') else vars(h))
            except Exception:
                pass

    # Normalize trades
    trades = []
    for i, t in enumerate(all_raw_trades):
        if isinstance(t, dict):
            trades.append(t)
        else:
            try:
                trades.append(t.to_dict() if hasattr(t, 'to_dict') else vars(t))
            except Exception:
                pass

    # Account summary — aggregate P&L from positions
    total_pnl = sum(p["pnl"] for p in positions)
    realized_pnl = 0.0
    for rp in all_raw_positions:
        if isinstance(rp, dict):
            realized_pnl += float(rp.get("rpnl") or rp.get("realised_pnl") or rp.get("realized_pnl") or 0)
    unrealized = total_pnl - realized_pnl

    return {
        "positions": positions,
        "holdings": holdings,
        "orders": orders,
        "trades": trades,
        "riskMetrics": None,
        "accountSummary": {
            "totalEquity": all_limits.get("totalBalance") or all_limits.get("grossAvailableMargin", 0.0),
            "dayPnl": total_pnl,
            "dayPnlPct": 0.0,
            "unrealizedPnl": unrealized,
            "realizedPnl": realized_pnl,
            "usedMargin": all_limits.get("marginUsed") or all_limits.get("utilizedDebits", 0.0),
            "availableMargin": all_limits.get("marginAvailable") or all_limits.get("cash", 0.0),
            "cash": all_limits.get("cash", 0.0),
            "payin": all_limits.get("payin", 0.0),
        },
        "source": "live" if is_live else "demo",
    }


@router.get("/account-info")
async def get_account_info(payload: dict = Depends(current_user), db: Session = Depends(get_db)):
    """Return broker account details: client ID, limits and connection info."""
    from db.database import BrokerSession as DBSession, BrokerConfig
    user_id = payload["sub"]
    session = _resolve_session(user_id, db)

    limits = {}
    client_id = None
    broker_name = None
    login_at = None

    # Get client_id from DB session
    try:
        db_sess = (
            db.query(DBSession)
            .filter(DBSession.user_id == user_id, DBSession.is_logged_in == True)
            .order_by(DBSession.login_at.desc())
            .first()
        )
        if db_sess:
            cfg = db.query(BrokerConfig).filter(BrokerConfig.id == db_sess.config_id).first()
            if cfg:
                client_id   = cfg.client_id
                broker_name = cfg.broker_name or cfg.broker_id
            login_at = db_sess.login_at.isoformat() if db_sess.login_at is not None else None
    except Exception as exc:
        logger.debug("account-info DB lookup failed: %s", exc)

    if not session.is_demo:
        limits = session.get_limits()

    return {
        "isLive":     not session.is_demo,
        "clientId":   client_id,
        "brokerName": broker_name,
        "loginAt":    login_at,
        "limits":     limits,
    }


# ── Holdings ──────────────────────────────────────────────────────────────────

@router.get("/holdings")
async def get_holdings(
    account_id: Optional[str] = None,
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """
    Return equity holdings (long-term demat positions).
    If account_id is provided → single account; otherwise all live accounts.
    """
    user_id = payload["sub"]
    from broker.multi_broker import registry as mb
    holdings = mb.get_holdings(user_id, config_id=account_id)
    return {"data": holdings, "count": len(holdings)}


# ── Funds / Margin ────────────────────────────────────────────────────────────

@router.get("/funds")
async def get_funds(
    account_id: Optional[str] = None,
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """
    Return available funds and margin utilisation.
    If account_id is provided → single account; otherwise all live accounts.
    """
    user_id = payload["sub"]
    from broker.multi_broker import registry as mb
    live_sessions = mb.list_live_sessions(user_id)

    if live_sessions:
        funds = mb.get_funds(user_id, config_id=account_id)
    else:
        # Legacy fallback
        session = _resolve_session(user_id, db)
        raw = session.get_limits()
        funds = [raw] if raw else []

    return {"data": funds, "count": len(funds)}


# ── Tradebook ─────────────────────────────────────────────────────────────────

@router.get("/tradebook")
async def get_tradebook(
    account_id: Optional[str] = None,
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """
    Return today's executed trades (fills).
    If account_id is provided → single account; otherwise all live accounts.
    """
    user_id = payload["sub"]
    from broker.multi_broker import registry as mb
    trades = mb.get_tradebook(user_id, config_id=account_id)
    return {"data": trades, "count": len(trades)}


# ── Per-broker Account Summary ────────────────────────────────────────────────

@router.get("/account-summary")
async def get_account_summary(
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """
    Return a summary card for every connected broker account belonging to this user.
    Each card includes: broker name, client ID, available margin, used margin,
    total balance, number of positions, and session mode.
    Empty list when no brokers are connected.
    """
    user_id = payload["sub"]
    from broker.multi_broker import registry as mb
    from db.database import BrokerConfig as DBConfig

    live_sessions = mb.list_live_sessions(user_id)

    # Also include every DB broker config so disconnected accounts show up
    db_configs = (
        db.query(DBConfig)
        .filter(DBConfig.user_id == user_id, DBConfig.is_active == True)
        .all()
    )
    db_by_id = {str(c.id): c for c in db_configs}

    summaries = []
    seen_ids = set()

    # First: all live sessions
    for sess in live_sessions:
        seen_ids.add(sess.config_id)
        limits = {}
        positions_count = 0
        try:
            limits = sess.get_limits() or {}
        except Exception:
            pass
        try:
            positions_count = len(sess.get_positions())
        except Exception:
            pass

        available  = (limits.get("marginAvailable") or limits.get("cash") or
                      limits.get("net") or limits.get("availablecash") or 0.0)
        used       = (limits.get("marginUsed") or limits.get("utilizedDebits") or
                      limits.get("debits") or 0.0)
        total      = (limits.get("totalBalance") or limits.get("grossAvailableMargin") or
                      (available + used) or 0.0)

        db_cfg = db_by_id.get(sess.config_id)
        summaries.append({
            "config_id":  sess.config_id,
            "broker_id":  sess.broker_id,
            "broker_name": (db_cfg.broker_name if db_cfg else sess.broker_id).title(),
            "client_id":  sess.client_id,
            "mode":       sess.mode,
            "is_live":    sess.is_live,
            "connected_at": sess.connected_at.isoformat() if sess.connected_at else None,
            "available":  float(available),
            "used":       float(used),
            "total":      float(total),
            "positions_count": positions_count,
            "error":      sess.error,
        })

    # Then: DB configs that are NOT in-memory (disconnected / never connected)
    for cfg in db_configs:
        if cfg.id in seen_ids:
            continue
        summaries.append({
            "config_id":  cfg.id,
            "broker_id":  cfg.broker_id,
            "broker_name": (cfg.broker_name or cfg.broker_id).title(),
            "client_id":  cfg.client_id,
            "mode":       "offline",
            "is_live":    False,
            "connected_at": None,
            "available":  0.0,
            "used":       0.0,
            "total":      0.0,
            "positions_count": 0,
            "error":      None,
        })

    return {"accounts": summaries, "count": len(summaries)}
