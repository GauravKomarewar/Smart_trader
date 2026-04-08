"""Orders router — place, modify, cancel, book."""

import logging
import re
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field, validator
from typing import Any, Optional, List
import psycopg2.extras
from sqlalchemy.orm import Session

from broker.shoonya_client import get_best_session, register_user_session
from broker.symbol_normalizer import (
    to_broker_symbol, from_broker_symbol, normalize_position_from_broker,
    lookup_by_trading_symbol, get_lot_size,
)
from core.deps import current_user
from db.database import get_db, BrokerSession as DBSession, BrokerConfig
from db.trading_db import get_trading_conn

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
    # Additional accounts for multi-account orders
    accountIds: Optional[List[str]] = None
    # Execution type for order tracking
    executionType: str = "ENTRY"  # ENTRY | EXIT | ADJUSTMENT
    # Strategy name for tracking
    strategyName: str = "manual"
    # Risk fields
    stopLoss: Optional[float] = None
    target: Optional[float] = None
    trailingType: Optional[str] = None
    trailingValue: Optional[float] = None


class ModifyOrderRequest(BaseModel):
    accountId: str                      # config_id of the broker account
    quantity: Optional[int] = None
    price: Optional[float] = None
    triggerPrice: Optional[float] = None
    orderType: Optional[str] = None     # LMT / MKT / SL-LMT / SL-MKT
    validity: Optional[str] = None


def _resolve_order_target(
    user_id: str,
    requested_account_id: Optional[str],
    order_id: str,
) -> tuple[Optional[str], Optional[str], bool]:
    """Resolve an incoming dashboard order id to (config_id, broker_order_id, actionable)."""
    conn = get_trading_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        if requested_account_id:
            cur.execute(
                """
                SELECT config_id, order_id
                FROM mgr_orders
                WHERE user_id = %s AND config_id = %s AND order_id = %s
                LIMIT 1
                """,
                (user_id, requested_account_id, order_id),
            )
            row = cur.fetchone()
            if row:
                return row["config_id"], row["order_id"], True

        cur.execute(
            """
            SELECT config_id, order_id
            FROM mgr_orders
            WHERE user_id = %s AND order_id = %s
            ORDER BY fetched_at DESC
            LIMIT 1
            """,
            (user_id, order_id),
        )
        row = cur.fetchone()
        if row:
            return row["config_id"], row["order_id"], True

        cur.execute(
            """
            SELECT command_id, broker_order_id, client_id
            FROM orders
            WHERE user_id = %s AND (command_id = %s OR broker_order_id = %s)
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (user_id, order_id, order_id),
        )
        record = cur.fetchone()
        if not record:
            return requested_account_id, order_id, bool(requested_account_id)

        broker_order_id = record.get("broker_order_id")
        if not broker_order_id:
            return requested_account_id, None, False

        cur.execute(
            """
            SELECT config_id
            FROM mgr_orders
            WHERE user_id = %s AND order_id = %s
            ORDER BY fetched_at DESC
            LIMIT 1
            """,
            (user_id, broker_order_id),
        )
        mgr_row = cur.fetchone()
        resolved_account_id = requested_account_id or (mgr_row["config_id"] if mgr_row else None)
        if not resolved_account_id and record.get("client_id"):
            cur.execute(
                """
                SELECT config_id
                FROM mgr_sessions
                WHERE user_id = %s AND client_id = %s
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (user_id, record["client_id"]),
            )
            session_row = cur.fetchone()
            if session_row:
                resolved_account_id = session_row["config_id"]
        return resolved_account_id, broker_order_id, bool(resolved_account_id)
    finally:
        conn.close()


def _update_mgr_order_cache(
    user_id: str,
    config_id: Optional[str],
    order_id: str,
    changes: dict[str, Any],
) -> None:
    if not config_id:
        return

    conn = get_trading_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT data
            FROM mgr_orders
            WHERE user_id = %s AND config_id = %s AND order_id = %s
            LIMIT 1
            """,
            (user_id, config_id, order_id),
        )
        row = cur.fetchone()
        if not row:
            return
        data = dict(row["data"] or {})
        data.update({k: v for k, v in changes.items() if v is not None})
        cur.execute(
            """
            UPDATE mgr_orders
            SET data = %s, fetched_at = NOW()
            WHERE user_id = %s AND config_id = %s AND order_id = %s
            """,
            (psycopg2.extras.Json(data), user_id, config_id, order_id),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        logger.debug("mgr_orders cache update failed for %s/%s", config_id, order_id, exc_info=True)
    finally:
        conn.close()


def _update_order_db(
    user_id: str,
    order_id: str,
    *,
    status: Optional[str] = None,
    quantity: Optional[int] = None,
    price: Optional[float] = None,
    order_type: Optional[str] = None,
) -> None:
    set_parts = []
    values: list[Any] = []
    if status is not None:
        set_parts.append("status = %s")
        values.append(status)
    if quantity is not None:
        set_parts.append("quantity = %s")
        values.append(quantity)
    if price is not None:
        set_parts.append("price = %s")
        values.append(price)
    if order_type is not None:
        set_parts.append("order_type = %s")
        values.append(order_type)
    if not set_parts:
        return

    conn = get_trading_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            UPDATE orders
            SET {", ".join(set_parts)}, updated_at = NOW()
            WHERE user_id = %s AND (broker_order_id = %s OR command_id = %s)
            """,
            values + [user_id, order_id, order_id],
        )
        conn.commit()
    except Exception:
        conn.rollback()
        logger.debug("orders table update failed for %s", order_id, exc_info=True)
    finally:
        conn.close()


@router.post("/place")
async def place_order(
    req: PlaceOrderRequest,
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """
    Place a new order via the Unified Order Intent Pipeline.

    ALL orders from the frontend go through the Trading Bot checkpoint:
      Frontend → Order Intent → ExecutionGuard → Risk Manager → CommandService → Broker

    Every order is persisted to PostgreSQL with full lifecycle tracking.

    Multi-broker routing:
      • If `accountIds` / `accountId` provided → routes to those specific accounts
      • Otherwise → routes to all live accounts

    Returns:
      Order intent processing result with per-account status.
    """
    from trading.order_intent import OrderIntentPayload, LegPayload, process_order_intent

    user_id = payload["sub"]

    # Map frontend order type to canonical format
    order_type_map = {
        "MKT": "MARKET", "MARKET": "MARKET",
        "LMT": "LIMIT", "LIMIT": "LIMIT",
        "SL-LMT": "SL", "SL": "SL",
        "SL-MKT": "SL-M", "SL-M": "SL-M",
    }
    canonical_order_type = order_type_map.get(req.orderType.upper(), "MARKET")

    # Map transactionType (B/S) to direction (BUY/SELL)
    direction = "BUY" if req.transactionType.upper() in ("B", "BUY") else "SELL"

    # Resolve execution type from tag if present
    exec_type = req.executionType.upper()
    if req.tag and ":" in req.tag:
        tag_exec = req.tag.split(":")[0].upper()
        if tag_exec in ("ENTRY", "EXIT", "ADJUSTMENT"):
            exec_type = tag_exec

    # Resolve broker accounts
    broker_accounts = []
    if req.accountIds:
        broker_accounts = req.accountIds
    elif req.accountId:
        broker_accounts = [req.accountId]
    else:
        # Auto-resolve all live accounts
        from trading.trading_bot import _get_all_live_config_ids
        broker_accounts = _get_all_live_config_ids(user_id)

    if not broker_accounts:
        raise HTTPException(
            status_code=400,
            detail="No live broker accounts connected — connect a broker first"
        )

    # Build unified order intent
    intent = OrderIntentPayload(
        execution_type=exec_type,
        strategy_name=req.strategyName,
        exchange=req.exchange,
        broker_accounts=broker_accounts,
        legs=[LegPayload(
            tradingsymbol=req.symbol,
            direction=direction,
            qty=req.quantity,
            order_type=canonical_order_type,
            price=req.price,
            trigger_price=req.triggerPrice or 0.0,
            product_type=req.productType,
            stop_loss=req.stopLoss,
            target=req.target,
            trailing_type=req.trailingType,
            trailing_value=req.trailingValue,
        )],
        remarks=req.tag or "SmartTrader",
    )

    # Process through unified pipeline
    result = process_order_intent(user_id=user_id, intent=intent, db_session=db)

    response = result.to_dict()

    # For backward compatibility, add 'success' and 'message' fields
    if response["success"]:
        # Return first successful order info for backward compat
        first_order = response["orders"][0] if response["orders"] else {}
        response["orderId"] = first_order.get("command_id", "")
        response["message"] = (
            f"Order intent processed through {len(broker_accounts)} account(s)"
        )
    else:
        detail = "; ".join(response.get("errors", ["Order failed"]))
        raise HTTPException(status_code=400, detail=detail)

    return response


class SquareOffRequest(BaseModel):
    symbol: str
    exchange: str = "NSE"
    product: str = "MIS"
    quantity: int = Field(ge=1)
    side: str            # "BUY" or "SELL" — the CURRENT position side to close
    accountId: str       # config_id of the broker account

class SquareOffAllRequest(BaseModel):
    accountId: str       # config_id of the broker account


@router.post("/squareoff")
async def squareoff_position(
    req: SquareOffRequest,
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """
    Square off a single position by placing a MARKET order in the opposite direction.
    """
    from trading.order_intent import OrderIntentPayload, LegPayload, process_order_intent
    user_id = payload["sub"]
    exit_direction = "SELL" if req.side.upper() in ("BUY", "B", "LONG") else "BUY"
    prd_map = {"MIS": "MIS", "NRML": "NRML", "CNC": "CNC",
               "M": "MIS", "C": "CNC", "I": "NRML", "D": "NRML"}
    product = prd_map.get(req.product.upper(), req.product.upper())
    intent = OrderIntentPayload(
        execution_type="EXIT",
        strategy_name="manual_squareoff",
        exchange=req.exchange,
        broker_accounts=[req.accountId],
        legs=[LegPayload(
            tradingsymbol=req.symbol,
            direction=exit_direction,
            qty=req.quantity,
            order_type="MARKET",
            price=0.0,
            trigger_price=0.0,
            product_type=product,
        )],
        remarks="EXIT:manual_squareoff",
    )
    result = process_order_intent(user_id=user_id, intent=intent, db_session=db)
    if not result.success:
        detail = "; ".join(result.errors) if result.errors else "Squareoff failed"
        raise HTTPException(status_code=400, detail=detail)
    return {"success": True, "symbol": req.symbol}


@router.post("/squareoff-all")
async def squareoff_all(
    req: SquareOffAllRequest,
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """
    Square off all open positions for a specific broker account.
    Fetches open positions from DB cache and places MARKET exit orders.
    """
    from trading.order_intent import OrderIntentPayload, LegPayload, process_order_intent
    from managers.supreme_manager import supreme
    user_id = payload["sub"]

    rows = supreme.get_positions(user_id, config_id=req.accountId)
    raw_positions = [r["data"] if "data" in r else r for r in rows]

    errors = []
    success_count = 0
    for i, p in enumerate(raw_positions):
        qty = float(p.get("netqty") or p.get("net_quantity") or p.get("qty", 0))
        if qty == 0:
            continue  # already flat
        norm = _normalize_position(p, i)
        exit_direction = "SELL" if qty > 0 else "BUY"
        prd = norm.get("product", "MIS")
        prd_map = {"MIS": "MIS", "NRML": "NRML", "CNC": "CNC"}
        product = prd_map.get(prd, prd)
        try:
            intent = OrderIntentPayload(
                execution_type="EXIT",
                strategy_name="manual_squareoff_all",
                exchange=norm["exchange"],
                broker_accounts=[req.accountId],
                legs=[LegPayload(
                    tradingsymbol=norm["tradingsymbol"],
                    direction=exit_direction,
                    qty=abs(int(qty)),
                    order_type="MARKET",
                    price=0.0,
                    trigger_price=0.0,
                    product_type=product,
                )],
                remarks="EXIT:manual_squareoff_all",
            )
            result = process_order_intent(user_id=user_id, intent=intent, db_session=db)
            if result.success:
                success_count += 1
            else:
                errors.append(f"{norm['tradingsymbol']}: {'; '.join(result.errors or [])}")
        except Exception as e:
            errors.append(f"{norm.get('tradingsymbol', '?')}: {e}")

    return {
        "success": len(errors) == 0,
        "placed": success_count,
        "errors": errors,
    }


@router.delete("/{order_id}")
async def cancel_order(
    order_id: str,
    account_id: str,
    payload: dict = Depends(current_user),
):
    """Cancel a single open order by order_id on the specified account."""
    from broker.multi_broker import registry
    user_id = payload["sub"]
    resolved_account_id, broker_order_id, actionable = _resolve_order_target(user_id, account_id, order_id)
    if not actionable or not resolved_account_id or not broker_order_id:
        raise HTTPException(400, "Order is not yet available for broker-side cancellation")

    result = registry.cancel_order(user_id, resolved_account_id, broker_order_id)
    if not result.get("success"):
        raise HTTPException(400, result.get("message", "Cancel failed"))
    _update_mgr_order_cache(
        user_id,
        resolved_account_id,
        broker_order_id,
        {"status": "CANCELLED", "remarks": result.get("message") or "Cancelled"},
    )
    _update_order_db(user_id, broker_order_id, status="CANCELLED")
    return result


@router.delete("/cancel-all/{account_id}")
async def cancel_all_orders(
    account_id: str,
    payload: dict = Depends(current_user),
):
    """Cancel all open orders on the specified broker account."""
    from broker.multi_broker import registry
    user_id = payload["sub"]
    result = registry.cancel_all_orders(user_id, account_id)
    if not result.get("success"):
        raise HTTPException(400, result.get("message", "Cancel all failed"))
    return result


@router.patch("/{order_id}")
async def modify_order(
    order_id: str,
    req: ModifyOrderRequest,
    payload: dict = Depends(current_user),
):
    """Modify an open order (price, quantity, order type)."""
    from broker.multi_broker import registry
    user_id = payload["sub"]
    resolved_account_id, broker_order_id, actionable = _resolve_order_target(user_id, req.accountId, order_id)
    if not actionable or not resolved_account_id or not broker_order_id:
        raise HTTPException(400, "Order is not yet available for broker-side modification")

    params = {}
    if req.quantity is not None:
        params["quantity"] = req.quantity
        params["qty"] = req.quantity
    if req.price is not None:
        params["price"] = req.price
    if req.triggerPrice is not None:
        params["trigger_price"] = req.triggerPrice
    if req.orderType is not None:
        params["order_type"] = _order_type_map(req.orderType)
    if req.validity is not None:
        params["validity"] = req.validity
    result = registry.modify_order(user_id, resolved_account_id, broker_order_id, params)
    if not result.get("success"):
        raise HTTPException(400, result.get("message", "Modify failed"))
    canonical_order_type = _order_type_map(req.orderType or "")
    _update_mgr_order_cache(
        user_id,
        resolved_account_id,
        broker_order_id,
        {
            "qty": req.quantity,
            "price": req.price,
            "trigger_price": req.triggerPrice,
            "order_type": canonical_order_type if req.orderType else None,
        },
    )
    _update_order_db(
        user_id,
        broker_order_id,
        quantity=req.quantity,
        price=req.price,
        order_type=canonical_order_type if req.orderType else None,
    )
    return result


# ── Position SL/TG/Trail settings ─────────────────────────────────────────────

class SLSettingsRequest(BaseModel):
    configId: str               # broker account config_id
    posKey: str                 # "SYMBOL|PRODUCT" e.g. "GOLDPETAL30APR26|M"
    active: bool = True
    stopLoss: Optional[float] = None
    target: Optional[float] = None
    trailingValue: Optional[float] = None
    trailWhen: Optional[float] = None
    initialLtp: Optional[float] = None
    baseStopLoss: Optional[float] = None

    @validator("stopLoss", "target", "trailingValue", "trailWhen", "initialLtp", "baseStopLoss", pre=True)
    def non_negative(cls, v):
        if v is not None and v < 0:
            raise ValueError("Value must be non-negative")
        return v

    @validator("posKey")
    def valid_pos_key(cls, v):
        if "|" not in v:
            raise ValueError("posKey must be in 'SYMBOL|PRODUCT' format")
        return v


@router.put("/positions/sl-settings")
async def set_position_sl_settings(
    req: SLSettingsRequest,
    payload: dict = Depends(current_user),
):
    """Save or update SL/TG/Trail settings for a position. Triggers live monitoring."""
    from trading.position_sl_manager import PositionSLManager
    user_id = payload["sub"]
    result = PositionSLManager.save_settings(
        user_id=user_id,
        config_id=req.configId,
        pos_key=req.posKey,
        active=req.active,
        stop_loss=req.stopLoss,
        target=req.target,
        trailing_value=req.trailingValue,
        trail_when=req.trailWhen,
        initial_ltp=req.initialLtp,
        base_stop_loss=req.baseStopLoss,
    )
    return {"success": True, "settings": result}


@router.get("/positions/sl-settings")
async def get_position_sl_settings(
    payload: dict = Depends(current_user),
):
    """Return all active SL/TG/Trail settings for the current user."""
    from trading.position_sl_manager import PositionSLManager
    user_id = payload["sub"]
    settings = PositionSLManager.get_settings(user_id)
    return {"data": settings, "count": len(settings)}



@router.get("/book")
async def get_order_book(
    account_id: Optional[str] = None,
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """
    Return today's order book from PostgreSQL (via SupremeManager).
    If account_id is provided → single account.
    Otherwise → aggregated across all live accounts.
    """
    user_id = payload["sub"]
    from managers.supreme_manager import supreme
    rows = supreme.get_orders(user_id, config_id=account_id)
    orders = [r["data"] if "data" in r else r for r in rows]
    return {"data": orders, "count": len(orders)}


@router.get("/positions")
async def get_positions(
    account_id: Optional[str] = None,
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """
    Return open positions from PostgreSQL (via SupremeManager).
    If account_id is provided → single account.
    Otherwise → aggregated across all live accounts.
    """
    user_id = payload["sub"]
    from managers.supreme_manager import supreme
    rows = supreme.get_positions(user_id, config_id=account_id)
    positions = [r["data"] if "data" in r else r for r in rows]
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
        "LIMIT": "LIMIT", "MARKET": "MARKET", "SL": "SL", "SL-M": "SL-M",
    }.get(str(prctyp).upper(), "LIMIT")

def _infer_instrument_type(symbol: str) -> str:
    clean = str(symbol or "").upper().replace(" ", "")
    inst = lookup_by_trading_symbol(clean)
    if not inst and ":" in clean:
        inst = lookup_by_trading_symbol(clean.split(":", 1)[-1])
    if inst and getattr(inst, "instrument_type", None):
        inst_type = str(inst.instrument_type).upper()
        if inst_type == "OPT":
            return "OPT"
        return inst_type
    if re.search(r"\d{3,}(CE|PE)$", clean):
        return "OPT"
    if re.search(r"\d{2}[A-Z]{3}\d{2}[CP]\d+$", clean):
        return "OPT"
    if re.search(r"\d+FUT$", clean):
        return "FUT"
    return "EQ"

# Fyers returns exchange as int (10=NSE, 11=MCX, 12=BSE)
_NUMERIC_EXCH_MAP = {10: "NSE", 11: "MCX", 12: "BSE"}

def _resolve_exchange(raw_exch, symbol: str = "") -> str:
    """Convert raw exchange value (possibly numeric from Fyers) to string."""
    if isinstance(raw_exch, int):
        exch = _NUMERIC_EXCH_MAP.get(raw_exch, "NSE")
    elif isinstance(raw_exch, str) and raw_exch.isdigit():
        exch = _NUMERIC_EXCH_MAP.get(int(raw_exch), raw_exch)
    else:
        exch = str(raw_exch) if raw_exch else "NSE"
    sym_up = symbol.upper()
    is_deriv = sym_up.endswith("CE") or sym_up.endswith("PE") or "FUT" in sym_up
    if is_deriv:
        if exch == "NSE":
            return "NFO"
        if exch == "BSE":
            return "BFO"
    return exch

def _normalize_position(p: dict, idx: int) -> dict:
    """Map raw Shoonya position fields to frontend Position schema.

    Enriches with ScriptMaster data (lot_size, underlying, expiry, etc.)
    for consistent display across all brokers.
    """
    tsym = p.get("tsym") or p.get("tradingsymbol") or p.get("symbol", f"UNKNOWN{idx}")
    # Strip exchange prefix for canonical symbol
    clean_sym = tsym.split(":", 1)[-1] if ":" in tsym else tsym
    netqty = float(p.get("netqty") or p.get("net_quantity") or p.get("qty", 0))
    lp     = float(p.get("lp")    or p.get("last_price") or p.get("ltp", 0))
    # Shoonya: avgprc can be 0 for sell-first positions; try multiple fields
    avgprc = float(
        p.get("netavgprc") or p.get("avgprc") or p.get("average_price") or p.get("avg_price") or
        p.get("totbuyavgprc") or p.get("totsellavgprc") or
        p.get("daybuyavgprc") or p.get("daysellavgprc") or
        p.get("upldprc") or 0
    )
    # Use key-presence checks to avoid 0.0 falsiness bug
    def _first_present(d, *keys, default=0.0):
        for k in keys:
            if k in d and d[k] is not None:
                return d[k]
        return default
    rpnl   = float(_first_present(p, "rpnl", "realised_pnl", "realized_pnl"))
    urmtom = float(_first_present(p, "urmtom", "unrealised_pnl", "unrealized_pnl"))
    exch   = _resolve_exchange(p.get("exch") or p.get("exchange", "NSE"), clean_sym)
    prd    = _prd_map(p.get("prd") or p.get("product", "MIS"))
    abs_qty = abs(int(netqty))
    pnl = rpnl + urmtom
    value = lp * abs_qty

    # Enrich with ScriptMaster metadata
    inst = lookup_by_trading_symbol(clean_sym)
    if not inst:
        inst = lookup_by_trading_symbol(tsym)  # try with exchange prefix
    lot_size = inst.lot_size if inst else 1
    underlying = inst.symbol if inst else clean_sym
    inst_type = inst.instrument_type if inst else _infer_instrument_type(clean_sym)
    # For derivatives, notional value uses lot_size
    notional = value * lot_size if lot_size > 1 else value
    # pnlPct: broker pnl / cost-basis
    cost_basis = avgprc * abs_qty * lot_size if lot_size > 1 else avgprc * abs_qty
    return {
        "id": p.get("token") or p.get("symboltoken") or f"pos-{idx}",
        "accountId": p.get("actid") or p.get("account_id") or "live",
        "symbol": clean_sym,
        "trading_symbol": clean_sym,
        "tradingsymbol": clean_sym,
        "underlying": underlying,
        "exchange": exch,
        "product": prd,
        "quantity": int(netqty),
        "avgPrice": avgprc,
        "ltp": lp,
        "pnl": pnl,
        "pnlPct": (pnl / cost_basis * 100) if cost_basis else 0.0,
        "dayPnl": urmtom,
        "value": notional,
        "multiplier": 1,
        "lot_size": lot_size,
        "side": _side_from_qty(netqty),
        "type": inst_type,
        "expiry": inst.expiry if inst else "",
        "strike": inst.strike if inst else 0,
        "option_type": inst.option_type if inst else "",
    }

def _normalize_db_order(rec, idx: int) -> dict:
    """Convert an OrderRecord from PostgreSQL to the frontend Order schema."""
    tsym = rec.symbol or f"UNKNOWN{idx}"
    exch = rec.exchange or "NSE"
    txn  = rec.side or "BUY"
    qty  = int(rec.quantity or 0)
    prc  = float(rec.price or 0)
    # Map OMS status → dashboard status
    _st_map = {
        "CREATED": "PENDING", "SENT_TO_BROKER": "OPEN",
        "EXECUTED": "COMPLETE", "FAILED": "REJECTED",
        "CANCELLED": "CANCELLED",
    }
    status = _st_map.get(rec.status, rec.status or "PENDING")
    row_id = rec.broker_order_id or rec.command_id
    order_id = rec.broker_order_id or rec.command_id
    placed_at = rec.created_at or datetime.now().isoformat()
    prctyp  = _order_type_map(rec.order_type or "MARKET")
    prd     = _prd_map(rec.product or "MIS")
    broker_order_id = rec.broker_order_id or ""
    return {
        "id": row_id,
        "accountId": "",
        "clientId": rec.client_id or "",
        "orderId": order_id,
        "brokerOrderId": broker_order_id,
        "commandId": rec.command_id,
        "symbol": tsym,
        "tradingsymbol": tsym,
        "exchange": exch,
        "type": _infer_instrument_type(tsym),
        "transactionType": txn,
        "orderType": prctyp,
        "product": prd,
        "quantity": qty,
        "filledQty": qty if status == "COMPLETE" else 0,
        "price": prc,
        "triggerPrice": None,
        "avgPrice": prc if status == "COMPLETE" else 0,
        "status": status,
        "statusMessage": None,
        "validity": "DAY",
        "tag": rec.tag or rec.strategy_name,
        "placedAt": placed_at,
        "updatedAt": rec.updated_at or placed_at,
        "actionable": bool(broker_order_id),
        "source": "smart_trader_db",
    }


def _normalize_holding(h: dict, idx: int) -> dict:
    """Map broker holding data to the frontend Holding schema."""
    symbol = h.get("symbol") or h.get("tsym") or h.get("tradingsymbol") or f"HOLDING{idx}"
    exchange = _resolve_exchange(h.get("exchange") or h.get("exch") or "NSE", symbol)
    account_id = h.get("account_id") or h.get("accountId") or h.get("actid") or "live"
    quantity = int(float(h.get("qty") or h.get("holdqty") or h.get("quantity") or 0))
    avg_cost = float(h.get("avg_price") or h.get("avgprc") or h.get("avgCost") or 0)
    ltp = float(h.get("ltp") or h.get("lp") or 0)
    invested = float(h.get("investedValue") or (avg_cost * quantity))
    current = float(h.get("currentValue") or (ltp * quantity if ltp else invested))
    pnl = float(h.get("pnl") if h.get("pnl") is not None else (current - invested))
    pnl_pct = float(
        h.get("pnl_pct")
        if h.get("pnl_pct") is not None
        else h.get("pnlPct")
        if h.get("pnlPct") is not None
        else ((pnl / invested) * 100 if invested else 0)
    )
    prev_close = float(
        h.get("close")
        or h.get("close_price")
        or h.get("prev_close")
        or h.get("previous_close")
        or 0
    )
    day_change = float(
        h.get("dayChange")
        if h.get("dayChange") is not None
        else (ltp - prev_close if ltp and prev_close else 0)
    )
    day_change_pct = float(
        h.get("dayChangePct")
        if h.get("dayChangePct") is not None
        else ((day_change / prev_close) * 100 if prev_close else 0)
    )

    return {
        "id": f"{account_id}:{exchange}:{symbol}",
        "accountId": account_id,
        "symbol": symbol,
        "exchange": exchange,
        "isin": h.get("isin") or "",
        "quantity": quantity,
        "avgCost": avg_cost,
        "ltp": ltp,
        "currentValue": current,
        "investedValue": invested,
        "pnl": pnl,
        "pnlPct": pnl_pct,
        "dayChange": day_change,
        "dayChangePct": day_change_pct,
    }


def _normalize_trade(t: dict, idx: int) -> dict:
    """Map broker trade data to the frontend Trade schema."""
    symbol = t.get("tradingsymbol") or t.get("symbol") or t.get("tsym") or f"TRADE{idx}"
    account_id = t.get("account_id") or t.get("accountId") or t.get("actid") or "live"
    trade_id = str(t.get("trade_id") or t.get("tradeId") or t.get("fillid") or t.get("flid") or idx)
    order_id = str(t.get("order_id") or t.get("orderId") or t.get("norenordno") or "")
    quantity = int(float(t.get("qty") or t.get("quantity") or 0))
    price = float(t.get("price") or t.get("flprc") or 0)
    exchange = _resolve_exchange(t.get("exchange") or t.get("exch") or "NSE", symbol)
    transaction_type = str(t.get("side") or t.get("transactionType") or "BUY").upper()
    if transaction_type not in ("BUY", "SELL"):
        transaction_type = "BUY" if transaction_type in ("B", "1") else "SELL"
    product = _prd_map(t.get("product") or t.get("prd") or "MIS")
    traded_at = t.get("tradedAt") or t.get("timestamp") or t.get("exch_tm") or ""
    value = float(t.get("value") or (quantity * price))
    charges = float(t.get("charges") or 0)

    return {
        "id": trade_id,
        "accountId": account_id,
        "orderId": order_id,
        "tradeId": trade_id,
        "symbol": symbol,
        "tradingsymbol": symbol,
        "exchange": exchange,
        "transactionType": transaction_type,
        "product": product,
        "quantity": quantity,
        "price": price,
        "value": value,
        "charges": charges,
        "tradedAt": traded_at,
    }


def _normalize_order(o: dict, idx: int) -> dict:
    """Map raw broker order fields to frontend Order schema (Shoonya + Fyers)."""
    tsym    = o.get("tsym") or o.get("tradingsymbol") or o.get("symbol", f"UNKNOWN{idx}")
    exch    = _resolve_exchange(o.get("exch") or o.get("exchange", "NSE"), tsym)
    trantype = o.get("trantype") or o.get("transaction_type") or o.get("side", "B")
    txn = "BUY" if str(trantype).upper() in ("B", "BUY", "1") else "SELL"
    qty     = int(float(o.get("qty") or o.get("quantity", 0)))
    filled  = int(float(o.get("fillshares") or o.get("filled_quantity")
                        or o.get("filled_qty", 0)))
    prc     = float(o.get("prc") or o.get("price", 0))
    avgprc  = float(o.get("avgprc") or o.get("average_price")
                    or o.get("avg_price", 0))
    trgprc  = float(o.get("trgprc") or o.get("trigger_price") or 0)
    status  = _order_status(o.get("status") or o.get("order_status", "OPEN"))
    prctyp  = _order_type_map(o.get("prctyp") or o.get("price_type") or o.get("order_type", "LMT"))
    prd     = _prd_map(o.get("prd") or o.get("product", "MIS"))
    order_id = o.get("norenordno") or o.get("order_id") or o.get("orderId", f"ord-{idx}")
    placed_at = (o.get("ordertime") or o.get("placed_at")
                 or o.get("timestamp") or datetime.now().isoformat())
    return {
        "id": order_id,
        "accountId": o.get("actid") or o.get("account_id") or "live",
        "clientId": o.get("client_id") or "",
        "orderId": order_id,
        "brokerOrderId": order_id,
        "commandId": o.get("command_id") or "",
        "symbol": tsym,
        "tradingsymbol": tsym,
        "exchange": exch,
        "type": _infer_instrument_type(tsym),
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
        "actionable": True,
    }


@router.get("/dashboard")
async def get_dashboard(payload: dict = Depends(current_user), db: Session = Depends(get_db)):
    """
    Return normalized live dashboard data from PostgreSQL (via SupremeManager).
    Positions, orders, holdings, trades, account summary — all from DB.
    """
    user_id = payload["sub"]
    from managers.supreme_manager import supreme

    dashboard = supreme.get_dashboard(user_id)

    # DB orders are already merged inside supreme.get_dashboard()
    dashboard["source"] = "live"
    return dashboard


@router.get("/account-info")
async def get_account_info(payload: dict = Depends(current_user), db: Session = Depends(get_db)):
    """Return broker account details: client ID, limits and connection info."""
    from db.database import BrokerSession as DBSession, BrokerConfig
    from managers.supreme_manager import supreme
    user_id = payload["sub"]

    client_id = None
    broker_name = None
    login_at = None
    limits = {}

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

    # Get limits from SupremeManager (reads from DB, no broker API calls)
    fund_rows = supreme.get_funds(user_id)
    if fund_rows:
        lim = fund_rows[0].get("data", fund_rows[0])
        limits = lim
    is_live = bool(supreme.get_sessions(user_id))

    return {
        "isLive":     is_live,
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
    Return equity holdings from PostgreSQL (via SupremeManager).
    If account_id is provided → single account; otherwise all live accounts.
    """
    user_id = payload["sub"]
    from managers.supreme_manager import supreme
    rows = supreme.get_holdings(user_id, config_id=account_id)
    raw_holdings = [r["data"] if "data" in r else r for r in rows]
    holdings = [_normalize_holding(h, i) for i, h in enumerate(raw_holdings)]
    return {"data": holdings, "count": len(holdings)}


# ── Funds / Margin ────────────────────────────────────────────────────────────

@router.get("/funds")
async def get_funds(
    account_id: Optional[str] = None,
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """
    Return available funds and margin utilisation from PostgreSQL (via SupremeManager).
    If account_id is provided → single account; otherwise all live accounts.
    """
    user_id = payload["sub"]
    from managers.supreme_manager import supreme
    rows = supreme.get_funds(user_id, config_id=account_id)
    funds = [r["data"] if "data" in r else r for r in rows]
    return {"data": funds, "count": len(funds)}


# ── Tradebook ─────────────────────────────────────────────────────────────────

@router.get("/tradebook")
async def get_tradebook(
    account_id: Optional[str] = None,
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """
    Return today's executed trades (fills) from PostgreSQL (via SupremeManager).
    If account_id is provided → single account; otherwise all live accounts.
    """
    user_id = payload["sub"]
    from managers.supreme_manager import supreme
    rows = supreme.get_tradebook(user_id, config_id=account_id)
    raw_trades = [r["data"] if "data" in r else r for r in rows]
    trades = [_normalize_trade(t, i) for i, t in enumerate(raw_trades)]
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
    from managers.supreme_manager import supreme
    from db.database import BrokerConfig as DBConfig

    # Get enriched accounts from SupremeManager (reads from DB, no broker API calls)
    accounts = supreme.get_broker_accounts(user_id)
    summaries = []
    seen_ids = set()

    for a in accounts:
        seen_ids.add(a["config_id"])
        summaries.append({
            "config_id":  a["config_id"],
            "broker_id":  a["broker_id"],
            "broker_name": a.get("broker_name", a["broker_id"]).title(),
            "client_id":  a["client_id"],
            "mode":       a.get("mode", "live"),
            "is_live":    a.get("is_live", True),
            "connected_at": a.get("connected_at"),
            "available":  float(a.get("available_margin", 0)),
            "used":       float(a.get("used_margin", 0)),
            "total":      float(a.get("total_balance", 0)),
            "positions_count": a.get("positions_count", 0),
            "error":      a.get("error"),
        })

    # Include offline / disconnected configs from SQLite
    db_configs = (
        db.query(DBConfig)
        .filter(DBConfig.user_id == user_id, DBConfig.is_active == True)
        .all()
    )
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


# ── Enriched Broker Account Cards ─────────────────────────────────────────────

@router.get("/broker-accounts")
async def get_broker_accounts(
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """
    Return enriched per-broker account cards from PostgreSQL (via SupremeManager).
    Each card includes: cash, collateral, client_id, margin used,
    day P&L, connection status, risk manager status, trades today, orders today.
    Also includes disconnected / offline configs from SQLite.
    """
    user_id = payload["sub"]
    from managers.supreme_manager import supreme
    from db.database import BrokerConfig as DBConfig, BrokerSession as DBSess

    accounts = supreme.get_broker_accounts(user_id)
    seen_ids = {a["config_id"] for a in accounts}

    # Add disconnected / offline configs from SQLite
    db_configs = (
        db.query(DBConfig)
        .filter(DBConfig.user_id == user_id, DBConfig.is_active == True)
        .all()
    )
    for cfg in db_configs:
        if cfg.id in seen_ids:
            continue
        db_sess = (
            db.query(DBSess)
            .filter(DBSess.config_id == cfg.id, DBSess.user_id == user_id)
            .order_by(DBSess.login_at.desc())
            .first()
        )
        accounts.append({
            "config_id":      cfg.id,
            "broker_id":      cfg.broker_id,
            "broker_name":    (cfg.broker_name or cfg.broker_id).title(),
            "client_id":      cfg.client_id,
            "is_live":        False,
            "mode":           "offline",
            "connected_at":   None,
            "login_at":       db_sess.login_at.isoformat() if db_sess and db_sess.login_at else None,
            "cash": 0, "collateral": 0, "available_margin": 0, "used_margin": 0, "total_balance": 0,
            "payin": 0, "payout": 0,
            "day_pnl": 0, "unrealized_pnl": 0, "realized_pnl": 0,
            "positions_count": 0, "orders_count": 0, "open_orders": 0,
            "completed_orders": 0, "trades_count": 0,
            "risk_status": True, "risk_daily_pnl": 0, "risk_halt_reason": None,
            "risk_force_exit": False, "error": None,
            "raw_limits": {},
        })

    return {"accounts": accounts, "count": len(accounts)}


def _get_order_status(order) -> str:
    """Extract order status from raw order dict or model."""
    if isinstance(order, dict):
        st = order.get("status", "") or order.get("stat", "")
        return str(st).upper().replace("_", " ").strip()
    return str(getattr(order, "status", "")).upper()


def _get_risk_status(user_id: str, config_id: str, broker_id: str, client_id: str) -> dict:
    """Get risk manager status for an account (safe fallback)."""
    try:
        from broker.account_risk import get_account_risk
        rm = get_account_risk(
            user_id=user_id, config_id=config_id,
            broker_id=broker_id, client_id=client_id,
        )
        return rm.get_status()
    except Exception:
        return {"trading_allowed": True, "daily_pnl": 0, "halt_reason": None, "force_exit_triggered": False}


# ── Broker-filtered data ──────────────────────────────────────────────────────

@router.get("/broker-data")
async def get_broker_data(
    config_id: str,
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """
    Return positions/holdings/orders/tradebook for a specific broker account.
    Reads from PostgreSQL via SupremeManager — no direct broker API calls.
    """
    user_id = payload["sub"]
    from managers.supreme_manager import supreme

    bdata = supreme.get_broker_data(user_id, config_id)
    if not bdata:
        raise HTTPException(404, "Broker account not connected or no data available")
    return bdata
