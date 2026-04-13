"""
Strategy Router — CRUD + Run/Stop for JSON-driven strategies
============================================================
Endpoints used by strategy_builder.html and StrategyPage.tsx:

  GET  /api/dashboard/strategy/configs                — list all saved configs
  GET  /api/dashboard/strategy/config/{name}          — load one config
  POST /api/dashboard/strategy/config/save-all        — create / overwrite config
  DELETE /api/dashboard/strategy/config/{name}        — delete config

  GET  /api/dashboard/option-chain/active-symbols     — symbols for the builder dropdown

  POST /api/strategy/run/{name}                       — enable + mark as running
  POST /api/strategy/stop/{name}                      — disable + mark as stopped
  GET  /api/strategy/status                           — status of all strategies
  GET  /api/strategy/monitor/{name}                   — live monitor data for running strategy
  GET  /api/strategy/monitor                          — aggregated positions from all strategies
"""

import json
import logging
import os
import re
import threading
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException
from sqlalchemy.orm import Session

from db.database import get_db
from routers.auth import current_user

router = APIRouter()
logger = logging.getLogger(__name__)

# ── Saved configs directory ────────────────────────────────────────────────
SAVED_CONFIGS_DIR = (
    Path(__file__).parent.parent / "trading" / "strategy_runner" / "saved_configs"
)
SAVED_CONFIGS_DIR.mkdir(parents=True, exist_ok=True)

# ── In-memory run-state registry (persist across API calls within same process)
_running: Dict[str, Dict[str, Any]] = {}  # safe_name → state dict
_lock = threading.Lock()


def cleanup_stale_configs() -> None:
    """Reset config files left as RUNNING from a previous server session.
    Call once at startup when _running is empty."""
    count = 0
    for path in SAVED_CONFIGS_DIR.glob("*.json"):
        if path.name.endswith(".schema.json"):
            continue
        try:
            with open(path) as fp:
                cfg = json.load(fp)
            if cfg.get("status") == "RUNNING" or cfg.get("enabled") is True:
                cfg["status"] = "STOPPED"
                cfg["enabled"] = False
                with open(path, "w") as fp:
                    json.dump(cfg, fp, indent=2)
                count += 1
        except Exception as exc:
            logger.warning("cleanup_stale_configs: %s: %s", path.name, exc)
    if count:
        logger.info("cleanup_stale_configs: Reset %d stale config(s) to STOPPED", count)


# ── Helpers ────────────────────────────────────────────────────────────────

def _safe_name(raw: str) -> str:
    """Normalise a strategy name to a safe filesystem identifier."""
    return re.sub(r"[^A-Za-z0-9_\-]", "_", str(raw))[:80]


def _config_path(name: str) -> Path:
    return SAVED_CONFIGS_DIR / f"{_safe_name(name)}.json"


# ══════════════════════════════════════════════════════════════════════════════
# CONFIG CRUD
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/dashboard/strategy/configs")
async def list_configs(payload: dict = Depends(current_user)):
    """List all saved strategy configs."""
    configs: List[Dict[str, Any]] = []
    for path in sorted(SAVED_CONFIGS_DIR.glob("*.json")):
        if path.name.endswith(".schema.json"):
            continue  # skip JSON schema files
        try:
            with open(path) as f:
                c = json.load(f)
            safe = _safe_name(c.get("name", path.stem))
            entry = _running.get(safe, {})
            thread: Optional[threading.Thread] = entry.get("thread")
            is_alive = thread.is_alive() if thread else False
            configs.append({
                "name": c.get("name", path.stem),
                "id": c.get("id", path.stem),
                "type": c.get("type", "neutral"),
                "description": c.get("description", ""),
                "enabled": c.get("enabled", True),
                "paper_mode": c.get("identity", {}).get("paper_mode", True),
                "underlying": c.get("identity", {}).get("underlying", ""),
                "exchange": c.get("identity", {}).get("exchange", "NFO"),
                "lots": c.get("identity", {}).get("lots", 1),
                "legs": len(c.get("entry", {}).get("legs", [])),
                "entry_time": (
                    c.get("timing", {}).get("entry_window_start", "")
                    or c.get("schedule", {}).get("entry_time", "")
                ),
                "exit_time": (
                    c.get("timing", {}).get("eod_exit_time", "")
                    or c.get("schedule", {}).get("exit_time", "")
                ),
                "status": "running" if is_alive else entry.get("status", "stopped"),
                "error": entry.get("error"),
                "file": path.name,
                "modified": datetime.fromtimestamp(path.stat().st_mtime).isoformat(),
                "schema_version": c.get("schema_version", ""),
                # Executor state if running
                "active_legs": 0,
                "entered_today": False,
                "combined_pnl": 0.0,
                "spot_price": 0.0,
                **(
                    {
                        "active_legs": entry.get("executor").state.active_legs_count,
                        "entered_today": entry.get("executor").state.entered_today,
                        "combined_pnl": entry.get("executor").state.combined_pnl,
                        "spot_price": entry.get("executor").state.spot_price,
                    }
                    if is_alive and entry.get("executor")
                    else {}
                ),
            })
        except Exception as exc:
            logger.warning("Failed to read config %s: %s", path, exc)
    return configs


@router.get("/dashboard/strategy/config/{name}")
async def get_config(name: str, payload: dict = Depends(current_user)):
    """Load a single strategy config by name."""
    path = _config_path(name)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Strategy '{name}' not found")
    with open(path) as f:
        return json.load(f)


@router.post("/dashboard/strategy/config/save-all")
async def save_config(
    config: Dict[str, Any] = Body(...),
    payload: dict = Depends(current_user),
):
    """Save (create or overwrite) a strategy config."""
    name = config.get("name") or config.get("id")
    if not name:
        raise HTTPException(
            status_code=422, detail="Strategy config must include a 'name' field"
        )
    safe = _safe_name(str(name))
    path = SAVED_CONFIGS_DIR / f"{safe}.json"

    # Stamp metadata
    config["_saved_at"] = datetime.utcnow().isoformat()
    config["_saved_by"] = payload.get("sub", "unknown")

    # Atomic write
    tmp = str(path) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(config, f, indent=2)
    os.replace(tmp, str(path))

    logger.info("Strategy config saved: %s → %s", name, path.name)
    return {"ok": True, "name": safe, "file": path.name}


@router.delete("/dashboard/strategy/config/{name}")
async def delete_config(name: str, payload: dict = Depends(current_user)):
    """Delete a strategy config."""
    path = _config_path(name)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Strategy '{name}' not found")
    path.unlink()
    with _lock:
        _running.pop(_safe_name(name), None)
    return {"ok": True, "deleted": _safe_name(name)}


# ══════════════════════════════════════════════════════════════════════════════
# OPTION CHAIN SYMBOLS (used by strategy_builder dropdown)
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/dashboard/option-chain/active-symbols")
async def active_symbols(payload: dict = Depends(current_user)):
    """Return all FNO indices, FNO stocks, BSE indices, and MCX commodities with option chains."""
    return [
        # ── NSE Indices (NFO) ──────────────────────────────
        {"symbol": "NIFTY",        "exchange": "NFO", "category": "index"},
        {"symbol": "BANKNIFTY",    "exchange": "NFO", "category": "index"},
        {"symbol": "FINNIFTY",     "exchange": "NFO", "category": "index"},
        {"symbol": "MIDCPNIFTY",   "exchange": "NFO", "category": "index"},
        {"symbol": "NIFTYNXT50",   "exchange": "NFO", "category": "index"},
        # ── BSE Indices (BFO) ─────────────────────────────
        {"symbol": "SENSEX",       "exchange": "BFO", "category": "index"},
        {"symbol": "BANKEX",       "exchange": "BFO", "category": "index"},
        {"symbol": "SENSEX50",     "exchange": "BFO", "category": "index"},
        # ── MCX Commodities (MCX) ─────────────────────────
        {"symbol": "CRUDEOIL",     "exchange": "MCX", "category": "commodity"},
        {"symbol": "NATURALGAS",   "exchange": "MCX", "category": "commodity"},
        {"symbol": "GOLD",         "exchange": "MCX", "category": "commodity"},
        {"symbol": "GOLDM",        "exchange": "MCX", "category": "commodity"},
        {"symbol": "SILVER",       "exchange": "MCX", "category": "commodity"},
        {"symbol": "SILVERM",      "exchange": "MCX", "category": "commodity"},
        {"symbol": "COPPER",       "exchange": "MCX", "category": "commodity"},
        {"symbol": "ZINC",         "exchange": "MCX", "category": "commodity"},
        {"symbol": "LEAD",         "exchange": "MCX", "category": "commodity"},
        {"symbol": "ALUMINIUM",    "exchange": "MCX", "category": "commodity"},
        {"symbol": "NICKEL",       "exchange": "MCX", "category": "commodity"},
        # ── NSE FNO Stocks (NFO) — top liquid names ──────
        {"symbol": "RELIANCE",     "exchange": "NFO", "category": "stock"},
        {"symbol": "TCS",          "exchange": "NFO", "category": "stock"},
        {"symbol": "HDFCBANK",     "exchange": "NFO", "category": "stock"},
        {"symbol": "INFY",         "exchange": "NFO", "category": "stock"},
        {"symbol": "ICICIBANK",    "exchange": "NFO", "category": "stock"},
        {"symbol": "SBIN",         "exchange": "NFO", "category": "stock"},
        {"symbol": "BHARTIARTL",   "exchange": "NFO", "category": "stock"},
        {"symbol": "AXISBANK",     "exchange": "NFO", "category": "stock"},
        {"symbol": "ITC",          "exchange": "NFO", "category": "stock"},
        {"symbol": "KOTAKBANK",    "exchange": "NFO", "category": "stock"},
        {"symbol": "LT",           "exchange": "NFO", "category": "stock"},
        {"symbol": "HINDUNILVR",   "exchange": "NFO", "category": "stock"},
        {"symbol": "BAJFINANCE",   "exchange": "NFO", "category": "stock"},
        {"symbol": "MARUTI",       "exchange": "NFO", "category": "stock"},
        {"symbol": "TATAMOTORS",   "exchange": "NFO", "category": "stock"},
        {"symbol": "SUNPHARMA",    "exchange": "NFO", "category": "stock"},
        {"symbol": "TITAN",        "exchange": "NFO", "category": "stock"},
        {"symbol": "HCLTECH",      "exchange": "NFO", "category": "stock"},
        {"symbol": "WIPRO",        "exchange": "NFO", "category": "stock"},
        {"symbol": "TATASTEEL",    "exchange": "NFO", "category": "stock"},
        {"symbol": "ADANIENT",     "exchange": "NFO", "category": "stock"},
        {"symbol": "ADANIPORTS",   "exchange": "NFO", "category": "stock"},
        {"symbol": "POWERGRID",    "exchange": "NFO", "category": "stock"},
        {"symbol": "NTPC",         "exchange": "NFO", "category": "stock"},
        {"symbol": "ONGC",         "exchange": "NFO", "category": "stock"},
        {"symbol": "JSWSTEEL",     "exchange": "NFO", "category": "stock"},
        {"symbol": "M&M",          "exchange": "NFO", "category": "stock"},
        {"symbol": "COALINDIA",    "exchange": "NFO", "category": "stock"},
        {"symbol": "BPCL",         "exchange": "NFO", "category": "stock"},
        {"symbol": "DRREDDY",      "exchange": "NFO", "category": "stock"},
        {"symbol": "DIVISLAB",     "exchange": "NFO", "category": "stock"},
        {"symbol": "CIPLA",        "exchange": "NFO", "category": "stock"},
        {"symbol": "APOLLOHOSP",   "exchange": "NFO", "category": "stock"},
        {"symbol": "TECHM",        "exchange": "NFO", "category": "stock"},
        {"symbol": "ULTRACEMCO",   "exchange": "NFO", "category": "stock"},
        {"symbol": "INDUSINDBK",   "exchange": "NFO", "category": "stock"},
        {"symbol": "PIDILITIND",   "exchange": "NFO", "category": "stock"},
        {"symbol": "TATACONSUM",   "exchange": "NFO", "category": "stock"},
        {"symbol": "DABUR",        "exchange": "NFO", "category": "stock"},
        {"symbol": "HAVELLS",      "exchange": "NFO", "category": "stock"},
        {"symbol": "GRASIM",       "exchange": "NFO", "category": "stock"},
        {"symbol": "HEROMOTOCO",   "exchange": "NFO", "category": "stock"},
        {"symbol": "EICHERMOT",    "exchange": "NFO", "category": "stock"},
        {"symbol": "BAJAJ-AUTO",   "exchange": "NFO", "category": "stock"},
        {"symbol": "BRITANNIA",    "exchange": "NFO", "category": "stock"},
        {"symbol": "NESTLEIND",    "exchange": "NFO", "category": "stock"},
        {"symbol": "TRENT",        "exchange": "NFO", "category": "stock"},
        {"symbol": "BEL",          "exchange": "NFO", "category": "stock"},
        {"symbol": "HAL",          "exchange": "NFO", "category": "stock"},
        {"symbol": "PNB",          "exchange": "NFO", "category": "stock"},
        {"symbol": "BANKBARODA",   "exchange": "NFO", "category": "stock"},
        {"symbol": "IDFCFIRSTB",   "exchange": "NFO", "category": "stock"},
        {"symbol": "IDEA",         "exchange": "NFO", "category": "stock"},
        {"symbol": "ZOMATO",       "exchange": "NFO", "category": "stock"},
        {"symbol": "PAYTM",       "exchange": "NFO", "category": "stock"},
        {"symbol": "IRCTC",        "exchange": "NFO", "category": "stock"},
        {"symbol": "DLF",          "exchange": "NFO", "category": "stock"},
        {"symbol": "GODREJCP",     "exchange": "NFO", "category": "stock"},
        {"symbol": "VOLTAS",       "exchange": "NFO", "category": "stock"},
        {"symbol": "BHEL",         "exchange": "NFO", "category": "stock"},
        {"symbol": "RECLTD",       "exchange": "NFO", "category": "stock"},
        {"symbol": "PFC",          "exchange": "NFO", "category": "stock"},
        {"symbol": "SAIL",         "exchange": "NFO", "category": "stock"},
        {"symbol": "GAIL",         "exchange": "NFO", "category": "stock"},
        {"symbol": "IOC",          "exchange": "NFO", "category": "stock"},
        {"symbol": "HINDPETRO",    "exchange": "NFO", "category": "stock"},
        {"symbol": "LICHSGFIN",    "exchange": "NFO", "category": "stock"},
        {"symbol": "MUTHOOTFIN",   "exchange": "NFO", "category": "stock"},
        {"symbol": "CANBK",        "exchange": "NFO", "category": "stock"},
    ]


# ══════════════════════════════════════════════════════════════════════════════
# BROKER LIST & AVAILABLE SYMBOLS  (used by strategy run UI)
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/strategy/brokers")
async def list_brokers(payload: dict = Depends(current_user)):
    """Return connected broker sessions available for strategy execution."""
    try:
        from broker.multi_broker import registry as broker_registry
        sessions = broker_registry.get_all_sessions()
        result = []
        for sess in sessions:
            result.append({
                "config_id": getattr(sess, "config_id", None),
                "broker_id": getattr(sess, "broker_id", "unknown"),
                "client_id": getattr(sess, "client_id", None),
                "mode": "LIVE" if getattr(sess, "is_live", False) else "PAPER",
                "label": f"{getattr(sess, 'broker_id', '?').upper()} — {getattr(sess, 'client_id', '?')}",
            })
        # Always include paper option
        result.insert(0, {
            "config_id": "__paper__",
            "broker_id": "paper",
            "client_id": "PAPER",
            "mode": "PAPER",
            "label": "🧪 Paper Trading (Simulated)",
        })
        return result
    except Exception as e:
        logger.warning("Failed to list brokers: %s", e)
        return [{"config_id": "__paper__", "broker_id": "paper", "client_id": "PAPER", "mode": "PAPER", "label": "🧪 Paper Trading (Simulated)"}]


@router.get("/strategy/available-symbols")
async def available_symbols(payload: dict = Depends(current_user)):
    """Return symbols that have option chain data available today, plus known FNO/MCX symbols."""
    from datetime import date as _date
    chain_dir = Path(__file__).parent.parent / "data" / "option_chain"

    today = _date.today()
    symbols_seen: Dict[str, Dict[str, Any]] = {}

    # Scan sqlite files for symbols with actual data
    if chain_dir.exists():
        for f in chain_dir.glob("*.sqlite"):
            parts = f.stem.split("_", 2)
            if len(parts) < 3:
                continue
            exchange, symbol = parts[0], parts[1]
            key = f"{exchange}:{symbol}"
            try:
                file_date_str = parts[2]
                from datetime import datetime as _dt
                file_date = _dt.strptime(file_date_str, "%d-%m-%Y").date()
            except Exception:
                file_date = None

            if key not in symbols_seen:
                symbols_seen[key] = {
                    "symbol": symbol,
                    "exchange": exchange,
                    "has_today": False,
                    "latest_file": f.name,
                    "category": "stock",
                }
            if file_date == today:
                symbols_seen[key]["has_today"] = True

    # Always include the full active symbols list as fallback
    full_list = await active_symbols(payload)
    for item in full_list:
        key = f"{item['exchange']}:{item['symbol']}"
        if key not in symbols_seen:
            symbols_seen[key] = {
                "symbol": item["symbol"],
                "exchange": item["exchange"],
                "has_today": False,
                "category": item.get("category", "stock"),
            }
        else:
            symbols_seen[key]["category"] = item.get("category", symbols_seen[key].get("category", "stock"))

    result = sorted(symbols_seen.values(), key=lambda x: (not x.get("has_today", False), x["symbol"]))
    return result


@router.get("/strategy/chain-status")
async def chain_status(
    symbol: str,
    exchange: str = "NFO",
    payload: dict = Depends(current_user),
):
    """
    Return whether a current option chain SQLite file exists for the given
    symbol/exchange, how old it is (seconds), and the current spot price.
    Used by the strategy page to show a live chain-ready indicator per symbol.
    """
    from datetime import date as _date
    from trading.strategy_runner.market_reader import MarketReader

    mr = MarketReader(exchange, symbol, max_stale_seconds=60)
    try:
        meta = mr.get_meta()
        spot = float(meta.get("spot_ltp", 0) or 0)
        snap_ts = float(meta.get("snapshot_ts", 0) or 0)
        import time as _t
        age = round(_t.time() - snap_ts, 1) if snap_ts > 0 else None
        db_path_str = mr._resolve_db_path()
        has_file = db_path_str is not None and Path(db_path_str).exists()
        return {
            "symbol": symbol.upper(),
            "exchange": exchange.upper(),
            "available": has_file and spot > 0,
            "spot": spot,
            "age_seconds": age,
            "file": Path(db_path_str).name if db_path_str else None,
        }
    except Exception as exc:
        logger.warning("chain_status failed for %s/%s: %s", symbol, exchange, exc)
        return {"symbol": symbol.upper(), "exchange": exchange.upper(), "available": False, "spot": 0, "age_seconds": None, "file": None}
    finally:
        mr.close_all()


@router.post("/strategy/fetch-chain")
async def fetch_chain(
    body: Dict[str, Any] = Body(default={}),
    payload: dict = Depends(current_user),
):
    """
    Trigger an immediate option chain fetch for a symbol/exchange.
    Called from the strategy page when chain data is missing or stale.
    """
    symbol = str(body.get("symbol", "NIFTY")).upper()
    exchange = str(body.get("exchange", "NFO")).upper()
    try:
        from trading.utils.option_chain_fetcher import fetch_and_store_from_broker
        result = fetch_and_store_from_broker(exchange, symbol)
        if result:
            return {"ok": True, "file": Path(result).name, "symbol": symbol, "exchange": exchange}
        return {"ok": False, "symbol": symbol, "exchange": exchange, "detail": "No broker source available"}
    except Exception as exc:
        logger.error("fetch_chain failed for %s/%s: %s", symbol, exchange, exc)
        return {"ok": False, "symbol": symbol, "exchange": exchange, "detail": str(exc)}


# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY RUN HISTORY
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/strategy/runs")
async def list_strategy_runs(
    strategy_name: str = None,
    status: str = None,
    limit: int = 50,
    payload: dict = Depends(current_user),
):
    """List strategy runs (completed, stopped, error, or running) with summary info."""
    from db.trading_db import trading_cursor
    with trading_cursor() as cur:
        where_clauses = []
        params = []
        if strategy_name:
            where_clauses.append("strategy_name = %s")
            params.append(strategy_name)
        if status:
            where_clauses.append("status = %s")
            params.append(status)
        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        params.append(min(limit, 200))
        cur.execute(f"""
            SELECT run_id, strategy_name, config_name, symbol, exchange,
                   paper_mode, broker_config_id, status,
                   cumulative_daily_pnl, peak_pnl,
                   adjustments_today, total_trades_today,
                   entry_reason, exit_reason,
                   entered_today, entry_time,
                   started_at, stopped_at, last_tick_at,
                   (SELECT COUNT(*) FROM strategy_legs sl WHERE sl.run_id = sr.run_id) AS total_legs,
                   (SELECT COUNT(*) FROM strategy_legs sl WHERE sl.run_id = sr.run_id AND sl.is_active = true) AS active_legs,
                   (SELECT COUNT(*) FROM strategy_events se WHERE se.run_id = sr.run_id) AS event_count
            FROM strategy_runs sr
            {where_sql}
            ORDER BY started_at DESC
            LIMIT %s
        """, params)
        rows = cur.fetchall()
    # Convert datetime fields to ISO strings for JSON response
    result = []
    for r in rows:
        row = dict(r)
        for key in ('entry_time', 'started_at', 'stopped_at', 'last_tick_at'):
            if row.get(key):
                row[key] = row[key].isoformat()
        result.append(row)
    return result


@router.get("/strategy/runs/{run_id}")
async def get_strategy_run(run_id: str, payload: dict = Depends(current_user)):
    """Get full detail of a strategy run including all legs."""
    from db.trading_db import trading_cursor
    with trading_cursor() as cur:
        cur.execute("SELECT * FROM strategy_runs WHERE run_id = %s", (run_id,))
        run = cur.fetchone()
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")
        cur.execute(
            "SELECT * FROM strategy_legs WHERE run_id = %s ORDER BY created_at",
            (run_id,),
        )
        legs = cur.fetchall()
    run_dict = dict(run)
    for key in ('entry_time', 'started_at', 'stopped_at', 'last_tick_at', 'updated_at'):
        if run_dict.get(key):
            run_dict[key] = run_dict[key].isoformat()
    legs_list = []
    for leg in legs:
        ld = dict(leg)
        for key in ('order_placed_at', 'entry_timestamp', 'exit_timestamp', 'created_at', 'updated_at'):
            if ld.get(key):
                ld[key] = ld[key].isoformat()
        legs_list.append(ld)
    run_dict["legs"] = legs_list
    return run_dict


@router.get("/strategy/runs/{run_id}/events")
async def get_run_events(run_id: str, payload: dict = Depends(current_user)):
    """Get all events for a strategy run (decision audit trail)."""
    from db.trading_db import trading_cursor
    with trading_cursor() as cur:
        cur.execute(
            "SELECT * FROM strategy_events WHERE run_id = %s ORDER BY created_at",
            (run_id,),
        )
        rows = cur.fetchall()
    result = []
    for r in rows:
        rd = dict(r)
        if rd.get("created_at"):
            rd["created_at"] = rd["created_at"].isoformat()
        result.append(rd)
    return result


@router.get("/strategy/runs/{run_id}/pnl")
async def get_run_pnl(run_id: str, payload: dict = Depends(current_user)):
    """Get PnL time series for charting."""
    from db.trading_db import trading_cursor
    with trading_cursor() as cur:
        cur.execute(
            "SELECT pnl, spot_price, active_legs, net_delta, created_at FROM pnl_snapshots WHERE run_id = %s ORDER BY created_at",
            (run_id,),
        )
        rows = cur.fetchall()
    result = []
    for r in rows:
        rd = dict(r)
        if rd.get("created_at"):
            rd["created_at"] = rd["created_at"].isoformat()
        result.append(rd)
    return result


# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY RUN / STOP / STATUS
# ══════════════════════════════════════════════════════════════════════════════

def _strategy_thread(safe_name: str, config_path: str, stop_evt: threading.Event) -> None:
    """
    Strategy execution thread using the full StrategyExecutor engine.
    Replaces the old heartbeat-only loop with real entry/exit/adjustment logic.
    """
    logger.info("Strategy thread started: %s", safe_name)
    executor = None
    db_persist = None
    try:
        from trading.strategy_runner.config_schema import validate_config  # type: ignore[import]
        with open(config_path) as fp:
            cfg = json.load(fp)

        ok, errors = validate_config(cfg)
        if not ok:
            errs = [str(e) for e in errors if e.severity == "error"]
            if errs:
                raise ValueError(f"Config validation failed: {'; '.join(errs[:3])}")

        # Build state persistence path
        state_dir = SAVED_CONFIGS_DIR / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        state_path = str(state_dir / f"{safe_name}_state.json")

        # Attempt to populate option chain data if not already available
        identity = cfg.get("identity", {})
        underlying = identity.get("underlying", "NIFTY")
        exchange = identity.get("exchange", "NFO")
        try:
            from trading.utils.option_chain_fetcher import fetch_and_store_from_broker
            result = fetch_and_store_from_broker(exchange, underlying)
            if result:
                logger.info("Option chain data fetched for %s: %s", underlying, result)
            else:
                logger.info(
                    "No broker data available for %s — executor will run idle "
                    "until option chain SQLite files are populated.",
                    underlying,
                )
        except Exception as e:
            logger.warning("Option chain fetch attempt failed: %s", e)

        # Create the StrategyExecutor with full engine pipeline
        from trading.strategy_runner.executor import StrategyExecutor
        executor = StrategyExecutor(config_path, state_path)

        # Wire DB persistence for restart-safe state
        db_persist = None
        try:
            from trading.strategy_runner.db_persistence import DBPersistence
            db_persist = DBPersistence(safe_name, cfg)
            # Check if there's a resumable run from a previous crash
            resumable = DBPersistence.find_resumable_run(safe_name)
            if resumable:
                db_persist.attach_run(resumable)
                db_state = db_persist.load()
                if db_state and db_state.legs:
                    executor.state = db_state
                    logger.info("RESUME | Restored %d legs from DB run %s", len(db_state.legs), resumable)
                else:
                    # No useful state — start fresh with new run
                    db_persist.mark_stopped("stale_no_legs")
                    db_persist = DBPersistence(safe_name, cfg)
                    db_persist.create_run(executor.state)
            else:
                db_persist.create_run(executor.state)
            executor.set_db_persistence(db_persist)
        except Exception as db_err:
            logger.warning("DB persistence setup failed (non-fatal): %s", db_err)

        # Wire OMS into the executor for order placement
        try:
            from trading.oms import OrderManagementSystem
            paper_mode = cfg.get("identity", {}).get("paper_mode", True)
            broker_adapter = None
            if not paper_mode:
                try:
                    from broker.multi_broker import registry as broker_registry
                    # Prefer the specific broker_config_id written at run-time.
                    # Fall back to the first live session if no specific id was set.
                    requested_cfg_id = cfg.get("_broker_config_id")
                    all_sessions = broker_registry.get_all_sessions()
                    if requested_cfg_id:
                        # Find session matching the requested config_id
                        broker_adapter = next(
                            (s for s in all_sessions if getattr(s, "config_id", None) == requested_cfg_id),
                            None,
                        )
                        if broker_adapter is None:
                            logger.warning(
                                "broker_config_id=%s not found in registry; "
                                "falling back to first live session",
                                requested_cfg_id,
                            )
                    if broker_adapter is None and all_sessions:
                        # Take the first non-demo session
                        broker_adapter = next(
                            (s for s in all_sessions if not getattr(s, "is_demo", True)),
                            all_sessions[0],
                        )
                except Exception as ba_err:
                    logger.warning("No live broker adapter available, falling back to paper: %s", ba_err)
            oms = OrderManagementSystem(broker_adapter=broker_adapter)
            executor.set_oms(oms)
            logger.info("OMS injected into executor (paper=%s, broker=%s config_id=%s)",
                        paper_mode, type(broker_adapter).__name__ if broker_adapter else "None",
                        getattr(broker_adapter, "config_id", "N/A") if broker_adapter else "N/A")
        except Exception as oms_err:
            logger.warning("OMS injection failed, executor will run without OMS: %s", oms_err)

        # Wire LiveTickService for real-time LTP overlay (replaces stale 30s SQLite reads)
        try:
            from broker.live_tick_service import get_tick_service
            tick_svc = get_tick_service()
            executor.set_tick_service(tick_svc)
            # Subscribe the spot/underlying symbol for live spot price at strategy start
            spot_symbols = [underlying]
            tick_svc.subscribe(spot_symbols)
            logger.info("LiveTickService injected + spot symbol subscribed: %s", spot_symbols)
            # If resuming with existing legs, subscribe their symbols too
            if executor.state.legs:
                executor.subscribe_leg_symbols()
        except Exception as ts_err:
            logger.warning("LiveTickService injection failed (non-fatal): %s", ts_err)

        # Store executor reference for monitoring endpoints
        with _lock:
            if safe_name in _running:
                _running[safe_name]["executor"] = executor

        # ── COMPREHENSIVE STARTUP LOG ──────────────────────────────────
        _identity = cfg.get("identity", {})
        _timing = cfg.get("timing", {})
        _entry = cfg.get("entry", {})
        _exit_cfg = cfg.get("exit", {})
        _adj = cfg.get("adjustment", {})
        _schedule = cfg.get("schedule", {})
        _broker_id = cfg.get("_broker_config_id", "none (paper)")
        _legs_def = _entry.get("legs", [])
        logger.info(
            "━━━ STRATEGY RUN START ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "  Name       : %s\n"
            "  Symbol     : %s  Exchange: %s\n"
            "  Paper Mode : %s\n"
            "  Broker CFG : %s\n"
            "  Lots       : %s  Order Type: %s  Product: %s\n"
            "  Entry Window: %s → %s   EOD Exit: %s\n"
            "  Schedule   : %s  Days: %s\n"
            "  Expiry Mode: %s  Max Re-entries: %s\n"
            "  Legs (%d)  :\n%s\n"
            "  Exit Rules : SL=%.1f%%  Target=%.1f%%  Trail=%s  TimeExit=%s\n"
            "  Risk       : MaxLoss=%s  MaxDelta=%s  MaxLots=%s\n"
            "  Adjustments: %d rules defined\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            safe_name,
            _identity.get("underlying", "?"), _identity.get("exchange", "?"),
            _identity.get("paper_mode", True),
            _broker_id,
            _identity.get("lots", 1), _identity.get("order_type", "MARKET"), _identity.get("product_type", "NRML"),
            _timing.get("entry_window_start", "?"), _timing.get("entry_window_end", "?"), _timing.get("eod_exit_time", "?"),
            _schedule.get("frequency", "daily"), _schedule.get("active_days", []),
            _schedule.get("expiry_mode", "weekly_auto"), _schedule.get("max_reentries_per_day", 1),
            len(_legs_def),
            "\n".join(
                f"    [{i+1}] {ld.get('tag','')} {ld.get('side','?')} {ld.get('option_type','?')} "
                f"strike={ld.get('strike_selection','?')}({ld.get('strike_value','?')}) "
                f"lots={ld.get('lots',1)}"
                for i, ld in enumerate(_legs_def)
            ) or "    (none)",
            _exit_cfg.get("per_leg", {}).get("stop_loss_pct", 0) or 0,
            _exit_cfg.get("per_leg", {}).get("target_pct", 0) or 0,
            _exit_cfg.get("per_leg", {}).get("trailing", {}).get("enabled", False),
            _exit_cfg.get("time", {}).get("strategy_exit_time", "?"),
            _exit_cfg.get("risk", {}).get("max_loss_per_day", "none"),
            _exit_cfg.get("risk", {}).get("max_delta", "none"),
            _exit_cfg.get("risk", {}).get("max_lots", "none"),
            len(_adj.get("rules", [])),
        )

        # Main tick loop — replaces the old heartbeat
        _backoff = 1
        tick_count = 0
        _last_chain_refresh = 0.0
        _CHAIN_REFRESH_INTERVAL = 5.0  # Refresh option chain data every 5 seconds
        while not stop_evt.is_set():
            try:
                # Periodic option chain data refresh from broker
                now_ts = __import__('time').time()
                if now_ts - _last_chain_refresh >= _CHAIN_REFRESH_INTERVAL:
                    try:
                        from trading.utils.option_chain_fetcher import fetch_and_store_from_broker
                        fetch_and_store_from_broker(exchange, underlying)
                        _last_chain_refresh = now_ts
                    except Exception as fetch_err:
                        logger.debug("Chain refresh: %s", fetch_err)

                executor._tick()
                tick_count += 1
                _backoff = 1  # reset on clean tick

                # Log heartbeat every 60 ticks (~1 min at 1s interval)
                if tick_count % 60 == 0:
                    state = executor.state
                    logger.info(
                        "HEARTBEAT | %s | tick=%d | entered=%s | legs=%d | "
                        "spot=%.2f | pnl=%.2f | adj_today=%d",
                        safe_name, tick_count,
                        state.entered_today,
                        state.active_legs_count,
                        state.spot_price,
                        state.combined_pnl,
                        state.adjustments_today,
                    )
            except Exception as e:
                logger.exception(
                    "Tick error for '%s' (backing off %ds): %s",
                    safe_name, _backoff, e,
                )
                try:
                    executor._save_state()
                except Exception:
                    pass
                _backoff = min(_backoff * 2, 60)

            # Wait for next tick (1s default) or until stop is signalled
            stop_evt.wait(timeout=max(1, _backoff))

        # Clean shutdown — save final state
        if executor:
            try:
                executor.unsubscribe_all()  # Release WS subscriptions
            except Exception:
                pass
            try:
                executor._save_state()
                logger.info("Strategy '%s' state saved on shutdown", safe_name)
            except Exception as e:
                logger.warning("Failed to save state for '%s': %s", safe_name, e)
        # Mark DB run as stopped
        if db_persist:
            try:
                db_persist.mark_stopped("normal_stop")
            except Exception as e:
                logger.warning("Failed to mark DB run stopped for '%s': %s", safe_name, e)

    except Exception as exc:
        logger.error("Strategy '%s' error: %s", safe_name, exc, exc_info=True)
        with _lock:
            if safe_name in _running:
                _running[safe_name]["status"] = "error"
                _running[safe_name]["error"] = str(exc)
        # Mark DB run as error
        if db_persist:
            try:
                db_persist.mark_error(str(exc))
            except Exception:
                pass
        return

    with _lock:
        if safe_name in _running:
            _running[safe_name]["status"] = "stopped"
    logger.info("Strategy thread stopped: %s", safe_name)


@router.post("/strategy/run/{name}")
async def run_strategy(
    name: str,
    body: Dict[str, Any] = Body(default={}),
    payload: dict = Depends(current_user),
):
    """
    Enable and start a strategy.
    Optional body fields for runtime overrides (config file is NOT modified):
      symbol      — underlying symbol (e.g. "NIFTY", "BANKNIFTY")
      exchange    — exchange code (e.g. "NFO", "BFO")
      paper_mode  — true/false
      broker_config_id — config_id of a connected broker session
    """
    safe = _safe_name(name)
    path = _config_path(name)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Strategy '{name}' not found")

    with _lock:
        entry = _running.get(safe, {})
        thread: Optional[threading.Thread] = entry.get("thread")
        if thread and thread.is_alive():
            raise HTTPException(status_code=409, detail=f"Strategy '{name}' is already running")
        # Clean up stale entry if thread is dead
        if thread and not thread.is_alive():
            logger.info("Cleaning up stale running entry for '%s' (thread dead)", safe)
            _running.pop(safe, None)

    # Pre-validate config before spawning thread
    # Apply overrides FIRST so validation sees the runtime symbol/exchange
    warnings_list: List[str] = []
    try:
        with open(path) as fp:
            cfg = json.load(fp)

        # Apply runtime overrides before validation
        override_symbol = body.get("symbol")
        override_exchange = body.get("exchange")
        override_paper = body.get("paper_mode")
        override_broker = body.get("broker_config_id")
        identity = cfg.setdefault("identity", {})
        if override_symbol:
            identity["underlying"] = override_symbol
        if override_exchange:
            identity["exchange"] = override_exchange
        # Set sensible defaults if still missing (symbol-agnostic configs)
        identity.setdefault("underlying", "NIFTY")
        identity.setdefault("exchange", "NFO")
        if override_paper is not None:
            identity["paper_mode"] = bool(override_paper)
        if override_broker and override_broker != "__paper__":
            identity["paper_mode"] = False
            cfg["_broker_config_id"] = override_broker
        elif override_broker == "__paper__":
            identity["paper_mode"] = True

        from trading.strategy_runner.config_schema import validate_config
        ok, errors = validate_config(cfg)
        if not ok:
            errs = [str(e) for e in errors if e.severity == "error"]
            if errs:
                raise HTTPException(
                    status_code=422,
                    detail=f"Config validation failed: {'; '.join(errs[:5])}"
                )
        # Collect warnings for the response
        warnings_list = [str(e) for e in errors if e.severity == "warning"]
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Cannot read config: {exc}")

    # Patch config file BEFORE starting thread to avoid race condition
    try:
        cfg["enabled"] = True
        cfg["status"] = "RUNNING"

        with open(path, "w") as fp:
            json.dump(cfg, fp, indent=2)
    except Exception as exc:
        logger.warning("Could not update enabled flag for %s: %s", name, exc)

    # Clear old state file so the strategy starts fresh (0 PnL).
    # State files are only useful for crash recovery, not for manual re-starts.
    state_dir = SAVED_CONFIGS_DIR / "state"
    old_state = state_dir / f"{safe}_state.json"
    if old_state.exists():
        try:
            old_state.unlink()
            logger.info("Cleared old state file for fresh start: %s", old_state.name)
        except Exception as exc:
            logger.warning("Could not remove old state file %s: %s", old_state, exc)

    with _lock:
        stop_evt = threading.Event()
        t = threading.Thread(
            target=_strategy_thread,
            args=(safe, str(path), stop_evt),
            daemon=True,
            name=f"strategy-{safe}",
        )
        _running[safe] = {
            "thread": t,
            "stop_event": stop_evt,
            "started_at": datetime.utcnow().isoformat(),
            "status": "running",
            "error": None,
        }
        t.start()

    return {
        "ok": True,
        "name": safe,
        "status": "running",
        "warnings": warnings_list[:5] if warnings_list else [],
    }


@router.post("/strategy/stop/{name}")
async def stop_strategy(name: str, payload: dict = Depends(current_user)):
    """Stop a running strategy."""
    safe = _safe_name(name)
    with _lock:
        entry = _running.get(safe)
    if not entry:
        raise HTTPException(
            status_code=404,
            detail=f"Strategy '{name}' is not in the running registry",
        )
    entry["stop_event"].set()
    entry["status"] = "stopping"

    # Patch config
    path = _config_path(name)
    try:
        with open(path) as fp:
            cfg = json.load(fp)
        cfg["enabled"] = False
        cfg["status"] = "STOPPED"
        with open(path, "w") as fp:
            json.dump(cfg, fp, indent=2)
    except Exception as exc:
        logger.warning("Could not update status for %s: %s", name, exc)

    return {"ok": True, "name": safe, "status": "stopping"}


def _get_all_strategy_status() -> list:
    """Return status of all saved strategies (usable from WS feed too)."""
    result: List[Dict[str, Any]] = []
    for path in sorted(SAVED_CONFIGS_DIR.glob("*.json")):
        if path.name.endswith(".schema.json"):
            continue
        try:
            with open(path) as fp:
                cfg = json.load(fp)
            safe = _safe_name(cfg.get("name", path.stem))
            with _lock:
                entry = _running.get(safe, {})
            thread: Optional[threading.Thread] = entry.get("thread")
            is_alive = thread.is_alive() if thread else False
            result.append({
                "name": safe,
                "display_name": cfg.get("name", safe),
                "description": cfg.get("description", ""),
                "type": cfg.get("type", "neutral"),
                "underlying": cfg.get("identity", {}).get("underlying", ""),
                "paper_mode": cfg.get("identity", {}).get("paper_mode", True),
                "status": "running" if is_alive else entry.get("status", "stopped"),
                "started_at": entry.get("started_at"),
                "error": entry.get("error"),
                "file": path.name,
                "entered_today": (
                    entry.get("executor").state.entered_today
                    if is_alive and entry.get("executor") else False
                ),
                "active_legs": (
                    entry.get("executor").state.active_legs_count
                    if is_alive and entry.get("executor") else 0
                ),
                "combined_pnl": (
                    entry.get("executor").state.combined_pnl
                    if is_alive and entry.get("executor") else 0.0
                ),
                "spot_price": (
                    entry.get("executor").state.spot_price
                    if is_alive and entry.get("executor") else 0.0
                ),
            })
        except Exception as exc:
            logger.warning("Status read error for %s: %s", path.name, exc)
    return result


@router.get("/strategy/status")
async def all_status(payload: dict = Depends(current_user)):
    """Return status of all saved strategies."""
    return _get_all_strategy_status()


# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY LIVE MONITOR — real-time state from running executors
# ══════════════════════════════════════════════════════════════════════════════

def _executor_legs_to_positions(executor, safe_name: str) -> List[Dict[str, Any]]:
    """Convert executor state legs to OMS-compatible position dicts."""
    import copy
    from trading.strategy_runner.models import Side as StratSide, InstrumentType, OptionType
    state = executor.state
    legs_snapshot = copy.deepcopy(state.legs)
    positions: List[Dict[str, Any]] = []
    for tag, leg in sorted(
        legs_snapshot.items(),
        key=lambda item: (
            0 if item[1].is_active else 1,
            float(item[1].strike or 0),
            str(item[1].side.value if item[1].side else ''),
            str(item[0]),
        ),
    ):
        opt_type_str = leg.option_type.value if leg.option_type else ""
        strike_str = f"{int(leg.strike)}" if leg.strike else ""
        tsym = leg.trading_symbol or f"{leg.symbol}{leg.expiry}{strike_str}{opt_type_str}"

        side_sign = 1 if leg.side == StratSide.BUY else -1
        order_qty = leg.order_qty               # total contracts = lots * lot_size
        netqty = side_sign * order_qty

        # For closed legs, use exit_price if available, otherwise the frozen ltp
        effective_exit = leg.exit_price if (leg.exit_price is not None and leg.exit_price > 0) else leg.ltp
        if leg.is_active:
            unrealized = leg.pnl
            realized = 0.0
        else:
            # Closed: compute realized using effective exit price
            if leg.side == StratSide.BUY:
                realized = (effective_exit - leg.entry_price) * order_qty
            else:
                realized = (leg.entry_price - effective_exit) * order_qty
            unrealized = 0.0

        positions.append({
            # Fields the LiveMonitorPanel reads
            "tsym": tsym,
            "symbol": leg.symbol,
            "tradingsymbol": tsym,
            "netqty": netqty if leg.is_active else 0,
            "qty": order_qty,                    # total contracts (lots * lot_size)
            "order_qty": order_qty,              # explicit total contracts
            "ltp": leg.ltp,
            "avgprc": leg.entry_price,
            "avg_price": leg.entry_price,
            "urmtom": unrealized,
            "unrealized_pnl": unrealized,
            "rpnl": realized,
            "realized_pnl": realized,
            "prd": "NRML",
            "product": "NRML",
            # Extra strategy-specific fields
            "strategy": safe_name,
            "tag": tag,
            "side": leg.side.value,
            "is_active": leg.is_active,
            "delta": leg.delta,
            "gamma": leg.gamma,
            "theta": leg.theta,
            "vega": leg.vega,
            "iv": leg.iv,
            "strike": leg.strike,
            "option_type": opt_type_str,
            "expiry": leg.expiry,
            "instrument": leg.instrument.value,
            "lots": leg.qty,                     # number of lots
            "lot_size": leg.lot_size,            # contracts per lot
            "entry_price": leg.entry_price,
            "exit_price": effective_exit if not leg.is_active else None,
            "order_status": leg.order_status,
        })
    return positions


@router.get("/strategy/monitor/{name}")
async def strategy_monitor(name: str, payload: dict = Depends(current_user)):
    """
    Get real-time monitoring data for a running strategy.
    Returns executor state in OMS-compatible format for the LiveMonitorPanel.
    """
    safe = _safe_name(name)
    with _lock:
        entry = _running.get(safe)
    if not entry:
        raise HTTPException(status_code=404, detail=f"Strategy '{name}' is not running")

    executor = entry.get("executor")
    thread: Optional[threading.Thread] = entry.get("thread")
    is_alive = thread.is_alive() if thread else False

    if not executor:
        return {
            "strategy": safe,
            "status": entry.get("status", "starting"),
            "legs": [],
            "market_data": {},
            "summary": {},
        }

    state = executor.state
    legs = _executor_legs_to_positions(executor, safe)

    return {
        "strategy": safe,
        "status": "running" if is_alive else entry.get("status", "stopped"),
        "error": entry.get("error"),
        "legs": legs,
        "market_data": {
            "spot_price": state.spot_price,
            "spot_open": state.spot_open,
            "atm_strike": state.atm_strike,
            "fut_ltp": state.fut_ltp,
            "pcr": state.pcr,
            "max_pain_strike": state.max_pain_strike,
        },
        "summary": {
            "combined_pnl": state.combined_pnl,
            "combined_pnl_pct": state.combined_pnl_pct,
            "realised_pnl": state.cumulative_daily_pnl,
            "unrealised_pnl": state.combined_pnl,
            "total_pnl": state.combined_pnl + state.cumulative_daily_pnl,
            "net_delta": state.net_delta,
            "portfolio_gamma": state.portfolio_gamma,
            "portfolio_theta": state.portfolio_theta,
            "portfolio_vega": state.portfolio_vega,
            "entered_today": state.entered_today,
            "entry_time": state.entry_time.isoformat() if state.entry_time else None,
            "adjustments_today": state.adjustments_today,
            "lifetime_adjustments": state.lifetime_adjustments,
            "active_legs": state.active_legs_count,
            "total_legs": len(state.legs),
            "trailing_stop_active": state.trailing_stop_active,
            "trailing_stop_level": state.trailing_stop_level,
            "peak_pnl": state.peak_pnl,
            "cumulative_daily_pnl": state.cumulative_daily_pnl,
            "minutes_to_exit": state.minutes_to_exit,
        },
    }


@router.get("/strategy/monitor")
async def all_strategy_positions(payload: dict = Depends(current_user)):
    """
    Aggregate positions from ALL running strategy executors.
    Returns a flat list of OMS-compatible positions for the LiveMonitorPanel.
    This endpoint is polled by the frontend as a fallback/supplement to /oms/positions.
    """
    all_positions: List[Dict[str, Any]] = []
    with _lock:
        running_copy = dict(_running)

    for safe_name, entry in running_copy.items():
        executor = entry.get("executor")
        thread: Optional[threading.Thread] = entry.get("thread")
        is_alive = thread.is_alive() if thread else False
        if executor and is_alive:
            try:
                positions = _executor_legs_to_positions(executor, safe_name)
                # Only include active legs for the live monitor
                all_positions.extend(p for p in positions if p.get("is_active"))
            except Exception as e:
                logger.warning("Monitor read error for %s: %s", safe_name, e)

    return all_positions
