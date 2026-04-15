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
import uuid
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

# ── In-memory run-state registry: keyed by run_id (not strategy name).
# This allows multiple simultaneous instances of the same strategy template
# running different symbols.
_running: Dict[str, Dict[str, Any]] = {}  # run_id → instance state dict
_lock = threading.Lock()


def cleanup_stale_configs() -> None:
    """Legacy no-op kept for backward compat.  Actual stale-run cleanup is now
    done via DB (abandon_stale_runs) and auto-resume instead of mutating
    template config files."""
    logger.info("cleanup_stale_configs: no-op (instance model active)")


def auto_resume_today_runs() -> None:
    """Restart threads for any strategy runs that were RUNNING when the server
    last stopped/crashed — but only runs from the last 12 hours (today's session).

    Older RUNNING runs (previous day) are abandoned by abandon_stale_runs().
    Called once at server startup after broker sessions are restored.
    """
    try:
        from trading.strategy_runner.db_persistence import DBPersistence
        runs = DBPersistence.get_resumable_today_runs()
    except Exception as exc:
        logger.warning("auto_resume_today_runs: DB query failed: %s", exc)
        return

    if not runs:
        logger.info("auto_resume_today_runs: no in-progress runs to resume")
        return

    logger.info("auto_resume_today_runs: found %d run(s) to resume", len(runs))
    state_dir = SAVED_CONFIGS_DIR / "state"
    state_dir.mkdir(parents=True, exist_ok=True)

    for row in runs:
        run_id = row["run_id"]
        safe_name = row["strategy_name"]

        with _lock:
            if run_id in _running:
                continue  # already running (shouldn't happen at startup)

        # Rebuild instance config from snapshot.
        # IMPORTANT: psycopg2 RealDictCursor returns JSONB columns as Python
        # dicts, NOT strings.  json.loads(dict) raises TypeError, so we must
        # handle both types.
        snap = row.get("config_snapshot")
        cfg: Optional[Dict[str, Any]] = None
        if snap is not None:
            if isinstance(snap, dict):
                cfg = snap            # already parsed by psycopg2 JSONB
            elif isinstance(snap, str):
                try:
                    cfg = json.loads(snap)
                except Exception:
                    cfg = None

        if not cfg:
            # Fallback: read the template file.  This MUST be augmented with
            # the authoritative symbol/exchange from the DB row so that a
            # BANKNIFTY instance doesn't silently become NIFTY.
            path = _config_path(safe_name)
            if path.exists():
                with open(path) as fp:
                    cfg = json.load(fp)
                logger.warning(
                    "auto_resume: config_snapshot missing/empty for %s — "
                    "falling back to template; injecting symbol=%s exchange=%s from DB",
                    run_id, row.get("symbol"), row.get("exchange"),
                )
            else:
                logger.warning("auto_resume: no config for run %s — skipping", run_id)
                try:
                    from trading.strategy_runner.db_persistence import DBPersistence
                    dp = DBPersistence(safe_name, {})
                    dp.run_id = run_id
                    dp.mark_stopped("resume_failed_no_config")
                except Exception:
                    pass
                continue

        # Always apply authoritative symbol/exchange/paper from DB row into
        # the config — even if config_snapshot was valid — so that the
        # executor never gets stale identity values.
        identity = cfg.setdefault("identity", {})
        if row.get("symbol"):
            identity["underlying"] = row["symbol"]
        if row.get("exchange"):
            identity["exchange"] = row["exchange"]
        if row.get("paper_mode") is not None:
            identity["paper_mode"] = bool(row["paper_mode"])
        # Tag instance-level metadata onto the config
        cfg["_run_id"] = run_id
        cfg["_template_name"] = safe_name

        # Write instance config to state dir for the executor to read
        instance_cfg_path = state_dir / f"{run_id}_config.json"
        try:
            with open(instance_cfg_path, "w") as fp:
                json.dump(cfg, fp, indent=2)
        except Exception as exc:
            logger.warning("auto_resume: could not write instance config for %s: %s", run_id, exc)
            continue

        symbol = identity.get("underlying", "NIFTY")
        exchange = identity.get("exchange", "NFO")
        paper_mode = identity.get("paper_mode", True)
        display_name = f"{cfg.get('name', safe_name)} · {symbol}"

        with _lock:
            stop_evt = threading.Event()
            t = threading.Thread(
                target=_strategy_thread,
                args=(run_id, safe_name, str(instance_cfg_path), stop_evt, True),
                daemon=True,
                name=f"strategy-{run_id[:30]}",
            )
            _running[run_id] = {
                "thread": t,
                "stop_event": stop_evt,
                "started_at": datetime.utcnow().isoformat(),
                "status": "running",
                "error": None,
                "template_name": safe_name,
                "symbol": symbol,
                "exchange": exchange,
                "paper_mode": paper_mode,
                "display_name": display_name,
                "run_id": run_id,
            }
            t.start()
        logger.info("auto_resume: started thread for run %s (%s %s)", run_id, safe_name, symbol)


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
    """List all saved strategy templates (read-only, never mutated by run/stop)."""
    # Build a set of template_names that have at least one running instance
    with _lock:
        running_copy = dict(_running)
    templates_with_instances: Dict[str, int] = {}  # template_name → count of live instances
    for run_id, entry in running_copy.items():
        thread: Optional[threading.Thread] = entry.get("thread")
        if thread and thread.is_alive():
            tn = entry.get("template_name", "")
            templates_with_instances[tn] = templates_with_instances.get(tn, 0) + 1

    configs: List[Dict[str, Any]] = []
    for path in sorted(SAVED_CONFIGS_DIR.glob("*.json")):
        if path.name.endswith(".schema.json"):
            continue  # skip JSON schema files
        try:
            with open(path) as f:
                c = json.load(f)
            safe = _safe_name(c.get("name", path.stem))
            instance_count = templates_with_instances.get(safe, 0)
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
                # Templates are never RUNNING — show per-instance count instead
                "status": "template",
                "running_instances": instance_count,
                "file": path.name,
                "modified": datetime.fromtimestamp(path.stat().st_mtime).isoformat(),
                "schema_version": c.get("schema_version", ""),
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
    """Delete a strategy config template."""
    path = _config_path(name)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Strategy '{name}' not found")
    path.unlink()
    # Note: does NOT stop running instances — instances must be stopped separately
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

def _strategy_thread(
    run_id: str,
    safe_name: str,
    instance_config_path: str,
    stop_evt: threading.Event,
    resume_mode: bool = False,
) -> None:
    """Instance-based strategy execution thread.

    Each call to this thread manages exactly one strategy *instance* identified
    by *run_id*.  Multiple instances of the same template (safe_name) can run
    concurrently for different symbols.

    Args:
        run_id: Unique instance ID (pre-generated by run_strategy or auto_resume).
        safe_name: Template name (filesystem safe, e.g. 'dnss_dynamic_straddle').
        instance_config_path: Path to the per-instance config JSON (with overrides).
        stop_evt: Signals the thread to stop cleanly.
        resume_mode: True when restarting after a crash (attaches to existing DB run).
    """
    logger.info("Strategy thread started: run_id=%s safe_name=%s resume=%s", run_id, safe_name, resume_mode)
    executor = None
    db_persist = None
    try:
        from trading.strategy_runner.config_schema import validate_config  # type: ignore[import]
        with open(instance_config_path) as fp:
            cfg = json.load(fp)

        ok, errors = validate_config(cfg)
        if not ok:
            errs = [str(e) for e in errors if e.severity == "error"]
            if errs:
                raise ValueError(f"Config validation failed: {'; '.join(errs[:3])}")

        # Per-instance state file — keyed by run_id so instances never share state
        state_dir = SAVED_CONFIGS_DIR / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        state_path = str(state_dir / f"{run_id}_state.json")

        identity = cfg.get("identity", {})
        underlying = identity.get("underlying", "NIFTY")
        exchange = identity.get("exchange", "NFO")

        # Fetch option chain data on startup
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

        # Create the StrategyExecutor (reads from instance_config_path)
        from trading.strategy_runner.executor import StrategyExecutor
        executor = StrategyExecutor(instance_config_path, state_path)

        # Wire DB persistence — use the pre-supplied run_id
        try:
            from trading.strategy_runner.db_persistence import DBPersistence
            db_persist = DBPersistence(safe_name, cfg)
            if resume_mode:
                # Attach to the existing DB run and restore state
                db_persist.attach_run(run_id)
                db_state = db_persist.load()
                if db_state and db_state.legs:
                    # Preserve last_date / entered_today from the local file state —
                    # the DB does not store last_date, so db_state.last_date is always
                    # None.  If we used it as-is, executor._tick()'s daily-reset guard
                    # would fire on the very first tick (last_date is None → reset
                    # entered_today=False) and allow a spurious re-entry even though
                    # the run already exited for the day.
                    _file_last_date = executor.state.last_date
                    _file_entered = executor.state.entered_today
                    executor.state = db_state
                    if _file_last_date is not None:
                        executor.state.last_date = _file_last_date
                        executor.state.entered_today = _file_entered
                    logger.info("RESUME | Restored %d legs from DB run %s", len(db_state.legs), run_id)
                else:
                    # Resuming but no useful state — start fresh within same run_id
                    logger.info("RESUME | No legs found for run %s — starting fresh", run_id)
                    db_persist.create_run(executor.state, pre_run_id=run_id)
            else:
                # Fresh start — create new DB row with the pre-supplied run_id
                db_persist.create_run(executor.state, pre_run_id=run_id)
            executor.set_db_persistence(db_persist)
        except Exception as db_err:
            logger.warning("DB persistence setup failed (non-fatal): %s", db_err)

        # Wire OMS
        try:
            from trading.oms import OrderManagementSystem
            paper_mode = cfg.get("identity", {}).get("paper_mode", True)
            broker_adapter = None
            if not paper_mode:
                try:
                    from broker.multi_broker import registry as broker_registry
                    requested_cfg_id = cfg.get("_broker_config_id")
                    all_sessions = broker_registry.get_all_sessions()
                    if requested_cfg_id:
                        broker_adapter = next(
                            (s for s in all_sessions if getattr(s, "config_id", None) == requested_cfg_id),
                            None,
                        )
                        if broker_adapter is None:
                            logger.warning(
                                "broker_config_id=%s not found in registry; "
                                "falling back to first live session", requested_cfg_id,
                            )
                    if broker_adapter is None and all_sessions:
                        broker_adapter = next(
                            (s for s in all_sessions if not getattr(s, "is_demo", True)),
                            all_sessions[0],
                        )
                except Exception as ba_err:
                    logger.warning("No live broker adapter available, falling back to paper: %s", ba_err)
            oms = OrderManagementSystem(broker_adapter=broker_adapter)
            executor.set_oms(oms)
            logger.info("OMS injected (paper=%s, broker=%s config_id=%s)",
                        paper_mode, type(broker_adapter).__name__ if broker_adapter else "None",
                        getattr(broker_adapter, "config_id", "N/A") if broker_adapter else "N/A")
        except Exception as oms_err:
            logger.warning("OMS injection failed, executor will run without OMS: %s", oms_err)

        # Wire LiveTickService
        try:
            from broker.live_tick_service import get_tick_service
            tick_svc = get_tick_service()
            executor.set_tick_service(tick_svc)
            tick_svc.subscribe([underlying])
            logger.info("LiveTickService injected + subscribed: %s", underlying)
            if executor.state.legs:
                executor.subscribe_leg_symbols()
        except Exception as ts_err:
            logger.warning("LiveTickService injection failed (non-fatal): %s", ts_err)

        # Store executor reference for monitoring
        with _lock:
            if run_id in _running:
                _running[run_id]["executor"] = executor

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
            "━━━ STRATEGY INSTANCE START ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "  run_id     : %s\n"
            "  Template   : %s\n"
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
            run_id,
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

        # Main tick loop
        _backoff = 1
        tick_count = 0
        _last_chain_refresh = 0.0
        _CHAIN_REFRESH_INTERVAL = 5.0
        while not stop_evt.is_set():
            try:
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
                _backoff = 1

                if tick_count % 60 == 0:
                    state = executor.state
                    logger.info(
                        "HEARTBEAT | %s | tick=%d | entered=%s | legs=%d | "
                        "spot=%.2f | pnl=%.2f | adj_today=%d",
                        run_id, tick_count,
                        state.entered_today,
                        state.active_legs_count,
                        state.spot_price,
                        state.combined_pnl,
                        state.adjustments_today,
                    )
            except Exception as e:
                logger.exception(
                    "Tick error for instance '%s' (backing off %ds): %s",
                    run_id, _backoff, e,
                )
                try:
                    executor._save_state()
                except Exception:
                    pass
                _backoff = min(_backoff * 2, 60)

            stop_evt.wait(timeout=max(1, _backoff))

        # Clean shutdown
        if executor:
            try:
                executor.unsubscribe_all()
            except Exception:
                pass
            try:
                executor._save_state()
                logger.info("Instance '%s' state saved on shutdown", run_id)
            except Exception as e:
                logger.warning("Failed to save state for '%s': %s", run_id, e)
        if db_persist:
            try:
                db_persist.mark_stopped("normal_stop")
            except Exception as e:
                logger.warning("Failed to mark DB run stopped for '%s': %s", run_id, e)

    except Exception as exc:
        logger.error("Instance '%s' error: %s", run_id, exc, exc_info=True)
        with _lock:
            if run_id in _running:
                _running[run_id]["status"] = "error"
                _running[run_id]["error"] = str(exc)
        if db_persist:
            try:
                db_persist.mark_error(str(exc))
            except Exception:
                pass
        return

    with _lock:
        if run_id in _running:
            _running[run_id]["status"] = "stopped"
    logger.info("Strategy instance stopped: %s", run_id)
    # Remove from registry after a short delay so the UI can display final state
    import threading as _threading
    def _deregister():
        import time as _time
        _time.sleep(30)   # keep visible for 30s after stop
        with _lock:
            _running.pop(run_id, None)
    _threading.Thread(target=_deregister, daemon=True).start()


@router.post("/strategy/run/{name}")
async def run_strategy(
    name: str,
    body: Dict[str, Any] = Body(default={}),
    payload: dict = Depends(current_user),
):
    """Start a new strategy *instance* from the named template.

    The template JSON file is **never mutated**.  Each call creates a fresh
    isolated instance with a unique run_id, so the same template can run
    concurrently for NIFTY, BANKNIFTY, etc.

    Optional body fields (runtime overrides applied only to instance config):
      symbol           — underlying symbol (e.g. "NIFTY", "BANKNIFTY")
      exchange         — exchange code (e.g. "NFO", "BFO")
      paper_mode       — true/false
      broker_config_id — config_id of a connected broker session
    """
    safe = _safe_name(name)
    path = _config_path(name)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Strategy '{name}' not found")

    # Load template — do NOT write back
    warnings_list: List[str] = []
    try:
        with open(path) as fp:
            cfg = json.load(fp)

        # Apply runtime overrides into a copy (template file unchanged)
        override_symbol = body.get("symbol")
        override_exchange = body.get("exchange")
        override_paper = body.get("paper_mode")
        override_broker = body.get("broker_config_id")
        identity = cfg.setdefault("identity", {})
        if override_symbol:
            identity["underlying"] = override_symbol
        if override_exchange:
            identity["exchange"] = override_exchange
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
        warnings_list = [str(e) for e in errors if e.severity == "warning"]
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Cannot read config: {exc}")

    # Generate unique run_id for this instance
    run_id = f"{safe}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

    # Write per-instance config (with overrides applied) to state directory
    state_dir = SAVED_CONFIGS_DIR / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    instance_config_path = state_dir / f"{run_id}_config.json"
    try:
        cfg["_run_id"] = run_id
        cfg["_template_name"] = safe
        with open(instance_config_path, "w") as fp:
            json.dump(cfg, fp, indent=2)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Cannot write instance config: {exc}")

    symbol = cfg.get("identity", {}).get("underlying", "NIFTY")
    exchange = cfg.get("identity", {}).get("exchange", "NFO")
    paper_mode = cfg.get("identity", {}).get("paper_mode", True)
    display_name = f"{safe} · {symbol}"

    with _lock:
        stop_evt = threading.Event()
        t = threading.Thread(
            target=_strategy_thread,
            args=(run_id, safe, str(instance_config_path), stop_evt, False),
            daemon=True,
            name=f"strategy-{run_id}",
        )
        _running[run_id] = {
            "run_id": run_id,
            "template_name": safe,
            "symbol": symbol,
            "exchange": exchange,
            "paper_mode": paper_mode,
            "display_name": display_name,
            "thread": t,
            "stop_event": stop_evt,
            "started_at": datetime.utcnow().isoformat(),
            "status": "running",
            "error": None,
            "executor": None,
        }
        t.start()

    logger.info("Started strategy instance: run_id=%s template=%s symbol=%s paper=%s", run_id, safe, symbol, paper_mode)

    return {
        "ok": True,
        "run_id": run_id,
        "name": safe,
        "symbol": symbol,
        "exchange": exchange,
        "paper_mode": paper_mode,
        "display_name": display_name,
        "status": "running",
        "warnings": warnings_list[:5] if warnings_list else [],
    }


@router.post("/strategy/stop/{run_id_or_name}")
async def stop_strategy(run_id_or_name: str, payload: dict = Depends(current_user)):
    """Stop a running strategy instance (or all instances of a template).

    Accepts either:
    - A specific run_id (e.g. 'dnss_dynamic_straddle_20260415_100000_abc123') — stops that instance.
    - A template name (e.g. 'dnss_dynamic_straddle') — stops ALL running instances of that template.
    """
    stopped: List[str] = []
    with _lock:
        # Try exact run_id match first
        if run_id_or_name in _running:
            entry = _running[run_id_or_name]
            entry["stop_event"].set()
            entry["status"] = "stopping"
            stopped.append(run_id_or_name)
        else:
            # Fall back: stop all instances whose template_name matches
            safe = _safe_name(run_id_or_name)
            for rid, entry in list(_running.items()):
                if entry.get("template_name") == safe:
                    entry["stop_event"].set()
                    entry["status"] = "stopping"
                    stopped.append(rid)

    if not stopped:
        raise HTTPException(
            status_code=404,
            detail=f"No running instance found for '{run_id_or_name}'",
        )

    logger.info("Stopping strategy instance(s): %s", stopped)
    return {"ok": True, "stopped": stopped, "status": "stopping"}


def _get_all_strategy_status() -> list:
    """Return status of all running strategy instances (keyed by run_id).

    Only active (thread-alive) and recent error instances are returned.
    Stopped instances are purged from _running and excluded from results
    to keep the Running tab clean.
    """
    result: List[Dict[str, Any]] = []
    with _lock:
        snapshot = dict(_running)

    # Purge cleanly-stopped threads so they don't pollute the Running tab.
    dead_run_ids = [
        rid for rid, entry in snapshot.items()
        if not (entry.get("thread") and entry["thread"].is_alive())
        and entry.get("status") not in ("running", "error")
    ]
    if dead_run_ids:
        with _lock:
            for rid in dead_run_ids:
                _running.pop(rid, None)
        for rid in dead_run_ids:
            snapshot.pop(rid, None)

    for run_id, entry in snapshot.items():
        thread: Optional[threading.Thread] = entry.get("thread")
        is_alive = thread.is_alive() if thread else False
        executor = entry.get("executor")
        result.append({
            "run_id": run_id,
            "name": run_id,                          # backward-compat key for LiveMonitorPanel
            "template_name": entry.get("template_name", ""),
            "display_name": entry.get("display_name", run_id),
            "symbol": entry.get("symbol", ""),
            "exchange": entry.get("exchange", ""),
            "paper_mode": entry.get("paper_mode", True),
            "status": "running" if is_alive else entry.get("status", "stopped"),
            "started_at": entry.get("started_at"),
            "error": entry.get("error"),
            "entered_today": (
                executor.state.entered_today if is_alive and executor else False
            ),
            "active_legs": (
                executor.state.active_legs_count if is_alive and executor else 0
            ),
            "combined_pnl": (
                executor.state.combined_pnl if is_alive and executor else 0.0
            ),
            "spot_price": (
                executor.state.spot_price if is_alive and executor else 0.0
            ),
        })
    return result


@router.get("/strategy/instances")
async def strategy_instances(payload: dict = Depends(current_user)):
    """Return all running strategy instances."""
    return _get_all_strategy_status()


@router.get("/strategy/status")
async def all_status(payload: dict = Depends(current_user)):
    """Return status of all running strategy instances (alias for /instances)."""
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


@router.get("/strategy/monitor/{run_id_or_name}")
async def strategy_monitor(run_id_or_name: str, payload: dict = Depends(current_user)):
    """Get real-time monitoring data for a running strategy instance.

    Accepts either a run_id (exact instance) or a template name (returns the
    first active instance of that template for backward compatibility).
    """
    with _lock:
        # Prefer exact run_id match
        entry = _running.get(run_id_or_name)
        if not entry:
            # Fallback: find first running instance of the template
            safe = _safe_name(run_id_or_name)
            entry = next(
                (e for e in _running.values() if e.get("template_name") == safe),
                None,
            )
            run_id_or_name = next(
                (rid for rid, e in _running.items() if e.get("template_name") == safe),
                run_id_or_name,
            )

    if not entry:
        raise HTTPException(status_code=404, detail=f"Strategy '{run_id_or_name}' is not running")

    executor = entry.get("executor")
    thread: Optional[threading.Thread] = entry.get("thread")
    is_alive = thread.is_alive() if thread else False
    run_id = entry.get("run_id", run_id_or_name)
    template_name = entry.get("template_name", run_id)

    if not executor:
        return {
            "strategy": run_id,
            "template_name": template_name,
            "display_name": entry.get("display_name", run_id),
            "status": entry.get("status", "starting"),
            "legs": [],
            "market_data": {},
            "summary": {},
        }

    state = executor.state
    legs = _executor_legs_to_positions(executor, template_name)

    return {
        "strategy": run_id,
        "template_name": template_name,
        "display_name": entry.get("display_name", run_id),
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
    """Aggregate positions from ALL running strategy instances."""
    all_positions: List[Dict[str, Any]] = []
    with _lock:
        running_copy = dict(_running)

    for run_id, entry in running_copy.items():
        executor = entry.get("executor")
        thread: Optional[threading.Thread] = entry.get("thread")
        is_alive = thread.is_alive() if thread else False
        if executor and is_alive:
            try:
                positions = _executor_legs_to_positions(executor, entry.get("template_name", run_id))
                all_positions.extend(p for p in positions if p.get("is_active"))
            except Exception as e:
                logger.warning("Monitor read error for %s: %s", run_id, e)

    return all_positions
