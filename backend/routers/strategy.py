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
    """Return supported option-chain underlyings for the strategy builder."""
    return [
        {"symbol": "NIFTY",      "exchange": "NFO"},
        {"symbol": "BANKNIFTY",  "exchange": "NFO"},
        {"symbol": "FINNIFTY",   "exchange": "NFO"},
        {"symbol": "MIDCPNIFTY", "exchange": "NFO"},
        {"symbol": "SENSEX",     "exchange": "BFO"},
        {"symbol": "BANKEX",     "exchange": "BFO"},
    ]


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

        # Store executor reference for monitoring endpoints
        with _lock:
            if safe_name in _running:
                _running[safe_name]["executor"] = executor

        logger.info(
            "Strategy '%s' executor initialized | underlying=%s exchange=%s paper=%s",
            safe_name,
            cfg.get("identity", {}).get("underlying", "?"),
            cfg.get("identity", {}).get("exchange", "?"),
            cfg.get("identity", {}).get("paper_mode", True),
        )

        # Main tick loop — replaces the old heartbeat
        _backoff = 1
        tick_count = 0
        _last_chain_refresh = 0.0
        _CHAIN_REFRESH_INTERVAL = 30.0  # Refresh option chain data every 30 seconds
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

                # Log heartbeat every 60 ticks (~2 min at 2s interval)
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

            # Wait for next tick (2s default) or until stop is signalled
            stop_evt.wait(timeout=max(2, _backoff))

        # Clean shutdown — save final state
        if executor:
            try:
                executor._save_state()
                logger.info("Strategy '%s' state saved on shutdown", safe_name)
            except Exception as e:
                logger.warning("Failed to save state for '%s': %s", safe_name, e)

    except Exception as exc:
        logger.error("Strategy '%s' error: %s", safe_name, exc, exc_info=True)
        with _lock:
            if safe_name in _running:
                _running[safe_name]["status"] = "error"
                _running[safe_name]["error"] = str(exc)
        return

    with _lock:
        if safe_name in _running:
            _running[safe_name]["status"] = "stopped"
    logger.info("Strategy thread stopped: %s", safe_name)


@router.post("/strategy/run/{name}")
async def run_strategy(name: str, payload: dict = Depends(current_user)):
    """Enable and start a strategy (paper mode background thread)."""
    safe = _safe_name(name)
    path = _config_path(name)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Strategy '{name}' not found")

    with _lock:
        entry = _running.get(safe, {})
        thread: Optional[threading.Thread] = entry.get("thread")
        if thread and thread.is_alive():
            raise HTTPException(status_code=409, detail=f"Strategy '{name}' is already running")

    # Pre-validate config before spawning thread
    warnings_list: List[str] = []
    try:
        with open(path) as fp:
            cfg = json.load(fp)
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
        with open(path) as fp:
            cfg = json.load(fp)
        cfg["enabled"] = True
        cfg["status"] = "RUNNING"
        with open(path, "w") as fp:
            json.dump(cfg, fp, indent=2)
    except Exception as exc:
        logger.warning("Could not update enabled flag for %s: %s", name, exc)

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


@router.get("/strategy/status")
async def all_status(payload: dict = Depends(current_user)):
    """Return status of all saved strategies."""
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
                # Add executor state if running
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


# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY LIVE MONITOR — real-time state from running executors
# ══════════════════════════════════════════════════════════════════════════════

def _executor_legs_to_positions(executor, safe_name: str) -> List[Dict[str, Any]]:
    """Convert executor state legs to OMS-compatible position dicts."""
    from trading.strategy_runner.models import Side as StratSide, InstrumentType, OptionType
    state = executor.state
    positions: List[Dict[str, Any]] = []
    for tag, leg in state.legs.items():
        opt_type_str = leg.option_type.value if leg.option_type else ""
        strike_str = f"{int(leg.strike)}" if leg.strike else ""
        tsym = leg.trading_symbol or f"{leg.symbol}{leg.expiry}{strike_str}{opt_type_str}"

        side_sign = 1 if leg.side == StratSide.BUY else -1
        netqty = side_sign * leg.order_qty

        positions.append({
            # Fields the LiveMonitorPanel reads
            "tsym": tsym,
            "symbol": leg.symbol,
            "tradingsymbol": tsym,
            "netqty": netqty if leg.is_active else 0,
            "qty": leg.qty,
            "ltp": leg.ltp,
            "avgprc": leg.entry_price,
            "avg_price": leg.entry_price,
            "urmtom": leg.pnl if leg.is_active else 0.0,
            "unrealized_pnl": leg.pnl if leg.is_active else 0.0,
            "rpnl": 0.0 if leg.is_active else leg.pnl,
            "realized_pnl": 0.0 if leg.is_active else leg.pnl,
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
            "lots": leg.qty,
            "lot_size": leg.lot_size,
            "entry_price": leg.entry_price,
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
