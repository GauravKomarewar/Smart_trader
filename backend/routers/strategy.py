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
"""

import json
import logging
import os
import re
import threading
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
                "status": "running" if is_alive else entry.get("status", "stopped"),
                "error": entry.get("error"),
                "file": path.name,
                "modified": datetime.fromtimestamp(path.stat().st_mtime).isoformat(),
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
    Lightweight background thread for paper / test mode simulation.
    For live mode the main trading bot manages execution via StrategyExecutorService.
    """
    logger.info("Strategy thread started: %s", safe_name)
    try:
        from trading.strategy_runner.config_schema import validate_config  # type: ignore[import]
        with open(config_path) as fp:
            cfg = json.load(fp)

        ok, errors = validate_config(cfg)
        if not ok and errors:
            msgs = [str(e) for e in errors[:3]]
            raise ValueError(f"Config validation failed: {'; '.join(msgs)}")

        # Simple heartbeat loop — replace with real executor tick when bot is available
        while not stop_evt.is_set():
            stop_evt.wait(timeout=5)

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

    return {"ok": True, "name": safe, "status": "running"}


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
            })
        except Exception as exc:
            logger.warning("Status read error for %s: %s", path.name, exc)
    return result
