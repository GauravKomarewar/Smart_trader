#!/usr/bin/env python3
"""
Smart Trader — FastAPI Backend
================================

Entry point for the Smart Trader backend server.

Routes:
  GET  /health               — health check
  POST /api/auth/login       — Shoonya OAuth login
  GET  /api/auth/status      — session status
  GET  /api/market/indices   — major indices quotes
  GET  /api/market/quote/{symbol}  — single quote
  GET  /api/market/screener  — stock screener
  GET  /api/market/option-chain/{symbol} — option chain
  GET  /api/market/search    — instrument search
  POST /api/orders/place     — place order
  GET  /api/orders/book      — order book
  GET  /api/orders/positions — open positions
  WS   /ws/market            — live market data WebSocket

Logging:
  logs/api.log        — HTTP requests
  logs/auth.log       — OAuth / session
  logs/broker.log     — Shoonya client calls
  logs/orders.log     — Order activity
  logs/websocket.log  — WS connections
  logs/errors.log     — All errors (shared)

Usage:
  cd /home/ubuntu/Smart_trader/backend
  python main.py

  # or with uvicorn directly:
  uvicorn main:app --host 0.0.0.0 --port 8001 --reload
"""

import os
import sys
import logging
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

# ── Resolve paths ──────────────────────────────────────────────────────────────
_HERE = Path(__file__).parent
sys.path.insert(0, str(_HERE))

# ── Bootstrap logging FIRST ───────────────────────────────────────────────────
from core.config import config
from core.logger import setup_logging

setup_logging(
    log_dir=str(_HERE / config.LOG_DIR),
    level=config.LOG_LEVEL,
    max_bytes=config.LOG_MAX_BYTES,
    backup_count=config.LOG_BACKUP_COUNT,
)

logger = logging.getLogger("smart_trader.api")
logger.info("=" * 60)
logger.info("Smart Trader Backend starting up")
logger.info("Mode: %s | Port: %d", "DEBUG" if config.DEBUG else "PRODUCTION", config.PORT)
logger.info("Shoonya configured: %s", config.is_shoonya_configured())
logger.info("Fyers configured:   %s", config.is_fyers_configured())
logger.info("Encryption key:     %s", "custom" if os.getenv("ENCRYPTION_KEY") else "derived from JWT_SECRET")
logger.info("Docs UI:            http://0.0.0.0:%d/docs", config.PORT)
logger.info("=" * 60)

# ── Import routers ─────────────────────────────────────────────────────────────
from routers.auth            import router as auth_router
from routers.market          import router as market_router
from routers.orders          import router as orders_router
from routers.websocket       import router as ws_router
from routers.admin           import router as admin_router
from routers.broker_sessions import router as broker_router
from routers.strategy        import router as strategy_router
from routers.risk            import risk_router
from routers.positions       import positions_router
from routers.greeks          import greeks_router
from routers.option_chain    import oc_router
from routers.analytics       import analytics_router
from trading.oms             import oms_router
from trading.trading_bot     import alert_router

# ── Create FastAPI app ─────────────────────────────────────────────────────────
app = FastAPI(
    title="Smart Trader API",
    description=(
        "Backend for Smart Trader — Multi-user, multi-broker trading terminal.\n\n"
        "Each user can connect multiple broker accounts (Shoonya, Zerodha, Fyers, Paper).\n"
        "Credentials are encrypted at rest. Per-account risk management enforced.\n\n"
        "Auth: `POST /api/auth/login` → pass `Authorization: Bearer <token>` header."
    ),
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# ── CORS ───────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Register routers ───────────────────────────────────────────────────────────
app.include_router(auth_router,    prefix="/api")
app.include_router(market_router,  prefix="/api")
app.include_router(orders_router,  prefix="/api")
app.include_router(ws_router)
app.include_router(admin_router,   prefix="/api")
app.include_router(broker_router,   prefix="/api")
app.include_router(strategy_router, prefix="/api")
app.include_router(oms_router,        prefix="/api")
app.include_router(risk_router,       prefix="/api")
app.include_router(positions_router,  prefix="/api")
app.include_router(greeks_router,     prefix="/api")
app.include_router(oc_router,         prefix="/api")
app.include_router(analytics_router,  prefix="/api")
app.include_router(alert_router,      prefix="/api")

# ── Health check ───────────────────────────────────────────────────────────────
@app.get("/health", tags=["health"])
async def health():
    from broker.shoonya_client import get_session
    from broker.multi_broker import registry
    session = get_session()
    all_sessions = registry.get_all_sessions()
    live_count = sum(1 for s in all_sessions if s.is_live or s.is_paper)
    return {
        "status":             "ok",
        "mode":               "demo" if session.is_demo else "live",
        "loggedIn":           session.is_logged_in,
        "shoonya_configured": config.is_shoonya_configured(),
        "fyers_live":         config.is_fyers_configured(),
        "active_accounts":    live_count,        # live wallet / paper accounts
        "total_sessions":     len(all_sessions),
        "version":            "2.0.0",
    }


@app.on_event("startup")
async def on_startup():
    # Initialize database tables
    from db.database import create_tables
    create_tables()

    # Initialize PostgreSQL trading tables
    try:
        from db.trading_db import init_trading_db
        init_trading_db()
    except Exception as exc:
        import logging
        logging.getLogger("smart_trader").warning("trading_db init failed: %s", exc)

    # Initialize and bootstrap the symbols DB once if it is empty.
    try:
        from db.symbols_db import (
            get_symbol_count,
            init_symbols_schema,
            populate_symbols_db,
            start_daily_refresh_scheduler,
        )
        from scripts.unified_scriptmaster import refresh_all

        init_symbols_schema()
        existing_symbols = get_symbol_count()
        if existing_symbols <= 0:
            logger.warning("Symbols DB empty — running one-time scriptmaster bootstrap")
            refresh_counts = refresh_all(force=True)
            logger.info("Bootstrap scriptmaster refresh completed: %s", refresh_counts)
            sym_counts = populate_symbols_db(force=True)
            logger.info("Symbols DB bootstrap population completed: %s", sym_counts)
        else:
            logger.info("Symbols DB already populated with %d rows — startup bootstrap skipped", existing_symbols)
        start_daily_refresh_scheduler()
    except Exception as exc:
        logger.warning("Symbols DB bootstrap failed (non-fatal): %s", exc)

    # Seed admin + demo users
    from db.database import SessionLocal, User
    from core.config import config
    from core.security import hash_password, generate_id
    db = SessionLocal()
    try:
        # Admin
        if not db.query(User).filter(User.email == config.ADMIN_EMAIL.lower()).first():
            admin = User(
                id=generate_id(),
                email=config.ADMIN_EMAIL.lower(),
                name=config.ADMIN_NAME,
                password_hash=hash_password(config.ADMIN_PASSWORD),
                role="admin",
            )
            db.add(admin)
            db.commit()
            logger.info("Admin user seeded: %s", config.ADMIN_EMAIL)

        # Demo user (always ensure exists)
        demo_email = os.getenv("DEMO_EMAIL", "demo@smarttrader.in")
        demo_password = os.getenv("DEMO_PASSWORD", "Demo@1234")
        if not db.query(User).filter(User.email == demo_email).first():
            demo = User(
                id=generate_id(),
                email=demo_email,
                name="Demo User",
                password_hash=hash_password(demo_password),
                role="viewer",
            )
            db.add(demo)
            db.commit()
            logger.info("Demo user seeded: %s", demo_email)
    finally:
        db.close()

    logger.info("FastAPI startup complete — listening on %s:%d", config.HOST, config.PORT)

    # ── Restore broker sessions from DB ──────────────────────────────────────
    # This allows sessions to survive server restarts (session tokens are persistent)
    try:
        _restore_broker_sessions()
    except Exception as exc:
        logger.warning("Broker session restore failed (non-fatal): %s", exc)

    # ── Start SupremeManager (DB-backed data managers) ───────────────────
    try:
        from managers.supreme_manager import supreme
        supreme.start()
        logger.info("SupremeManager started — all data managers active")
    except Exception as exc:
        logger.warning("SupremeManager start failed (non-fatal): %s", exc)

    # ── Start PositionWatcher (real-time risk monitoring) ────────────────────
    try:
        from trading.position_watcher import position_watcher
        position_watcher.start()
        logger.info("PositionWatcher started — real-time risk monitoring active")
    except Exception as exc:
        logger.warning("PositionWatcher start failed (non-fatal): %s", exc)

    # ── Start PositionSLManager (per-position SL/TG/trailing engine) ─────────
    try:
        from trading.position_sl_manager import position_sl_manager
        position_sl_manager.start()
        logger.info("PositionSLManager started — SL/TG/Trail execution active")
    except Exception as exc:
        logger.warning("PositionSLManager start failed (non-fatal): %s", exc)

    # ── Start ManualPositionManager (orphan/manual-position rules) ──────────
    try:
        from trading.services.manual_position_manager import ManualPositionManager
        _manual_pos_mgr = ManualPositionManager()
        _manual_pos_mgr.start()
        logger.info("ManualPositionManager started — orphan/manual-position rule engine active")
    except Exception as exc:
        logger.warning("ManualPositionManager start failed (non-fatal): %s", exc)

    # ── Start SessionScheduler (SEBI daily login/logout) ─────────────────────
    try:
        from trading.session_scheduler import session_scheduler
        session_scheduler.start()
        logger.info("SessionScheduler started — auto-login@08:45 IST, auto-logout@23:55 IST")
    except Exception as exc:
        logger.warning("SessionScheduler start failed (non-fatal): %s", exc)

    # ── Start LiveTickService (Fyers + Shoonya WebSocket dual pipeline) ───────
    try:
        import asyncio
        from broker.live_tick_service import get_tick_service
        loop = asyncio.get_event_loop()
        svc = get_tick_service()
        svc.set_event_loop(loop)
        logger.info("LiveTickService started — Fyers+Shoonya dual pipeline active")
    except Exception as exc:
        logger.warning("LiveTickService start failed (non-fatal): %s", exc)

def _restore_broker_sessions():
    """
    Restore previously connected broker sessions from DB on startup.
    For each saved session with a valid token, re-inject into multi-broker registry.
    This mimics shoonya_platform's recovery service pattern.
    """
    from db.database import SessionLocal, BrokerSession as DBSession, BrokerConfig
    from broker.multi_broker import registry
    from core.security import decrypt_credentials

    db = SessionLocal()
    try:
        active_sessions = (
            db.query(DBSession)
            .filter(DBSession.is_logged_in == True)
            .all()
        )
        if not active_sessions:
            logger.info("No active broker sessions to restore")
            return

        restored = 0
        for db_sess in active_sessions:
            cfg = db.query(BrokerConfig).filter(BrokerConfig.id == db_sess.config_id).first()
            if not cfg:
                continue

            try:
                creds = decrypt_credentials(str(cfg.credentials))
            except Exception as e:
                logger.warning("Cannot decrypt creds for config=%s: %s", cfg.id[:8], e)
                continue

            client_id = cfg.client_id or creds.get("USER_ID") or creds.get("CLIENT_ID") or "?"
            token = db_sess.session_token

            if db_sess.broker_id == "shoonya":
                # Prefer the shared cache when it has the currently-active Shoonya token.
                shoonya_token = None
                cached_token = None
                try:
                    from trading.session_scheduler import (
                        _load_shoonya_cached_token,
                        _validate_shoonya_token,
                    )
                    cached_token = _load_shoonya_cached_token(
                        creds.get("USER_ID", client_id)
                    )
                except Exception:
                    cached_token = None

                if cached_token:
                    shoonya_token = cached_token
                    if cached_token != token:
                        logger.info(
                            "Using fresher shared Shoonya token for restore: user=%s client=%s",
                            db_sess.user_id[:8], client_id,
                        )
                elif token:
                    try:
                        from trading.session_scheduler import _validate_shoonya_token
                        if _validate_shoonya_token(creds.get("USER_ID", client_id), token):
                            shoonya_token = token
                        else:
                            logger.warning(
                                "Stored Shoonya DB token expired — skipping stale restore token: user=%s client=%s",
                                db_sess.user_id[:8], client_id,
                            )
                    except Exception:
                        shoonya_token = token

                if not shoonya_token:
                    try:
                        from trading.session_scheduler import _load_shoonya_cached_token
                        shoonya_token = _load_shoonya_cached_token(
                            creds.get("USER_ID", client_id)
                        )
                        if shoonya_token:
                            logger.info("Using cached Shoonya token from shared file for user=%s", db_sess.user_id[:8])
                    except Exception:
                        pass

                if not shoonya_token:
                    logger.warning("No Shoonya token available for user=%s — skipping restore", db_sess.user_id[:8])
                    continue

                try:
                    sess = registry.register_shoonya(
                        user_id=db_sess.user_id,
                        config_id=cfg.id,
                        client_id=client_id,
                        creds=creds,
                        token=shoonya_token,
                    )
                    if sess.is_live:
                        restored += 1
                        logger.info(
                            "Restored Shoonya session: user=%s client=%s",
                            db_sess.user_id[:8], client_id,
                        )
                        # Update DB with fresh token if it came from cache
                        if shoonya_token != token:
                            db_sess.session_token = shoonya_token
                            db.commit()
                    else:
                        logger.warning(
                            "Shoonya session restore failed (token may be expired): user=%s client=%s",
                            db_sess.user_id[:8], client_id,
                        )
                        db_sess.is_logged_in = False
                        db.commit()
                except Exception as e:
                    logger.warning("Shoonya restore error for user=%s: %s", db_sess.user_id[:8], e)

            elif db_sess.broker_id == "fyers" and token:
                try:
                    sess = registry.register_fyers(
                        user_id=db_sess.user_id,
                        config_id=cfg.id,
                        client_id=client_id,
                        creds=creds,
                        token=token,
                    )
                    if sess.is_live:
                        restored += 1
                        logger.info(
                            "Restored Fyers session: user=%s client=%s",
                            db_sess.user_id[:8], client_id,
                        )
                except Exception as e:
                    logger.warning("Fyers restore error for user=%s: %s", db_sess.user_id[:8], e)

            elif db_sess.broker_id == "paper_trade":
                try:
                    registry.register_paper(
                        user_id=db_sess.user_id,
                        config_id=cfg.id,
                        client_id="PAPER",
                    )
                    restored += 1
                    logger.info("Restored paper session: user=%s", db_sess.user_id[:8])
                except Exception as e:
                    logger.warning("Paper restore error: %s", e)

        logger.info("Session restore complete: %d/%d sessions restored", restored, len(active_sessions))
    finally:
        db.close()


@app.on_event("shutdown")
async def on_shutdown():
    # Stop SupremeManager first (stops all data managers)
    try:
        from managers.supreme_manager import supreme
        supreme.stop()
    except Exception:
        pass

    # Stop PositionWatcher
    try:
        from trading.position_watcher import position_watcher
        position_watcher.stop()
    except Exception:
        pass

    # Stop PositionSLManager
    try:
        from trading.position_sl_manager import position_sl_manager
        position_sl_manager.stop()
    except Exception:
        pass

    from broker.shoonya_client import get_session
    try:
        get_session().logout()
    except Exception:
        pass
    logger.info("Smart Trader Backend shutting down")


# ── Serve frontend (Vite build) ───────────────────────────────────────────────
_FRONTEND_DIST = Path(__file__).resolve().parent.parent / "frontend" / "dist"
if _FRONTEND_DIST.is_dir():
    # Mount static assets (JS/CSS/images) under /assets
    _ASSETS_DIR = _FRONTEND_DIST / "assets"
    if _ASSETS_DIR.is_dir():
        app.mount("/assets", StaticFiles(directory=str(_ASSETS_DIR)), name="frontend-assets")

    # Serve other static files at root (logo.svg, etc.)
    @app.get("/logo.svg", include_in_schema=False)
    async def _logo():
        return FileResponse(str(_FRONTEND_DIST / "logo.svg"))

    # SPA catch-all: any non-API, non-docs route serves index.html
    _INDEX_HTML = _FRONTEND_DIST / "index.html"

    @app.get("/{full_path:path}", include_in_schema=False)
    async def _spa_catch_all(request: Request, full_path: str):
        # Skip API, docs, health, and websocket paths
        if full_path.startswith(("api/", "docs", "redoc", "openapi.json", "health", "ws")):
            return JSONResponse({"detail": "Not Found"}, status_code=404)
        # Serve actual static files if they exist in dist
        candidate = _FRONTEND_DIST / full_path
        if ".." in full_path:
            return JSONResponse({"detail": "Not Found"}, status_code=404)
        if candidate.is_file():
            return FileResponse(str(candidate))
        # Serve directory index.html (e.g. /strategy_builder/ → strategy_builder/index.html)
        if candidate.is_dir() and (candidate / "index.html").is_file():
            return FileResponse(str(candidate / "index.html"))
        # Otherwise serve index.html for SPA routing
        return HTMLResponse(_INDEX_HTML.read_text())

    logger.info("Frontend served from %s", _FRONTEND_DIST)
else:
    logger.warning("Frontend dist not found at %s — run 'npm run build' in frontend/", _FRONTEND_DIST)


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run(
        app,
        host=config.HOST,
        port=config.PORT,
        log_level=config.LOG_LEVEL.lower(),
        access_log=False,
    )
