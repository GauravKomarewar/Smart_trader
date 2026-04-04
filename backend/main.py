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
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

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
from trading.oms             import oms_router
from trading.supreme_risk    import risk_router
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
app.include_router(oms_router,      prefix="/api")
app.include_router(risk_router,    prefix="/api")
app.include_router(alert_router,   prefix="/api")

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


@app.on_event("shutdown")
async def on_shutdown():
    from broker.shoonya_client import get_session
    try:
        get_session().logout()
    except Exception:
        pass
    logger.info("Smart Trader Backend shutting down")


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run(
        app,
        host=config.HOST,
        port=config.PORT,
        log_level=config.LOG_LEVEL.lower(),
        access_log=False,
    )
