"""
Manager DB Schema — PostgreSQL tables for the manager architecture.

Each table stores per-user, per-broker-account data fetched by managers.
All data is stored as JSONB rows for flexibility across broker types.
Tables use UPSERT (INSERT ... ON CONFLICT UPDATE) for atomic refresh.
"""

import logging
from db.trading_db import get_trading_conn

logger = logging.getLogger("smart_trader.managers.schema")

# ── Schema DDL ─────────────────────────────────────────────────────────────────

_TABLES = [
    # ── Positions (one row per position per account) ──────────────────────
    """
    CREATE TABLE IF NOT EXISTS mgr_positions (
        id              BIGSERIAL PRIMARY KEY,
        user_id         TEXT NOT NULL,
        config_id       TEXT NOT NULL,
        broker_id       TEXT NOT NULL,
        client_id       TEXT NOT NULL,
        symbol          TEXT NOT NULL,
        exchange        TEXT NOT NULL DEFAULT '',
        data            JSONB NOT NULL DEFAULT '{}',
        fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE(user_id, config_id, symbol, exchange)
    );
    CREATE INDEX IF NOT EXISTS idx_mgr_pos_user        ON mgr_positions(user_id);
    CREATE INDEX IF NOT EXISTS idx_mgr_pos_user_cfg    ON mgr_positions(user_id, config_id);
    CREATE INDEX IF NOT EXISTS idx_mgr_pos_fetched     ON mgr_positions(fetched_at);
    """,

    # ── Orders (one row per order per account) ────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS mgr_orders (
        id              BIGSERIAL PRIMARY KEY,
        user_id         TEXT NOT NULL,
        config_id       TEXT NOT NULL,
        broker_id       TEXT NOT NULL,
        client_id       TEXT NOT NULL,
        order_id        TEXT NOT NULL,
        data            JSONB NOT NULL DEFAULT '{}',
        fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE(user_id, config_id, order_id)
    );
    CREATE INDEX IF NOT EXISTS idx_mgr_ord_user        ON mgr_orders(user_id);
    CREATE INDEX IF NOT EXISTS idx_mgr_ord_user_cfg    ON mgr_orders(user_id, config_id);
    CREATE INDEX IF NOT EXISTS idx_mgr_ord_fetched     ON mgr_orders(fetched_at);
    """,

    # ── Holdings (one row per holding per account) ────────────────────────
    """
    CREATE TABLE IF NOT EXISTS mgr_holdings (
        id              BIGSERIAL PRIMARY KEY,
        user_id         TEXT NOT NULL,
        config_id       TEXT NOT NULL,
        broker_id       TEXT NOT NULL,
        client_id       TEXT NOT NULL,
        symbol          TEXT NOT NULL,
        exchange        TEXT NOT NULL DEFAULT '',
        data            JSONB NOT NULL DEFAULT '{}',
        fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE(user_id, config_id, symbol, exchange)
    );
    CREATE INDEX IF NOT EXISTS idx_mgr_hold_user       ON mgr_holdings(user_id);
    CREATE INDEX IF NOT EXISTS idx_mgr_hold_user_cfg   ON mgr_holdings(user_id, config_id);
    """,

    # ── Tradebook (one row per trade per account) ─────────────────────────
    """
    CREATE TABLE IF NOT EXISTS mgr_tradebook (
        id              BIGSERIAL PRIMARY KEY,
        user_id         TEXT NOT NULL,
        config_id       TEXT NOT NULL,
        broker_id       TEXT NOT NULL,
        client_id       TEXT NOT NULL,
        trade_id        TEXT NOT NULL,
        data            JSONB NOT NULL DEFAULT '{}',
        fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE(user_id, config_id, trade_id)
    );
    CREATE INDEX IF NOT EXISTS idx_mgr_trade_user      ON mgr_tradebook(user_id);
    CREATE INDEX IF NOT EXISTS idx_mgr_trade_user_cfg  ON mgr_tradebook(user_id, config_id);
    """,

    # ── Funds / Account snapshot (one row per broker account) ─────────────
    """
    CREATE TABLE IF NOT EXISTS mgr_funds (
        id              BIGSERIAL PRIMARY KEY,
        user_id         TEXT NOT NULL,
        config_id       TEXT NOT NULL,
        broker_id       TEXT NOT NULL,
        client_id       TEXT NOT NULL,
        data            JSONB NOT NULL DEFAULT '{}',
        fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE(user_id, config_id)
    );
    CREATE INDEX IF NOT EXISTS idx_mgr_funds_user      ON mgr_funds(user_id);
    """,

    # ── Session status (one row per broker account) ───────────────────────
    """
    CREATE TABLE IF NOT EXISTS mgr_sessions (
        id              BIGSERIAL PRIMARY KEY,
        user_id         TEXT NOT NULL,
        config_id       TEXT NOT NULL,
        broker_id       TEXT NOT NULL,
        client_id       TEXT NOT NULL,
        state           TEXT NOT NULL DEFAULT 'disconnected',
        mode            TEXT NOT NULL DEFAULT 'demo',
        is_live         BOOLEAN NOT NULL DEFAULT FALSE,
        error           TEXT,
        connected_at    TIMESTAMPTZ,
        last_heartbeat  TIMESTAMPTZ,
        extra           JSONB NOT NULL DEFAULT '{}',
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE(user_id, config_id)
    );
    CREATE INDEX IF NOT EXISTS idx_mgr_sess_user       ON mgr_sessions(user_id);
    """,

    # ── Alerts / webhook events ───────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS mgr_alerts (
        id              BIGSERIAL PRIMARY KEY,
        user_id         TEXT NOT NULL,
        source          TEXT NOT NULL DEFAULT 'system',
        level           TEXT NOT NULL DEFAULT 'INFO',
        title           TEXT NOT NULL DEFAULT '',
        message         TEXT NOT NULL,
        data            JSONB NOT NULL DEFAULT '{}',
        acknowledged    BOOLEAN NOT NULL DEFAULT FALSE,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_mgr_alerts_user     ON mgr_alerts(user_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_mgr_alerts_unack    ON mgr_alerts(user_id, acknowledged)
        WHERE acknowledged = FALSE;
    """,

    # ── Manager health (one row per manager) ──────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS mgr_health (
        id              BIGSERIAL PRIMARY KEY,
        manager_name    TEXT NOT NULL UNIQUE,
        status          TEXT NOT NULL DEFAULT 'stopped',
        last_run_at     TIMESTAMPTZ,
        last_run_ms     INTEGER DEFAULT 0,
        cycle_count     BIGINT DEFAULT 0,
        error_count     BIGINT DEFAULT 0,
        last_error      TEXT,
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """,
]


def init_manager_schema():
    """Create all manager tables. Safe to call on every startup."""
    conn = get_trading_conn()
    try:
        cur = conn.cursor()
        for ddl in _TABLES:
            cur.execute(ddl)
        conn.commit()
        logger.info("Manager schema initialized (PostgreSQL)")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
