"""
Smart Trader — PostgreSQL Trading Database
==========================================

Manages all trading data: orders, positions, audit logs, PnL history.
User auth stays in the existing SQLite db.

This module is the SINGLE SOURCE OF TRUTH for all trading persistence.

Schema:
  orders          - Full order lifecycle (mirrors shoonya_platform)
  audit_log       - Append-only audit trail
  pnl_history     - Daily PnL records per user
  option_chain    - Latest option chain snapshot cache
"""

import os
import logging
from contextlib import contextmanager
import psycopg2
import psycopg2.extras

logger = logging.getLogger("smart_trader.trading_db")

# ─── Connection config ─────────────────────────────────────────────────────

_DB_DSN = os.environ.get(
    "TRADING_DB_URL",
    "dbname=smart_trader_trading user=smarttrader password=st_trading_2026 host=localhost port=5432"
)


# ─── Schema DDL ────────────────────────────────────────────────────────────

_CREATE_ORDERS = """
CREATE TABLE IF NOT EXISTS orders (
    id              BIGSERIAL PRIMARY KEY,
    user_id         TEXT NOT NULL,
    client_id       TEXT NOT NULL,

    command_id      TEXT NOT NULL,
    source          TEXT NOT NULL,
    broker_user     TEXT NOT NULL,
    strategy_name   TEXT NOT NULL,

    exchange        TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    side            TEXT NOT NULL,
    quantity        INTEGER NOT NULL,
    product         TEXT NOT NULL,

    order_type      TEXT NOT NULL DEFAULT 'MARKET',
    price           DOUBLE PRECISION DEFAULT 0,

    stop_loss               DOUBLE PRECISION,
    target                  DOUBLE PRECISION,
    trailing_type           TEXT,
    trailing_value          DOUBLE PRECISION,
    trail_when              DOUBLE PRECISION,
    managed_anchor_ltp      DOUBLE PRECISION,
    managed_base_stop_loss  DOUBLE PRECISION,

    broker_order_id TEXT,
    execution_type  TEXT NOT NULL,

    status          TEXT NOT NULL DEFAULT 'CREATED',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    tag             TEXT,

    UNIQUE(command_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_orders_user_status    ON orders(user_id, status);
CREATE INDEX IF NOT EXISTS idx_orders_user_updated   ON orders(user_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_broker_id      ON orders(broker_order_id, user_id);
CREATE INDEX IF NOT EXISTS idx_orders_strategy       ON orders(user_id, strategy_name);
"""

_CREATE_AUDIT = """
CREATE TABLE IF NOT EXISTS audit_log (
    id          BIGSERIAL PRIMARY KEY,
    ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    user_id     TEXT NOT NULL,
    client_id   TEXT NOT NULL,
    command_id  TEXT,
    action      TEXT NOT NULL,
    old_value   TEXT,
    new_value   TEXT,
    source      TEXT DEFAULT 'system',
    detail      TEXT
);

CREATE INDEX IF NOT EXISTS idx_audit_user_ts ON audit_log(user_id, ts DESC);
"""

_CREATE_PNL_HISTORY = """
CREATE TABLE IF NOT EXISTS pnl_history (
    id            BIGSERIAL PRIMARY KEY,
    user_id       TEXT NOT NULL,
    client_id     TEXT NOT NULL,
    trade_date    DATE NOT NULL,
    strategy_name TEXT NOT NULL DEFAULT '',
    pnl           DOUBLE PRECISION NOT NULL DEFAULT 0,
    gross_pnl     DOUBLE PRECISION NOT NULL DEFAULT 0,
    charges       DOUBLE PRECISION NOT NULL DEFAULT 0,
    trade_count   INTEGER NOT NULL DEFAULT 0,
    win_count     INTEGER NOT NULL DEFAULT 0,
    loss_count    INTEGER NOT NULL DEFAULT 0,
    extra         JSONB NOT NULL DEFAULT '{}',
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, client_id, trade_date, strategy_name)
);

CREATE INDEX IF NOT EXISTS idx_pnl_user_date ON pnl_history(user_id, trade_date DESC);
"""

_CREATE_PNL_OHLC = """
CREATE TABLE IF NOT EXISTS pnl_ohlc (
    id          BIGSERIAL PRIMARY KEY,
    user_id     TEXT NOT NULL,
    client_id   TEXT NOT NULL,
    bar_time    TIMESTAMPTZ NOT NULL,
    timeframe   TEXT NOT NULL,
    open        DOUBLE PRECISION NOT NULL,
    high        DOUBLE PRECISION NOT NULL,
    low         DOUBLE PRECISION NOT NULL,
    close       DOUBLE PRECISION NOT NULL,
    UNIQUE(user_id, client_id, timeframe, bar_time)
);
"""

_CREATE_OPTION_CHAIN = """
CREATE TABLE IF NOT EXISTS option_chain_cache (
    id          BIGSERIAL PRIMARY KEY,
    symbol      TEXT NOT NULL,
    expiry      TEXT NOT NULL,
    strike      DOUBLE PRECISION NOT NULL,
    ce_ltp      DOUBLE PRECISION,
    pe_ltp      DOUBLE PRECISION,
    ce_iv       DOUBLE PRECISION,
    pe_iv       DOUBLE PRECISION,
    ce_delta    DOUBLE PRECISION,
    pe_delta    DOUBLE PRECISION,
    ce_gamma    DOUBLE PRECISION,
    pe_gamma    DOUBLE PRECISION,
    ce_theta    DOUBLE PRECISION,
    pe_theta    DOUBLE PRECISION,
    ce_vega     DOUBLE PRECISION,
    pe_vega     DOUBLE PRECISION,
    oi_ce       BIGINT,
    oi_pe       BIGINT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(symbol, expiry, strike)
);
"""

_CREATE_COPY_TRADE_CONFIG = """
CREATE TABLE IF NOT EXISTS copy_trade_config (
    id          BIGSERIAL PRIMARY KEY,
    master_user_id  INTEGER NOT NULL,
    follower_user_id INTEGER NOT NULL,
    qty_multiplier  DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(master_user_id, follower_user_id)
);
"""

_CREATE_RISK_STATE = """
CREATE TABLE IF NOT EXISTS risk_state (
    id              BIGSERIAL PRIMARY KEY,
    user_id         INTEGER NOT NULL,
    client_id       TEXT NOT NULL,
    trade_date      DATE NOT NULL,
    daily_pnl       DOUBLE PRECISION NOT NULL DEFAULT 0,
    dynamic_max_loss DOUBLE PRECISION NOT NULL DEFAULT 0,
    highest_profit  DOUBLE PRECISION NOT NULL DEFAULT 0,
    daily_loss_hit  BOOLEAN NOT NULL DEFAULT FALSE,
    force_exit_in_progress BOOLEAN NOT NULL DEFAULT FALSE,
    warning_sent    BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, client_id, trade_date)
);
"""

_CREATE_POSITION_SL_SETTINGS = """
CREATE TABLE IF NOT EXISTS position_sl_settings (
    user_id         TEXT NOT NULL,
    config_id       TEXT NOT NULL,
    pos_key         TEXT NOT NULL,
    active          BOOLEAN NOT NULL DEFAULT TRUE,
    stop_loss       DOUBLE PRECISION,
    target          DOUBLE PRECISION,
    trailing_value  DOUBLE PRECISION,
    trail_when      DOUBLE PRECISION,
    trail_stop      DOUBLE PRECISION,
    initial_ltp     DOUBLE PRECISION,
    base_stop_loss  DOUBLE PRECISION,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, config_id, pos_key)
);
"""

_CREATE_MARKET_TICKS = """
CREATE TABLE IF NOT EXISTS market_ticks (
    id          BIGSERIAL PRIMARY KEY,
    symbol      VARCHAR(50)  NOT NULL,
    exchange    VARCHAR(10)  NOT NULL DEFAULT 'NSE',
    token       VARCHAR(30),
    ltp         DOUBLE PRECISION NOT NULL,
    bid         DOUBLE PRECISION,
    ask         DOUBLE PRECISION,
    volume      BIGINT,
    oi          BIGINT,
    open_price  DOUBLE PRECISION,
    high_price  DOUBLE PRECISION,
    low_price   DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    tick_time   TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source      VARCHAR(20)
);

CREATE INDEX IF NOT EXISTS idx_ticks_sym_time ON market_ticks(symbol, tick_time DESC);
CREATE INDEX IF NOT EXISTS idx_ticks_received  ON market_ticks(received_at DESC);
"""

_CREATE_MARKET_OHLCV = """
CREATE TABLE IF NOT EXISTS market_ohlcv (
    id          BIGSERIAL PRIMARY KEY,
    symbol      VARCHAR(50) NOT NULL,
    exchange    VARCHAR(10) NOT NULL DEFAULT 'NSE',
    timeframe   VARCHAR(5)  NOT NULL,
    bar_time    TIMESTAMPTZ NOT NULL,
    open        DOUBLE PRECISION NOT NULL,
    high        DOUBLE PRECISION NOT NULL,
    low         DOUBLE PRECISION NOT NULL,
    close       DOUBLE PRECISION NOT NULL,
    volume      BIGINT NOT NULL DEFAULT 0,
    oi          BIGINT,
    UNIQUE(symbol, exchange, timeframe, bar_time)
);

CREATE INDEX IF NOT EXISTS idx_ohlcv_sym_tf_time ON market_ohlcv(symbol, timeframe, bar_time DESC);
"""

# ─── Connection factory ─────────────────────────────────────────────────────

def get_trading_conn():
    """Get a psycopg2 connection with RealDictCursor row factory."""
    conn = psycopg2.connect(_DB_DSN)
    conn.autocommit = False
    return conn


def get_trading_cursor(conn):
    """Get a cursor that returns dicts (requires an existing connection)."""
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


@contextmanager
def trading_cursor():
    """Context manager: opens a connection, yields a RealDictCursor, commits on exit."""
    conn = get_trading_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ─── Schema init ────────────────────────────────────────────────────────────

def _migrate_pnl_history(cur):
    """
    Migrate pnl_history from the old OHLC-based schema to the current
    strategy/trade-count schema.  Safe to call on every startup — it's a
    no-op when the schema is already correct.
    """
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name='pnl_history'"
    )
    cols = {r[0] for r in cur.fetchall()}
    if cols and "trade_count" not in cols:
        logger.warning("pnl_history has old schema — running migration")
        cur.execute("ALTER TABLE pnl_history RENAME TO pnl_history_old")
        cur.execute(_CREATE_PNL_HISTORY.strip())
        logger.info("pnl_history migrated to new schema (old data in pnl_history_old)")


def init_trading_db():
    """Create all tables if they don't exist. Call once at startup."""
    ddls = [
        _CREATE_ORDERS,
        _CREATE_AUDIT,
        _CREATE_PNL_HISTORY,
        _CREATE_PNL_OHLC,
        _CREATE_OPTION_CHAIN,
        _CREATE_COPY_TRADE_CONFIG,
        _CREATE_RISK_STATE,
        _CREATE_POSITION_SL_SETTINGS,
        _CREATE_MARKET_TICKS,
        _CREATE_MARKET_OHLCV,
    ]
    with get_trading_conn() as conn:
        cur = conn.cursor()
        _migrate_pnl_history(cur)
        for ddl in ddls:
            cur.execute(ddl)
        conn.commit()
    logger.info("Trading DB schema initialized (PostgreSQL)")
