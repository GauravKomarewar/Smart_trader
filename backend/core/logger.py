"""
Smart Trader — Centralized Logging Configuration
=================================================

Provides per-component rotating file handlers + console output.

Components / log files:
  api.log        — HTTP API access + general requests
  auth.log       — OAuth login, session management
  broker.log     — Shoonya client calls, WebSocket feed
  orders.log     — All order activity (place/modify/cancel/fill)
  websocket.log  — WebSocket connections from frontend clients
  errors.log     — ERROR + CRITICAL across all components
"""

import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Optional

LOG_FORMAT   = "%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s"
LOG_DATETIME = "%Y-%m-%d %H:%M:%S"


class _ComponentRegistry:
    """Singleton that tracks which component handlers have been created."""
    _handlers: dict[str, logging.handlers.RotatingFileHandler] = {}
    _error_handler: Optional[logging.handlers.RotatingFileHandler] = None
    _console_handler: Optional[logging.StreamHandler] = None
    _initialized: bool = False
    _log_dir: Path = Path("logs")


_reg = _ComponentRegistry()

# Component name → (logger_name, file_name)
_COMPONENTS = {
    "api":              ("smart_trader.api",              "api.log"),
    "auth":             ("smart_trader.auth",             "auth.log"),
    "broker":           ("smart_trader.broker",           "broker.log"),
    "broker.connect":   ("smart_trader.broker.connect",   "broker.log"),
    "broker.session":   ("smart_trader.broker.session",   "broker.log"),
    "broker.registry":  ("smart_trader.broker.registry",  "broker.log"),
    "account_risk":     ("smart_trader.account_risk",     "risk.log"),
    "orders":           ("smart_trader.orders",           "orders.log"),
    "oms":              ("smart_trader.oms",              "orders.log"),
    "websocket":        ("smart_trader.websocket",        "websocket.log"),
    "fyers":            ("smart_trader.fyers",            "broker.log"),
    "app":              ("smart_trader",                  "api.log"),
    # ── Position / risk managers ─────────────────────────────────────────
    "pos_sl":           ("smart_trader.pos_sl",           "positions.log"),
    "position_watcher": ("smart_trader.position_watcher", "positions.log"),
    "manual_pos":       ("smart_trader.manual_position_mgr", "positions.log"),
    "mgr.positions":    ("smart_trader.mgr.positions",    "positions.log"),
    "mgr.holdings":     ("smart_trader.mgr.holdings",     "orders.log"),
    "mgr.orders":       ("smart_trader.mgr.orders",       "orders.log"),
    "mgr.supreme":      ("smart_trader.mgr.supreme",      "api.log"),
    # ── Broker subsystems ────────────────────────────────────────────────
    "session_scheduler":("smart_trader.session_scheduler", "broker.log"),
    "live_tick":        ("smart_trader.live_tick",         "broker.log"),
    "symbol_normalizer":("smart_trader.symbol_normalizer", "broker.log"),
    "symbols_db":       ("smart_trader.symbols_db",        "broker.log"),
}


def setup_logging(
    log_dir: str = "logs",
    level: str = "INFO",
    max_bytes: int = 10 * 1024 * 1024,  # 10 MB
    backup_count: int = 5,
) -> None:
    """
    Initialize application-wide logging with per-component rotating handlers.
    Must be called ONCE at startup in main().

    Args:
        log_dir:       Directory to store log files.
        level:         Root log level (DEBUG / INFO / WARNING / ERROR).
        max_bytes:     Max log file size before rotation (default 10 MB).
        backup_count:  How many rotated backups to keep (default 5).
    """
    if _reg._initialized:
        return

    _reg._log_dir = Path(log_dir)
    _reg._log_dir.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATETIME)
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    # ── Root logger ──────────────────────────────────────────────────────────
    root = logging.getLogger()
    root.setLevel(numeric_level)
    for h in list(root.handlers):
        root.removeHandler(h)

    # ── Console handler ──────────────────────────────────────────────────────
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    console.setLevel(numeric_level)
    root.addHandler(console)
    _reg._console_handler = console

    # ── Shared errors.log (ERROR+) ───────────────────────────────────────────
    err_handler = logging.handlers.RotatingFileHandler(
        str(_reg._log_dir / "errors.log"),
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    err_handler.setFormatter(formatter)
    err_handler.setLevel(logging.ERROR)
    root.addHandler(err_handler)
    _reg._error_handler = err_handler

    # ── Per-component rotating handlers ─────────────────────────────────────
    # Track which logger names map to which log files so we don't add
    # duplicate handlers when a parent and child share the same log file.
    # e.g. smart_trader.broker *and* smart_trader.broker.connect both map to
    # broker.log — the child must NOT get its own handler or each message is
    # written twice (once by child handler, once by parent handler).
    # But if they map to DIFFERENT files, both get their own handler.
    logger_file_map: dict[str, str] = {}   # logger_name → file_name

    for key, (logger_name, file_name) in _COMPONENTS.items():
        # Skip adding a file handler if any ancestor logger already has
        # a handler writing to the SAME file (avoids duplicates).
        parts = logger_name.split(".")
        has_ancestor_same_file = any(
            logger_file_map.get(".".join(parts[:i])) == file_name
            for i in range(1, len(parts))
        )

        comp_logger = logging.getLogger(logger_name)
        comp_logger.propagate = True  # always propagate to root (console + errors.log)

        if not has_ancestor_same_file:
            file_handler = logging.handlers.RotatingFileHandler(
                str(_reg._log_dir / file_name),
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding="utf-8",
            )
            file_handler.setFormatter(formatter)
            file_handler.setLevel(numeric_level)
            comp_logger.addHandler(file_handler)
            _reg._handlers[key] = file_handler
            logger_file_map[logger_name] = file_name

    # ── Quiet noisy libraries ────────────────────────────────────────────────
    for noisy in ("uvicorn.access", "uvicorn.error", "websockets", "httpx"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    _reg._initialized = True
    logging.getLogger("smart_trader").info(
        "Logging initialized | dir=%s | level=%s | max_bytes=%s | backups=%s",
        log_dir, level, max_bytes, backup_count,
    )


def get_logger(component: str) -> logging.Logger:
    """
    Return a logger for a named component.

    Args:
        component: One of api / auth / broker / orders / websocket / app,
                   or a custom dotted name like 'smart_trader.mymodule'.
    """
    if component in _COMPONENTS:
        logger_name, _ = _COMPONENTS[component]
        return logging.getLogger(logger_name)
    return logging.getLogger(f"smart_trader.{component}")
