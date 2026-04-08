"""
ManualPositionManager — Rule-based management for orphan / manual positions.
============================================================================

Manages positions created directly on the broker (outside Smart Trader
strategy engine).  Applies user-defined rules to auto-exit/reduce.

Ported from shoonya_platform/services/orphan_position_manager.py,
adapted for Smart Trader's multi-broker, multi-user architecture.

Rule types
----------
  PRICE    — target / stoploss / trailing stop based on LTP
  GREEK    — delta / gamma / theta / vega threshold (requires OC data)
  COMBINED — net greek threshold across multiple symbols

Rules are persisted in PostgreSQL (mgr_manual_rules table) so they
survive restarts.  Rules execute only once then deactivate.
"""

import json
import logging
import threading
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger("smart_trader.manual_position_mgr")

_POLL_INTERVAL = 3.0   # seconds between rule checks


# ── Rule definitions ──────────────────────────────────────────────────────────

@dataclass
class ManualPositionRule:
    """
    A single rule applied to one or more manual/orphan positions.
    """
    rule_id:     str
    user_id:     str
    config_id:   str
    rule_type:   str = "PRICE"   # PRICE | GREEK | COMBINED
    symbols:     list = field(default_factory=list)   # ["NIFTY08APR26P23500", ...]
    exchange:    str  = "NFO"
    product:     str  = ""       # MIS | NRML | CNC
    active:      bool = True
    created_at:  str  = ""

    # PRICE rule fields
    target:      Optional[float] = None
    stoploss:    Optional[float] = None
    trailing:    Optional[float] = None
    trail_when:  Optional[float] = None
    trail_stop:  Optional[float] = None  # current trailing stop level

    # GREEK rule fields
    greek_name:  Optional[str]   = None  # delta | gamma | theta | vega
    greek_op:    str             = "GT"  # GT | LT | ABS_GT | ABS_LT
    greek_threshold: Optional[float] = None

    # Execution metadata
    last_checked: Optional[str]  = None
    triggered_at: Optional[str]  = None
    trigger_reason: str          = ""

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "ManualPositionRule":
        known = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in d.items() if k in known})


# ── Manager ──────────────────────────────────────────────────────────────────

class ManualPositionManager:
    """
    Background engine that monitors orphan/manual positions and enforces rules.
    One global instance; handles all users/accounts via DB-stored rules.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._exit_callback: Optional[Callable] = None  # (user_id, config_id, symbol, exchange, side, qty, reason)

    # ── Lifecycle ─────────────────────────────────────────────────────────

    def start(self):
        if self._running:
            return
        self._ensure_schema()
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop, name="manual-pos-mgr", daemon=True,
        )
        self._thread.start()
        logger.info("ManualPositionManager started (interval=%.1fs)", _POLL_INTERVAL)

    def stop(self):
        self._running = False
        self._stop_event.set()

    def set_exit_callback(self, fn: Callable) -> None:
        """Register callback for rule-triggered exits."""
        self._exit_callback = fn

    # ── Rule CRUD ─────────────────────────────────────────────────────────

    def add_rule(self, rule: ManualPositionRule) -> str:
        """Persist a new rule. Returns rule_id."""
        from db.trading_db import get_trading_conn
        import psycopg2.extras

        if not rule.created_at:
            rule.created_at = datetime.now(timezone.utc).isoformat()

        conn = get_trading_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                """INSERT INTO mgr_manual_rules
                    (rule_id, user_id, config_id, rule_type, active, payload, created_at)
                   VALUES (%s, %s, %s, %s, TRUE, %s, NOW())
                   ON CONFLICT (rule_id) DO UPDATE
                   SET payload = EXCLUDED.payload, active = TRUE""",
                (rule.rule_id, rule.user_id, rule.config_id, rule.rule_type,
                 psycopg2.extras.Json(rule.to_dict())),
            )
            conn.commit()
            logger.info("Rule added: %s type=%s symbols=%s",
                        rule.rule_id, rule.rule_type, rule.symbols)
            return rule.rule_id
        finally:
            conn.close()

    def remove_rule(self, rule_id: str) -> bool:
        from db.trading_db import get_trading_conn
        conn = get_trading_conn()
        try:
            cur = conn.cursor()
            cur.execute("UPDATE mgr_manual_rules SET active = FALSE WHERE rule_id = %s",
                        (rule_id,))
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    def get_rules(self, user_id: str, config_id: Optional[str] = None) -> List[dict]:
        from db.trading_db import get_trading_conn
        import psycopg2.extras
        conn = get_trading_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if config_id:
                cur.execute(
                    "SELECT payload FROM mgr_manual_rules "
                    "WHERE user_id = %s AND config_id = %s AND active = TRUE",
                    (user_id, config_id),
                )
            else:
                cur.execute(
                    "SELECT payload FROM mgr_manual_rules WHERE user_id = %s AND active = TRUE",
                    (user_id,),
                )
            return [dict(r["payload"]) for r in cur.fetchall()]
        finally:
            conn.close()

    # ── Background loop ───────────────────────────────────────────────────

    def _loop(self):
        while not self._stop_event.is_set():
            t0 = time.monotonic()
            try:
                self._evaluate_all_rules()
            except Exception as e:
                logger.error("ManualPositionManager loop error: %s", e, exc_info=True)
            elapsed = time.monotonic() - t0
            self._stop_event.wait(timeout=max(0, _POLL_INTERVAL - elapsed))

    def _evaluate_all_rules(self):
        """Load all active rules and check each against current positions."""
        from db.trading_db import get_trading_conn
        import psycopg2.extras

        conn = get_trading_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                "SELECT rule_id, payload FROM mgr_manual_rules WHERE active = TRUE"
            )
            rows = cur.fetchall()
        finally:
            conn.close()

        for row in rows:
            try:
                rule = ManualPositionRule.from_dict(row["payload"])
                rule.rule_id = row["rule_id"]
                triggered, reason = self._check_rule(rule)
                if triggered:
                    self._execute_rule(rule, reason)
            except Exception as e:
                logger.debug("Error evaluating rule %s: %s", row["rule_id"], e)

    # ── Rule evaluation ───────────────────────────────────────────────────

    def _check_rule(self, rule: ManualPositionRule) -> tuple:
        """
        Evaluate a single rule against current market data.
        Returns (triggered: bool, reason: str).
        """
        if rule.rule_type == "PRICE":
            return self._check_price_rule(rule)
        elif rule.rule_type == "GREEK":
            return self._check_greek_rule(rule)
        elif rule.rule_type == "COMBINED":
            return self._check_combined_rule(rule)
        return False, ""

    def _check_price_rule(self, rule: ManualPositionRule) -> tuple:
        """Check LTP vs target/stoploss/trailing for each symbol."""
        positions = self._get_positions(rule.user_id, rule.config_id)

        for symbol in rule.symbols:
            pos = positions.get(symbol)
            if not pos:
                continue

            ltp = float(pos.get("ltp") or pos.get("last_price") or 0)
            if ltp <= 0:
                continue

            net_qty = int(pos.get("net_qty") or pos.get("netqty") or pos.get("qty") or 0)
            if net_qty == 0:
                continue

            is_long = net_qty > 0

            # Target
            if rule.target and ltp > 0:
                if is_long and ltp >= rule.target:
                    return True, f"TARGET hit: {symbol} LTP={ltp} >= target={rule.target}"
                if not is_long and ltp <= rule.target:
                    return True, f"TARGET hit: {symbol} LTP={ltp} <= target={rule.target}"

            # Stop loss
            if rule.stoploss and ltp > 0:
                if is_long and ltp <= rule.stoploss:
                    return True, f"STOPLOSS hit: {symbol} LTP={ltp} <= SL={rule.stoploss}"
                if not is_long and ltp >= rule.stoploss:
                    return True, f"STOPLOSS hit: {symbol} LTP={ltp} >= SL={rule.stoploss}"

            # Trailing stop
            if rule.trailing and rule.trail_when and ltp > 0:
                if rule.trail_stop:
                    # Check trail
                    if is_long and ltp <= rule.trail_stop:
                        return True, f"TRAIL_STOP hit: {symbol} LTP={ltp} <= trail={rule.trail_stop}"
                    if not is_long and ltp >= rule.trail_stop:
                        return True, f"TRAIL_STOP hit: {symbol} LTP={ltp} >= trail={rule.trail_stop}"
                    # Ratchet trail
                    if is_long and ltp > (rule.trail_stop + rule.trailing):
                        rule.trail_stop = ltp - rule.trailing
                        self._update_rule_payload(rule)
                    elif not is_long and ltp < (rule.trail_stop - rule.trailing):
                        rule.trail_stop = ltp + rule.trailing
                        self._update_rule_payload(rule)
                elif (is_long and ltp >= rule.trail_when) or \
                     (not is_long and ltp <= rule.trail_when):
                    # Activate trailing
                    rule.trail_stop = (ltp - rule.trailing) if is_long else (ltp + rule.trailing)
                    self._update_rule_payload(rule)
                    logger.info("Trail activated: %s trail_stop=%.2f", symbol, rule.trail_stop)

        return False, ""

    def _check_greek_rule(self, rule: ManualPositionRule) -> tuple:
        """Check greek value vs threshold for option positions."""
        for symbol in rule.symbols:
            greek_val = self._get_greek_for_position(
                rule.user_id, rule.config_id, symbol, rule.greek_name or "delta",
            )
            if greek_val is None:
                continue

            threshold = rule.greek_threshold or 0
            op = rule.greek_op.upper()

            if op == "GT" and greek_val > threshold:
                return True, f"GREEK {rule.greek_name}={greek_val:.4f} > {threshold}"
            if op == "LT" and greek_val < threshold:
                return True, f"GREEK {rule.greek_name}={greek_val:.4f} < {threshold}"
            if op == "ABS_GT" and abs(greek_val) > abs(threshold):
                return True, f"GREEK |{rule.greek_name}|={abs(greek_val):.4f} > {abs(threshold)}"
            if op == "ABS_LT" and abs(greek_val) < abs(threshold):
                return True, f"GREEK |{rule.greek_name}|={abs(greek_val):.4f} < {abs(threshold)}"

        return False, ""

    def _check_combined_rule(self, rule: ManualPositionRule) -> tuple:
        """Check net greek across multiple symbols."""
        net_greek = 0.0
        for symbol in rule.symbols:
            val = self._get_greek_for_position(
                rule.user_id, rule.config_id, symbol, rule.greek_name or "delta",
            )
            if val is not None:
                net_greek += val

        threshold = rule.greek_threshold or 0
        op = rule.greek_op.upper()

        if op == "GT" and net_greek > threshold:
            return True, f"COMBINED net_{rule.greek_name}={net_greek:.4f} > {threshold}"
        if op == "LT" and net_greek < threshold:
            return True, f"COMBINED net_{rule.greek_name}={net_greek:.4f} < {threshold}"
        if op == "ABS_GT" and abs(net_greek) > abs(threshold):
            return True, f"COMBINED |net_{rule.greek_name}|={abs(net_greek):.4f} > {abs(threshold)}"
        if op == "ABS_LT" and abs(net_greek) < abs(threshold):
            return True, f"COMBINED |net_{rule.greek_name}|={abs(net_greek):.4f} < {abs(threshold)}"

        return False, ""

    # ── Rule execution ────────────────────────────────────────────────────

    def _execute_rule(self, rule: ManualPositionRule, reason: str):
        """Trigger exit for matched rule, deactivate it."""
        logger.warning("Rule triggered: %s — %s", rule.rule_id, reason)

        rule.active = False
        rule.triggered_at = datetime.now(timezone.utc).isoformat()
        rule.trigger_reason = reason

        # Deactivate in DB
        self.remove_rule(rule.rule_id)

        # Persist trigger info
        self._update_rule_payload(rule)

        # Fire exit callback
        if self._exit_callback:
            for symbol in rule.symbols:
                try:
                    pos = self._get_positions(rule.user_id, rule.config_id).get(symbol)
                    if not pos:
                        continue
                    net_qty = int(pos.get("net_qty") or pos.get("netqty") or pos.get("qty") or 0)
                    if net_qty == 0:
                        continue
                    exit_side = "SELL" if net_qty > 0 else "BUY"
                    self._exit_callback(
                        rule.user_id, rule.config_id, symbol,
                        rule.exchange, exit_side, abs(net_qty), reason,
                    )
                except Exception as e:
                    logger.error("Rule exit callback failed for %s: %s", symbol, e)

    # ── Data helpers ──────────────────────────────────────────────────────

    def _get_positions(self, user_id: str, config_id: str) -> Dict[str, dict]:
        """Fetch current positions from mgr_positions keyed by symbol."""
        from db.trading_db import get_trading_conn
        import psycopg2.extras
        conn = get_trading_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                "SELECT symbol, data FROM mgr_positions "
                "WHERE user_id = %s AND config_id = %s",
                (user_id, config_id),
            )
            return {r["symbol"]: dict(r["data"]) for r in cur.fetchall()}
        finally:
            conn.close()

    def _get_greek_for_position(
        self, user_id: str, config_id: str, symbol: str, greek: str,
    ) -> Optional[float]:
        """Lookup greek value for a position from option chain store."""
        try:
            from trading.market_data.option_chain.store import OptionChainStore
            # Access the global store instance through the trading bot
            # Greeks are enriched by the supervisor into the store
            # For now, return None if OC data not available
            return None  # TODO: wire to global OC store when bot instance is accessible
        except Exception:
            return None

    def _update_rule_payload(self, rule: ManualPositionRule):
        """Update the JSON payload of an existing rule in DB."""
        from db.trading_db import get_trading_conn
        import psycopg2.extras
        conn = get_trading_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                "UPDATE mgr_manual_rules SET payload = %s WHERE rule_id = %s",
                (psycopg2.extras.Json(rule.to_dict()), rule.rule_id),
            )
            conn.commit()
        except Exception as e:
            logger.debug("Failed to update rule payload %s: %s", rule.rule_id, e)
        finally:
            conn.close()

    # ── Schema ────────────────────────────────────────────────────────────

    @staticmethod
    def _ensure_schema():
        from db.trading_db import get_trading_conn
        conn = get_trading_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mgr_manual_rules (
                    rule_id     TEXT PRIMARY KEY,
                    user_id     TEXT NOT NULL,
                    config_id   TEXT NOT NULL,
                    rule_type   TEXT NOT NULL DEFAULT 'PRICE',
                    active      BOOLEAN NOT NULL DEFAULT TRUE,
                    payload     JSONB NOT NULL DEFAULT '{}',
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_manual_rules_user
                    ON mgr_manual_rules(user_id, active);
                CREATE INDEX IF NOT EXISTS idx_manual_rules_config
                    ON mgr_manual_rules(user_id, config_id, active);
            """)
            conn.commit()
        except Exception:
            conn.rollback()
        finally:
            conn.close()
