"""
PositionSLManager — Background engine that monitors per-position SL/Target/Trail
and fires broker exit orders when thresholds are hit.

Design:
  • Polls `position_sl_settings` table every 2s for active settings.
  • For each active setting, reads current LTP from `mgr_positions` (live DB data).
  • Compares LTP vs stop_loss / target / trailing stop.
  • On threshold breach: places exit order via MultiAccountRegistry, deactivates setting.
  • Trailing stop: updates trail_stop as price moves favorably.

Thread-safety: DB reads and writes are wrapped in their own connections.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Optional

logger = logging.getLogger("smart_trader.pos_sl")

_POLL_INTERVAL   = 2.0   # seconds between checks
_TRAIL_BUF_PCT   = 0.001  # trail_stop updated only when price moves >0.1% beyond activation


class PositionSLManager:
    """Singleton background engine for per-position SL/TG/trailing."""

    def __init__(self):
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._missing_counts: dict[tuple[str, str, str], int] = {}

    def start(self):
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop, name="pos_sl_mgr", daemon=True
        )
        self._thread.start()
        logger.info("PositionSLManager started (interval=%.1fs)", _POLL_INTERVAL)

    def stop(self):
        self._running = False
        self._stop_event.set()

    # ── Main loop ─────────────────────────────────────────────────────────────

    def _loop(self):
        while not self._stop_event.is_set():
            t0 = time.monotonic()
            try:
                self._check_all()
            except Exception as e:
                logger.error("PositionSLManager loop error: %s", e, exc_info=True)
            elapsed = time.monotonic() - t0
            self._stop_event.wait(timeout=max(0, _POLL_INTERVAL - elapsed))

    def _check_all(self):
        from db.trading_db import get_trading_conn
        try:
            conn = get_trading_conn()
        except Exception as e:
            logger.debug("DB connect failed: %s", e)
            return
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT user_id, config_id, pos_key, stop_loss, target,
                       trailing_value, trail_when, trail_stop, initial_ltp, base_stop_loss
                FROM position_sl_settings
                WHERE active = TRUE
            """)
            rows = cur.fetchall()
        finally:
            conn.close()

        for row in rows:
            try:
                self._check_row(row)
            except Exception as e:
                logger.debug("Error checking SL row %s: %s", row, e)

    def _check_row(self, row):
        (user_id, config_id, pos_key, stop_loss, target,
         trailing_value, trail_when, trail_stop, initial_ltp, base_stop_loss) = row

        position = self._get_position_snapshot(user_id, config_id, pos_key)
        state_key = (user_id, config_id, pos_key)
        if position is None or position["ltp"] is None or position["qty"] == 0:
            misses = self._missing_counts.get(state_key, 0) + 1
            self._missing_counts[state_key] = misses
            if misses >= 3:
                logger.info(
                    "Deactivating managed position %s/%s %s — position no longer present",
                    config_id[:8], user_id[:8], pos_key,
                )
                self._deactivate(user_id, config_id, pos_key)
            return

        self._missing_counts.pop(state_key, None)
        ltp = position["ltp"]
        side = position["side"]
        qty = position["qty"]
        exchange = position["exchange"]
        product = position["product"]

        is_long = side == "BUY"

        # ── Step-based trailing stop (Shoonya-style POINTS mode) ──────────
        if trailing_value and trailing_value > 0 and initial_ltp and initial_ltp > 0:
            step_trigger = float(trail_when) if trail_when and trail_when > 0 else float(trailing_value)
            step_move = float(trailing_value)
            anchor = float(initial_ltp)
            bsl = float(base_stop_loss) if base_stop_loss is not None else (float(stop_loss) if stop_loss else None)

            if bsl is not None and step_trigger > 0 and step_move > 0:
                favorable_move = (ltp - anchor) if is_long else (anchor - ltp)
                favorable_move = max(0.0, favorable_move)
                steps = int(favorable_move // step_trigger)

                if steps > 0:
                    if is_long:
                        new_sl = bsl + (steps * step_move)
                        if stop_loss is None or new_sl > stop_loss:
                            stop_loss = new_sl
                            trail_stop = new_sl
                            self._save_sl_and_trail(user_id, config_id, pos_key, stop_loss, trail_stop)
                    else:
                        new_sl = bsl - (steps * step_move)
                        if stop_loss is None or new_sl < stop_loss:
                            stop_loss = new_sl
                            trail_stop = new_sl
                            self._save_sl_and_trail(user_id, config_id, pos_key, stop_loss, trail_stop)

        # ── Check exit conditions ─────────────────────────────────────────
        exit_reason = None
        if is_long:
            if stop_loss and ltp <= stop_loss:
                exit_reason = f"SL hit {ltp:.2f} ≤ {stop_loss:.2f}"
            elif target and ltp >= target:
                exit_reason = f"Target hit {ltp:.2f} ≥ {target:.2f}"
        else:  # SELL short
            if stop_loss and ltp >= stop_loss:
                exit_reason = f"SL hit {ltp:.2f} ≥ {stop_loss:.2f}"
            elif target and ltp <= target:
                exit_reason = f"Target hit {ltp:.2f} ≤ {target:.2f}"

        if exit_reason:
            logger.info("EXIT TRIGGERED: %s/%s %s — %s", config_id[:8], pos_key, side, exit_reason)
            self._fire_exit(user_id, config_id, pos_key, exchange, product, side, qty, exit_reason)
            self._deactivate(user_id, config_id, pos_key)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _get_position_snapshot(self, user_id: str, config_id: str, pos_key: str) -> Optional[dict]:
        """Return the latest position snapshot for a managed `pos_key`, if present."""
        from db.trading_db import get_trading_conn
        try:
            symbol, product = (pos_key.split("|") + [""])[:2]
            conn = get_trading_conn()
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT data, exchange
                    FROM mgr_positions
                    WHERE user_id = %s AND config_id = %s AND symbol = %s
                    ORDER BY fetched_at DESC
                    """,
                    (user_id, config_id, symbol),
                )
                rows = cur.fetchall()
            finally:
                conn.close()

            for data, exchange in rows:
                if not data:
                    continue
                p = data if isinstance(data, dict) else None
                if not p:
                    continue
                psym = str(p.get("tradingsymbol") or p.get("symbol") or "")
                pprd = str(p.get("product") or "")
                if psym != symbol or pprd != product:
                    continue

                qty_raw = float(
                    p.get("netQty")
                    or p.get("netqty")
                    or p.get("qty")
                    or 0
                )
                side = str(p.get("side") or ("SELL" if qty_raw < 0 else "BUY")).upper()
                return {
                    "ltp": float(p.get("ltp") or p.get("lastPrice") or p.get("lp") or 0),
                    "side": side,
                    "qty": int(abs(qty_raw)),
                    "avg_price": float(p.get("avgPrice") or p.get("averagePrice") or p.get("avg_price") or 0),
                    "exchange": str(p.get("exchange") or p.get("exch") or exchange or "NSE"),
                    "product": pprd,
                }
        except Exception as e:
            logger.debug("_get_position_snapshot error: %s", e)
        return None

    def _save_sl_and_trail(self, user_id: str, config_id: str, pos_key: str,
                           stop_loss: float, trail_stop=None):
        """Update stop_loss (and optionally trail_stop) in DB."""
        from db.trading_db import get_trading_conn
        try:
            conn = get_trading_conn()
            try:
                cur = conn.cursor()
                cur.execute(
                    """UPDATE position_sl_settings
                       SET stop_loss = %s, trail_stop = %s, updated_at = NOW()
                       WHERE user_id = %s AND config_id = %s AND pos_key = %s""",
                    (stop_loss, trail_stop, user_id, config_id, pos_key),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            logger.debug("_save_sl_and_trail error: %s", e)

    def _save_trail_stop(self, user_id: str, config_id: str, pos_key: str, trail_stop: float):
        from db.trading_db import get_trading_conn
        try:
            conn = get_trading_conn()
            try:
                cur = conn.cursor()
                cur.execute(
                    """UPDATE position_sl_settings
                       SET trail_stop = %s, updated_at = NOW()
                       WHERE user_id = %s AND config_id = %s AND pos_key = %s""",
                    (trail_stop, user_id, config_id, pos_key),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            logger.debug("_save_trail_stop error: %s", e)

    def _deactivate(self, user_id: str, config_id: str, pos_key: str):
        from db.trading_db import get_trading_conn
        self._missing_counts.pop((user_id, config_id, pos_key), None)
        try:
            conn = get_trading_conn()
            try:
                cur = conn.cursor()
                cur.execute(
                    """UPDATE position_sl_settings
                       SET active = FALSE, updated_at = NOW()
                       WHERE user_id = %s AND config_id = %s AND pos_key = %s""",
                    (user_id, config_id, pos_key),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            logger.debug("_deactivate error: %s", e)

    def _fire_exit(
        self,
        user_id: str,
        config_id: str,
        pos_key: str,
        exchange: str,
        product: str,
        side: str,
        qty: int,
        reason: str,
    ):
        """Place a market exit order to close the position."""
        try:
            symbol, _ = (pos_key.split("|") + [""])[:2]
            from broker.multi_broker import registry
            exit_side = "SELL" if side == "BUY" else "BUY"
            order = {
                "symbol":            symbol,
                "exchange":          exchange,
                "side":              exit_side,
                "transaction_type":  "S" if exit_side == "SELL" else "B",
                "product":           product,
                "order_type":        "MARKET",
                "quantity":          qty,
                "price":             0,
                "tag":               f"SL-MGR:{reason[:30]}",
            }

            result = registry.execute_order(user_id, config_id, order)
            if result.get("success"):
                logger.info("Exit order placed: %s %d %s — %s", symbol, qty, exit_side, reason)
            else:
                logger.error("Exit order FAILED: %s — %s", symbol, result.get("message"))
        except Exception as e:
            logger.error("_fire_exit error: %s", e, exc_info=True)

    # ── Public API for saving settings ────────────────────────────────────────

    @staticmethod
    def save_settings(
        user_id: str,
        config_id: str,
        pos_key: str,
        active: bool = True,
        stop_loss: Optional[float] = None,
        target: Optional[float] = None,
        trailing_value: Optional[float] = None,
        trail_when: Optional[float] = None,
        initial_ltp: Optional[float] = None,
        base_stop_loss: Optional[float] = None,
    ) -> dict:
        """Upsert SL settings for a position. Preserves trail state on update."""
        from db.trading_db import get_trading_conn
        conn = get_trading_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO position_sl_settings
                    (user_id, config_id, pos_key, active, stop_loss, target,
                     trailing_value, trail_when, trail_stop, initial_ltp, base_stop_loss, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NULL, %s, %s, NOW())
                ON CONFLICT (user_id, config_id, pos_key)
                DO UPDATE SET
                    active          = EXCLUDED.active,
                    stop_loss       = EXCLUDED.stop_loss,
                    target          = EXCLUDED.target,
                    trailing_value  = EXCLUDED.trailing_value,
                    trail_when      = EXCLUDED.trail_when,
                    initial_ltp     = COALESCE(EXCLUDED.initial_ltp, position_sl_settings.initial_ltp),
                    base_stop_loss  = COALESCE(EXCLUDED.base_stop_loss, position_sl_settings.base_stop_loss),
                    updated_at      = NOW()
            """, (user_id, config_id, pos_key, active, stop_loss, target,
                  trailing_value, trail_when, initial_ltp, base_stop_loss))
            conn.commit()
            return {
                "user_id": user_id, "config_id": config_id, "pos_key": pos_key,
                "active": active, "stop_loss": stop_loss, "target": target,
                "trailing_value": trailing_value, "trail_when": trail_when,
                "initial_ltp": initial_ltp, "base_stop_loss": base_stop_loss,
            }
        finally:
            conn.close()

    @staticmethod
    def get_settings(user_id: str) -> list:
        """Return all SL settings for a user."""
        from db.trading_db import get_trading_conn
        conn = get_trading_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT user_id, config_id, pos_key, active, stop_loss, target,
                       trailing_value, trail_when, trail_stop, initial_ltp,
                       base_stop_loss, updated_at
                FROM position_sl_settings
                WHERE user_id = %s
                ORDER BY updated_at DESC
            """, (user_id,))
            rows = cur.fetchall()
            keys = ["user_id", "config_id", "pos_key", "active", "stop_loss", "target",
                    "trailing_value", "trail_when", "trail_stop", "initial_ltp",
                    "base_stop_loss", "updated_at"]
            return [dict(zip(keys, r)) for r in rows]
        finally:
            conn.close()


# Module-level singleton
position_sl_manager = PositionSLManager()
