"""
Smart Trader — Recovery Bootstrap Service  (v2)
=================================================

Runs on bot startup only.
1. Fetches broker truth (positions + orders)
2. Reconciles DB: fixes SENT_TO_BROKER → EXECUTED/FAILED
3. Expires stale CREATED orders from previous days
4. Detects active strategies from DB
5. Validates each open DB position against broker truth
6. Classifies strategy runtime state (IDLE/ACTIVE/UNBALANCED/FAILED)
7. Rebuilds ExecutionGuard state for open legs
8. Saves audit trail to logs/recovery/

v2: Enhanced with Shoonya Platform recovery patterns — strategy state
detection, stale order expiry, and richer reconciliation.
"""

import json
import logging
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from trading.persistence.repository import OrderRepository

logger = logging.getLogger("smart_trader.recovery")


# ── Strategy runtime state classification ─────────────────────────────────────

class StrategyRuntimeState:
    """Enum-like for strategy states detected during recovery."""
    IDLE            = "IDLE"            # No open positions
    ACTIVE          = "ACTIVE"          # Positions match expected legs
    UNBALANCED      = "UNBALANCED"      # Some legs closed externally
    FAILED          = "FAILED"          # Unexpected position count
    STALE_ORDERS    = "STALE_ORDERS"    # Has orders from previous days


class RecoveryBootstrap:
    """One-shot startup recovery. Does NOT place any orders."""

    def __init__(self, bot):
        self.bot  = bot
        self.repo = OrderRepository(bot.user_id, bot.client_id)
        self._log = {
            "timestamp":       datetime.now(timezone.utc).isoformat(),
            "user_id":         bot.user_id,
            "client_id":       bot.client_id,
            "broker_positions": [],
            "reconciled":      [],
            "stale_expired":   0,
            "strategy_states": {},
            "recovered":       [],
            "skipped":         [],
            "errors":          [],
        }

    def run(self):
        logger.warning("Recovery Bootstrap START | client=%s", self.bot.client_id)

        # 1. Fetch broker truth
        try:
            broker_positions = self.bot.api.get_positions() or []
            broker_orders    = self.bot.api.get_order_book() or []
        except Exception as e:
            logger.warning("Recovery skipped — broker unreachable: %s", e)
            return

        # Build broker position map {symbol: {qty, side, product, avg_price}}
        broker_pos_map: Dict[str, Dict] = {}
        for p in broker_positions:
            # Support both dict (legacy) and Position dataclass (adapter)
            if isinstance(p, dict):
                symbol = p.get("tsym") or p.get("symbol", "")
                try:
                    netqty = int(p.get("netqty", 0) or 0)
                except Exception:
                    netqty = 0
            else:
                symbol = getattr(p, "symbol", "")
                try:
                    netqty = int(getattr(p, "net_qty", 0) or 0)
                except Exception:
                    netqty = 0
            if netqty == 0:
                continue
            if isinstance(p, dict):
                broker_pos_map[symbol] = {
                    "exchange":  p.get("exch", ""),
                    "qty":       abs(netqty),
                    "side":      "BUY" if netqty > 0 else "SELL",
                    "product":   p.get("prd", ""),
                    "avg_price": float(p.get("avgprc", 0) or 0),
                }
            else:
                broker_pos_map[symbol] = {
                    "exchange":  getattr(p, "exchange", ""),
                    "qty":       abs(netqty),
                    "side":      "BUY" if netqty > 0 else "SELL",
                    "product":   getattr(p, "product", ""),
                    "avg_price": float(getattr(p, "avg_price", 0) or 0),
                }
        self._log["broker_positions"] = list(broker_pos_map.keys())
        logger.info("Broker truth: %d open positions", len(broker_pos_map))

        # 2. Reconcile DB orders against broker order book
        for o in broker_orders:
            # Support both dict (legacy) and Order dataclass (adapter)
            if isinstance(o, dict):
                order_id = o.get("norenordno") or o.get("orderid", "")
                status   = (o.get("status") or "").upper()
            else:
                order_id = getattr(o, "order_id", "") or ""
                status   = (getattr(o, "status", "") or "").upper()
            if not order_id:
                continue
            db_rec = self.repo.get_by_broker_id(order_id)
            if not db_rec:
                continue
            try:
                if status == "COMPLETE" and db_rec.status != "EXECUTED":
                    self.repo.update_status_by_broker_id(order_id, "EXECUTED")
                    self._log["reconciled"].append({"order_id": order_id, "new_status": "EXECUTED"})
                elif status in ("CANCELLED", "REJECTED") and db_rec.status != "FAILED":
                    self.repo.update_status_by_broker_id(order_id, "FAILED")
                    self._log["reconciled"].append({"order_id": order_id, "new_status": "FAILED"})
            except Exception:
                logger.exception("Reconcile failed for broker_order=%s", order_id)

        # 2b. Expire stale CREATED/SENT_TO_BROKER orders from previous days
        stale_expired = self._expire_stale_orders()
        if stale_expired:
            self._log["stale_expired"] = stale_expired
            logger.info("Expired %d stale orders from previous days", stale_expired)

        # 3. Detect strategies
        strategies = self._detect_strategies()
        if not strategies:
            logger.info("No strategies to recover — clean start")
            self._save_audit()
            return

        # 4. Recover each strategy with state classification
        for strat in strategies:
            try:
                state = self._recover_strategy(strat, broker_pos_map)
                self._log.setdefault("strategy_states", {})[strat] = state
            except Exception as e:
                logger.exception("Recovery error for strategy=%s", strat)
                self._log["errors"].append({"strategy": strat, "error": str(e)})

        logger.warning(
            "Recovery COMPLETE | strategies=%d | recovered=%d | skipped=%d | errors=%d",
            len(strategies),
            len(self._log["recovered"]),
            len(self._log["skipped"]),
            len(self._log["errors"]),
        )
        self._save_audit()

    # -------------------------------------------------------------------------

    def _detect_strategies(self) -> List[str]:
        rows = self.repo.get_open_orders()
        seen = set()
        result = []
        for r in rows:
            name = r.strategy_name or ""
            if name and name not in seen:
                seen.add(name)
                result.append(name)
        return result

    def _expire_stale_orders(self) -> int:
        """
        Expire CREATED / SENT_TO_BROKER orders from previous days.
        Prevents stale exit orders from firing on restart.
        Ported from shoonya_platform's daily boundary fix.
        """
        today_str = date.today().isoformat()
        expired_count = 0
        try:
            all_open = self.repo.get_open_orders() or []
            for rec in all_open:
                if rec.status not in ("CREATED", "SENT_TO_BROKER"):
                    continue
                created_date = ""
                if hasattr(rec, "created_at") and rec.created_at:
                    try:
                        if isinstance(rec.created_at, datetime):
                            created_date = rec.created_at.date().isoformat()
                        else:
                            created_date = str(rec.created_at)[:10]
                    except Exception:
                        pass
                if created_date and created_date < today_str:
                    try:
                        self.repo.update_status(rec.command_id, "EXPIRED")
                        expired_count += 1
                        logger.info("Expired stale order %s (%s) from %s",
                                    rec.command_id, rec.status, created_date)
                    except Exception as e:
                        logger.debug("Failed to expire %s: %s", rec.command_id, e)
        except Exception as e:
            logger.warning("_expire_stale_orders failed: %s", e)
        return expired_count

    def _recover_strategy(self, strategy_name: str, broker_pos_map: Dict) -> str:
        """
        Recover a strategy and classify its runtime state.
        Returns StrategyRuntimeState value.
        """
        db_legs = self.repo.get_open_positions_by_strategy(strategy_name)
        if not db_legs:
            logger.info("Strategy %s has no DB legs — state=IDLE", strategy_name)
            return StrategyRuntimeState.IDLE

        logger.info("Recovering strategy=%s | legs=%d", strategy_name, len(db_legs))

        matched = 0
        mismatched = 0
        missing = 0

        for leg in db_legs:
            symbol = leg.get("symbol", "")
            if symbol not in broker_pos_map:
                logger.warning("SKIP: %s/%s — not in broker positions", strategy_name, symbol)
                self._log["skipped"].append({
                    "strategy": strategy_name, "symbol": symbol, "reason": "not_in_broker"
                })
                missing += 1
                continue

            bp = broker_pos_map[symbol]
            if leg.get("side") != bp["side"]:
                logger.error("SKIP: %s/%s side mismatch DB=%s Broker=%s",
                             strategy_name, symbol, leg.get("side"), bp["side"])
                self._log["skipped"].append({
                    "strategy": strategy_name, "symbol": symbol, "reason": "side_mismatch"
                })
                mismatched += 1
                continue

            # Rebuild ExecutionGuard if available
            if hasattr(self.bot, "execution_guard") and self.bot.execution_guard:
                try:
                    self.bot.execution_guard.mark_open(
                        symbol=symbol,
                        side=bp["side"],
                        qty=bp["qty"],
                        strategy_name=strategy_name,
                    )
                except Exception:
                    pass

            self._log["recovered"].append({
                "strategy": strategy_name, "symbol": symbol,
                "side": bp["side"], "qty": bp["qty"],
            })
            logger.info("RECOVERED: %s | %s | %s x%d",
                        strategy_name, symbol, bp["side"], bp["qty"])
            matched += 1

        # Classify strategy state
        total_legs = len(db_legs)
        if matched == total_legs:
            state = StrategyRuntimeState.ACTIVE
        elif matched == 0:
            state = StrategyRuntimeState.IDLE
        elif missing > 0 and matched > 0:
            state = StrategyRuntimeState.UNBALANCED
        elif mismatched > 0:
            state = StrategyRuntimeState.FAILED
        else:
            state = StrategyRuntimeState.UNBALANCED

        logger.info(
            "Strategy %s state=%s (matched=%d missing=%d mismatch=%d total=%d)",
            strategy_name, state, matched, missing, mismatched, total_legs,
        )
        return state

    def _save_audit(self):
        try:
            log_dir = Path("logs/recovery")
            log_dir.mkdir(parents=True, exist_ok=True)
            fname = log_dir / f"recovery_{self.bot.client_id}_{int(time.time())}.json"
            with open(fname, "w") as f:
                json.dump(self._log, f, indent=2)
            logger.info("Recovery audit saved: %s", fname)
        except Exception:
            logger.exception("Failed to save recovery audit")


# ---------------------------------------------------------------------------
# OrphanPositionManager
# ---------------------------------------------------------------------------

class OrphanPositionManager:
    """
    Detects broker positions that have NO matching DB record.
    Reports them but does NOT exit automatically (safety-first).
    """

    def __init__(self, bot):
        self.bot  = bot
        self.repo = OrderRepository(bot.user_id, bot.client_id)

    def find_orphans(self) -> List[Dict]:
        """Return list of broker positions with no corresponding EXECUTED DB order."""
        try:
            broker_positions = self.bot.api.get_positions() or []
        except Exception as e:
            logger.warning("OrphanManager: get_positions() failed: %s", e)
            return []

        orphans = []
        for p in broker_positions:
            symbol = p.get("tsym") or p.get("symbol", "")
            try:
                netqty = int(p.get("netqty", 0) or 0)
            except Exception:
                netqty = 0
            if netqty == 0:
                continue

            # Check if any EXECUTED order in DB matches this symbol
            db_open = self.repo.get_open_orders()
            symbols_in_db = {r.symbol for r in db_open}
            if symbol not in symbols_in_db:
                orphans.append({
                    "symbol":  symbol,
                    "qty":     abs(netqty),
                    "side":    "BUY" if netqty > 0 else "SELL",
                    "product": p.get("prd", ""),
                    "ltp":     float(p.get("lp", 0) or p.get("ltp", 0) or 0),
                })

        if orphans:
            logger.warning("ORPHAN POSITIONS DETECTED: %d | %s",
                           len(orphans), [o["symbol"] for o in orphans])
        return orphans
