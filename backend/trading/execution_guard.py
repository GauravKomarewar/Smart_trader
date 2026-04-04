"""
Smart Trader — Execution Guard
================================

PRODUCTION-GRADE: Defensive execution layer.

Enforces:
  - Strategy isolation (no cross-contamination)
  - Duplicate ENTRY protection
  - Delta execution for ADJUST
  - Cross-strategy conflict prevention
  - EXIT always allowed (never blocked)
  - Thread-safe under concurrent strategy execution

Synopsis:
  guard = ExecutionGuard()
  allowed = guard.validate_and_prepare(intents, "ENTRY")
"""

import threading
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

logger = logging.getLogger("smart_trader.execution_guard")


@dataclass(frozen=True)
class LegIntent:
    strategy_id: str
    symbol:      str
    direction:   str   # BUY | SELL
    qty:         int
    tag:         str   # ENTRY | EXIT | ADJUST


@dataclass
class _Position:
    symbol:    str
    direction: str
    qty:       int


class ExecutionGuard:
    """
    Thread-safe execution guard.

    State:
      _strategy_positions:  strategy_id -> symbol -> _Position
      _global_positions:    symbol -> direction -> total qty
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._strategy_positions: Dict[str, Dict[str, _Position]] = {}
        self._global_positions:   Dict[str, Dict[str, int]] = {}

    # ── Public API ───────────────────────────────────────────────────────────

    def validate_and_prepare(
        self,
        intents: List[LegIntent],
        execution_type: str,  # ENTRY | ADJUST | EXIT
    ) -> List[LegIntent]:
        """
        Validate and return allowed intents.
        Raises RuntimeError to block execution.
        """
        if not intents:
            return []

        with self._lock:
            execution_type = execution_type.upper()

            if execution_type == "EXIT":
                return self._handle_exit(intents)

            self._validate_structure(intents)

            strategy = intents[0].strategy_id

            if execution_type == "ENTRY":
                if (strategy in self._strategy_positions
                        and self._strategy_positions[strategy]):
                    raise RuntimeError(
                        f"Duplicate ENTRY blocked for strategy '{strategy}'"
                    )

            self._check_cross_strategy_conflicts(intents)
            return self._handle_entry_or_adjust(intents)

    def on_fill(self, strategy_id: str, symbol: str, direction: str, qty: int):
        """Call this when broker confirms a fill."""
        with self._lock:
            strat_pos = self._strategy_positions.setdefault(strategy_id, {})
            if symbol in strat_pos:
                pos = strat_pos[symbol]
                new_qty = pos.qty + qty if direction == pos.direction else pos.qty - qty
                if new_qty <= 0:
                    del strat_pos[symbol]
                else:
                    strat_pos[symbol] = _Position(symbol, direction, new_qty)
            else:
                strat_pos[symbol] = _Position(symbol, direction, qty)

            # Global
            global_sym = self._global_positions.setdefault(symbol, {})
            global_sym[direction] = global_sym.get(direction, 0) + qty
            if global_sym[direction] <= 0:
                del global_sym[direction]

    def on_exit_fill(self, strategy_id: str, symbol: str, qty: int):
        """Call after exit fill to clean up position state."""
        with self._lock:
            strat_pos = self._strategy_positions.get(strategy_id, {})
            if symbol in strat_pos:
                pos = strat_pos[symbol]
                new_qty = pos.qty - qty
                if new_qty <= 0:
                    del strat_pos[symbol]
                else:
                    strat_pos[symbol] = _Position(symbol, pos.direction, new_qty)

                # global
                global_sym = self._global_positions.get(symbol, {})
                if pos.direction in global_sym:
                    global_sym[pos.direction] = max(0, global_sym[pos.direction] - qty)
                    if global_sym[pos.direction] == 0:
                        del global_sym[pos.direction]

    def clear_strategy(self, strategy_id: str):
        """Remove all state for a strategy (called after full exit)."""
        with self._lock:
            self._strategy_positions.pop(strategy_id, None)
            logger.info("ExecutionGuard: state cleared for strategy=%s", strategy_id)

    def get_strategy_positions(self, strategy_id: str) -> List[dict]:
        with self._lock:
            strat = self._strategy_positions.get(strategy_id, {})
            return [
                {"symbol": p.symbol, "direction": p.direction, "qty": p.qty}
                for p in strat.values()
            ]

    def has_open_position(self, strategy_id: str) -> bool:
        with self._lock:
            return bool(self._strategy_positions.get(strategy_id))

    # ── Internal helpers ────────────────────────────────────────────────────

    def _validate_structure(self, intents: List[LegIntent]):
        strategies = {i.strategy_id for i in intents}
        if len(strategies) > 1:
            raise RuntimeError("Mixed strategy_ids in a single batch")
        for i in intents:
            if i.direction not in ("BUY", "SELL"):
                raise RuntimeError(f"Invalid direction: {i.direction}")
            if i.qty <= 0:
                raise RuntimeError(f"Non-positive qty for {i.symbol}: {i.qty}")

    def _check_cross_strategy_conflicts(self, intents: List[LegIntent]):
        """Block if another strategy already holds an opposing position in same symbol."""
        incoming_strategy = intents[0].strategy_id
        for intent in intents:
            for sid, positions in self._strategy_positions.items():
                if sid == incoming_strategy:
                    continue
                if intent.symbol in positions:
                    other_pos = positions[intent.symbol]
                    if other_pos.direction != intent.direction:
                        raise RuntimeError(
                            f"Cross-strategy conflict: '{incoming_strategy}' wants "
                            f"{intent.direction} {intent.symbol} but '{sid}' holds "
                            f"{other_pos.direction}"
                        )

    def _handle_exit(self, intents: List[LegIntent]) -> List[LegIntent]:
        # EXIT is always allowed — guard doesn't block exits
        return intents

    def _handle_entry_or_adjust(self, intents: List[LegIntent]) -> List[LegIntent]:
        strategy = intents[0].strategy_id
        strat_pos = self._strategy_positions.setdefault(strategy, {})
        allowed = []

        for intent in intents:
            existing = strat_pos.get(intent.symbol)
            if existing and existing.direction == intent.direction:
                delta = intent.qty - existing.qty
                if delta <= 0:
                    logger.debug("Guard: %s already holds %d %s — no delta needed",
                                 strategy, existing.qty, intent.symbol)
                    continue
                # Only send the delta
                allowed.append(LegIntent(
                    strategy_id=intent.strategy_id,
                    symbol=intent.symbol,
                    direction=intent.direction,
                    qty=delta,
                    tag=intent.tag,
                ))
            else:
                allowed.append(intent)

        return allowed
