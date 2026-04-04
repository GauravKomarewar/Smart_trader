"""
Smart Trader — PositionExitService
====================================

Ported from shoonya_platform/execution/position_exit_service.py

Routes EXIT decisions to OrderWatcher via the DB.
- Never submits directly to broker
- Creates EXIT OrderRecords in DB
- OrderWatcher picks them up and executes

Scope types: ALL | STRATEGY | SYMBOL
"""

import uuid
import logging
from datetime import datetime, timezone
from typing import List, Optional

from trading.persistence.order_record import OrderRecord
from trading.persistence.repository import OrderRepository

logger = logging.getLogger("smart_trader.position_exit_service")


class PositionExitService:
    """
    Route all exit requests through the database to OrderWatcherEngine.

    EXIT FLOW:
      RMS / Strategy → PositionExitService.exit_positions()
        → Create EXIT records in DB
        → OrderWatcher picks up CREATED EXIT orders
        → Broker execution + status reconciliation
    """

    def __init__(self, broker_client, order_repo: OrderRepository, user_id: int, client_id: str):
        self.broker_client = broker_client
        self.order_repo = order_repo
        self.user_id = user_id
        self.client_id = client_id

    def exit_positions(
        self,
        *,
        scope: str,                       # ALL | STRATEGY | SYMBOL
        strategy_name: Optional[str] = None,
        symbols: Optional[List[str]] = None,
        product_scope: str = "ALL",
        reason: str = "EXIT",
        source: str = "SYSTEM",
        stop_loss: Optional[float] = None,
        target: Optional[float] = None,
        trailing_type: Optional[str] = None,
        trailing_value: Optional[float] = None,
        trail_when: Optional[float] = None,
    ):
        """
        Create EXIT order records in DB for all matching open positions.
        OrderWatcher will execute them.
        """
        try:
            positions = self._get_matching_positions(
                scope=scope,
                strategy_name=strategy_name,
                symbols=symbols,
                product_scope=product_scope,
            )
        except Exception as e:
            logger.exception("PositionExitService: failed to fetch positions: %s", e)
            return

        if not positions:
            logger.info("PositionExitService: no positions to exit | scope=%s reason=%s", scope, reason)
            return

        for pos in positions:
            self._create_exit_record(
                pos,
                reason=reason,
                source=source,
                stop_loss=stop_loss,
                target=target,
                trailing_type=trailing_type,
                trailing_value=trailing_value,
                trail_when=trail_when,
            )

    def _get_matching_positions(
        self,
        scope: str,
        strategy_name: Optional[str],
        symbols: Optional[List[str]],
        product_scope: str,
    ) -> list:
        """Get broker positions matching the exit scope."""
        try:
            broker_positions = self.broker_client.get_positions() or []
        except Exception as e:
            logger.exception("PositionExitService: broker get_positions failed: %s", e)
            # Fall back to DB open orders
            broker_positions = []

        results = []
        for pos in broker_positions:
            sym = pos.get("tsym") or pos.get("symbol") or ""
            product = pos.get("prd") or pos.get("product") or ""
            net_qty = int(pos.get("netqty") or pos.get("net_qty") or pos.get("qty") or 0)

            if net_qty == 0:
                continue

            if scope == "STRATEGY" and strategy_name:
                # Check DB to see if this symbol belongs to strategy
                open_orders = self.order_repo.get_open_orders_by_strategy(strategy_name)
                sym_in_strategy = any(o.symbol == sym for o in open_orders)
                if not sym_in_strategy:
                    continue

            if scope == "SYMBOL" and symbols:
                if sym not in symbols:
                    continue

            if product_scope != "ALL" and product and product != product_scope:
                continue

            results.append(pos)

        return results

    def _create_exit_record(self, pos: dict, *, reason: str, source: str,
                             stop_loss=None, target=None,
                             trailing_type=None, trailing_value=None, trail_when=None):
        """Create a CREATED EXIT record in DB for OrderWatcher to execute."""
        sym = pos.get("tsym") or pos.get("symbol") or ""
        net_qty = int(pos.get("netqty") or pos.get("net_qty") or pos.get("qty") or 0)
        s = pos.get("side") or ("SELL" if net_qty > 0 else "BUY")
        exit_side = "SELL" if s == "BUY" else "BUY"
        product = pos.get("prd") or pos.get("product") or "MIS"
        exchange = pos.get("exch") or pos.get("exchange") or "NFO"
        ltp = float(pos.get("ltp") or pos.get("lp") or 0)

        # Try to find the strategy from DB
        strategy = self._find_strategy_for_symbol(sym)

        record = OrderRecord(
            command_id      = str(uuid.uuid4()),
            source          = source,
            broker_user     = self.client_id,
            strategy_name   = strategy or "EXIT",
            exchange        = exchange,
            symbol          = sym,
            side            = exit_side,
            quantity        = abs(net_qty),
            product         = product,
            order_type      = "MARKET",
            price           = 0.0,
            stop_loss       = stop_loss,
            target          = target,
            trailing_type   = trailing_type,
            trailing_value  = trailing_value,
            trail_when      = trail_when,
            managed_anchor_ltp = ltp if ltp > 0 else None,
            broker_order_id = None,
            execution_type  = "EXIT",
            status          = "CREATED",
            created_at      = datetime.now(timezone.utc).isoformat(),
            updated_at      = datetime.now(timezone.utc).isoformat(),
            tag             = f"EXIT|{reason}",
        )

        try:
            self.order_repo.create(record)
            logger.info(
                "EXIT_RECORD_CREATED | sym=%s qty=%s side=%s reason=%s",
                sym, abs(net_qty), exit_side, reason,
            )
        except Exception:
            logger.exception("PositionExitService: create EXIT record failed for %s", sym)

    def _find_strategy_for_symbol(self, symbol: str) -> Optional[str]:
        """Look up strategy name for a symbol from open orders."""
        try:
            open_orders = self.order_repo.get_open_orders()
            for o in open_orders:
                if o.symbol == symbol:
                    return o.strategy_name
        except Exception:
            pass
        return None
