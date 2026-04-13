from typing import List, Dict, Any, Optional, Callable
import re
from .state import StrategyState, LegState
from .models import InstrumentType, OptionType, Side
import logging

logger = logging.getLogger(__name__)


class BrokerReconciliation:
    def __init__(
        self,
        state: StrategyState,
        lot_size_resolver: Optional[Callable[[Optional[str]], int]] = None,
    ):
        self.state = state
        self._lot_size_resolver = lot_size_resolver

    def reconcile(self, broker_positions: List[Dict[str, Any]]) -> List[str]:
        """
        Reconcile state legs against a list of positions that have 'tag' fields.
        Use this for internal position snapshots.
        For live broker reconciliation, use reconcile_from_broker().
        """
        warnings = []
        broker_tags = {p.get("tag") for p in broker_positions if p.get("tag")}
        has_tagged_positions = bool(broker_tags)

        # Do not perform destructive tag-based close logic on broker snapshots
        # that do not carry strategy leg tags (common for OMS/broker position APIs).
        if broker_positions and not has_tagged_positions:
            warnings.append(
                "Broker positions are untagged; skipping strict tag reconciliation to avoid false closes"
            )
            logger.warning(
                "RECONCILE | Untagged broker positions snapshot (%d rows) — "
                "skip tag-based state close",
                len(broker_positions),
            )

        for tag, leg in self.state.legs.items():
            if has_tagged_positions and leg.is_active and tag not in broker_tags:
                warnings.append(f"Leg {tag} is active in state but missing in broker")
                leg.close()

        for pos in broker_positions:
            tag = pos.get("tag")
            if tag and tag not in self.state.legs:
                warnings.append(f"Broker has extra position {tag} not in state")
                self._reconstruct_leg(pos)

        for pos in broker_positions:
            tag = pos.get("tag")
            if tag and tag in self.state.legs:
                leg = self.state.legs[tag]
                leg.ltp = pos.get("ltp", leg.ltp)
                if "delta" in pos:
                    leg.delta = pos["delta"]
                if "qty" in pos:
                    leg.qty = pos["qty"]
                    if leg.qty == 0:
                        leg.close()

        return warnings

    def reconcile_from_broker(self, broker_view) -> List[str]:
        """
        Reconcile strategy state against live broker positions.

        Args:
            broker_view: BrokerView instance (bot.broker_view)

        Returns:
            List of warning strings describing mismatches found.
        """
        warnings: List[str] = []

        try:
            broker_positions = broker_view.get_positions(force_refresh=True) or []
        except Exception as e:
            warnings.append(f"RECONCILE: Failed to fetch broker positions: {e}")
            logger.error(f"BrokerReconciliation.reconcile_from_broker: fetch failed: {e}")
            return warnings

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # BUG-11 FIX: Guard against session-failure returning empty positions.
        # If broker returns ZERO positions but we believe we have active legs,
        # this is almost certainly a stale/failed API response — skip
        # reconciliation entirely instead of wiping all legs inactive.
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        active_leg_count = sum(1 for leg in self.state.legs.values() if leg.is_active)
        if not broker_positions and active_leg_count > 0:
            msg = (
                f"RECONCILE: Broker returned EMPTY positions but state has "
                f"{active_leg_count} active leg(s) — skipping reconciliation "
                f"(possible session/API issue)"
            )
            warnings.append(msg)
            logger.warning(msg)
            return warnings

        broker_netqty: Dict[str, int] = {}
        # ✅ BUG-003 FIX: Also collect avg entry price from broker for sync
        broker_avg_price: Dict[str, float] = {}
        for pos in broker_positions:
            tsym = pos.get("tsym", "")
            net = int(pos.get("netqty", 0))
            if tsym:
                broker_netqty[tsym] = broker_netqty.get(tsym, 0) + net
                # Capture average entry price if available
                avg = pos.get("netavgprc") or pos.get("upldprc") or pos.get("daybuyavgprc") or pos.get("daysellavgprc")
                if avg:
                    try:
                        broker_avg_price[tsym] = float(avg)
                    except (ValueError, TypeError):
                        pass

        # ✅ BUG-003 FIX: Attempt to fetch LTP from broker quotes for active legs
        active_symbols = [
            getattr(leg, "trading_symbol", None) or leg.symbol
            for leg in self.state.legs.values()
            if leg.is_active
        ]
        broker_ltp: Dict[str, float] = {}
        try:
            if hasattr(broker_view, 'get_quotes') and active_symbols:
                quotes = broker_view.get_quotes(active_symbols)
                if isinstance(quotes, dict):
                    for sym, q in quotes.items():
                        if isinstance(q, dict) and 'ltp' in q:
                            try:
                                broker_ltp[sym] = float(q['ltp'])
                            except (ValueError, TypeError):
                                pass
        except Exception as e:
            logger.debug("RECONCILE | Could not fetch broker quotes for LTP: %s", e)

        for tag, leg in list(self.state.legs.items()):
            if not leg.is_active:
                continue

            sym = getattr(leg, "trading_symbol", None) or leg.symbol
            if not sym:
                warnings.append(f"Leg {tag}: no symbol - cannot reconcile")
                continue

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # BUG-13 FIX: If trading_symbol was never populated, sym may
            # be just the underlying name (e.g. "NIFTY") which won't match
            # the broker's full tsym like "NIFTY10MAR26C24850".
            # Detect this and skip reconciliation for the leg rather than
            # falsely concluding it's flat.
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            if not re.search(r'\d', sym):
                # Symbol has no digits → likely bare underlying, not full trading symbol
                warnings.append(
                    f"Leg {tag}: symbol '{sym}' looks like underlying (no strike/expiry) "
                    f"— skipping reconciliation for this leg"
                )
                logger.warning(
                    "RECONCILE | %s | symbol '%s' has no digits — "
                    "trading_symbol may not be populated, skipping",
                    tag, sym,
                )
                continue

            net = broker_netqty.get(sym, 0)

            # ✅ BUG-003 FIX: Update LTP from broker quotes if available
            if sym in broker_ltp and broker_ltp[sym] > 0:
                leg.ltp = broker_ltp[sym]

            # ✅ BUG-003 FIX: Sync average entry price from broker if available
            if sym in broker_avg_price and broker_avg_price[sym] > 0:
                broker_entry = broker_avg_price[sym]
                if abs(broker_entry - leg.entry_price) > 0.01:
                    logger.info(
                        "RECONCILE | %s (%s) | avg_price sync state=%.2f -> broker=%.2f",
                        tag, sym, leg.entry_price, broker_entry,
                    )
                    leg.entry_price = broker_entry

            if net == 0:
                warnings.append(
                    f"Leg {tag} ({sym}): state=ACTIVE but broker netqty=0 -> marking inactive"
                )
                logger.warning(f"RECONCILE | {tag} ({sym}) | state says active but broker is flat")
                leg.close()
            else:
                # ✅ BUG-003 FIX: Verify side matches broker direction
                broker_side = Side.BUY if net > 0 else Side.SELL
                if broker_side != leg.side:
                    warnings.append(
                        f"Leg {tag} ({sym}): side mismatch — state={leg.side.value} "
                        f"but broker netqty={net} implies {broker_side.value}"
                    )
                    logger.warning(
                        "RECONCILE | %s (%s) | SIDE MISMATCH state=%s broker=%s",
                        tag, sym, leg.side.value, broker_side.value,
                    )

                broker_qty = abs(net)
                broker_lots = self._contracts_to_lots(broker_qty, leg.expiry)
                if leg.qty != broker_lots:
                    warnings.append(
                        f"Leg {tag} ({sym}): state lots={leg.qty} but broker lots={broker_lots} (netqty={broker_qty}) -> syncing"
                    )
                    logger.info(
                        f"RECONCILE | {tag} ({sym}) | lots state={leg.qty} -> broker_lots={broker_lots} (netqty={broker_qty})"
                    )
                    leg.qty = broker_lots

        tracked_symbols = {
            (getattr(leg, "trading_symbol", None) or leg.symbol)
            for leg in self.state.legs.values()
            if leg.is_active
        }
        for sym, net in broker_netqty.items():
            if net != 0 and sym not in tracked_symbols:
                warnings.append(
                    f"Broker has untracked position: {sym} netqty={net} "
                    f"(use /strategy/recover-resume to adopt it)"
                )
                logger.warning(f"RECONCILE | UNTRACKED BROKER POSITION | {sym} netqty={net}")

        if warnings:
            logger.warning(f"RECONCILE WARNINGS ({len(warnings)}): " + "; ".join(warnings))
        else:
            logger.debug("RECONCILE: state and broker are in sync")

        return warnings

    def _contracts_to_lots(self, broker_qty: int, expiry: Optional[str]) -> int:
        if broker_qty <= 0:
            return 0

        lot_size = 1
        if self._lot_size_resolver:
            try:
                resolved = int(self._lot_size_resolver(expiry))
                if resolved > 0:
                    lot_size = resolved
            except Exception:
                lot_size = 1

        if lot_size <= 1:
            return broker_qty

        lots = broker_qty // lot_size
        if lots == 0:
            return 1

        if broker_qty % lot_size != 0:
            logger.warning(
                "RECONCILE | broker qty %s not multiple of lot_size %s (expiry=%s), flooring to %s lots",
                broker_qty,
                lot_size,
                expiry,
                lots,
            )
        return lots

    def _reconstruct_leg(self, pos: Dict[str, Any]):
        leg = LegState(
            tag=pos.get("tag", "UNKNOWN"),
            symbol=pos.get("symbol", "UNKNOWN"),
            instrument=InstrumentType(pos.get("instrument", "OPT")),
            option_type=OptionType(pos.get("option_type")) if pos.get("option_type") else None,
            strike=pos.get("strike"),
            expiry=pos.get("expiry", "UNKNOWN"),
            side=Side(pos.get("side", "BUY")),
            qty=pos.get("qty", 0),
            entry_price=pos.get("entry_price", 0.0),
            ltp=pos.get("ltp", 0.0),
            group=pos.get("group", ""),
            label=pos.get("label", ""),
            oi=pos.get("oi", 0),
            oi_change=pos.get("oi_change", 0),
            volume=pos.get("volume", 0),
        )
        self.state.legs[leg.tag] = leg
