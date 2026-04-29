from typing import List, Dict, Any, Optional, Callable
import re
import time
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
        self._last_untagged_warn_at: float = 0.0
        self._empty_untagged_streak: int = 0
        self._untagged_leg_miss_streak: Dict[str, int] = {}
        self._tagged_zero_streak: Dict[str, int] = {}

    @staticmethod
    def _canon_symbol(value: Any) -> str:
        s = str(value or "").strip().upper()
        if ":" in s:
            s = s.split(":", 1)[1].strip()
        s = s.replace(" ", "").replace("-", "")
        return s

    @staticmethod
    def _parse_symbol_option_fields(sym: str) -> tuple[Optional[int], Optional[OptionType]]:
        s = str(sym or "").upper().strip()
        if not s:
            return None, None

        m = re.search(r"(\d{4,6})(CE|PE)$", s)
        if m:
            try:
                return int(m.group(1)), OptionType(m.group(2))
            except Exception:
                return None, None

        # Shoonya style: ...C24500 / ...P24000
        m = re.search(r"([CP])(\d{4,6})$", s)
        if m:
            try:
                strike = int(m.group(2))
                opt = OptionType.CE if m.group(1) == "C" else OptionType.PE
                return strike, opt
            except Exception:
                return None, None

        return None, None

    def _resolve_leg_net(
        self,
        leg: LegState,
        broker_netqty: Dict[str, int],
    ) -> Optional[int]:
        sym = self._canon_symbol(getattr(leg, "trading_symbol", None) or leg.symbol)
        if sym in broker_netqty:
            return broker_netqty[sym]

        leg_opt = leg.option_type if leg.instrument == InstrumentType.OPT else None
        leg_strike = int(leg.strike) if leg.instrument == InstrumentType.OPT and leg.strike is not None else None
        if leg_opt is None or leg_strike is None:
            return None

        underlying_hint = self._canon_symbol(leg.symbol)
        fallback_net = 0
        matched = False
        for broker_sym, net in broker_netqty.items():
            if underlying_hint and underlying_hint not in broker_sym:
                continue
            b_strike, b_opt = self._parse_symbol_option_fields(broker_sym)
            if b_strike is None or b_opt is None:
                continue
            if b_opt == leg_opt and b_strike == leg_strike:
                fallback_net += net
                matched = True

        if matched:
            return fallback_net
        return None

    def reconcile(self, broker_positions: List[Dict[str, Any]]) -> List[str]:
        """
        Reconcile state legs against a list of positions that have 'tag' fields.
        Use this for internal position snapshots.
        For live broker reconciliation, use reconcile_from_broker().
        """
        warnings = []
        broker_tags = {p.get("tag") for p in broker_positions if p.get("tag")}
        has_tagged_positions = bool(broker_tags)

        # Build symbol-level aggregate for untagged reconciliation and price sync.
        broker_netqty: Dict[str, int] = {}
        broker_avg_price: Dict[str, float] = {}
        broker_ltp: Dict[str, float] = {}
        for p in broker_positions:
            sym = self._canon_symbol(
                p.get("symbol") or p.get("tsym") or p.get("tradingsymbol")
            )
            if not sym:
                continue

            # Broker schemas differ; support common net quantity keys.
            net_raw = p.get("netqty")
            if net_raw is None:
                net_raw = p.get("net_qty")
            if net_raw is None:
                net_raw = p.get("quantity")
            if net_raw is None:
                net_raw = p.get("qty")
            try:
                net = int(float(net_raw or 0))
            except Exception:
                net = 0
            broker_netqty[sym] = broker_netqty.get(sym, 0) + net

            avg = (
                p.get("netavgprc")
                or p.get("avg_price")
                or p.get("upldprc")
                or p.get("daybuyavgprc")
                or p.get("daysellavgprc")
                or p.get("avgprc")
            )
            if avg is not None:
                try:
                    broker_avg_price[sym] = float(avg)
                except Exception:
                    pass

            ltp = p.get("ltp") or p.get("last_price") or p.get("lp")
            if ltp is not None:
                try:
                    broker_ltp[sym] = float(ltp)
                except Exception:
                    pass

        # For untagged snapshots, prefer symbol/netqty reconciliation. Keep a
        # throttled warning so operators know strict tag mode is not being used.
        if not has_tagged_positions:
            if broker_positions:
                warnings.append(
                    f"Untagged broker snapshot ({len(broker_positions)} rows) — using symbol/netqty reconciliation"
                )
            now_ts = time.time()
            if now_ts - self._last_untagged_warn_at >= 60:
                logger.warning(
                    "RECONCILE | Untagged broker snapshot (%d rows) — "
                    "using symbol/netqty reconciliation",
                    len(broker_positions),
                )
                self._last_untagged_warn_at = now_ts

            if broker_positions:
                self._empty_untagged_streak = 0
            else:
                self._empty_untagged_streak += 1

        for tag, leg in self.state.legs.items():
            if not leg.is_active:
                continue

            # Tagged path: legacy strict behavior.
            if has_tagged_positions:
                if tag not in broker_tags:
                    warnings.append(f"Leg {tag} is active in state but missing in broker")
                    leg.close()
                continue

            # Untagged path: reconcile by trading symbol and net quantity.
            sym = self._canon_symbol(getattr(leg, "trading_symbol", None) or leg.symbol)
            if not sym:
                continue

            # Empty untagged snapshots can be transient API/session glitches.
            # Require 2 consecutive empty reads before closing active legs.
            if not broker_positions:
                if self._empty_untagged_streak >= 2:
                    warnings.append(
                        f"Leg {tag} ({sym}) active in state but broker returned empty positions twice"
                    )
                    logger.warning(
                        "RECONCILE | %s (%s) | closing after consecutive empty untagged snapshots",
                        tag,
                        sym,
                    )
                    leg.close()
                continue

            net = self._resolve_leg_net(leg, broker_netqty)
            if net is None or net == 0:
                streak = self._untagged_leg_miss_streak.get(tag, 0) + 1
                self._untagged_leg_miss_streak[tag] = streak
                if streak < 3:
                    warnings.append(
                        f"Leg {tag} ({sym}) unresolved/flat in untagged broker snapshot (streak={streak})"
                    )
                    continue
                warnings.append(
                    f"Leg {tag} ({sym}) unresolved/flat in untagged broker snapshot for {streak} ticks — closing"
                )
                leg.close()
                continue
            self._untagged_leg_miss_streak[tag] = 0

            # Sync side/qty and pricing from broker so strategy PnL aligns.
            broker_side = Side.BUY if net > 0 else Side.SELL
            if broker_side != leg.side:
                warnings.append(
                    f"Leg {tag} ({sym}) side mismatch state={leg.side.value} broker={broker_side.value}"
                )

            broker_qty = abs(net)
            broker_lots = self._contracts_to_lots(broker_qty, leg.expiry)
            if leg.qty != broker_lots:
                logger.info(
                    "RECONCILE | %s (%s) | lots state=%s -> broker=%s",
                    tag,
                    sym,
                    leg.qty,
                    broker_lots,
                )
                leg.qty = broker_lots

            avg = broker_avg_price.get(sym)
            if avg is not None and avg > 0 and abs(avg - leg.entry_price) > 0.01:
                logger.info(
                    "RECONCILE | %s (%s) | entry_price sync %.2f -> %.2f",
                    tag,
                    sym,
                    leg.entry_price,
                    avg,
                )
                leg.entry_price = avg

            ltp = broker_ltp.get(sym)
            if ltp is not None and ltp > 0:
                leg.ltp = ltp

        for pos in broker_positions:
            tag = pos.get("tag")
            if tag and tag not in self.state.legs:
                warnings.append(f"Broker has extra position {tag} not in state")
                self._reconstruct_leg(pos)

        # Untagged resume recovery path: if broker rows are untagged but clearly
        # map to an existing inactive leg, revive it so monitoring can continue.
        if not has_tagged_positions:
            for tag, leg in self.state.legs.items():
                if leg.is_active:
                    continue
                net = self._resolve_leg_net(leg, broker_netqty)
                if not net:
                    continue
                broker_qty = abs(net)
                broker_lots = self._contracts_to_lots(broker_qty, leg.expiry)
                if broker_lots > 0:
                    leg.qty = broker_lots
                leg.side = Side.BUY if net > 0 else Side.SELL
                leg.is_active = True
                leg.exit_timestamp = None
                leg.exit_price = None
                self._untagged_leg_miss_streak[tag] = 0
                warnings.append(
                    f"Recovered leg {tag} from untagged broker snapshot (netqty={net})"
                )

        # Consolidate tagged rows before applying updates. Some brokers emit
        # multiple rows for a single instrument where one row can be net=0;
        # processing rows one-by-one can incorrectly re-close recovered legs.
        tagged_agg: Dict[str, Dict[str, Any]] = {}
        for pos in broker_positions:
            tag = pos.get("tag")
            if not tag or tag not in self.state.legs:
                continue

            bucket = tagged_agg.setdefault(tag, {"net": 0, "ltp": None, "avg": None, "delta": None})

            net_raw = pos.get("netqty")
            if net_raw is None:
                net_raw = pos.get("net_qty")
            if net_raw is None:
                net_raw = pos.get("qty")
            if net_raw is None:
                net_raw = pos.get("quantity")
            try:
                bucket["net"] += int(float(net_raw or 0))
            except Exception:
                pass

            ltp_v = pos.get("ltp") or pos.get("last_price") or pos.get("lp")
            if ltp_v is not None:
                try:
                    bucket["ltp"] = float(ltp_v)
                except Exception:
                    pass

            avg_v = (
                pos.get("netavgprc")
                or pos.get("avg_price")
                or pos.get("upldprc")
                or pos.get("daybuyavgprc")
                or pos.get("daysellavgprc")
                or pos.get("avgprc")
            )
            if avg_v is not None:
                try:
                    avg_f = float(avg_v)
                    if avg_f > 0:
                        bucket["avg"] = avg_f
                except Exception:
                    pass

            if "delta" in pos:
                try:
                    bucket["delta"] = float(pos["delta"])
                except Exception:
                    pass

        for tag, agg in tagged_agg.items():
            leg = self.state.legs[tag]
            if agg.get("ltp") is not None:
                leg.ltp = float(agg["ltp"])
            if agg.get("delta") is not None:
                leg.delta = float(agg["delta"])

            net = int(agg.get("net") or 0)
            if net == 0:
                streak = self._tagged_zero_streak.get(tag, 0) + 1
                self._tagged_zero_streak[tag] = streak
                if streak < 3:
                    warnings.append(
                        f"Leg {tag} tagged snapshot net=0 (streak={streak}) — retaining state"
                    )
                    continue
                if leg.is_active:
                    warnings.append(
                        f"Leg {tag} tagged snapshot net=0 for {streak} ticks — closing"
                    )
                    leg.close()
                continue
            self._tagged_zero_streak[tag] = 0

            broker_qty = abs(net)
            broker_lots = self._contracts_to_lots(broker_qty, leg.expiry)
            if broker_lots > 0:
                leg.qty = broker_lots

            broker_side = Side.BUY if net > 0 else Side.SELL
            if leg.side != broker_side:
                leg.side = broker_side

            if not leg.is_active:
                # Resume-safe recovery: if broker still has this tagged leg
                # open, reactivate it in state so monitoring can continue.
                leg.is_active = True
                leg.exit_timestamp = None
                leg.exit_price = None
                warnings.append(
                    f"Recovered leg {tag} from broker snapshot (netqty={net})"
                )

            avg_final = agg.get("avg")
            if avg_final is not None:
                leg.entry_price = float(avg_final)

        # Final guard: if broker still reports tagged non-zero nets but state
        # ended with zero active legs, revive those tagged legs. This protects
        # monitor continuity against transient close/reopen races.
        if has_tagged_positions and not any(l.is_active for l in self.state.legs.values()):
            repaired = 0
            for tag, agg in tagged_agg.items():
                net = int(agg.get("net") or 0)
                if net == 0 or tag not in self.state.legs:
                    continue
                leg = self.state.legs[tag]
                leg.is_active = True
                leg.exit_timestamp = None
                leg.exit_price = None
                leg.side = Side.BUY if net > 0 else Side.SELL
                lots = self._contracts_to_lots(abs(net), leg.expiry)
                if lots > 0:
                    leg.qty = lots
                repaired += 1
            if repaired:
                warnings.append(
                    f"Invariant repair: revived {repaired} tagged leg(s) from broker netqty"
                )

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
