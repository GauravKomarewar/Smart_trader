"""
Smart Trader — OrderWatcherEngine
===================================

Production-grade execution engine ported from shoonya_platform.

STEPS:
  Step 6: Poll broker for EXECUTED truth (SENT_TO_BROKER → EXECUTED/FAILED)
  Step 7: Dispatch CREATED EXIT orders to broker
  Step 8: Monitor managed exits (SL / Target / Trailing)

Invariants:
  - Broker is the ONLY source of EXECUTED truth
  - EXIT execution is idempotent and persistent
  - OrderWatcher executes intents, NEVER decides them
  - ExecutionGuard reconciled ONLY from broker truth

Multi-tenant: each bot instance has its own watcher (user_id + client_id scoped).
"""

import time
import logging
import threading
from datetime import datetime, time as dtime
from typing import Dict, List, Optional, Set

from trading.execution.intent import UniversalOrderCommand
from trading.persistence.order_record import OrderRecord
from trading.persistence.repository import OrderRepository

logger = logging.getLogger("smart_trader.order_watcher")


class OrderWatcherEngine(threading.Thread):
    """
    OrderWatcherEngine — Steps 6, 7, 8 of the execution pipeline.

    One instance per active TradingBot (= per active broker session per user).
    """

    def __init__(self, bot, poll_interval: float = 2.0):
        super().__init__(daemon=True, name=f"OrderWatcher:{bot.client_id}")
        self.bot = bot
        self.poll_interval = poll_interval
        self._running = True
        self.repo = OrderRepository(bot.user_id, bot.client_id)

        # Managed EXIT state: command_id → state dict
        self._managed_exits: Dict[str, dict] = {}
        self._managed_lock = threading.Lock()

        # Prevent re-dispatch within session (idempotency)
        self._dispatched_exits: Set[str] = set()
        self._dispatch_lock = threading.Lock()

        # Failure log dedup
        self._failure_log: Dict[tuple, float] = {}
        self._failure_ttl = 60.0
        self._ob_fail_count = 0

    def stop(self):
        self._running = False

    # ── Thread lifecycle ────────────────────────────────────────────────────

    def run(self):
        logger.info("OrderWatcherEngine STARTED | user_id=%s client=%s",
                    self.bot.user_id, self.bot.client_id)

        while self._running:
            try:
                self._reconcile_broker_orders()   # Step 6
                self._dispatch_pending_exits()     # Step 7
                self._monitor_managed_exits()      # Step 8
            except RuntimeError:
                raise
            except Exception:
                logger.exception("OrderWatcher loop error")
            time.sleep(self.poll_interval)

        logger.info("OrderWatcherEngine STOPPED | client=%s", self.bot.client_id)

    # ── STEP 6: Reconcile with broker ──────────────────────────────────────

    def _reconcile_broker_orders(self):
        """Poll broker order book and update DB to EXECUTED/FAILED."""
        try:
            broker_orders = self.bot.api.get_order_book() or []
        except Exception as e:
            self._ob_fail_count += 1
            if self._ob_fail_count <= 3 or self._ob_fail_count % 20 == 0:
                logger.error("OrderWatcher: get_order_book() failed (%d): %s",
                             self._ob_fail_count, e)
            return

        self._ob_fail_count = 0
        for bo in broker_orders:
            # Adapters may return Order dataclass objects — convert to dict
            if hasattr(bo, "to_dict"):
                bo = bo.to_dict()
            elif not isinstance(bo, dict):
                continue
            bid = bo.get("norenordno") or bo.get("order_id") or bo.get("orderid")
            status = (bo.get("status") or bo.get("order_status") or "").upper()

            if not bid or not status:
                continue

            record = self.repo.get_by_broker_id(bid)
            if not record:
                continue

            if record.status in ("EXECUTED", "FAILED"):
                continue

            # 6A: Broker failure
            if status in ("REJECTED", "CANCELLED", "EXPIRED"):
                self.repo.update_status(record.command_id, "FAILED")
                self.repo.update_tag(record.command_id, f"BROKER_{status}")
                self._clear_guard(record)
                logger.warning("BROKER_FAILURE | cmd_id=%s broker_id=%s status=%s",
                               record.command_id, bid, status)
                continue

            # 6B: Broker fill (definitive truth)
            if status in ("COMPLETE", "FILLED", "TRADED"):
                self.repo.update_status(record.command_id, "EXECUTED")
                logger.info("BROKER_EXECUTED | cmd_id=%s broker_id=%s", record.command_id, bid)
                try:
                    self._notify_fill(record, bo)
                except Exception:
                    logger.exception("on_fill callback failed | cmd_id=%s", record.command_id)
                self._reconcile_guard(record.strategy_name, record.symbol)

    # ── STEP 7: Dispatch pending EXIT orders ────────────────────────────────

    def _dispatch_pending_exits(self):
        """Pick up CREATED EXIT orders from DB and execute them."""
        today = datetime.now().strftime("%Y-%m-%d")

        with self._dispatch_lock:
            try:
                open_orders = self.repo.get_open_orders()
            except Exception as e:
                logger.warning("OrderWatcher: get_open_orders failed: %s", e)
                return

            for record in open_orders:
                if record.status != "CREATED":
                    continue
                if (record.execution_type or "").upper() != "EXIT":
                    continue

                # Skip stale orders from previous days
                created_day = str(record.created_at or "")[:10]
                if created_day < today:
                    logger.warning("SKIPPING stale EXIT | cmd_id=%s created=%s sym=%s",
                                   record.command_id, created_day, record.symbol)
                    continue

                has_sl = (record.stop_loss or 0) > 0
                has_target = (record.target or 0) > 0
                has_trail = (
                    record.trailing_type and record.trailing_type != "NONE"
                    and (record.trailing_value or 0) > 0
                )

                if has_sl or has_target or has_trail:
                    with self._managed_lock:
                        if record.command_id not in self._managed_exits:
                            self._register_managed_exit(record)
                else:
                    if record.command_id not in self._dispatched_exits:
                        success = self._dispatch_simple_exit(record)
                        if success:
                            self._dispatched_exits.add(record.command_id)

    def _dispatch_simple_exit(self, record: OrderRecord) -> bool:
        """Dispatch a simple MARKET EXIT immediately. Returns True on success."""
        try:
            self.repo.update_tag(record.command_id, "DISPATCHING")
            cmd = UniversalOrderCommand.from_record(
                record, order_type="MARKET", price=0.0, source="ORDER_WATCHER"
            )
            logger.info("DISPATCH_EXIT | cmd_id=%s | %s %s qty=%s",
                        record.command_id, record.symbol, record.side, record.quantity)
            result = self.bot.execute_command(command=cmd)
            if result and result.success:
                return True
            err = getattr(result, "error_message", "no result") if result else "no result"
            logger.error("EXIT_FAILED | cmd_id=%s | %s | err=%s",
                         record.command_id, record.symbol, err)
            return False
        except Exception:
            logger.exception("EXIT_EXCEPTION | cmd_id=%s | %s",
                             record.command_id, record.symbol)
            return False

    def _register_managed_exit(self, record: OrderRecord):
        """Register an EXIT order with SL/target/trailing for monitoring."""
        self.repo.update_tag(record.command_id, "MANAGED_EXIT")

        initial_ltp = record.managed_anchor_ltp
        if initial_ltp is None:
            initial_ltp = self._lookup_ltp(record.symbol, record.product)

        base_sl = record.managed_base_stop_loss or record.stop_loss
        self._managed_exits[record.command_id] = {
            "record":               record,
            "stop_loss":            record.stop_loss,
            "base_stop_loss":       base_sl,
            "target":               record.target,
            "trailing_type":        record.trailing_type,
            "trailing_value":       record.trailing_value,
            "trail_when":           record.trail_when,
            "initial_ltp":          initial_ltp,
            "activation_price":     None,
            "trailing_activated":   False,
            "highest_price":        None,
            "lowest_price":         None,
            "registered_at":        time.time(),
            "_dispatch_failures":   0,
            "_exit_next_retry":     0,
        }
        logger.warning(
            "MANAGED_EXIT_REGISTERED | cmd_id=%s | %s side=%s qty=%s | SL=%s target=%s trail=%s/%s",
            record.command_id, record.symbol, record.side, record.quantity,
            record.stop_loss, record.target, record.trailing_type, record.trailing_value,
        )

    # ── STEP 8: Monitor managed exits ──────────────────────────────────────

    def _monitor_managed_exits(self):
        """Check SL/target levels and trail SL for managed exits."""
        with self._managed_lock:
            if not self._managed_exits:
                return
            snapshot = [(cid, dict(st)) for cid, st in self._managed_exits.items()]

        try:
            positions = self.bot.api.get_positions() or []
        except Exception as e:
            logger.warning("OrderWatcher: positions fetch failed: %s", e)
            return

        pos_map: Dict[str, dict] = {}
        for p in positions:
            sym = p.get("tsym") or p.get("symbol")
            if sym:
                pos_map[sym] = p

        to_remove: List[str] = []
        updated: Dict[str, dict] = {}

        for cid, state in snapshot:
            record: OrderRecord = state["record"]

            # Verify order is still CREATED in DB
            try:
                cur = self.repo.get_by_id(cid)
                if not cur or cur.status != "CREATED":
                    to_remove.append(cid)
                    continue
            except Exception:
                continue

            if not self._is_exchange_open(record.exchange, record.symbol):
                updated[cid] = state
                continue

            if time.time() < state.get("_exit_next_retry", 0):
                updated[cid] = state
                continue

            pos = pos_map.get(record.symbol)
            net_qty = int((pos or {}).get("netqty") or (pos or {}).get("net_qty") or 0)

            if net_qty == 0:
                logger.info("MANAGED_EXIT_FLAT | cmd_id=%s %s", cid, record.symbol)
                self.repo.update_status(cid, "FAILED")
                self.repo.update_tag(cid, "POSITION_ALREADY_FLAT")
                to_remove.append(cid)
                continue

            ltp = float((pos or {}).get("ltp") or (pos or {}).get("lp") or 0)
            if ltp <= 0:
                updated[cid] = state
                continue

            is_long = net_qty > 0
            sl = state.get("stop_loss")
            target = state.get("target")
            trailing_type = state.get("trailing_type")
            trailing_value = state.get("trailing_value")
            trigger_exit = False
            trigger_reason = ""

            # ── Trailing SL ──
            if trailing_type and (trailing_value or 0) > 0:
                state = self._update_trailing(state, ltp, is_long)
                sl = state.get("stop_loss")

            # ── Check SL hit ──
            if sl is not None:
                if is_long and ltp <= sl:
                    trigger_exit = True
                    trigger_reason = f"SL_HIT ltp={ltp} sl={sl}"
                elif not is_long and ltp >= sl:
                    trigger_exit = True
                    trigger_reason = f"SL_HIT ltp={ltp} sl={sl}"

            # ── Check target hit ──
            if target is not None and not trigger_exit:
                if is_long and ltp >= target:
                    trigger_exit = True
                    trigger_reason = f"TARGET_HIT ltp={ltp} target={target}"
                elif not is_long and ltp <= target:
                    trigger_exit = True
                    trigger_reason = f"TARGET_HIT ltp={ltp} target={target}"

            if trigger_exit:
                logger.warning("MANAGED_TRIGGER | cmd_id=%s %s %s qty=%d",
                               cid, record.symbol, trigger_reason, abs(net_qty))
                ok = self._execute_managed_exit(record, abs(net_qty))
                if ok:
                    to_remove.append(cid)
                else:
                    fc = state.get("_dispatch_failures", 0) + 1
                    state["_dispatch_failures"] = fc
                    if fc >= 5:
                        self.repo.update_status(cid, "FAILED")
                        self.repo.update_tag(cid, "BROKER_UNAVAILABLE_GAVE_UP")
                        to_remove.append(cid)
                    else:
                        delay = min(30 * (2 ** (fc - 1)), 240)
                        state["_exit_next_retry"] = time.time() + delay
                        updated[cid] = state
            else:
                updated[cid] = state

        # Merge state back
        with self._managed_lock:
            for cid, st in updated.items():
                if cid in self._managed_exits:
                    self._managed_exits[cid].update(st)
            for cid in to_remove:
                self._managed_exits.pop(cid, None)

    def _update_trailing(self, state: dict, ltp: float, is_long: bool) -> dict:
        """Update trailing SL state, returns (possibly modified) state dict."""
        record: OrderRecord = state["record"]
        trailing_type = state.get("trailing_type", "")
        trailing_value = float(state.get("trailing_value") or 0)
        sl = state.get("stop_loss")
        trail_when = state.get("trail_when")

        if state.get("initial_ltp") is None:
            state["initial_ltp"] = ltp

        # Activate trailing
        if not state.get("trailing_activated"):
            if trail_when and trail_when > 0:
                init = float(state.get("initial_ltp") or ltp)
                act = init + trail_when if is_long else init - trail_when
                state["activation_price"] = act
                if (is_long and ltp >= act) or (not is_long and ltp <= act):
                    state["trailing_activated"] = True
            else:
                state["trailing_activated"] = True

        if not state.get("trailing_activated"):
            return state

        base_sl = state.get("base_stop_loss")
        init = float(state.get("initial_ltp") or ltp)

        if trailing_type == "POINTS":
            if base_sl is None and sl is not None:
                base_sl = float(sl)
                state["base_stop_loss"] = base_sl
            step_trigger = float(trail_when) if trail_when and trail_when > 0 else trailing_value
            step_move = trailing_value
            if base_sl is not None and init > 0 and step_trigger > 0:
                fav = max(0.0, (ltp - init) if is_long else (init - ltp))
                steps = int(fav // step_trigger)
                if steps > 0:
                    new_sl = float(base_sl) + steps * step_move if is_long else float(base_sl) - steps * step_move
                    if sl is None or (is_long and new_sl > sl) or (not is_long and new_sl < sl):
                        state["stop_loss"] = new_sl
                        try:
                            self.repo.update_risk_fields(record.command_id, {
                                "stop_loss": new_sl,
                                "managed_anchor_ltp": init,
                                "managed_base_stop_loss": base_sl,
                            })
                        except Exception:
                            pass

        elif trailing_type == "PERCENT":
            if is_long:
                hp = state.get("highest_price")
                if hp is None or ltp > hp:
                    state["highest_price"] = ltp
                new_sl = state["highest_price"] * (1 - trailing_value / 100)
                if sl is None or new_sl > sl:
                    state["stop_loss"] = new_sl
            else:
                lp = state.get("lowest_price")
                if lp is None or ltp < lp:
                    state["lowest_price"] = ltp
                new_sl = state["lowest_price"] * (1 + trailing_value / 100)
                if sl is None or new_sl < sl:
                    state["stop_loss"] = new_sl

        elif trailing_type == "ABSOLUTE":
            # Fixed SL — no trailing
            pass

        return state

    def _execute_managed_exit(self, record: OrderRecord, qty: int) -> bool:
        """Execute a managed exit via a new MARKET order. Returns True on success."""
        try:
            import uuid as _uuid
            eid = f"MANAGED_EXIT_{record.symbol}_{int(time.time()*1000)}_{_uuid.uuid4().hex[:6]}"
            now = datetime.utcnow().isoformat()

            exit_rec = OrderRecord(
                command_id      = eid,
                source          = "ORDER_WATCHER",
                broker_user     = record.broker_user,
                strategy_name   = record.strategy_name,
                exchange        = record.exchange,
                symbol          = record.symbol,
                side            = record.side,
                quantity        = qty,
                product         = record.product,
                order_type      = "MARKET",
                price           = 0.0,
                stop_loss       = None,
                target          = None,
                trailing_type   = None,
                trailing_value  = None,
                broker_order_id = None,
                execution_type  = "EXIT",
                status          = "CREATED",
                created_at      = now,
                updated_at      = now,
                tag             = "MANAGED_EXIT_MARKET",
            )
            self.repo.create(exit_rec)

            cmd = UniversalOrderCommand.from_record(
                exit_rec, order_type="MARKET", price=0.0, source="ORDER_WATCHER"
            )
            result = self.bot.execute_command(command=cmd)
            if result and result.success:
                self.repo.update_status(record.command_id, "FAILED")
                self.repo.update_tag(record.command_id, "MANAGED_EXIT_TRIGGERED")
                return True
            err = getattr(result, "error_message", "no result") if result else "no result"
            logger.error("MANAGED_EXIT_BROKER_FAIL | cmd_id=%s err=%s", eid, err)
            return False
        except Exception:
            logger.exception("MANAGED_EXIT_EXCEPTION | %s", record.command_id)
            return False

    # ── Helpers ────────────────────────────────────────────────────────────

    def _lookup_ltp(self, symbol: str, product: str) -> Optional[float]:
        try:
            positions = self.bot.api.get_positions() or []
            for p in positions:
                if p.get("tsym") == symbol or p.get("symbol") == symbol:
                    pp = p.get("prd") or p.get("product")
                    if product and pp and pp != product:
                        continue
                    return float(p.get("ltp") or p.get("lp") or 0) or None
        except Exception:
            pass
        return None

    def _clear_guard(self, record: OrderRecord):
        try:
            self.bot.execution_guard.force_clear_symbol(
                strategy_id=record.strategy_name,
                symbol=record.symbol,
            )
        except Exception:
            pass

    def _reconcile_guard(self, strategy_name: str, executed_symbol: str):
        try:
            broker_positions = self.bot.api.get_positions() or []
            pos_map = {}
            for p in broker_positions:
                sym = p.get("tsym") or p.get("symbol")
                qty = int(p.get("netqty") or p.get("net_qty") or 0)
                if sym and qty != 0:
                    pos_map[sym] = qty
            self.bot.execution_guard.reconcile_with_broker(
                strategy_id=strategy_name,
                broker_positions=pos_map,
            )
        except Exception:
            pass

    def _notify_fill(self, record: OrderRecord, broker_order: dict):
        """Notify the bot/strategy of a fill."""
        try:
            sym = broker_order.get("tsym") or broker_order.get("symbol") or record.symbol
            qty = int(broker_order.get("fillshares") or broker_order.get("qty") or 0)
            price = float(broker_order.get("avgprc") or broker_order.get("flprc") or 0)
            raw_side = (broker_order.get("trantype") or broker_order.get("side") or record.side or "").upper()
            side = {"B": "BUY", "S": "SELL"}.get(raw_side, raw_side)

            if hasattr(self.bot, "notify_fill"):
                self.bot.notify_fill(symbol=sym, side=side, qty=qty, price=price,
                                     strategy_name=record.strategy_name)
        except Exception:
            logger.exception("_notify_fill failed")

    # Exchange hours guard
    _MCX_AGRI = frozenset({
        "GOLDPETAL", "SILVERMIC", "CASTORSEED", "CARDAMOM",
        "MENTHAOIL", "COTTON", "KAPAS", "CORIANDER", "JEERASEED"
    })
    _EXCHANGE_HOURS = {
        "MCX":     (dtime(9, 0),  dtime(23, 30)),
        "MCX_AGRI":(dtime(9, 0),  dtime(17, 0)),
        "NFO":     (dtime(9, 15), dtime(15, 30)),
        "BFO":     (dtime(9, 15), dtime(15, 30)),
        "NSE":     (dtime(9, 15), dtime(15, 30)),
        "BSE":     (dtime(9, 15), dtime(15, 30)),
    }

    def _is_exchange_open(self, exchange: str, symbol: str) -> bool:
        try:
            exch = (exchange or "").upper()
            if exch == "MCX":
                root = (symbol or "").upper()
                if any(root.startswith(a) for a in self._MCX_AGRI):
                    exch = "MCX_AGRI"
            hrs = self._EXCHANGE_HOURS.get(exch)
            if not hrs:
                return True
            now = dtime(datetime.now().hour, datetime.now().minute, datetime.now().second)
            return hrs[0] <= now <= hrs[1]
        except Exception:
            return True
