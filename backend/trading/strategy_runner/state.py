from dataclasses import dataclass, field
from typing import Dict, Optional, List, Any
from datetime import datetime, date
from .models import InstrumentType, OptionType, Side


@dataclass
class PnLSnapshot:
    """Point-in-time PnL snapshot for historical tracking."""
    timestamp: datetime
    pnl: float
    pnl_pct: float
    ltp: float
    underlying_price: float


@dataclass
class AdjustmentEvent:
    """Record of one adjustment applied to the strategy."""
    timestamp: datetime
    rule_name: str
    action_type: str
    affected_legs: List[str]
    reason: str
    market_data_snapshot: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LegState:
    tag: str
    symbol: str
    instrument: InstrumentType
    option_type: Optional[OptionType]   # None for FUT
    strike: Optional[float]             # None for FUT
    expiry: str                          # YYYY-MM-DD or special code
    side: Side
    qty: int                              # current lots (can be reduced)
    entry_price: float
    ltp: float = 0.0
    delta: float = 0.0
    gamma: float = 0.0
    theta: float = 0.0
    vega: float = 0.0
    iv: float = 0.0
    is_active: bool = True

    bid: float = 0.0
    ask: float = 0.0
    bid_qty: int = 0
    ask_qty: int = 0
    prev_oi: int = 0

    # New fields from gap analysis
    group: str = ""               # for group exits
    label: str = ""                # user label
    oi: int = 0
    oi_change: int = 0
    volume: int = 0
    bid_ask_spread: float = 0.0   # latest bid-ask spread (filled by market data feed)
    trading_symbol: str = ""      # ✅ BUG-002 FIX: resolved broker tradingsymbol (e.g. "NIFTY25FEB26C22000CE")

    order_id: Optional[str] = None          # broker order ID
    command_id: Optional[str] = None        # command ID from intent
    order_status: str = "PENDING"           # PENDING, FILLED, FAILED, CANCELLED
    filled_qty: int = 0                      # filled quantity in contracts (or lots)
    order_placed_at: Optional[datetime] = None
    lot_size: int = 1                        # contract lot size (e.g. 75 for NIFTY, 25 for BANKNIFTY, 10 for CRUDEOILM)

    # ✅ BUG-001 FIX: PnL and history tracking per leg
    pnl_history: List[PnLSnapshot] = field(default_factory=list)
    entry_reason: str = ""
    entry_timestamp: Optional[datetime] = None
    exit_timestamp: Optional[datetime] = None
    exit_reason: str = ""
    exit_price: Optional[float] = None

    def record_pnl_snapshot(self, underlying_price: float):
        """Record current PnL for historical tracking."""
        snapshot = PnLSnapshot(
            timestamp=datetime.now(),
            pnl=self.pnl,
            pnl_pct=self.pnl_pct,
            ltp=self.ltp,
            underlying_price=underlying_price,
        )
        self.pnl_history.append(snapshot)
        # Cap history to prevent memory bloat
        if len(self.pnl_history) > 1000:
            self.pnl_history = self.pnl_history[-500:]

    @property
    def max_pnl(self) -> float:
        if not self.pnl_history:
            return self.pnl
        return max(s.pnl for s in self.pnl_history)

    @property
    def min_pnl(self) -> float:
        if not self.pnl_history:
            return self.pnl
        return min(s.pnl for s in self.pnl_history)

    @property
    def order_qty(self) -> int:
        """Broker contract quantity = lots * lot_size."""
        return self.qty * max(1, self.lot_size)

    def close(self, price: Optional[float] = None):
        """Close this leg, freeze exit_price and mark inactive."""
        self.is_active = False
        self.exit_price = price if (price is not None and price > 0) else self.ltp
        self.exit_timestamp = datetime.now()

    @property
    def pnl(self) -> float:
        """PnL in absolute currency terms (price_diff * lots * lot_size).
        For closed legs, uses exit_price if available.
        Uses entry_price as effective LTP when ltp is 0 or None (no tick received yet),
        so pnl returns 0 rather than a spuriously large value.
        """
        # For closed legs, prefer frozen exit_price
        if not self.is_active and self.exit_price is not None and self.exit_price > 0:
            effective = self.exit_price
        else:
            effective = self.ltp if (self.ltp is not None and self.ltp != 0.0) else self.entry_price
        if self.side == Side.BUY:
            return (effective - self.entry_price) * self.order_qty
        else:
            return (self.entry_price - effective) * self.order_qty

    @property
    def pnl_pct(self) -> float:
        if self.entry_price == 0:
            return 0.0
        # lot_size cancels in numerator/denominator — percentage is per-unit
        return ((self.pnl / (self.entry_price * self.order_qty)) * 100) if self.order_qty else 0.0

    @property
    def abs_delta(self) -> float:
        return abs(self.delta or 0.0)

    @property
    def abs_gamma(self) -> float:
        return abs(self.gamma or 0.0)

    @property
    def abs_theta(self) -> float:
        return abs(self.theta or 0.0)

    @property
    def abs_vega(self) -> float:
        return abs(self.vega or 0.0)

   
@dataclass
class StrategyState:
    legs: Dict[str, LegState] = field(default_factory=dict)

    spot_price: float = 0.0
    spot_open: float = 0.0
    atm_strike: float = 0.0
    fut_ltp: float = 0.0
    # index_data: key = symbol (e.g., "INDIAVIX"), value = dict of metrics (ltp, change, etc.)
    index_data: Dict[str, Dict[str, float]] = field(default_factory=dict)
    # Chain-level analytics (refreshed by executor from option-chain DB)
    pcr: float = 0.0
    pcr_volume: float = 0.0
    max_pain_strike: float = 0.0
    total_oi_ce: float = 0.0
    total_oi_pe: float = 0.0
    oi_buildup_ce: float = 0.0
    oi_buildup_pe: float = 0.0

    adjustments_today: int = 0
    total_trades_today: int = 0
    cumulative_daily_pnl: float = 0.0
    minutes_to_exit: int = 0   # minutes until EOD exit
    entry_time: Optional[datetime] = None
    last_adjustment_time: Optional[datetime] = None

    trailing_stop_active: bool = False
    trailing_stop_level: float = 0.0
    peak_pnl: float = 0.0

    # Additional runtime fields
    current_time: Optional[datetime] = None

    # New fields from gap analysis
    prev_values: Dict[str, Any] = field(default_factory=dict)   # for crosses_above/below
    lifetime_adjustments: int = 0
    current_profit_step: int = -1
    last_date: Optional[date] = None
    entered_today: bool = False
    _net_delta_override: Optional[float] = None
    _combined_pnl_override: Optional[float] = None

    # ✅ BUG-002 FIX: Adjustment history for detailed reporting
    adjustment_history: List[AdjustmentEvent] = field(default_factory=list)
    entry_reason: str = ""
    exit_reason: str = ""

    def record_adjustment(self, rule_name: str, action_type: str,
                          affected_legs: List[str], reason: str = ""):
        """Record an adjustment event for audit trail."""
        event = AdjustmentEvent(
            timestamp=datetime.now(),
            rule_name=rule_name,
            action_type=action_type,
            affected_legs=affected_legs,
            reason=reason,
            market_data_snapshot={
                "spot_price": self.spot_price,
                "atm_strike": self.atm_strike,
                "net_delta": self.net_delta,
                "combined_pnl": self.combined_pnl,
            },
        )
        self.adjustment_history.append(event)
        # Cap history to prevent unbounded growth
        if len(self.adjustment_history) > 200:
            self.adjustment_history = self.adjustment_history[-100:]

    @property
    def net_delta(self) -> float:
        if self._net_delta_override is not None:
            return self._net_delta_override
        return sum((leg.delta or 0.0) for leg in self.legs.values() if leg.is_active)

    @net_delta.setter
    def net_delta(self, value: float):
        self._net_delta_override = float(value)

    @property
    def delta_diff(self) -> float:
        ce = next((leg for leg in self.legs.values() if leg.is_active and leg.option_type == OptionType.CE), None)
        pe = next((leg for leg in self.legs.values() if leg.is_active and leg.option_type == OptionType.PE), None)
        return ((ce.delta or 0.0) if ce else 0.0) - ((pe.delta or 0.0) if pe else 0.0)

    @property
    def portfolio_delta(self) -> float:
        # Alias retained for strategy-builder compatibility.
        return self.net_delta

    @property
    def combined_pnl(self) -> float:
        if self._combined_pnl_override is not None:
            return self._combined_pnl_override
        return sum(leg.pnl for leg in self.legs.values() if leg.is_active)

    @combined_pnl.setter
    def combined_pnl(self, value: float):
        self._combined_pnl_override = float(value)

    @property
    def total_premium(self) -> float:
        total = 0.0
        for leg in self.legs.values():
            if leg.is_active and leg.instrument == InstrumentType.OPT:
                if leg.side == Side.SELL:
                    total += leg.entry_price * leg.order_qty
                else:
                    total -= leg.entry_price * leg.order_qty
        return total

    @property
    def premium_collected(self) -> float:
        return self.total_premium

    @property
    def total_cost_basis(self) -> float:
        return sum((leg.entry_price * leg.order_qty) for leg in self.legs.values() if leg.is_active)

    @property
    def unrealised_pnl(self) -> float:
        return self.combined_pnl

    @property
    def realised_pnl(self) -> float:
        return self.cumulative_daily_pnl

    @property
    def profit_step(self) -> int:
        return max(0, int(self.current_profit_step))

    @property
    def max_leg_delta(self) -> float:
        if not self.legs:
            return 0.0
        return max((leg.abs_delta for leg in self.legs.values() if leg.is_active), default=0.0)

    @property
    def min_leg_delta(self) -> float:
        if not self.legs:
            return 0.0
        return min((leg.abs_delta for leg in self.legs.values() if leg.is_active), default=0.0)

    @property
    def most_profitable_leg(self) -> Optional[str]:
        if not self.legs:
            return None
        active = [leg for leg in self.legs.values() if leg.is_active]
        if not active:
            return None
        return max(active, key=lambda l: l.pnl).tag

    @property
    def least_profitable_leg(self) -> Optional[str]:
        if not self.legs:
            return None
        active = [leg for leg in self.legs.values() if leg.is_active]
        if not active:
            return None
        return min(active, key=lambda l: l.pnl).tag

    @property
    def higher_delta_leg(self) -> Optional[str]:
        active = [leg for leg in self.legs.values() if leg.is_active]
        if not active:
            return None
        return max(active, key=lambda l: l.abs_delta).tag

    @property
    def lower_delta_leg(self) -> Optional[str]:
        active = [leg for leg in self.legs.values() if leg.is_active]
        if not active:
            return None
        return min(active, key=lambda l: l.abs_delta).tag

    @property
    def higher_theta_leg(self) -> Optional[str]:
        active = [leg for leg in self.legs.values() if leg.is_active]
        if not active:
            return None
        return max(active, key=lambda l: abs(l.theta)).tag

    @property
    def lower_theta_leg(self) -> Optional[str]:
        active = [leg for leg in self.legs.values() if leg.is_active]
        if not active:
            return None
        return min(active, key=lambda l: abs(l.theta)).tag

    @property
    def higher_iv_leg(self) -> Optional[str]:
        active = [leg for leg in self.legs.values() if leg.is_active]
        if not active:
            return None
        return max(active, key=lambda l: l.iv).tag

    @property
    def lower_iv_leg(self) -> Optional[str]:
        active = [leg for leg in self.legs.values() if leg.is_active]
        if not active:
            return None
        return min(active, key=lambda l: l.iv).tag

    @property
    def deepest_itm_leg(self) -> Optional[str]:
        active = [leg for leg in self.legs.values() if leg.is_active]
        if not active:
            return None

        def moneyness(leg):
            # Positive = OTM, Negative = ITM for both CE and PE
            if leg.option_type is None or leg.strike is None or self.spot_price == 0:
                return 0.0
            if leg.option_type == OptionType.CE:
                return (leg.strike - self.spot_price) / self.spot_price
            else:
                return (self.spot_price - leg.strike) / self.spot_price

        # ✅ BUG FIX: min() picks most negative moneyness = deepest ITM
        return min(active, key=moneyness).tag

    @property
    def most_otm_leg(self) -> Optional[str]:
        active = [leg for leg in self.legs.values() if leg.is_active]
        if not active:
            return None

        def moneyness(leg):
            # Positive = OTM, Negative = ITM for both CE and PE
            if leg.option_type is None or leg.strike is None or self.spot_price == 0:
                return 0.0
            if leg.option_type == OptionType.CE:
                return (leg.strike - self.spot_price) / self.spot_price
            else:
                return (self.spot_price - leg.strike) / self.spot_price

        # ✅ BUG FIX: max() picks most positive moneyness = most OTM
        return max(active, key=moneyness).tag

    # New computed properties
    @property
    def portfolio_gamma(self) -> float:
        return sum((leg.gamma or 0.0) for leg in self.legs.values() if leg.is_active)

    @property
    def portfolio_theta(self) -> float:
        return sum((leg.theta or 0.0) for leg in self.legs.values() if leg.is_active)

    @property
    def portfolio_vega(self) -> float:
        return sum((leg.vega or 0.0) for leg in self.legs.values() if leg.is_active)

    @property
    def combined_pnl_pct(self) -> float:
        # BUG-A3 FIX: Guard against total_premium=0 (buy-only / no premium strategies).
        # Fall back to total_cost_basis so pct is meaningful for debit strategies.
        base = self.total_premium
        if base == 0:
            base = self.total_cost_basis
        if base == 0:
            return 0.0
        return (self.combined_pnl / abs(base)) * 100

    @property
    def iv_skew(self) -> float:
        ce = next((leg for leg in self.legs.values() if leg.is_active and leg.option_type == OptionType.CE), None)
        pe = next((leg for leg in self.legs.values() if leg.is_active and leg.option_type == OptionType.PE), None)
        return (pe.iv if pe else 0.0) - (ce.iv if ce else 0.0)

    @property
    def atm_iv(self) -> float:
        ivs = [leg.iv for leg in self.legs.values() if leg.is_active and leg.option_type in (OptionType.CE, OptionType.PE)]
        return (sum(ivs) / len(ivs)) if ivs else 0.0

    @property
    def ce_iv(self) -> float:
        leg = next((l for l in self.legs.values() if l.is_active and l.option_type == OptionType.CE), None)
        return leg.iv if leg else 0.0

    @property
    def pe_iv(self) -> float:
        leg = next((l for l in self.legs.values() if l.is_active and l.option_type == OptionType.PE), None)
        return leg.iv if leg else 0.0

    @property
    def ce_premium_decay_pct(self) -> float:
        leg = next((l for l in self.legs.values() if l.is_active and l.option_type == OptionType.CE), None)
        if not leg or leg.entry_price == 0:
            return 0.0
        return ((leg.entry_price - leg.ltp) / leg.entry_price) * 100

    @property
    def pe_premium_decay_pct(self) -> float:
        leg = next((l for l in self.legs.values() if l.is_active and l.option_type == OptionType.PE), None)
        if not leg or leg.entry_price == 0:
            return 0.0
        return ((leg.entry_price - leg.ltp) / leg.entry_price) * 100

    @property
    def total_premium_decay_pct(self) -> float:
        entry = sum((leg.entry_price * leg.order_qty) for leg in self.legs.values() if leg.is_active)
        current = sum((leg.ltp * leg.order_qty) for leg in self.legs.values() if leg.is_active)
        if entry == 0:
            return 0.0
        return ((entry - current) / entry) * 100

    @property
    def max_profit_potential(self) -> float:
        return self.total_premium

    @property
    def adjustment_count(self) -> int:
        return int(self.lifetime_adjustments)

    @property
    def spot_change(self) -> float:
        return self.spot_price - self.spot_open

    @property
    def spot_change_pct(self) -> float:
        if self.spot_open == 0:
            return 0.0
        return (self.spot_change / self.spot_open) * 100

    @property
    def active_legs_count(self) -> int:
        return sum(1 for leg in self.legs.values() if leg.is_active)

    @property
    def closed_legs_count(self) -> int:
        return len(self.legs) - self.active_legs_count

    @property
    def any_leg_active(self) -> bool:
        return self.active_legs_count > 0

    @property
    def all_legs_active(self) -> bool:
        return self.active_legs_count == len(self.legs)

    @property
    def time_in_position_sec(self) -> float:
        if self.entry_time is None:
            return 0.0
        delta = (datetime.now() - self.entry_time).total_seconds()
        return max(0.0, delta)

    @property
    def time_since_last_adj_sec(self) -> float:
        if self.last_adjustment_time is None:
            return 999999.0
        return (datetime.now() - self.last_adjustment_time).total_seconds()

    # Breakeven calculations (simplified – assumes short strangle/straddle)
    # BUG-H1 FIX: Use per-unit premium (sum of entry prices), not total_premium
    # which includes lot_size multiplication and would be off by a factor of lot_size.
    @property
    def _per_unit_premium(self) -> float:
        """Net per-unit premium collected (sum of entry_prices, not multiplied by qty)."""
        total = 0.0
        for leg in self.legs.values():
            if leg.is_active and leg.instrument == InstrumentType.OPT:
                if leg.side == Side.SELL:
                    total += leg.entry_price
                else:
                    total -= leg.entry_price
        return total

    @property
    def breakeven_upper(self) -> float:
        return self.atm_strike + self._per_unit_premium

    @property
    def breakeven_lower(self) -> float:
        return self.atm_strike - self._per_unit_premium

    @property
    def breakeven_distance(self) -> float:
        return min(abs(self.spot_price - self.breakeven_upper),
                   abs(self.spot_price - self.breakeven_lower))

    @property
    def spot_vs_upper_be(self) -> float:
        return self.spot_price - self.breakeven_upper

    @property
    def spot_vs_lower_be(self) -> float:
        return self.spot_price - self.breakeven_lower

    @property
    def spot_vs_max_pain(self) -> float:
        if self.max_pain_strike == 0:
            return 0.0
        return self.spot_price - self.max_pain_strike

    @property
    def days_to_expiry(self) -> int:
        """Minimum days to expiry among active legs."""
        if not self.legs:
            return 0
        today = datetime.now().date()
        min_days = 999
        # ✅ BUG-018 FIX: Scripmaster uses "%d-%b-%Y" (e.g. "27-FEB-2025") but state/config
        # may store ISO "2025-02-27". A ValueError from the wrong format was silently caught,
        # returning 0, which made is_expiry_day=True and caused premature exits.
        # Try both formats; skip only if neither parses.
        _DATE_FORMATS = ["%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y"]
        for leg in self.legs.values():
            if not leg.is_active:
                continue
            exp_date = None
            for fmt in _DATE_FORMATS:
                try:
                    exp_date = datetime.strptime(leg.expiry, fmt).date()
                    break
                except ValueError:
                    continue
            if exp_date is None:
                continue
            days = (exp_date - today).days
            if days < min_days:
                min_days = days
        return min_days if min_days != 999 else 0

    @property
    def is_expiry_day(self) -> bool:
        return self.days_to_expiry == 0

    @property
    def session_type(self) -> str:
        """Return 'morning' if before 12:00, else 'afternoon'."""
        now = datetime.now()
        return "morning" if now.hour < 12 else "afternoon"

    def set_index_ticks(self, ticks: Dict[str, Dict[str, float]]):
        """
        Accepts broker-style keys and normalizes to index_data metrics used by conditions.
        """
        normalized: Dict[str, Dict[str, float]] = {}
        for symbol, data in (ticks or {}).items():
            key = str(symbol).upper()
            row = dict(data or {})
            if "change_pct" not in row and "pc" in row:
                row["change_pct"] = row["pc"]
            if "change" not in row and "c" in row:
                row["change"] = row["c"]
            normalized[key] = row
        self.index_data.update(normalized)
