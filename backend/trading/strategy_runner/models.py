from enum import Enum
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Union

class InstrumentType(Enum):
    OPT = "OPT"
    FUT = "FUT"

class OptionType(Enum):
    CE = "CE"
    PE = "PE"

class Side(Enum):
    BUY = "BUY"
    SELL = "SELL"

class OrderType(Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    SL = "SL"
    SLM = "SLM"

class StrikeMode(Enum):
    STANDARD = "standard"
    EXACT = "exact"
    ATM_POINTS = "atm_points"
    ATM_PCT = "atm_pct"
    MATCH_LEG = "match_leg"

class Comparator(Enum):
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="
    EQ = "=="
    NEQ = "!="
    APPROX = "~="
    BETWEEN = "between"
    NOT_BETWEEN = "not_between"
    CROSSES_ABOVE = "crosses_above"
    CROSSES_BELOW = "crosses_below"
    IS_TRUE = "is_true"
    IS_FALSE = "is_false"

class JoinOperator(Enum):
    AND = "AND"
    OR = "OR"

@dataclass
class Condition:
    parameter: str          # e.g. "spot_price", "tag.LEG@1.delta"
    comparator: Comparator
    value: Optional[Union[float, str]] = None    # None for is_true / is_false
    value2: Optional[Union[float, str]] = None   # for between
    join: Optional[JoinOperator] = None          # for combining in a list

@dataclass
class StrikeConfig:
    mode: StrikeMode
    side: Side
    option_type: OptionType
    lots: int
    order_type: Optional[OrderType] = None

    # Standard mode
    strike_selection: Optional[str] = None        # e.g. "atm", "delta"
    strike_value: Optional[Union[str, float]] = None

    # Exact mode
    exact_strike: Optional[float] = None

    # ATM points mode
    atm_offset_points: Optional[float] = None

    # ATM pct mode
    atm_offset_pct: Optional[float] = None

    # Match leg mode
    match_leg: Optional[str] = None               # tag of reference leg
    match_param: Optional[str] = None             # e.g. "abs_delta"
    match_offset: float = 0.0
    match_multiplier: float = 1.0

    # Common rounding for all modes
    rounding: Optional[float] = None