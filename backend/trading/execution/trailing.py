"""
Smart Trader — Trailing Stop-Loss Engine
=========================================

Ported from shoonya_platform/execution/trailing.py

All engines inherit TrailingEngine base class.
Rules:
  - compute_new_sl() MUST NEVER reduce protection (never move SL backward)
  - Engine selection is dynamic (types: POINTS, PERCENT, ABSOLUTE)
"""

from abc import ABC, abstractmethod
from typing import Optional


class TrailingEngine(ABC):
    """Base interface for all trailing stop-loss engines."""

    @abstractmethod
    def compute_new_sl(self, current_price: float, last_sl: float) -> float:
        """
        Return updated stop loss.
        Must NEVER reduce existing protection.
        """


class PointsTrailing(TrailingEngine):
    """
    Trail stop-loss by fixed points distance from current price.

    Example: price=100, points=10 → SL=90
    If price rises to 110: SL=max(100, 90) = 100 (protected)
    """
    def __init__(self, points: float, step: Optional[float] = None):
        self.points = points
        self.step = step

    def compute_new_sl(self, current_price: float, last_sl: float) -> float:
        new_sl = current_price - self.points
        return max(new_sl, last_sl)


class PercentTrailing(TrailingEngine):
    """
    Trail stop-loss by percentage of current price.

    Example: price=100, percent=10 → SL=90
    If price rises to 120: SL=max(108, 90) = 108
    """
    def __init__(self, percent: float):
        self.percent = percent

    def compute_new_sl(self, current_price: float, last_sl: float) -> float:
        new_sl = current_price * (1 - self.percent / 100)
        return max(new_sl, last_sl)


class AbsoluteTrailing(TrailingEngine):
    """
    Fixed absolute stop-loss price (manual override).
    Does not trail — always returns the configured price.
    """
    def __init__(self, price: float):
        self.price = price

    def compute_new_sl(self, *_) -> float:
        return self.price


def make_trailing_engine(
    trailing_type: Optional[str],
    trailing_value: Optional[float],
    trail_step: Optional[float] = None,
) -> Optional[TrailingEngine]:
    """
    Factory to build a TrailingEngine from order config.
    Returns None if no trailing configured.
    """
    if not trailing_type or trailing_type == "NONE":
        return None
    if trailing_value is None or trailing_value <= 0:
        return None

    t = (trailing_type or "").upper()
    if t == "POINTS":
        return PointsTrailing(trailing_value, step=trail_step)
    elif t == "PERCENT":
        return PercentTrailing(trailing_value)
    elif t == "ABSOLUTE":
        return AbsoluteTrailing(trailing_value)
    return None
