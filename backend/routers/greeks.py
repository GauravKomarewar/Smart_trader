"""
Smart Trader — Greeks Router
==============================

Endpoints:
  POST /api/greeks/calculate  — compute BS greeks for given option params
  POST /api/greeks/iv         — implied volatility only
"""

from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional

from trading.utils.bs_greeks import (
    bs_greeks,
    bs_price,
    implied_volatility,
    time_to_expiry_seconds,
)

greeks_router = APIRouter(prefix="/greeks", tags=["greeks"])


class GreeksRequest(BaseModel):
    spot:         float
    strike:       float
    expiry:       str           # DDMMMYY e.g. "25JUL25"
    option_type:  str = "CE"    # CE | PE
    risk_free:    float = 0.065
    market_close: str = "15:30"
    ltp:          Optional[float] = None   # market price for IV calc
    dividend_yield: float = 0.0


class IVRequest(BaseModel):
    spot:         float
    strike:       float
    expiry:       str
    option_type:  str = "CE"
    market_price: float
    risk_free:    float = 0.065
    market_close: str = "15:30"
    dividend_yield: float = 0.0


@greeks_router.post("/calculate")
def calculate_greeks(req: GreeksRequest):
    """
    Calculate BS price + Greeks.
    If ltp is provided, also computes implied volatility.
    """
    T = time_to_expiry_seconds(req.expiry, req.market_close)

    # Use IV if ltp provided, fallback to default sigma 0.15
    sigma = 0.15
    iv_val = None
    if req.ltp and req.ltp > 0:
        iv_pct = implied_volatility(
            req.ltp, req.spot, req.strike, T, req.risk_free,
            req.option_type, q=req.dividend_yield)
        if iv_pct:
            sigma = iv_pct / 100
            iv_val = iv_pct

    greeks = bs_greeks(req.spot, req.strike, T, req.risk_free,
                       sigma, req.option_type, q=req.dividend_yield)
    price  = bs_price(req.spot, req.strike, T, req.risk_free,
                      sigma, req.option_type, q=req.dividend_yield)

    return {
        "bs_price": round(price, 2),
        "iv":       iv_val,
        "sigma":    round(sigma * 100, 2),   # as %
        "T":        round(T, 6),
        **{k: round(v, 6) for k, v in greeks.items()},
    }


@greeks_router.post("/iv")
def get_implied_volatility(req: IVRequest):
    """Compute implied volatility only."""
    T = time_to_expiry_seconds(req.expiry, req.market_close)
    iv = implied_volatility(
        req.market_price, req.spot, req.strike, T, req.risk_free,
        req.option_type, q=req.dividend_yield)
    return {
        "iv":   iv,
        "T":    round(T, 6),
        "spot": req.spot, "strike": req.strike, "expiry": req.expiry,
    }
