"""
Complete Black-Scholes Greeks Module with Robust IV Solver
============================================================
Includes all original functions + fixes for IV calculation.
"""

import math
import numpy as np
from scipy.stats import norm
from datetime import datetime


# =============================================================================
# TIME TO EXPIRY FUNCTIONS (ORIGINAL - KEPT AS IS)
# =============================================================================

def time_to_expiry(expiry: str, time_str: str) -> float:
    """
    Calculate time to expiry with custom time.
    
    Args:
        expiry: Expiry date in DDMMMYY format (e.g., "28DEC25")
        time_str: Time in HH:MM format (e.g., "15:30")
    
    Returns:
        Time to expiry in years
    """
    # 1. Parse the date (e.g., "28DEC25" or "28-Apr-2026" or "28-04-2026")
    expiry_date = _parse_expiry_date(expiry)
    
    # 2. Parse the time (e.g., "15:30")
    time_parts = datetime.strptime(time_str, "%H:%M")
    
    # 3. Combine date and time
    expiry_dt = expiry_date.replace(hour=time_parts.hour, minute=time_parts.minute)
    
    # 4. Calculate the difference in years
    now = datetime.now()
    delta = (expiry_dt - now).total_seconds()
    
    # Return years (or 0 if already expired)
    return max(delta / (365 * 24 * 3600), 0)


def _parse_expiry_date(expiry_str: str) -> datetime:
    """Parse expiry date string in any common format."""
    for fmt in ("%d%b%y", "%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d", "%d%b%Y"):
        try:
            return datetime.strptime(expiry_str.strip(), fmt)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse expiry date: {expiry_str!r}")


def time_to_expiry_seconds(
    expiry_str: str,
    market_close_time: str
) -> float:
    """
    High-precision time to expiry using calendar seconds.
    Black–Scholes compatible and numerically stable.
    
    Args:
        expiry_str: Expiry date (e.g. "28APR26", "28-Apr-2026", "28-04-2026")
        market_close_time: Market close time in HH:MM format
    
    Returns:
        Time to expiry in years
    """
    now = datetime.now()

    expiry_date = _parse_expiry_date(expiry_str)
    close_h, close_m = map(int, market_close_time.split(":"))

    expiry_dt = expiry_date.replace(
        hour=close_h,
        minute=close_m,
        second=0,
        microsecond=0
    )

    seconds_remaining = (expiry_dt - now).total_seconds()

    if seconds_remaining <= 0:
        return 1e-9  # prevent divide-by-zero & IV blowups

    SECONDS_IN_YEAR = 365.25 * 24 * 3600  # leap-year safe

    return seconds_remaining / SECONDS_IN_YEAR


def get_trading_time_fraction(
    expiry_str: str, 
    market_close_time: str, 
    market_start_time: str = "09:15", 
    holidays: int = 15
) -> float:
    """
    Calculate time to expiry as fraction of trading time.
    Accounts for trading hours and holidays.
    
    Args:
        expiry_str: Expiry date in DDMMMYY format
        market_close_time: Market close time in HH:MM format
        market_start_time: Market start time in HH:MM format (default: "09:15")
        holidays: Number of holidays in the year (default: 15)
    
    Returns:
        Trading time fraction (years)
    """
    now = datetime.now()
    expiry_date = _parse_expiry_date(expiry_str)
    
    # 1. Calculate Total Trading Seconds in the Year
    year = expiry_date.year
    start_year = np.datetime64(f'{year}-01-01')
    end_year = np.datetime64(f'{year+1}-01-01')
    
    # .item() or float() ensures this converts from numpy.int to Python int
    total_weekdays = int(np.busday_count(start_year, end_year))
    trading_days_in_year = total_weekdays - holidays
    
    # Calculate daily session duration
    fmt = "%H:%M"
    session_delta = datetime.strptime(market_close_time, fmt) - \
                    datetime.strptime(market_start_time, fmt)
    seconds_per_day = session_delta.total_seconds()
    
    total_trading_seconds_year = trading_days_in_year * seconds_per_day

    # 2. Calculate Remaining Seconds until Expiry
    close_h, close_m = map(int, market_close_time.split(":"))
    expiry_dt = expiry_date.replace(hour=close_h, minute=close_m, second=0, microsecond=0)
    
    seconds_remaining = (expiry_dt - now).total_seconds()
    
    # 3. Prevent Pylance error by explicit casting to float
    if seconds_remaining <= 0:
        return 1e-9
        
    # Wrapping in float() solves the "floating[_NBitInt]" error
    return float(seconds_remaining / total_trading_seconds_year)


# =============================================================================
# BLACK-SCHOLES PRICING (IMPROVED ERROR HANDLING)
# =============================================================================

def bs_price(S, K, T, r, sigma, option_type="CE", q=0.0):
    """
    Calculate Generalized Black-Scholes option price.
    
    Args:
        S: Spot price
        K: Strike price
        T: Time to expiry (years)
        r: Risk-free rate
        sigma: Volatility (as decimal, e.g., 0.20 for 20%)
        option_type: "CE" for Call, "PE" for Put
        q: Continuous dividend yield (0 for commodities, ~0.012 for NIFTY)
    
    Returns:
        Option price
    """
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0

    try:
        sqrt_T = math.sqrt(T)
        d1 = (math.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * sqrt_T)
        d2 = d1 - sigma * sqrt_T

        if option_type == "CE":
            return S * math.exp(-q * T) * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)
        else:
            return K * math.exp(-r * T) * norm.cdf(-d2) - S * math.exp(-q * T) * norm.cdf(-d1)
    except (ValueError, OverflowError):
        return 0.0


# =============================================================================
# GREEKS CALCULATION (IMPROVED ERROR HANDLING)
# =============================================================================

def bs_greeks(S, K, T, r, sigma, opt_type="CE", q=0.0):
    """
    Calculate option Greeks (Generalized Black-Scholes with dividend yield).
    
    Args:
        S: Spot price
        K: Strike price
        T: Time to expiry (years)
        r: Risk-free rate
        sigma: Volatility (as decimal)
        opt_type: "CE" for Call, "PE" for Put
        q: Continuous dividend yield (0 for commodities, ~0.012 for NIFTY)
    
    Returns:
        Dictionary with delta, gamma, theta, vega, rho
    """
    # Support both opt_type and option_type parameter names
    option_type = opt_type
    
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return dict(delta=0, gamma=0, theta=0, vega=0, rho=0)

    try:
        sqrt_T = math.sqrt(T)
        d1 = (math.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * sqrt_T)
        d2 = d1 - sigma * sqrt_T

        pdf = norm.pdf(d1)
        eq_T = math.exp(-q * T)
        er_T = math.exp(-r * T)

        delta = eq_T * norm.cdf(d1) if option_type == "CE" else -eq_T * norm.cdf(-d1)
        gamma = eq_T * pdf / (S * sigma * sqrt_T)
        vega = S * eq_T * pdf * sqrt_T / 100

        if option_type == "CE":
            theta = (-S * eq_T * pdf * sigma / (2 * sqrt_T)
                     + q * S * eq_T * norm.cdf(d1)
                     - r * K * er_T * norm.cdf(d2)) / 365
            rho = K * T * er_T * norm.cdf(d2) / 100
        else:
            theta = (-S * eq_T * pdf * sigma / (2 * sqrt_T)
                     - q * S * eq_T * norm.cdf(-d1)
                     + r * K * er_T * norm.cdf(-d2)) / 365
            rho = -K * T * er_T * norm.cdf(-d2) / 100

        return {
            "delta": delta,
            "gamma": gamma,
            "theta": theta,
            "vega": vega,
            "rho": rho
        }
    except (ValueError, OverflowError, ZeroDivisionError):
        return dict(delta=0, gamma=0, theta=0, vega=0, rho=0)


# =============================================================================
# IMPLIED VOLATILITY
# =============================================================================
def _implied_vol_bisection(
    price, S, K, T, r, opt_type,
    sigma_low=1e-4,
    sigma_high=5.0,
    tol=1e-4,
    max_iter=60,
    q=0.0,
):
    from math import isnan

    # Theoretical price at bounds
    price_low = bs_price(S, K, T, r, sigma_low, opt_type, q=q)
    price_high = bs_price(S, K, T, r, sigma_high, opt_type, q=q)

    # If price not bracketed, no solution
    if (price_low - price) * (price_high - price) > 0:
        return None

    intrinsic = max(S * math.exp(-q * T) - K * math.exp(-r * T), 0) if opt_type == "CE" else max(K * math.exp(-r * T) - S * math.exp(-q * T), 0)
    if price <= intrinsic:
        return None

    sigma_mid = 0.5 * (sigma_low + sigma_high)  # initialise before loop
    for _ in range(max_iter):
        sigma_mid = 0.5 * (sigma_low + sigma_high)
        price_mid = bs_price(S, K, T, r, sigma_mid, opt_type, q=q)

        if isnan(price_mid):
            return None

        if abs(price_mid - price) < tol:
            return sigma_mid

        if (price_low - price) * (price_mid - price) < 0:
            sigma_high = sigma_mid
            price_high = price_mid
        else:
            sigma_low = sigma_mid
            price_low = price_mid

    return sigma_mid

#==================================================================================
def implied_volatility(
    market_price, 
    S, 
    K, 
    T, 
    r, 
    option_type="CE", 
    max_iterations=100, 
    tolerance=1e-5,
    q=0.0,
):
    """
    Calculate implied volatility using Newton-Raphson with robust error handling.
    
    Args:
        market_price: Market price of option
        S: Spot price
        K: Strike price
        T: Time to expiry (years)
        r: Risk-free rate
        option_type: "CE" or "PE"
        max_iterations: Maximum iterations (default: 100)
        tolerance: Convergence tolerance (default: 1e-5)
        q: Continuous dividend yield (0 for commodities, ~0.012 for NIFTY)
    
    Returns:
        Implied volatility as percentage (e.g., 25.5 for 25.5%)
        Returns None if calculation fails
    """
    # Input validation
    if market_price <= 0 or T <= 0 or S <= 0 or K <= 0:
        return None
    
    # Check if option is already expired
    if T < 1e-9:
        return None
    
    # Intrinsic value check
    intrinsic = max(S - K, 0) if option_type == "CE" else max(K - S, 0)
    if market_price <= intrinsic * 1.01:  # Must have time value
        return None
    
    # Initial guess based on Brenner-Subrahmanyam approximation
    sqrt_T = math.sqrt(max(T, 1e-15))  # safe initialisation before try block
    try:
        sqrt_T = math.sqrt(T)
        atm_vol = math.sqrt(2 * math.pi / T) * (market_price / S)
        sigma = max(0.05, min(atm_vol, 2.0))  # Bounded initial guess
    except (ValueError, ZeroDivisionError):
        sigma = 0.20  # Fallback
    
    # Newton-Raphson iteration
    for iteration in range(max_iterations):
        try:
            # Calculate BS price and vega
            price = bs_price(S, K, T, r, sigma, option_type, q=q)
            
            # Calculate vega (derivative of price w.r.t. sigma)
            eq_T = math.exp(-q * T)
            d1 = (math.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * sqrt_T)
            vega = S * eq_T * norm.pdf(d1) * sqrt_T
            
            # Check for numerical issues
            if not np.isfinite(price) or not np.isfinite(vega):
                return None
            
            # Vega too small = flat pricing curve
            if abs(vega) < 1e-10:
                return None
            
            # Calculate difference
            diff = price - market_price
            
            # Check convergence
            if abs(diff) < tolerance:
                # Converged successfully
                result = sigma * 100
                # Final sanity check
                if 0.1 <= result <= 500:  # 0.1% to 500% IV
                    return round(result, 2)
                else:
                    return None
            
            # Newton-Raphson update
            sigma_new = sigma - diff / vega
            
            # Apply bounds
            sigma_new = max(0.001, min(sigma_new, 5.0))
            
            # Check for oscillation (no progress)
            if iteration > 5 and abs(sigma_new - sigma) < 1e-8:
                # Not converging, but close enough?
                if abs(diff) < market_price * 0.01:  # Within 1% of market price
                    result = sigma * 100
                    if 0.1 <= result <= 500:
                        return round(result, 2)
                return None
            
            sigma = sigma_new
            
        except (ValueError, OverflowError, ZeroDivisionError):
            return None
    
    # Max iterations reached without convergence
    # Check if we're close enough
    try:
        final_price = bs_price(S, K, T, r, sigma, option_type, q=q)
        if abs(final_price - market_price) < market_price * 0.02:  # Within 2%
            result = sigma * 100
            if 0.1 <= result <= 500:
                return round(result, 2)
    except Exception:
        pass
    
    # ------------------------------------------------------------------
    # FINAL FALLBACK: Bisection (robust, slow, guaranteed if solution exists)
    # ------------------------------------------------------------------
    try:
        iv = _implied_vol_bisection(
            market_price,
            S,
            K,
            T,
            r,
            option_type,
            q=q,
        )
        if iv is not None:
            result = iv * 100
            if 0.1 <= result <= 500:
                return round(result, 2)
    except Exception:
        pass

    return None
