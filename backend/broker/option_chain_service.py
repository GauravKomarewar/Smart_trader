"""
Smart Trader — Option Chain Subscription & Greeks Service
===========================================================
Manages real-time subscriptions for option chain instruments.

When a user views the option chain for NIFTY/BANKNIFTY etc., this service:
  1. Extracts all option symbols from the current chain
  2. Subscribes them to the live tick service (Fyers + Shoonya pipelines)
  3. Calculates Black-Scholes Greeks using live LTPs
  4. Persists snapshots to `option_chain_cache` table

Usage:
    from broker.option_chain_service import oc_service
    oc_service.subscribe_chain(chain_data, underlying, expiry)
    enriched = oc_service.enrich_with_live_greeks(chain_data)
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("smart_trader.oc_service")


class OptionChainService:
    """
    Subscribes option chain symbols to live tick feed and computes Greeks.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._subscribed_underlyings: Dict[str, datetime] = {}  # underlying → last_subscribed_at
        logger.info("OptionChainService initialised")

    def subscribe_chain(
        self,
        chain_data: Dict[str, Any],
        underlying: str,
        expiry: str,
    ) -> None:
        """
        Extract all option symbols from the chain and subscribe to real-time ticks.
        """
        symbols = self._extract_option_symbols(chain_data)
        if not symbols:
            return

        try:
            from broker.live_tick_service import get_tick_service
            svc = get_tick_service()
            svc.subscribe(symbols)
            with self._lock:
                self._subscribed_underlyings[underlying] = datetime.now(timezone.utc)
            logger.info("Subscribed %d option symbols for %s/%s",
                        len(symbols), underlying, expiry)
        except Exception as e:
            logger.debug("OC subscribe error: %s", e)

    def enrich_with_live_greeks(
        self,
        chain_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Walk the option chain rows and inject live LTP + computed Greeks.
        Returns the enriched chain_data (mutates in-place for efficiency).
        """
        underlying = chain_data.get("underlying", "")
        spot = chain_data.get("underlyingLtp", 0.0)
        expiry_str = chain_data.get("expiry", "")
        rows = chain_data.get("rows", [])

        if not rows:
            return chain_data

        try:
            from broker.live_tick_service import get_tick_service
            tick_svc = get_tick_service()

            for row in rows:
                strike = float(row.get("strike", 0))
                for side_key, otype in [("call", "CE"), ("put", "PE")]:
                    leg = row.get(side_key)
                    if not leg or not isinstance(leg, dict):
                        continue
                    # Resolve Fyers symbol for live tick lookup
                    fyers_sym = leg.get("fyers_symbol", "")
                    trading_sym = leg.get("trading_symbol", "")
                    live_ltp = 0.0
                    for key in [fyers_sym, trading_sym]:
                        if key:
                            t = tick_svc.get_latest(key.replace("NFO:", "").replace("NSE:", ""))
                            if t and t.get("ltp", 0) > 0:
                                live_ltp = t["ltp"]
                                leg["ltp"] = live_ltp
                                break

                    # Compute Greeks if we have spot and LTP
                    if spot > 0 and expiry_str:
                        greeks = self._compute_greeks(
                            spot=spot, strike=strike, expiry_str=expiry_str,
                            option_type=otype, ltp=live_ltp,
                        )
                        leg.update(greeks)

        except Exception as e:
            logger.debug("enrich_with_live_greeks error: %s", e)

        return chain_data

    def persist_snapshot(
        self,
        chain_data: Dict[str, Any],
    ) -> None:
        """Store current option chain snapshot to `option_chain_cache` table."""
        underlying = chain_data.get("underlying", "")
        expiry = chain_data.get("expiry", "")
        rows = chain_data.get("rows", [])
        if not rows or not underlying:
            return
        try:
            from db.trading_db import trading_cursor
            with trading_cursor() as cur:
                records = []
                for row in rows:
                    strike = float(row.get("strike", 0))
                    c = row.get("call", {}) or {}
                    p = row.get("put",  {}) or {}
                    records.append((
                        underlying, expiry, strike,
                        c.get("ltp", 0), p.get("ltp", 0),
                        c.get("iv", 0),  p.get("iv", 0),
                        c.get("delta", 0), p.get("delta", 0),
                        c.get("gamma", 0), p.get("gamma", 0),
                        c.get("theta", 0), p.get("theta", 0),
                        c.get("vega", 0),  p.get("vega", 0),
                        c.get("oi", 0), p.get("oi", 0),
                    ))
                cur.executemany("""
                    INSERT INTO option_chain_cache
                        (symbol, expiry, strike,
                         ce_ltp, pe_ltp, ce_iv, pe_iv,
                         ce_delta, pe_delta, ce_gamma, pe_gamma,
                         ce_theta, pe_theta, ce_vega, pe_vega,
                         oi_ce, oi_pe, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, NOW())
                    ON CONFLICT (symbol, expiry, strike) DO UPDATE SET
                        ce_ltp   = EXCLUDED.ce_ltp,
                        pe_ltp   = EXCLUDED.pe_ltp,
                        ce_iv    = EXCLUDED.ce_iv,
                        pe_iv    = EXCLUDED.pe_iv,
                        ce_delta = EXCLUDED.ce_delta,
                        pe_delta = EXCLUDED.pe_delta,
                        ce_gamma = EXCLUDED.ce_gamma,
                        pe_gamma = EXCLUDED.pe_gamma,
                        ce_theta = EXCLUDED.ce_theta,
                        pe_theta = EXCLUDED.pe_theta,
                        ce_vega  = EXCLUDED.ce_vega,
                        pe_vega  = EXCLUDED.pe_vega,
                        oi_ce    = EXCLUDED.oi_ce,
                        oi_pe    = EXCLUDED.oi_pe,
                        updated_at = NOW()
                """, records)
        except Exception as e:
            logger.debug("OC snapshot persist error: %s", e)

    # ── helpers ────────────────────────────────────────────────────────────────

    @staticmethod
    def _extract_option_symbols(chain_data: Dict[str, Any]) -> List[str]:
        """Return all fyers_symbol/trading_symbol strings from chain rows."""
        syms: List[str] = []
        for row in chain_data.get("rows", []):
            for side_key in ("call", "put"):
                leg = row.get(side_key) or {}
                for k in ("fyers_symbol", "trading_symbol"):
                    v = leg.get(k, "")
                    if v:
                        syms.append(v)
        return list(dict.fromkeys(syms))  # unique, preserve order

    @staticmethod
    def _compute_greeks(
        spot: float,
        strike: float,
        expiry_str: str,
        option_type: str = "CE",
        ltp: float = 0.0,
    ) -> Dict[str, float]:
        """
        Compute Black-Scholes Greeks for a single option.
        Returns a dict with keys: iv, delta, gamma, theta, vega, bs_price.
        Accepts expiry in ISO YYYY-MM-DD, DD-MM-YYYY, or DDMMMYY format.
        """
        try:
            from trading.utils.bs_greeks import (
                bs_greeks, bs_price, implied_volatility, time_to_expiry_seconds,
            )
            # Normalise expiry to DDMMMYY for time_to_expiry_seconds
            exp_ddmmmyy = expiry_str
            if len(expiry_str) == 10 and expiry_str[4] == "-":
                # ISO YYYY-MM-DD → DDMMMYY
                from datetime import datetime as _dt
                d = _dt.strptime(expiry_str, "%Y-%m-%d")
                exp_ddmmmyy = d.strftime("%d%b%y").upper()
            elif len(expiry_str) == 10 and expiry_str[2] == "-" and expiry_str[5] == "-":
                # DD-MM-YYYY → DDMMMYY
                from datetime import datetime as _dt
                d = _dt.strptime(expiry_str, "%d-%m-%Y")
                exp_ddmmmyy = d.strftime("%d%b%y").upper()

            T = time_to_expiry_seconds(exp_ddmmmyy, "15:30")
            if T <= 0:
                return {}

            # Derive IV from market LTP if available
            sigma = 0.18
            iv_val = 0.0
            if ltp and ltp > 0:
                iv_pct = implied_volatility(ltp, spot, strike, T, 0.065, option_type)
                if iv_pct and iv_pct > 0:
                    sigma = iv_pct / 100
                    iv_val = round(iv_pct, 2)

            g = bs_greeks(spot, strike, T, 0.065, sigma, option_type) or {}
            price = bs_price(spot, strike, T, 0.065, sigma, option_type) or 0.0
            return {
                "iv":       iv_val,
                "bs_price": round(price, 2),
                "delta":    round(g.get("delta", 0), 4),
                "gamma":    round(g.get("gamma", 0), 6),
                "theta":    round(g.get("theta", 0), 4),
                "vega":     round(g.get("vega", 0), 4),
            }
        except Exception as e:
            logger.debug("Greek calc error: %s", e)
            return {}


# ── Singleton ──────────────────────────────────────────────────────────────────

_oc_service: Optional[OptionChainService] = None
_oc_lock = threading.Lock()


def get_oc_service() -> OptionChainService:
    global _oc_service
    with _oc_lock:
        if _oc_service is None:
            _oc_service = OptionChainService()
    return _oc_service
