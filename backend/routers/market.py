"""Market data router — quotes, option chain, indices, global markets."""

import logging
from collections import defaultdict
from fastapi import APIRouter, Query
from typing import Optional

from broker.fyers_client import get_fyers_client
from broker.shoonya_client import get_session, _make_demo_quote

logger = logging.getLogger("smart_trader.api")
router = APIRouter(prefix="/market", tags=["market"])

INDICES = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX"]
NIFTY50_SYMBOLS = [
    "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "HINDUNILVR",
    "ITC", "SBIN", "BHARTIARTL", "KOTAKBANK", "LT", "BAJFINANCE",
    "HCLTECH", "WIPRO", "AXISBANK", "TECHM", "ASIANPAINT", "MARUTI",
    "SUNPHARMA", "TITAN", "ULTRACEMCO", "POWERGRID", "NTPC", "ONGC",
    "COALINDIA", "BAJAJFINSV", "NESTLEIND", "JSWSTEEL", "HINDALCO", "TATASTEEL",
]

# ── no demo stubs — empty data returned when no live source ────────────────


# ── /market/indices ────────────────────────────────────────────────────────────

@router.get("/indices")
async def get_indices():
    """Return latest prices for major Indian indices. Fyers first, then demo."""
    fyers = get_fyers_client()

    if fyers.is_live:
        data = fyers.get_indices()
        if data:
            return {"data": data, "count": len(data), "source": "fyers"}

    # Shoonya fallback
    session = get_session()
    if not session.is_demo:
        result = []
        for sym in INDICES:
            q = session.get_ltp("NSE", sym)
            if q:
                result.append(q)
        if result:
            return {"data": result, "count": len(result), "source": "shoonya"}

    # No live source available — return empty (no fake data)
    return {"data": [], "count": 0, "source": "unavailable"}


# ── /market/global ─────────────────────────────────────────────────────────────

@router.get("/global")
async def get_global_markets():
    """Return global commodities and forex rates from Fyers."""
    fyers = get_fyers_client()

    if fyers.is_live:
        data = fyers.get_global_markets()
        if data:
            return {"data": data, "count": len(data), "source": "fyers"}

    # No live source — return empty (no fake data)
    return {"data": [], "count": 0, "source": "unavailable"}


# ── /market/quote/{symbol} ─────────────────────────────────────────────────────

@router.get("/quote/{symbol}")
async def get_quote(
    symbol: str,
    exchange: str = Query("NSE", description="Exchange: NSE / BSE / NFO / MCX"),
):
    """Get live quote for a single symbol."""
    fyers = get_fyers_client()
    sym_upper = symbol.upper()

    if fyers.is_live:
        fyers_sym = f"{exchange}:{sym_upper}-EQ" if exchange in ("NSE", "BSE") else f"{exchange}:{sym_upper}"
        q = fyers.get_quote(fyers_sym)
        if q:
            return q

    session = get_session()
    if not session.is_demo:
        q = session.get_ltp(exchange, sym_upper)
        if q:
            return q

    return _make_demo_quote(sym_upper)


# ── /market/screener ───────────────────────────────────────────────────────────

@router.get("/screener")
async def get_screener(
    filter: str = Query("all", description="all / gainers / losers"),
    limit: int = Query(50, ge=5, le=200),
):
    """Return stock screener data. Live prices from Fyers; no random financial ratios."""
    fyers = get_fyers_client()
    symbols = NIFTY50_SYMBOLS[:limit]
    rows: list = []

    if fyers.is_live:
        live_rows = fyers.get_screener(symbols)
        if live_rows:
            for r in live_rows:
                base = r.get("ltp", 0)
                r.update({
                    "marketCap": 0,
                    "pe":        0,
                    "high52w":   round(max(r.get("high", base), base), 2),
                    "low52w":    round(min(r.get("low",  base), base), 2),
                    "rsi":       0,
                })
            rows = live_rows

    if not rows:
        session = get_session()
        if not session.is_demo:
            for sym in symbols:
                q = session.get_ltp("NSE", sym)
                if q:
                    base = q.get("ltp", 100)
                    rows.append({
                        "symbol": sym, "name": sym, "tradingsymbol": sym,
                        "exchange": "NSE", "ltp": base,
                        "change": q.get("change", 0), "changePct": q.get("changePct", 0),
                        "volume": q.get("volume", 0),
                        "marketCap": 0, "pe": 0,
                        "high52w": q.get("high", base), "low52w": q.get("low", base),
                        "rsi": 0, "source": "shoonya",
                    })

    if not rows:
        # No live source — return empty (no fake data)
        return {"data": [], "count": 0}

    if filter == "gainers":
        rows = [r for r in rows if r.get("changePct", 0) > 0]
    elif filter == "losers":
        rows = [r for r in rows if r.get("changePct", 0) < 0]

    rows.sort(key=lambda r: abs(r.get("changePct", 0)), reverse=True)
    return {"data": rows, "count": len(rows)}


# ── /market/option-chain/{symbol} ─────────────────────────────────────────────

def _convert_fyers_oc(fyers_data: dict, underlying: str) -> Optional[dict]:
    """Convert Fyers optionchain response to our standard OptionChainData format."""
    try:
        chain       = fyers_data.get("optionsChain", [])
        expiry_list = fyers_data.get("expiryData", [])

        # Spot from the index row (strike_price == -1)
        index_row = next((r for r in chain if r.get("strike_price") == -1), None)
        spot = index_row["ltp"] if index_row else 0

        nearest_expiry = expiry_list[0]["date"] if expiry_list else ""
        expiries       = [e["date"] for e in expiry_list[:12]]

        by_strike: dict = defaultdict(dict)
        for row in chain:
            if row.get("strike_price", -1) < 0:
                continue
            strike   = row["strike_price"]
            opt_type = row.get("option_type", "")
            leg = {
                "ltp":      row.get("ltp", 0),
                "iv":       0.0,
                "delta":    0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0,
                "oi":       row.get("oi", 0),
                "oiChange": row.get("oich", 0),
                "volume":   row.get("volume", 0),
                "bid":      row.get("bid", 0),
                "ask":      row.get("ask", 0),
            }
            if opt_type == "CE":
                by_strike[strike]["call"] = leg
            elif opt_type == "PE":
                by_strike[strike]["put"]  = leg

        if not spot or not by_strike:
            return None

        atm_strike = min(by_strike.keys(), key=lambda s: abs(s - spot))

        rows = []
        for strike in sorted(by_strike.keys()):
            d = by_strike[strike]
            if "call" not in d or "put" not in d:
                continue
            rows.append({"strike": strike, "isATM": strike == atm_strike,
                         "call": d["call"], "put": d["put"]})

        if not rows:
            return None

        total_ce_oi = sum(r["call"]["oi"] for r in rows)
        total_pe_oi = sum(r["put"]["oi"]  for r in rows)
        pcr = round(total_pe_oi / max(total_ce_oi, 1), 4)

        try:
            max_pain = min(rows, key=lambda row: sum(
                max(0, row2["strike"] - row["strike"]) * row2["call"]["oi"] +
                max(0, row["strike"] - row2["strike"]) * row2["put"]["oi"]
                for row2 in rows
            ))["strike"]
        except Exception:
            max_pain = atm_strike

        return {
            "underlying":    underlying,
            "underlyingLtp": spot,
            "expiry":        nearest_expiry,
            "expiries":      expiries,
            "pcr":           pcr,
            "maxPainStrike": max_pain,
            "rows":          rows,
            "source":        "fyers",
        }
    except Exception as e:
        logger.debug("Fyers OC conversion error: %s", e)
        return None


@router.get("/option-chain/{symbol}")
async def get_option_chain(
    symbol: str,
    exchange: str = Query("NSE"),
    strikes: int = Query(10, ge=5, le=30),
    expiry: Optional[str] = Query(None),
):
    """Return option chain. Fyers primary → Shoonya → demo."""
    sym   = symbol.upper()
    fyers = get_fyers_client()

    if fyers.is_live:
        raw = fyers.get_option_chain(sym)
        if raw:
            oc = _convert_fyers_oc(raw, sym)
            if oc:
                return oc

    session = get_session()
    if not session.is_demo:
        data = session.get_option_chain(exchange=exchange, symbol=sym, strike_count=strikes)
        if data:
            return data

    from broker.shoonya_client import _make_demo_option_chain
    return _make_demo_option_chain(sym, strikes)


# ── /market/search ─────────────────────────────────────────────────────────────

@router.get("/search")
async def search_instruments(
    q: str = Query(..., min_length=1, max_length=50),
    exchange: str = Query("NSE"),
):
    """Search instruments by symbol prefix."""
    session = get_session()
    q_upper = q.upper()

    results = []
    if not session.is_demo and hasattr(session, "_client") and session._client and hasattr(session._client, "searchscrip"):
        try:
            resp = session._client.searchscrip(exchange=exchange, searchtext=q_upper)
            if resp and isinstance(resp, dict) and resp.get("values"):
                for item in resp["values"][:20]:
                    results.append({
                        "symbol": item.get("tsym", ""),
                        "name": item.get("cname", ""),
                        "exchange": exchange,
                        "token": item.get("token", ""),
                        "type": item.get("instname", "EQ"),
                    })
        except Exception as e:
            logger.debug("searchscrip error: %s", e)

    if not results:
        all_symbols = INDICES + NIFTY50_SYMBOLS
        results = [
            {"symbol": s, "name": s, "exchange": "NSE", "token": "",
             "type": "INDEX" if s in INDICES else "EQ"}
            for s in all_symbols if q_upper in s
        ][:10]

    return {"data": results, "query": q}


# ── /market/fyers-status ───────────────────────────────────────────────────────

@router.get("/fyers-status")
async def fyers_status():
    """Return Fyers data provider status."""
    from core.config import config
    fyers = get_fyers_client()
    # app_id from live client takes precedence over config (legacy path auto-load)
    live_app_id = fyers._app_id if fyers.is_live else None
    return {
        "live":       fyers.is_live,
        "configured": fyers.is_live or config.is_fyers_configured(),
        "app_id":     live_app_id or config.FYERS_APP_ID or None,
    }


@router.post("/fyers-reload")
async def fyers_reload():
    """Force-reload the Fyers client (useful after token refresh)."""
    fyers = get_fyers_client()
    fyers.reload()
    return {"live": fyers.is_live}
