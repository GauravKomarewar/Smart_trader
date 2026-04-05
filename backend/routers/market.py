"""Market data router — quotes, option chain, indices, global markets."""

import logging
from collections import defaultdict
from fastapi import APIRouter, Query
from typing import Optional

from broker.fyers_client import get_fyers_client
from broker.shoonya_client import get_session, _make_demo_quote
from broker.symbol_normalizer import (
    search_instruments, get_expiries, get_lot_size,
    enrich_option_chain_row, to_broker_symbol, from_broker_symbol,
    lookup_by_trading_symbol, expiry_to_iso,
)

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
    """Get live quote for a single symbol. Resolves via ScriptMaster when available."""
    fyers = get_fyers_client()
    sym_upper = symbol.upper()

    if fyers.is_live:
        # Try ScriptMaster lookup first for proper Fyers symbol
        inst = lookup_by_trading_symbol(sym_upper)
        if not inst:
            inst = lookup_by_trading_symbol(f"{exchange}:{sym_upper}")
        if inst and inst.fyers_symbol:
            fyers_sym = inst.fyers_symbol
        elif exchange in ("NSE", "BSE"):
            fyers_sym = f"{exchange}:{sym_upper}-EQ"
        else:
            fyers_sym = f"{exchange}:{sym_upper}"
        q = fyers.get_quote(fyers_sym)
        if q:
            # Enrich with canonical symbol info
            if inst:
                q.setdefault("trading_symbol", inst.trading_symbol)
                q.setdefault("lot_size", inst.lot_size)
                q.setdefault("instrument_type", inst.instrument_type)
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
    """Return option chain. Fyers primary → Shoonya → demo.

    Response rows are enriched with trading_symbol and lot_size from ScriptMaster
    so the frontend can directly place orders / build basket legs.
    """
    sym   = symbol.upper()
    fyers = get_fyers_client()
    oc = None

    if fyers.is_live:
        raw = fyers.get_option_chain(sym)
        if raw:
            oc = _convert_fyers_oc(raw, sym)

    if not oc:
        session = get_session()
        if not session.is_demo:
            data = session.get_option_chain(exchange=exchange, symbol=sym, strike_count=strikes)
            if data:
                oc = data

    if not oc:
        from broker.shoonya_client import _make_demo_option_chain
        oc = _make_demo_option_chain(sym, strikes)

    # Enrich with ScriptMaster expiries if broker didn't provide them
    if not oc.get("expiries"):
        oc["expiries"] = get_expiries(sym, exchange="NFO", instrument_type="OPT")

    # Enrich each row with trading_symbol + lot_size for basket ordering
    oc_expiry = oc.get("expiry", "")
    oc_exchange = "NFO" if exchange in ("NSE", "NFO") else exchange
    for row in oc.get("rows", []):
        enrich_option_chain_row(row, sym, oc_exchange, oc_expiry)

    # Add lot_size at top level
    oc["lot_size"] = get_lot_size(sym, oc_exchange)

    return oc


@router.get("/expiries/{symbol}")
async def get_expiries_api(
    symbol: str,
    exchange: str = Query("NFO"),
    instrument_type: str = Query("OPT", alias="type"),
):
    """Return available expiry dates (ISO format) for a symbol from ScriptMaster."""
    return {
        "symbol": symbol.upper(),
        "exchange": exchange,
        "expiries": get_expiries(symbol.upper(), exchange, instrument_type),
    }


# ── /market/search ─────────────────────────────────────────────────────────────

@router.get("/search")
async def search_instruments_api(
    q: str = Query(..., min_length=1, max_length=50),
    exchange: str = Query(""),
    instrument_type: str = Query("", alias="type"),
):
    """
    Search instruments from ScriptMaster — broker-agnostic, always available.

    Falls back to broker searchscrip only if ScriptMaster returns nothing.
    """
    q_upper = q.upper().strip()
    results = []

    # Primary: ScriptMaster search (120K+ instruments, no broker needed)
    normalized = search_instruments(q_upper, exchange=exchange,
                                     instrument_type=instrument_type, limit=30)
    if normalized:
        results = [inst.to_search_result() for inst in normalized]

    # Fallback: live broker searchscrip (for symbols not in Fyers CSVs)
    if not results:
        session = get_session()
        exch = exchange or "NSE"
        if not session.is_demo and hasattr(session, "_client") and session._client and hasattr(session._client, "searchscrip"):
            try:
                resp = session._client.searchscrip(exchange=exch, searchtext=q_upper)
                if resp and isinstance(resp, dict) and resp.get("values"):
                    for item in resp["values"][:20]:
                        tsym = item.get("tsym", "")
                        results.append({
                            "symbol": tsym,
                            "trading_symbol": tsym,
                            "tradingsymbol": tsym,
                            "exchange": exch,
                            "token": item.get("token", ""),
                            "type": item.get("instname", "EQ"),
                            "name": item.get("cname", tsym),
                            "lot_size": 1,
                            "expiry": "",
                            "strike": 0,
                            "option_type": "",
                        })
            except Exception as e:
                logger.debug("searchscrip error: %s", e)

    # Last-resort fallback
    if not results:
        all_symbols = INDICES + NIFTY50_SYMBOLS
        results = [
            {"symbol": s, "trading_symbol": s, "tradingsymbol": s,
             "name": s, "exchange": "NSE", "token": "",
             "type": "INDEX" if s in INDICES else "EQ",
             "lot_size": 1, "expiry": "", "strike": 0, "option_type": ""}
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
