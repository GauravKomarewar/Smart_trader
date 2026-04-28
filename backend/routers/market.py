"""Market data router — quotes, option chain, indices, global markets."""

import re
import logging
from collections import defaultdict
from fastapi import APIRouter, Query
from typing import Optional

from broker.fyers_client import get_fyers_client
from broker.shoonya_client import get_session, _make_demo_quote
from broker.symbol_normalizer import (
    search_instruments, get_expiries, get_lot_size,
    enrich_option_chain_row, to_broker_symbol, from_broker_symbol,
    lookup_by_trading_symbol, expiry_to_iso, resolve_symbol_for_broker,
    build_option_chain_from_scriptmaster,
)

logger = logging.getLogger("smart_trader.api")
router = APIRouter(prefix="/market", tags=["market"])


# Tiny in-process cache for option-chain responses to avoid repeated broker calls
# from concurrent WS/REST requests in the same second.
_option_chain_cache: dict[str, tuple[float, dict]] = {}
_OPTION_CHAIN_CACHE_TTL_SEC = 1.0


def _detect_exchange(sym: str, fallback: str = "NSE") -> str:
    """Auto-detect exchange from symbol pattern. Options/futures → NFO, else fallback."""
    upper = sym.upper().replace(" ", "")
    fallback_upper = (fallback or "NSE").upper()

    # Index aliases must never drift into derivatives/currency segments.
    if upper in {
        "NIFTY50", "NIFTY", "NIFTYBANK", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"
    }:
        return "NSE"
    if upper in {"SENSEX", "BANKEX"}:
        return "BSE"

    # Resolve in the requested fallback exchange first to avoid cross-segment
    # accidental matches (e.g. CDS rows outranking NSE index rows).
    inst = None
    if fallback_upper:
        inst = lookup_by_trading_symbol(upper, fallback_upper)
        if not inst:
            inst = lookup_by_trading_symbol(f"{fallback_upper}:{upper}", fallback_upper)
    if not inst:
        inst = lookup_by_trading_symbol(upper)
    if inst and getattr(inst, "exchange", None):
        return str(inst.exchange)
    if re.search(r'\d{3,}(CE|PE)$', upper):
        return "NFO"
    if re.search(r'\d{2}[A-Z]{3}\d{2}[CP]\d+$', upper):
        return "NFO"
    if re.search(r'\d+FUT$', upper):
        return "NFO"
    return fallback_upper


def _as_broker_symbol(sym: str, exchange: str) -> str:
    """Build a best-effort broker symbol when scriptmaster lookup is unavailable."""
    s = sym.upper().replace(" ", "")
    ex = (exchange or "NSE").upper()
    # Dated derivatives like GOLDPETAL30APR26 / NIFTY24APR25.
    # If CE/PE missing and FUT missing, treat as futures contract.
    if re.search(r"\d{1,2}[A-Z]{3}\d{2}$", s) and not re.search(r"(CE|PE)$", s) and not s.endswith("FUT"):
        if ex in ("NFO", "BFO", "MCX"):
            return f"{ex}:{s}FUT"
    if ex in ("NFO", "BFO", "MCX"):
        return f"{ex}:{s}"
    if ex in ("NSE", "BSE"):
        return f"{ex}:{s}-EQ"
    return f"{ex}:{s}"


def _resolve_fyers_symbol(sym: str, exchange: str) -> tuple[str, str, Optional[object]]:
    """Resolve user symbol to canonical Fyers symbol via symbols DB first."""
    resolved_exchange = _detect_exchange(sym, exchange)
    canonical, ex, fyers_sym = resolve_symbol_for_broker(sym, resolved_exchange, "fyers")
    inst = lookup_by_trading_symbol(canonical, ex) or lookup_by_trading_symbol(f"{ex}:{canonical}", ex)
    if not fyers_sym or ":" not in fyers_sym:
        fyers_sym = _as_broker_symbol(sym, ex)
    return fyers_sym, ex, inst

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

# Common display name → Fyers symbol mapping for indices
_INDEX_ALIAS: dict = {
    "NIFTY 50":    "NSE:NIFTY50-INDEX",
    "NIFTY50":     "NSE:NIFTY50-INDEX",
    "NIFTY":       "NSE:NIFTY50-INDEX",
    "NIFTY BANK":  "NSE:NIFTYBANK-INDEX",
    "NIFTYBANK":   "NSE:NIFTYBANK-INDEX",
    "BANKNIFTY":   "NSE:NIFTYBANK-INDEX",
    "BANK NIFTY":  "NSE:NIFTYBANK-INDEX",
    "FINNIFTY":    "NSE:FINNIFTY-INDEX",
    "FIN NIFTY":   "NSE:FINNIFTY-INDEX",
    "MIDCPNIFTY":  "NSE:MIDCPNIFTY-INDEX",
    "SENSEX":      "BSE:SENSEX-INDEX",
}

@router.get("/quote/{symbol}")
async def get_quote(
    symbol: str,
    exchange: str = Query("NSE", description="Exchange: NSE / BSE / NFO / MCX"),
):
    """Get live quote for a single symbol. Resolves via ScriptMaster when available."""
    fyers = get_fyers_client()
    sym_upper = symbol.upper().strip()

    if fyers.is_live:
        # 1) Check index alias table (handles "NIFTY 50", "NIFTY BANK" etc.)
        fyers_sym = _INDEX_ALIAS.get(sym_upper)
        inst = None
        if not fyers_sym:
            fyers_sym, _, inst = _resolve_fyers_symbol(sym_upper, exchange)
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
        q = session.get_ltp(_detect_exchange(sym_upper, exchange), sym_upper)
        if q:
            return q

    return _make_demo_quote(sym_upper)


# ── /market/depth/{symbol} ─────────────────────────────────────────────────────

@router.get("/depth/{symbol}")
async def get_market_depth(
    symbol: str,
    exchange: str = Query("NSE", description="Exchange: NSE / BSE / NFO / MCX"),
):
    """Get market depth (best 5 bids/asks) for a symbol via Fyers depth API."""
    fyers = get_fyers_client()
    sym_upper = symbol.upper().strip()
    effective_exchange = _detect_exchange(sym_upper, exchange)

    if fyers.is_live:
        depth_data = fyers.get_depth(sym_upper, effective_exchange)
        if depth_data:
            return depth_data

    # Fallback: return empty depth structure
    return {
        "symbol": sym_upper,
        "exchange": effective_exchange,
        "bids": [],
        "asks": [],
        "ltp": 0, "open": 0, "high": 0, "low": 0, "close": 0,
        "volume": 0, "oi": 0,
        "total_buy_qty": 0, "total_sell_qty": 0,
    }


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


# ── /market/fno-underlyings ────────────────────────────────────────────────────

# Simple in-process cache: refresh at most every 5 minutes
_fno_underlyings_cache: list = []
_fno_underlyings_ts: float = 0.0

@router.get("/fno-underlyings")
async def get_fno_underlyings():
    """
    Return distinct FNO underlying symbols from the symbols DB.

    Response: [{"symbol": "NIFTY", "exchange": "NFO", "instrument_types": ["OPT", "FUT"]}, ...]

    Used by OptionChainPage, StrategyPage and WatchlistPage to populate symbol dropdowns.
    Falls back to a hardcoded list when the DB is unavailable.
    """
    import time as _t
    global _fno_underlyings_cache, _fno_underlyings_ts

    # Serve from cache if fresh (< 5 min)
    if _fno_underlyings_cache and (_t.time() - _fno_underlyings_ts) < 300:
        return _fno_underlyings_cache

    try:
        from db.symbols_db import get_fno_underlyings as _db_fno
        rows = _db_fno()
        if rows:
            _fno_underlyings_cache = rows
            _fno_underlyings_ts = _t.time()
            return rows
    except Exception as exc:
        logger.warning("DB fno-underlyings failed, using fallback: %s", exc)

    # Hardcoded fallback when DB unavailable
    return [
        {"symbol": "NIFTY",      "exchange": "NFO", "instrument_types": ["OPT", "FUT"]},
        {"symbol": "BANKNIFTY",  "exchange": "NFO", "instrument_types": ["OPT", "FUT"]},
        {"symbol": "FINNIFTY",   "exchange": "NFO", "instrument_types": ["OPT", "FUT"]},
        {"symbol": "MIDCPNIFTY", "exchange": "NFO", "instrument_types": ["OPT", "FUT"]},
        {"symbol": "SENSEX",     "exchange": "BFO", "instrument_types": ["OPT"]},
        {"symbol": "BANKEX",     "exchange": "BFO", "instrument_types": ["OPT"]},
        {"symbol": "CRUDEOIL",   "exchange": "MCX", "instrument_types": ["OPT", "FUT"]},
        {"symbol": "GOLD",       "exchange": "MCX", "instrument_types": ["OPT", "FUT"]},
        {"symbol": "SILVER",     "exchange": "MCX", "instrument_types": ["OPT", "FUT"]},
        {"symbol": "USDINR",     "exchange": "CDS", "instrument_types": ["OPT", "FUT"]},
    ]


# ── /market/option-chain/{symbol} ─────────────────────────────────────────────

def _normalise_expiry_date(raw: str) -> str:
    """Normalise any date string to ISO YYYY-MM-DD for consistent frontend parsing."""
    if not raw:
        return ""
    # Already ISO  "2026-04-07"
    if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
        return raw
    # DD-MM-YYYY  "07-04-2026"
    if len(raw) == 10 and raw[2] == "-" and raw[5] == "-":
        dd, mm, yyyy = raw.split("-")
        return f"{yyyy}-{mm}-{dd}"
    # Epoch seconds
    try:
        ts = int(raw)
        from datetime import datetime as _dt
        return _dt.utcfromtimestamp(ts).strftime("%Y-%m-%d")
    except (ValueError, OSError):
        pass
    return raw


# Cache expiry timestamp mapping for Fyers expiry switching
_fyers_expiry_ts: dict = {}   # "NIFTY" → { "2026-04-07": 1775688000, ... }


def _convert_fyers_oc(fyers_data: dict, underlying: str) -> Optional[dict]:
    """Convert Fyers optionchain response to our standard OptionChainData format."""
    try:
        chain       = fyers_data.get("optionsChain", [])
        expiry_list = fyers_data.get("expiryData", [])

        # Spot from the index row (strike_price == -1)
        index_row = next((r for r in chain if r.get("strike_price") == -1), None)
        spot = index_row["ltp"] if index_row else 0

        # Build normalised expiries + timestamp mapping for switching
        nearest_raw = expiry_list[0]["date"] if expiry_list else ""
        nearest_expiry = _normalise_expiry_date(nearest_raw)
        expiries: list[str] = []
        ts_map: dict[str, str] = {}
        for e in expiry_list[:12]:
            iso = _normalise_expiry_date(e.get("date", ""))
            expiries.append(iso)
            # store the Fyers timestamp for later look-up
            raw_ts = e.get("expiry") or e.get("timestamp") or ""
            if raw_ts:
                ts_map[iso] = str(raw_ts)
        _fyers_expiry_ts[underlying.upper()] = ts_map

        by_strike: dict = defaultdict(dict)
        for row in chain:
            if row.get("strike_price", -1) < 0:
                continue
            strike   = row["strike_price"]
            opt_type = row.get("option_type", "")
            _ltp  = row.get("ltp") or None      # 0 → None (no active trade price)
            leg = {
                "ltp":      _ltp,
                "iv":       None,    # greeks computed later by oc_svc.enrich_with_live_greeks
                "delta":    None, "gamma": None, "theta": None, "vega": None,
                "oi":       row.get("oi", 0),
                "oiChange": row.get("oich", 0),
                "volume":   row.get("volume", 0),
                "bid":      row.get("bid") or None,
                "ask":      row.get("ask") or None,
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
    strikes: int = Query(15, ge=5, le=30),
    expiry: Optional[str] = Query(None),
):
    """Return option chain.

    Priority: Fyers live → Shoonya live → ScriptMaster (structure only) → empty.
    Response rows are enriched with trading_symbol and lot_size from ScriptMaster
    so the frontend can directly place orders / build basket legs.
    """
    sym   = symbol.upper()
    fyers = get_fyers_client()
    oc = None
    cache_key = f"{sym}:{exchange.upper()}:{expiry or ''}:{int(strikes)}"

    try:
        import time as _t
        cached = _option_chain_cache.get(cache_key)
        if cached and (_t.time() - cached[0]) <= _OPTION_CHAIN_CACHE_TTL_SEC:
            return cached[1]
    except Exception:
        pass

    # 1) Fyers live data — resolve expiry to Fyers timestamp if switching
    if fyers.is_live:
        fyers_ts = ""
        if expiry:
            # Convert ISO date "2026-04-07" or DD-MM-YYYY to Fyers timestamp
            iso_exp = _normalise_expiry_date(expiry)
            ts_map = _fyers_expiry_ts.get(sym, {})
            fyers_ts = ts_map.get(iso_exp, "")
        raw = fyers.get_option_chain(sym, expiry_ts=fyers_ts)
        if raw:
            oc = _convert_fyers_oc(raw, sym)

    # 2) Shoonya live data
    if not oc:
        session = get_session()
        if not session.is_demo:
            data = session.get_option_chain(exchange=exchange, symbol=sym, strike_count=strikes)
            if data and data.get("rows"):
                oc = data

    # 3) ScriptMaster chain (structure with strikes, zero prices)
    # Map equity exchange to F&O exchange for option chain lookup
    _fo_map = {"NSE": "NFO", "NFO": "NFO", "BSE": "BFO", "BFO": "BFO",
               "MCX": "MCX", "CDS": "CDS"}
    oc_exchange = _fo_map.get(exchange, exchange)
    if not oc or not oc.get("rows"):
        # Try to get live spot price for better ATM selection
        spot = 0.0
        if fyers.is_live:
            idx_sym = _INDEX_ALIAS.get(sym) or _INDEX_ALIAS.get(sym.replace(" ", ""))
            if idx_sym:
                q = fyers.get_quote(idx_sym)
                if q:
                    spot = q.get("ltp", 0)
        sm_chain = build_option_chain_from_scriptmaster(
            sym, exchange=oc_exchange, expiry=expiry or "",
            strikes_per_side=strikes, spot_price=spot,
        )
        if sm_chain and sm_chain.get("rows"):
            oc = sm_chain

    # 4) Empty fallback
    if not oc:
        oc = {
            "underlying": sym, "underlyingLtp": 0,
            "expiry": "", "expiries": get_expiries(sym, exchange=oc_exchange, instrument_type="OPT"),
            "pcr": 0, "maxPainStrike": 0, "rows": [],
            "lot_size": get_lot_size(sym, oc_exchange), "source": "unavailable",
        }

    # Enrich with ScriptMaster expiries if broker didn't provide them
    if not oc.get("expiries"):
        oc["expiries"] = get_expiries(sym, exchange=oc_exchange, instrument_type="OPT")

    # Enrich each row with trading_symbol + lot_size for basket ordering
    # (skip if source is scriptmaster — already enriched)
    if oc.get("source") != "scriptmaster":
        oc_expiry = oc.get("expiry", "")
        for row in oc.get("rows", []):
            enrich_option_chain_row(row, sym, oc_exchange, oc_expiry)

    # Add lot_size and exchange at top level
    oc.setdefault("lot_size", get_lot_size(sym, oc_exchange))
    oc["exchange"] = oc_exchange

    # ── Subscribe option symbols to live tick service + compute Greeks ────────
    try:
        from broker.option_chain_service import get_oc_service
        oc_svc = get_oc_service()
        # Subscribe to live ticks for all option symbols in this chain
        oc_svc.subscribe_chain(oc, sym, oc.get("expiry", ""))
        # Enrich with live LTPs from tick store and compute Greeks
        oc = oc_svc.enrich_with_live_greeks(oc)
        # Persist snapshot (non-blocking — runs async in background)
        import threading
        threading.Thread(
            target=oc_svc.persist_snapshot, args=(oc,), daemon=True
        ).start()
    except Exception as _oc_err:
        logger.debug("OC service enrich failed (non-fatal): %s", _oc_err)

    try:
        import time as _t
        _option_chain_cache[cache_key] = (_t.time(), oc)
        # Keep cache bounded to prevent unbounded growth with random symbols.
        if len(_option_chain_cache) > 256:
            oldest_key = min(_option_chain_cache.items(), key=lambda x: x[1][0])[0]
            _option_chain_cache.pop(oldest_key, None)
    except Exception:
        pass

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
    Search instruments across ALL broker scriptmasters (Fyers + Shoonya).

    Returns unified results with per-broker trading symbols for
    NSE, NFO, BSE, BFO, MCX exchanges.
    """
    q_upper = q.upper().strip()
    results = []

    # Primary: unified symbols DB (canonical, normalized, broker-enriched)
    try:
        from db.symbols_db import search_symbols as db_search_symbols

        db_rows = db_search_symbols(
            q_upper,
            exchange=exchange,
            instrument_type=instrument_type,
            limit=30,
        )
        for rec in db_rows:
            results.append({
                "symbol":                    rec.get("symbol", ""),
                "trading_symbol":            rec.get("trading_symbol", ""),
                "tradingsymbol":             rec.get("trading_symbol", ""),
                "normalized_trading_symbol": rec.get("normalized_trading_symbol") or rec.get("trading_symbol", ""),
                "exchange":                  rec.get("exchange", ""),
                "type":                      rec.get("instrument_type", "EQ"),
                "token":                     rec.get("exchange_token", ""),
                "name":                      rec.get("description") or rec.get("symbol", ""),
                "lot_size":                  rec.get("lot_size", 1),
                "expiry":                    str(rec.get("expiry") or ""),
                "strike":                    rec.get("strike", 0),
                "option_type":               rec.get("option_type", ""),
                "fyers_symbol":              rec.get("fyers_symbol", ""),
                "shoonya_symbol":            rec.get("shoonya_tsym", ""),
                "shoonya_token":             rec.get("shoonya_token", ""),
                "angelone_symbol":           rec.get("angelone_tsym", ""),
                "angelone_token":            rec.get("angelone_token", ""),
                "dhan_security_id":          rec.get("dhan_security_id", ""),
                "kite_instrument_token":     rec.get("kite_instrument_token", ""),
                "upstox_ikey":               rec.get("upstox_ikey", ""),
            })
    except Exception as exc:
        logger.warning("Symbols DB search error: %s", exc)

    # Secondary fallback: Unified ScriptMaster search (raw broker dumps)
    if not results:
        try:
            from scripts.unified_scriptmaster import search_all
            unified = search_all(q_upper, exchange=exchange,
                                  instrument_type=instrument_type, limit=30)
            for rec in unified:
                results.append({
                    "symbol":          rec.get("symbol", ""),
                    "trading_symbol":  rec.get("trading_symbol", ""),
                    "tradingsymbol":   rec.get("trading_symbol", ""),
                    "exchange":        rec.get("exchange", ""),
                    "type":            rec.get("instrument_type", "EQ"),
                    "token":           rec.get("token", ""),
                    "name":            rec.get("description") or rec.get("symbol", ""),
                    "lot_size":        rec.get("lot_size", 1),
                    "expiry":          rec.get("expiry", ""),
                    "strike":          rec.get("strike", 0),
                    "option_type":     rec.get("option_type", ""),
                    "fyers_symbol":    rec.get("fyers_symbol", ""),
                    "shoonya_symbol":  rec.get("shoonya_symbol", ""),
                })
        except Exception as exc:
            logger.warning("Unified search error: %s", exc)

    # Fallback: old ScriptMaster search
    if not results:
        normalized = search_instruments(q_upper, exchange=exchange,
                                         instrument_type=instrument_type, limit=30)
        if normalized:
            results = [inst.to_search_result() for inst in normalized]

    # Fallback: live broker searchscrip
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


# ── /market/symbols — Unified Symbols DB endpoints ──────────────────────────

@router.get("/symbols/lookup")
async def symbols_lookup(
    exchange: str = Query("", description="Exchange: NSE / NFO / MCX"),
    token: str = Query("", description="Exchange token number"),
    trading_symbol: str = Query("", alias="tsym", description="Trading symbol"),
    fyers_symbol: str = Query("", alias="fyers", description="Fyers symbol"),
):
    """Lookup a single symbol from the unified symbols DB by token, trading_symbol, or fyers_symbol."""
    try:
        from db.symbols_db import lookup_by_token, lookup_by_trading_symbol, lookup_by_fyers_symbol
        rec = None
        if exchange and token:
            rec = lookup_by_token(exchange, token)
        elif fyers_symbol:
            rec = lookup_by_fyers_symbol(fyers_symbol)
        elif trading_symbol:
            rec = lookup_by_trading_symbol(trading_symbol, exchange)
        if rec:
            return {"found": True, "data": rec}
        return {"found": False, "data": None}
    except Exception as e:
        logger.debug("Symbol lookup error: %s", e)
        return {"found": False, "data": None, "error": str(e)}


@router.get("/symbols/search")
async def symbols_search(
    q: str = Query(..., min_length=1, max_length=50),
    exchange: str = Query(""),
    instrument_type: str = Query("", alias="type"),
    limit: int = Query(30, ge=1, le=100),
):
    """Search the unified symbols DB. Faster than in-memory ScriptMaster for large sets."""
    try:
        from db.symbols_db import search_symbols
        results = search_symbols(q, exchange=exchange, instrument_type=instrument_type, limit=limit)
        return {"data": results, "count": len(results), "query": q}
    except Exception as e:
        logger.debug("Symbol search error: %s", e)
        return {"data": [], "count": 0, "query": q, "error": str(e)}


@router.get("/symbols/stats")
async def symbols_stats():
    """Get summary statistics of the unified symbols DB."""
    try:
        from db.symbols_db import get_symbol_count
        from db.trading_db import get_trading_conn
        import psycopg2.extras
        conn = get_trading_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("""
                SELECT exchange, instrument_type, COUNT(*) as cnt
                FROM symbols GROUP BY exchange, instrument_type
                ORDER BY exchange, instrument_type
            """)
            breakdown = [dict(r) for r in cur.fetchall()]
            total = sum(r["cnt"] for r in breakdown)
            # Broker coverage
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE fyers_symbol != '') as fyers,
                    COUNT(*) FILTER (WHERE shoonya_token != '') as shoonya,
                    COUNT(*) FILTER (WHERE angelone_token != '') as angelone,
                    COUNT(*) FILTER (WHERE dhan_security_id != '') as dhan,
                    COUNT(*) FILTER (WHERE kite_instrument_token != '') as kite
                FROM symbols
            """)
            coverage = dict(cur.fetchone() or {})
        finally:
            conn.close()
        return {"total": total, "breakdown": breakdown, "broker_coverage": coverage}
    except Exception as e:
        return {"total": 0, "error": str(e)}


# ── /market/ohlcv/{symbol} — chart candle data from stored ticks ───────────────

@router.get("/ohlcv/{symbol}")
async def get_ohlcv(
    symbol: str,
    timeframe: str = Query("1m", description="1m | 3m | 5m | 15m | 30m | 1h | 4h | D"),
    exchange: str = Query("NSE"),
    limit: int = Query(500, ge=1, le=2000),
):
    """
    Return OHLCV candles for a symbol from the `market_ohlcv` table.
    If the timeframe is not 1m, candles are re-sampled from 1m bars.
    """
    sym = symbol.upper().replace(" ", "")
    # Auto-detect exchange for option/future symbols
    exchange = _detect_exchange(sym, exchange)
    _TF_MINUTES = {
        "1m": 1, "3m": 3, "5m": 5, "15m": 15,
        "30m": 30, "1h": 60, "4h": 240, "D": 1440,
    }
    tf_min = _TF_MINUTES.get(timeframe, 1)

    candles: list = []

    # Normalize well-known index aliases so "NIFTY50" also matches DB rows stored as "NIFTY"
    _SYM_ALIASES: dict = {
        "NIFTY50": "NIFTY", "NIFTY 50": "NIFTY",
        "NIFTYBANK": "NIFTYBANK", "BANKNIFTY": "NIFTYBANK",
    }
    db_sym = _SYM_ALIASES.get(sym, sym)

    def _rows_to_candles(rows):
        return [
            {
                "time":   int(row["bar_time"].timestamp()),
                "open":   row["open"],
                "high":   row["high"],
                "low":    row["low"],
                "close":  row["close"],
                "volume": row.get("volume", 0),
                "oi":     row.get("oi") or 0,
            }
            for row in reversed(rows)
        ]

    # 1) Try local DB first
    try:
        from db.trading_db import trading_cursor
        with trading_cursor() as cur:
            # Try all symbol variants to search — original sym and any alias
            sym_variants = list(dict.fromkeys([db_sym, sym]))  # deduplicated, alias first
            rows = []
            for s in sym_variants:
                if tf_min == 1:
                    cur.execute("""
                        SELECT bar_time, open, high, low, close, volume, oi
                        FROM market_ohlcv
                        WHERE symbol = %s AND exchange = %s AND timeframe = '1m'
                        ORDER BY bar_time DESC
                        LIMIT %s
                    """, (s, exchange, limit))
                else:
                    cur.execute("""
                        SELECT
                            date_trunc('minute', bar_time -
                                (EXTRACT(MINUTE FROM bar_time)::int %% %s) * interval '1 minute')
                                AS bar_time,
                            (array_agg(open  ORDER BY bar_time))[1] AS open,
                            MAX(high)     AS high,
                            MIN(low)      AS low,
                            (array_agg(close ORDER BY bar_time DESC))[1] AS close,
                            SUM(volume)   AS volume,
                            (array_agg(oi   ORDER BY bar_time DESC))[1] AS oi
                        FROM market_ohlcv
                        WHERE symbol = %s AND exchange = %s AND timeframe = '1m'
                        GROUP BY 1
                        ORDER BY 1 DESC
                        LIMIT %s
                    """, (tf_min, s, exchange, limit))
                rows = cur.fetchall()
                if rows:
                    break

            # Fallback: drop exchange constraint — handles indices stored under a
            # different exchange code (e.g. NIFTY50 stored as CDS vs queried as NSE)
            if not rows:
                for s in sym_variants:
                    if tf_min == 1:
                        cur.execute("""
                            SELECT bar_time, open, high, low, close, volume, oi
                            FROM market_ohlcv
                            WHERE symbol = %s AND timeframe = '1m'
                            ORDER BY bar_time DESC
                            LIMIT %s
                        """, (s, limit))
                    else:
                        cur.execute("""
                            SELECT
                                date_trunc('minute', bar_time -
                                    (EXTRACT(MINUTE FROM bar_time)::int %% %s) * interval '1 minute')
                                    AS bar_time,
                                (array_agg(open  ORDER BY bar_time))[1] AS open,
                                MAX(high)     AS high,
                                MIN(low)      AS low,
                                (array_agg(close ORDER BY bar_time DESC))[1] AS close,
                                SUM(volume)   AS volume,
                                (array_agg(oi   ORDER BY bar_time DESC))[1] AS oi
                            FROM market_ohlcv
                            WHERE symbol = %s AND timeframe = '1m'
                            GROUP BY 1
                            ORDER BY 1 DESC
                            LIMIT %s
                        """, (tf_min, s, limit))
                    rows = cur.fetchall()
                    if rows:
                        break

        candles = _rows_to_candles(rows)
    except Exception as e:
        logger.debug("OHLCV DB fetch error for %s: %s", sym, e)

    # 2) Fallback: Fyers historical candle API when DB has no data
    if not candles:
        try:
            fyers = get_fyers_client()
            if fyers.is_live:
                # Resolve to Fyers symbol
                fyers_sym = _INDEX_ALIAS.get(sym)
                if not fyers_sym:
                    fyers_sym, _, _ = _resolve_fyers_symbol(sym, exchange)
                # Map days_back based on timeframe
                days = 5 if tf_min <= 5 else (30 if tf_min <= 60 else 365)
                candles = fyers.get_history(fyers_sym, timeframe, days)
                if candles:
                    candles = candles[-limit:]
        except Exception as e:
            logger.debug("OHLCV Fyers fallback error for %s: %s", sym, e)

    return {"symbol": sym, "exchange": exchange, "timeframe": timeframe, "candles": candles}


# ── /market/subscribe — REST-based subscription (for non-WS clients) ──────────

@router.post("/subscribe")
async def subscribe_symbols(
    body: dict,
):
    """
    Subscribe symbols to the live tick service via REST.
    Body: {"symbols": ["NIFTY", "RELIANCE", ...]}
    """
    symbols = [str(s).upper() for s in body.get("symbols", [])]
    if not symbols:
        return {"subscribed": [], "message": "No symbols provided"}
    try:
        from broker.live_tick_service import get_tick_service
        svc = get_tick_service()
        svc.subscribe(symbols)
        return {"subscribed": symbols, "message": f"Subscribed {len(symbols)} symbols"}
    except Exception as e:
        logger.warning("subscribe_symbols error: %s", e)
        return {"subscribed": [], "message": str(e)}


@router.get("/tick/{symbol}")
async def get_latest_tick(
    symbol: str,
    exchange: str = Query("NSE"),
):
    """Return the latest tick for a symbol from the in-memory tick store."""
    sym = symbol.upper()
    try:
        from broker.live_tick_service import get_tick_service
        svc = get_tick_service()
        tick = svc.get_latest(sym)
        if tick:
            return {"symbol": sym, "tick": tick, "source": "live"}
    except Exception:
        pass
    # Fallback: REST quote from broker
    fyers = get_fyers_client()
    if fyers.is_live:
        fyers_sym, _, _ = _resolve_fyers_symbol(sym, exchange)
        q = fyers.get_quote(fyers_sym)
        if q:
            return {"symbol": sym, "tick": q, "source": "fyers_rest"}
    return {"symbol": sym, "tick": None, "source": "unavailable"}
