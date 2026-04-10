"""
Smart Trader — Universal Symbol Normalizer
============================================

Central abstraction layer that:
  1. Defines a canonical instrument format used everywhere in the app
  2. Converts canonical ↔ broker-specific symbol formats
  3. Provides ScriptMaster-powered instrument search / lookup
  4. Normalizes broker responses (positions, orders) to canonical format

Every UI page — place order, search, option chain, watchlist — goes through
this module so instrument data is consistent regardless of connected broker.

Canonical Symbol Format
-----------------------
  trading_symbol : str   — compact human-readable, NO exchange prefix
                           e.g. "NIFTY26APR24500CE", "RELIANCE-EQ"
  symbol         : str   — underlying name ("NIFTY", "RELIANCE")
  exchange       : str   — "NSE", "NFO", "BSE", "BFO", "MCX", "CDS"
  instrument_type: str   — "EQ", "FUT", "OPT", "IDX"
  expiry         : str   — ISO date "YYYY-MM-DD" or "" for EQ/IDX
  strike         : float — 0.0 for non-options
  option_type    : str   — "CE", "PE", or "" for non-options
  lot_size       : int   — contract lot size (1 for equity)
  tick_size      : float — minimum price movement
  token          : str   — exchange numeric token

Broker Symbol Formats
---------------------
  Fyers:   "EXCHANGE:SYMBOL"  → "NSE:NIFTY26APR24500CE"
  Shoonya: plain "SYMBOL"     → "NIFTY26APR24500CE"  + separate exchange
  Paper:   same as canonical  → "NIFTY26APR24500CE"
"""
from __future__ import annotations

import re
import logging
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger("smart_trader.symbol_normalizer")


# ── Canonical Data Model ─────────────────────────────────────────────────────

@dataclass
class NormalizedInstrument:
    """Canonical instrument representation used across the entire app."""
    symbol:          str            # Underlying: "NIFTY", "RELIANCE"
    trading_symbol:  str            # Compact: "NIFTY26APR24500CE" (no exchange prefix)
    exchange:        str            # "NSE", "NFO", "BSE", "BFO", "MCX"
    instrument_type: str            # "EQ", "FUT", "OPT", "IDX"
    expiry:          str = ""       # ISO "YYYY-MM-DD" or ""
    strike:          float = 0.0    # 0 for non-options
    option_type:     str = ""       # "CE", "PE", or ""
    lot_size:        int = 1
    tick_size:       float = 0.05
    token:           str = ""       # Exchange numeric token
    fyers_symbol:    str = ""       # Full Fyers key: "NSE:NIFTY26APR24500CE"
    description:     str = ""       # Human-readable description

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_search_result(self) -> Dict[str, Any]:
        """Format for /market/search API response (consumed by frontend)."""
        return {
            "symbol":          self.symbol,
            "trading_symbol":  self.trading_symbol,
            "tradingsymbol":   self.trading_symbol,   # legacy alias
            "exchange":        self.exchange,
            "type":            self.instrument_type,
            "token":           self.token,
            "name":            self.description or self.symbol,
            "lot_size":        self.lot_size,
            "expiry":          self.expiry,
            "strike":          self.strike,
            "option_type":     self.option_type,
            "fyers_symbol":    self.fyers_symbol,
            "shoonya_symbol":  self.trading_symbol,   # plain symbol = Shoonya format
        }


# ── Expiry format helpers ────────────────────────────────────────────────────

_MONTH_MAP = {
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
    "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
    "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
}
_MONTH_ABBR = {v: k for k, v in _MONTH_MAP.items()}


def expiry_to_iso(raw: str) -> str:
    """
    Convert any expiry string to ISO YYYY-MM-DD.

    Handles:
      "28-Apr-2026"     → "2026-04-28"
      "28-APR-2026"     → "2026-04-28"
      "2026-04-28"      → "2026-04-28"  (pass-through)
      "10-Apr-26"       → "2026-04-10"
      ""                → ""
    """
    if not raw:
        return ""
    raw = raw.strip()
    # Already ISO?
    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
        return raw
    # DD-Mon-YYYY  or  DD-Mon-YY
    m = re.match(r"^(\d{1,2})-([A-Za-z]{3})-(\d{2,4})$", raw)
    if m:
        day, mon, year = m.group(1), m.group(2).upper(), m.group(3)
        if len(year) == 2:
            year = "20" + year
        mm = _MONTH_MAP.get(mon[:3], "01")
        return f"{year}-{mm}-{day.zfill(2)}"
    # Try python datetime parse as fallback
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return raw


def iso_to_display(iso: str) -> str:
    """Convert ISO date to human display: "2026-04-28" → "28 Apr 2026"."""
    if not iso:
        return ""
    try:
        dt = datetime.strptime(iso, "%Y-%m-%d")
        return dt.strftime("%-d %b %Y")
    except ValueError:
        return iso


def iso_to_broker_expiry(iso: str) -> str:
    """Convert ISO date to DD-Mon-YYYY for ScriptMaster: "2026-04-28" → "28-Apr-2026"."""
    if not iso:
        return ""
    try:
        dt = datetime.strptime(iso, "%Y-%m-%d")
        return dt.strftime("%-d-%b-%Y")
    except ValueError:
        return iso


# ── Broker ↔ Canonical Symbol Conversion ─────────────────────────────────────

def to_broker_symbol(
    trading_symbol: str,
    exchange: str,
    broker: str,
) -> str:
    """
    Convert canonical trading_symbol to broker-specific format.

    Args:
        trading_symbol: "NIFTY26APR24500CE" (no exchange prefix)
        exchange:       "NFO"
        broker:         "fyers" | "shoonya" | "paper"

    Returns:
        Fyers:   "NFO:NIFTY26APR24500CE"
        Shoonya: "NIFTY26APR24500CE"
        Paper:   "NIFTY26APR24500CE"
    """
    # Strip any existing exchange prefix first
    clean = trading_symbol.split(":", 1)[-1] if ":" in trading_symbol else trading_symbol

    if broker == "fyers":
        return f"{exchange}:{clean}"
    # Shoonya and Paper use plain symbols
    return clean


def from_broker_symbol(
    broker_symbol: str,
    broker: str,
    fallback_exchange: str = "NSE",
) -> tuple:
    """
    Parse broker-specific symbol to (trading_symbol, exchange).

    Args:
        broker_symbol:    "NFO:NIFTY26APR24500CE" or "NIFTY26APR24500CE"
        broker:           "fyers" | "shoonya" | "paper"
        fallback_exchange: used if exchange can't be extracted

    Returns:
        ("NIFTY26APR24500CE", "NFO")
    """
    if ":" in broker_symbol:
        parts = broker_symbol.split(":", 1)
        return parts[1], parts[0]
    return broker_symbol, fallback_exchange


# ── Symbols DB-backed Search & Lookup ────────────────────────────────────────


def _record_to_normalized(rec: Dict[str, Any]) -> NormalizedInstrument:
    """Convert either a symbols DB row or scriptmaster-style record to NormalizedInstrument."""
    fyers_sym = rec.get("FyersSymbol") or rec.get("fyers_symbol") or ""
    tsym = (
        rec.get("TradingSymbol")
        or rec.get("trading_symbol")
        or (fyers_sym.split(":", 1)[-1] if ":" in fyers_sym else fyers_sym)
    )

    raw_expiry = rec.get("Expiry") or rec.get("expiry") or ""
    iso_expiry = expiry_to_iso(str(raw_expiry)) if raw_expiry else ""

    instr = rec.get("Instrument") or rec.get("instrument_type") or ""
    itype = "EQ"
    if instr == "FUT":
        itype = "FUT"
    elif instr == "OPT":
        itype = "OPT"
    elif instr == "IDX" or "INDEX" in fyers_sym.upper() or str(tsym).upper().endswith("-INDEX"):
        itype = "IDX"

    strike = rec.get("StrikePrice") if "StrikePrice" in rec else rec.get("strike")
    if strike and float(strike) > 0:
        strike = float(strike)
    else:
        strike = 0.0

    return NormalizedInstrument(
        symbol=rec.get("Underlying") or rec.get("symbol") or rec.get("Symbol", ""),
        trading_symbol=tsym,
        exchange=rec.get("Exchange") or rec.get("exchange") or "",
        instrument_type=itype,
        expiry=iso_expiry,
        strike=strike,
        option_type=rec.get("OptionType") or rec.get("option_type") or "",
        lot_size=int(rec.get("LotSize") or rec.get("lot_size") or 1),
        tick_size=float(rec.get("TickSize") or rec.get("tick_size") or 0.05),
        token=str(rec.get("Token") or rec.get("exchange_token") or ""),
        fyers_symbol=fyers_sym,
        description=rec.get("Description") or rec.get("description") or rec.get("Name") or "",
    )


def search_instruments(
    query: str,
    exchange: str = "",
    instrument_type: str = "",
    limit: int = 30,
) -> List[NormalizedInstrument]:
    """
    Search instruments from ScriptMaster.

    Args:
        query:           Search text (case-insensitive)
        exchange:        Filter by exchange ("NFO", "NSE", etc.) or "" for all
        instrument_type: Filter by type ("FUT", "OPT", "EQ") or "" for all
        limit:           Max results

    Returns:
        List of NormalizedInstrument sorted by relevance.
    """
    try:
        from db.symbols_db import search_symbols

        rows = search_symbols(query, exchange, instrument_type, limit)
        if not rows:
            return _fallback_search(query, limit)
        return [_record_to_normalized(row) for row in rows[:limit]]
    except Exception:
        return _fallback_search(query, limit)


# Well-known indices (always available even without ScriptMaster)
_KNOWN_INDICES = [
    ("NIFTY",       "NSE", "NIFTY 50",       "NSE:NIFTY50-INDEX"),
    ("BANKNIFTY",   "NSE", "Bank Nifty",     "NSE:NIFTYBANK-INDEX"),
    ("FINNIFTY",    "NSE", "Finnifty",       "NSE:FINNIFTY-INDEX"),
    ("MIDCPNIFTY",  "NSE", "Midcap Nifty",   "NSE:MIDCPNIFTY-INDEX"),
    ("SENSEX",      "BSE", "Sensex",         "BSE:SENSEX-INDEX"),
    ("BANKEX",      "BSE", "Bankex",         "BSE:BANKEX-INDEX"),
]


def _fallback_search(query: str, limit: int) -> List[NormalizedInstrument]:
    """Fallback search when ScriptMaster is not available."""
    q = query.upper()
    results = []
    for sym, exch, name, fyers_sym in _KNOWN_INDICES:
        if q in sym:
            results.append(NormalizedInstrument(
                symbol=sym, trading_symbol=fyers_sym.split(":", 1)[-1],
                exchange=exch, instrument_type="IDX",
                fyers_symbol=fyers_sym, description=name,
            ))
    return results[:limit]


def lookup_by_trading_symbol(trading_symbol: str, exchange: str = "") -> Optional[NormalizedInstrument]:
    """
    Look up a single instrument by its trading_symbol (with or without exchange prefix).

    Args:
        trading_symbol: e.g. "SBIN-EQ", "NIFTY26APR24500CE", "NSE:SBIN-EQ"
        exchange:       Optional exchange filter for disambiguation (e.g. "NSE", "MCX")

    Returns NormalizedInstrument or None.
    """
    try:
        from db.symbols_db import lookup_by_trading_symbol as db_lookup

        rec = db_lookup(trading_symbol, exchange)
        return _record_to_normalized(rec) if rec else None
    except Exception:
        return None


def resolve_symbol_for_broker(
    symbol: str,
    exchange: str = "NSE",
    broker: str = "fyers",
) -> tuple[str, str, str]:
    """
    Resolve a user-facing symbol to canonical + broker-specific forms.

    Resolution order (strict):
      1) symbols DB exact lookup (with/without exchange prefix)
      2) symbols DB lookup on compact symbol (spaces removed)
      3) deterministic fallback conversion

    Returns:
      (canonical_trading_symbol, resolved_exchange, broker_symbol)
    """
    raw = str(symbol or "").strip()
    if not raw:
        ex = (exchange or "NSE").upper()
        return "", ex, ""

    ex_hint = (exchange or "").upper().strip()
    clean = raw
    if ":" in raw:
        pfx, rest = raw.split(":", 1)
        if not ex_hint:
            ex_hint = pfx.upper().strip()
        clean = rest.strip()

    clean_compact = clean.upper().replace(" ", "")

    inst = (
        lookup_by_trading_symbol(raw)
        or lookup_by_trading_symbol(clean)
        or lookup_by_trading_symbol(clean_compact)
    )
    if not inst and ex_hint:
        inst = (
            lookup_by_trading_symbol(raw, ex_hint)
            or lookup_by_trading_symbol(clean, ex_hint)
            or lookup_by_trading_symbol(clean_compact, ex_hint)
            or lookup_by_trading_symbol(f"{ex_hint}:{clean_compact}", ex_hint)
        )

    if inst:
        resolved_exchange = (inst.exchange or ex_hint or "NSE").upper()
        canonical = inst.trading_symbol
        if broker == "fyers":
            broker_symbol = inst.fyers_symbol or to_broker_symbol(canonical, resolved_exchange, broker)
        else:
            broker_symbol = to_broker_symbol(canonical, resolved_exchange, broker)
        return canonical, resolved_exchange, broker_symbol

    resolved_exchange = (ex_hint or "NSE").upper()
    canonical = clean_compact
    return canonical, resolved_exchange, to_broker_symbol(canonical, resolved_exchange, broker)


def lookup_instrument(
    symbol: str,
    exchange: str = "NFO",
    instrument_type: str = "FUT",
    expiry: str = "",
    strike: float = 0.0,
    option_type: str = "",
) -> Optional[NormalizedInstrument]:
    """
    Look up instrument from ScriptMaster by components.

    Args:
        symbol:          Underlying ("NIFTY")
        exchange:        "NFO", "NSE", etc.
        instrument_type: "FUT", "OPT", "EQ", "IDX"
        expiry:          ISO date "YYYY-MM-DD" or DD-Mon-YYYY
        strike:          Strike price (for options)
        option_type:     "CE" or "PE" (for options)

    Returns:
        NormalizedInstrument or None
    """
    if instrument_type == "FUT":
        try:
            from db.symbols_db import get_future as db_get_future

            rec = db_get_future(symbol, exchange, result=0)
            return _record_to_normalized(rec) if rec else None
        except Exception:
            return None

    if instrument_type == "OPT":
        if not strike or not option_type:
            return None
        try:
            from db.symbols_db import lookup_by_components

            rec = lookup_by_components(
                symbol=symbol,
                exchange=exchange,
                instrument_type="OPT",
                expiry=expiry,
                strike=strike,
                option_type=option_type,
            )
            return _record_to_normalized(rec) if rec else None
        except Exception:
            return None

    try:
        from db.symbols_db import lookup_by_components

        rec = lookup_by_components(
            symbol=symbol,
            exchange=exchange,
            instrument_type=instrument_type,
        )
        return _record_to_normalized(rec) if rec else None
    except Exception:
        return None


def _from_scriptmaster_standard(rec: Dict[str, Any]) -> NormalizedInstrument:
    """Convert scripts/scriptmaster.py standard record to NormalizedInstrument."""
    tsym = rec.get("TradingSymbol", "") or rec.get("tsym", "")
    clean = tsym.split(":", 1)[-1] if ":" in tsym else tsym
    fyers_sym = tsym if ":" in tsym else ""

    raw_expiry = rec.get("Expiry", "")
    iso_expiry = expiry_to_iso(raw_expiry)

    instr = rec.get("Instrument", "")
    itype = "EQ"
    if instr == "FUT":
        itype = "FUT"
    elif instr == "OPT":
        itype = "OPT"
    elif "INDEX" in tsym.upper():
        itype = "IDX"

    strike = rec.get("StrikePrice")
    strike = float(strike) if strike and float(strike) > 0 else 0.0

    return NormalizedInstrument(
        symbol=rec.get("Symbol", "") or rec.get("Underlying", ""),
        trading_symbol=clean,
        exchange=rec.get("Exchange", "") or rec.get("exch", ""),
        instrument_type=itype,
        expiry=iso_expiry,
        strike=strike,
        option_type=rec.get("OptionType", "") or "",
        lot_size=int(rec.get("LotSize", 1) or rec.get("ls", 1) or 1),
        tick_size=float(rec.get("TickSize", 0.05) or 0.05),
        token=str(rec.get("Token", "")),
        fyers_symbol=fyers_sym,
        description=rec.get("Description", ""),
    )


def get_expiries(
    symbol: str,
    exchange: str = "NFO",
    instrument_type: str = "OPT",
) -> List[str]:
    """
    Get available expiry dates (ISO format) for a symbol.

    Returns sorted list of ISO date strings.
    """
    try:
        from db.symbols_db import get_expiry_list as db_get_expiry_list

        return db_get_expiry_list(symbol, exchange, instrument_type)
    except Exception:
        return []


def get_lot_size(symbol: str, exchange: str = "NFO") -> int:
    """Get lot size for a symbol from the symbols DB."""
    try:
        from db.symbols_db import get_lot_size as db_get_lot_size

        return db_get_lot_size(symbol, exchange)
    except Exception:
        return 1


# ── Order / Position Symbol Normalization ────────────────────────────────────

def normalize_order_for_broker(
    order: Dict[str, Any],
    broker: str,
) -> Dict[str, Any]:
    """
    Prepare an order dict for a specific broker by converting the symbol format.

    Mutates and returns the order dict.
    """
    sym = order.get("symbol", "")
    exchange = order.get("exchange", "NSE")
    order["symbol"] = to_broker_symbol(sym, exchange, broker)

    # Normalize side
    side = str(order.get("side", "") or order.get("transaction_type", "")).upper()
    if side in ("B", "BUY"):
        order["side"] = "BUY"
    elif side in ("S", "SELL"):
        order["side"] = "SELL"

    # Normalize product
    prd = str(order.get("product", "") or order.get("product_type", "")).upper()
    _prd = {"I": "MIS", "C": "CNC", "M": "NRML", "INTRADAY": "MIS", "MARGIN": "NRML"}
    order["product"] = _prd.get(prd, prd) if prd else "MIS"

    # Normalize order_type
    otype = str(order.get("order_type", "") or order.get("price_type", "")).upper()
    _otype = {"MKT": "MARKET", "LMT": "LIMIT", "SL-LMT": "SL", "SL-MKT": "SL-M"}
    order["order_type"] = _otype.get(otype, otype) if otype else "MARKET"

    return order


def normalize_position_from_broker(
    position: Dict[str, Any],
    broker: str,
) -> Dict[str, Any]:
    """
    Normalize a broker position response to canonical format.

    Ensures symbol has no exchange prefix and all field names are canonical.
    """
    sym = (position.get("symbol") or position.get("tsym")
           or position.get("tradingsymbol") or "")
    tsym, exchange = from_broker_symbol(sym, broker,
                                         fallback_exchange=position.get("exchange", "NSE"))

    position["symbol"] = tsym
    position["trading_symbol"] = tsym
    position["exchange"] = exchange

    # Enrich with ScriptMaster data if available
    inst = lookup_by_trading_symbol(tsym) or lookup_by_trading_symbol(f"{exchange}:{tsym}")
    if inst:
        position.setdefault("lot_size", inst.lot_size)
        position.setdefault("underlying", inst.symbol)
        position.setdefault("instrument_type", inst.instrument_type)
        position.setdefault("expiry", inst.expiry)
        position.setdefault("strike", inst.strike)
        position.setdefault("option_type", inst.option_type)

    return position


# ── Option Chain symbol enrichment ───────────────────────────────────────────

def enrich_option_chain_row(
    row: Dict[str, Any],
    underlying: str,
    exchange: str = "NFO",
    expiry: str = "",
) -> Dict[str, Any]:
    """
    Add trading_symbol to option chain row's call/put for basket ordering.

    Row format: {"strike": 24500, "call": {...}, "put": {...}}
    After: call/put get "trading_symbol" and "lot_size" fields.
    """
    strike = row.get("strike", 0)
    lot = get_lot_size(underlying, exchange)

    for side_key, otype in [("call", "CE"), ("put", "PE")]:
        leg = row.get(side_key)
        if not leg or not isinstance(leg, dict):
            continue
        # Try ScriptMaster lookup for exact trading symbol
        inst = lookup_instrument(
            underlying, exchange, "OPT",
            expiry=expiry, strike=float(strike), option_type=otype,
        )
        if inst:
            leg["trading_symbol"] = inst.trading_symbol
            leg["fyers_symbol"] = inst.fyers_symbol
        else:
            leg.setdefault("trading_symbol", "")
            leg.setdefault("fyers_symbol", "")

        leg["lot_size"] = lot
        leg["exchange"] = exchange
        leg["option_type"] = otype
        leg["underlying"] = underlying
        leg["strike"] = strike

    row["lot_size"] = lot
    return row


# ── ScriptMaster-based option chain builder ──────────────────────────────────

_STRIKE_GAPS: Dict[str, int] = {
    "NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50,
    "MIDCPNIFTY": 25, "SENSEX": 100, "BANKEX": 100,
    "CRUDEOIL": 50, "GOLD": 100, "SILVER": 1000,
    "NATURALGAS": 5, "COPPER": 5, "USDINR": 0.25,
}


def build_option_chain_from_scriptmaster(
    symbol: str,
    exchange: str = "NFO",
    expiry: str = "",
    strikes_per_side: int = 15,
    spot_price: float = 0,
) -> Optional[Dict[str, Any]]:
    """
    Build a full option chain structure from ScriptMaster data.

    When no live broker data is available (market closed, no broker connected),
    this builds the chain structure with all strikes, trading_symbols, lot_sizes
    and zero prices. The frontend can still display the chain and allow basket
    creation.

    Follows the same pattern as shoonya_platform's OptionChainData.load_from_scriptmaster().

    Args:
        symbol:           Underlying ("NIFTY", "BANKNIFTY")
        exchange:         "NFO", "BFO"
        expiry:           ISO date "YYYY-MM-DD" or "" for nearest
        strikes_per_side: How many strikes on each side of ATM
        spot_price:       Current spot (0 = try to derive from ScriptMaster/fallback)

    Returns:
        Option chain dict matching the frontend's expected format, or None.
    """
    try:
        from db.symbols_db import get_expiry_list as db_get_expiry_list
        from db.symbols_db import get_lot_size as db_get_lot_size
        from db.symbols_db import get_options as db_get_options
    except ImportError:
        return None

    sym = symbol.upper()

    # ── Resolve expiry ──
    all_expiries_iso = db_get_expiry_list(sym, exchange, "OPT")
    if not all_expiries_iso:
        return None

    if expiry:
        expiry = expiry_to_iso(expiry)
        if expiry not in all_expiries_iso:
            return None
    else:
        expiry = all_expiries_iso[0]

    # ── Fetch options for this expiry ──
    options = db_get_options(symbol=sym, exchange=exchange, expiry=expiry)

    if not options:
        return None

    # ── Build strike map: {strike: {"CE": rec, "PE": rec}} ──
    by_strike: Dict[float, Dict[str, Dict]] = {}
    for rec in options:
        strike = rec.get("strike")
        otype = rec.get("option_type", "")
        if not strike or otype not in ("CE", "PE"):
            continue
        by_strike.setdefault(float(strike), {})[otype] = rec

    if not by_strike:
        return None

    # ── ATM calculation ──
    all_strikes = sorted(by_strike.keys())
    gap = _STRIKE_GAPS.get(sym, 50)
    if spot_price > 0:
        atm_strike = round(spot_price / gap) * gap
    else:
        # No spot — use median of available strikes as rough ATM
        atm_strike = all_strikes[len(all_strikes) // 2]

    # ── Select strikes around ATM ──
    try:
        atm_idx = min(range(len(all_strikes)), key=lambda i: abs(all_strikes[i] - atm_strike))
    except ValueError:
        return None

    start_idx = max(0, atm_idx - strikes_per_side)
    end_idx = min(len(all_strikes), atm_idx + strikes_per_side + 1)
    selected_strikes = all_strikes[start_idx:end_idx]

    lot = db_get_lot_size(sym, exchange)

    # ── Build rows ──
    rows = []
    for strike in selected_strikes:
        pair = by_strike.get(strike, {})
        ce_rec = pair.get("CE")
        pe_rec = pair.get("PE")
        if not ce_rec or not pe_rec:
            continue

        ce_fyers = ce_rec.get("fyers_symbol") or f"{exchange}:{ce_rec.get('trading_symbol', '')}"
        pe_fyers = pe_rec.get("fyers_symbol") or f"{exchange}:{pe_rec.get('trading_symbol', '')}"
        ce_tsym = ce_rec.get("trading_symbol", "")
        pe_tsym = pe_rec.get("trading_symbol", "")

        row = {
            "strike": int(strike) if strike == int(strike) else strike,
            "isATM": abs(strike - atm_strike) < gap * 0.5,
            "call": {
                "ltp": 0, "iv": 0, "delta": 0, "gamma": 0, "theta": 0, "vega": 0,
                "oi": 0, "oiChange": 0, "volume": 0, "bid": 0, "ask": 0,
                "trading_symbol": ce_tsym,
                "fyers_symbol": ce_fyers,
                "lot_size": lot,
                "exchange": exchange,
                "option_type": "CE",
                "underlying": sym,
                "strike": int(strike) if strike == int(strike) else strike,
            },
            "put": {
                "ltp": 0, "iv": 0, "delta": 0, "gamma": 0, "theta": 0, "vega": 0,
                "oi": 0, "oiChange": 0, "volume": 0, "bid": 0, "ask": 0,
                "trading_symbol": pe_tsym,
                "fyers_symbol": pe_fyers,
                "lot_size": lot,
                "exchange": exchange,
                "option_type": "PE",
                "underlying": sym,
                "strike": int(strike) if strike == int(strike) else strike,
            },
        }
        rows.append(row)

    if not rows:
        return None

    total_ce_oi = sum(r["call"]["oi"] for r in rows)
    total_pe_oi = sum(r["put"]["oi"] for r in rows)
    pcr = round(total_pe_oi / max(total_ce_oi, 1), 4)

    return {
        "underlying": sym,
        "underlyingLtp": spot_price,
        "expiry": expiry,
        "expiries": all_expiries_iso,
        "pcr": pcr,
        "maxPainStrike": int(atm_strike),
        "rows": rows,
        "lot_size": lot,
        "source": "scriptmaster",
    }
