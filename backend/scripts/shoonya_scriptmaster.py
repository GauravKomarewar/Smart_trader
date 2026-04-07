"""
Shoonya ScriptMaster — Instrument Master Loader
=================================================

Downloads and caches Shoonya instrument data from the public API.
Modeled after shoonya_platform/scripts/scriptmaster.py.

Data source (no auth required):
    https://api.shoonya.com/NSE_symbols.txt.zip
    https://api.shoonya.com/NFO_symbols.txt.zip
    https://api.shoonya.com/BSE_symbols.txt.zip
    https://api.shoonya.com/BFO_symbols.txt.zip
    https://api.shoonya.com/MCX_symbols.txt.zip

Each ZIP contains a single CSV with columns (comma-separated):
    Exchange, Token, LotSize, Symbol, TradingSymbol, Expiry,
    Instrument, OptionType, StrikePrice, TickSize, ...

Downloaded once per day, served from disk cache for the rest of the session.
"""
from __future__ import annotations

import csv
import io
import logging
import re
import threading
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from core.daily_refresh import current_refresh_cycle_start, now_ist

logger = logging.getLogger("smart_trader.shoonya_scriptmaster")

# ── Config ────────────────────────────────────────────────────────────────────

SCRIPTMASTER_VERSION = "1.0"
BASE_URL = "https://api.shoonya.com"

SCRIPTMASTER_URLS: Dict[str, str] = {
    "NSE": f"{BASE_URL}/NSE_symbols.txt.zip",
    "NFO": f"{BASE_URL}/NFO_symbols.txt.zip",
    "BSE": f"{BASE_URL}/BSE_symbols.txt.zip",
    "BFO": f"{BASE_URL}/BFO_symbols.txt.zip",
    "MCX": f"{BASE_URL}/MCX_symbols.txt.zip",
    "CDS": f"{BASE_URL}/CDS_symbols.txt.zip",
}

# ── Canonical instrument groups ───────────────────────────────────────────────

FUTURE_INSTRUMENTS = {
    "NFO": {"FUTIDX", "FUTSTK"},
    "BFO": {"FUTIDX", "FUTSTK"},
    "MCX": {"FUTCOM"},
    "CDS": {"FUTCUR", "FUTIRC"},
}

OPTION_INSTRUMENTS = {
    "NFO": {"OPTIDX", "OPTSTK"},
    "BFO": {"OPTIDX", "OPTSTK"},
    "MCX": {"OPTFUT", "OPTCOM"},
    "CDS": {"OPTCUR"},
}

# ── In-memory stores ─────────────────────────────────────────────────────────

# SHOONYA_SCRIPTMASTER[exchange][token] = record dict
SHOONYA_SCRIPTMASTER: Dict[str, Dict[str, Dict[str, Any]]] = {}
# Universal flat index: "EXCHANGE|TOKEN" → record
SHOONYA_UNIVERSAL: Dict[str, Dict[str, Any]] = {}
# Expiry calendar: exchange → underlying → {"FUTURE": [...], "OPTION": [...]}
EXPIRY_CALENDAR: Dict[str, Dict[str, Dict[str, List[str]]]] = {}

_LOCK = threading.RLock()
_last_refresh: Optional[datetime] = None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _today_ist() -> str:
    return now_ist().strftime("%Y-%m-%d")


def _needs_refresh() -> bool:
    global _last_refresh
    return not (_last_refresh and _last_refresh >= current_refresh_cycle_start())


def _extract_underlying(tradingsymbol: str, symbol: Optional[str] = None) -> str:
    """Extract underlying from a Shoonya TradingSymbol using Symbol as anchor."""
    if not tradingsymbol:
        return ""
    ts = tradingsymbol.upper()
    # Trust Symbol column if it is a prefix
    if symbol:
        sym = symbol.upper()
        if ts.startswith(sym):
            return sym
    # Fallback: strip known suffixes
    ts = re.sub(r"(CE|PE|FUT|F)$", "", ts)
    ts = re.sub(
        r"\d{2}(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\d{0,2}",
        "", ts,
    )
    ts = re.sub(r"\d+$", "", ts)
    return ts or ""


# ── CSV parsing ───────────────────────────────────────────────────────────────

# Column aliases: Shoonya CSV may use either set of names depending on exchange
_COL_MAP = {
    "Token":        ["Token", "token"],
    "TradingSymbol": ["TradingSymbol", "tsym"],
    "Symbol":       ["Symbol", "symname"],
    "LotSize":      ["LotSize", "ls"],
    "Expiry":       ["Expiry", "expdt"],
    "Instrument":   ["Instrument", "instname"],
    "OptionType":   ["OptionType", "optt"],
    "StrikePrice":  ["StrikePrice", "strprc"],
    "TickSize":     ["TickSize", "ti"],
}


def _parse_zip(zip_bytes: bytes, exchange: str) -> Dict[str, Dict[str, Any]]:
    """Extract CSV from ZIP and parse into exchange record dict keyed by Token."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        txt_name = next(n for n in z.namelist() if n.lower().endswith(".txt"))
        with z.open(txt_name) as f:
            content = f.read().decode("utf-8", errors="replace")

    reader = csv.DictReader(io.StringIO(content))
    if not reader.fieldnames:
        return {}

    # Build field name resolution
    field_resolve: Dict[str, str] = {}
    for canonical, aliases in _COL_MAP.items():
        for alias in aliases:
            if alias in reader.fieldnames:
                field_resolve[canonical] = alias
                break

    records: Dict[str, Dict[str, Any]] = {}
    for row in reader:
        token = (row.get(field_resolve.get("Token", "Token")) or "").strip()
        tsym = (row.get(field_resolve.get("TradingSymbol", "TradingSymbol")) or "").strip()
        if not token or not tsym:
            continue

        symbol = (row.get(field_resolve.get("Symbol", "Symbol")) or "").strip().upper()
        instrument = (row.get(field_resolve.get("Instrument", "Instrument")) or "").strip()
        expiry = (row.get(field_resolve.get("Expiry", "Expiry")) or "").strip()
        option_type = (row.get(field_resolve.get("OptionType", "OptionType")) or "").strip()

        try:
            lot_size = int(float(row.get(field_resolve.get("LotSize", "LotSize")) or "1"))
        except (ValueError, TypeError):
            lot_size = 1

        try:
            strike = float(row.get(field_resolve.get("StrikePrice", "StrikePrice")) or "0")
        except (ValueError, TypeError):
            strike = 0.0

        try:
            tick_sz = float(row.get(field_resolve.get("TickSize", "TickSize")) or "0.05")
        except (ValueError, TypeError):
            tick_sz = 0.05

        underlying = _extract_underlying(tsym, symbol)

        records[token] = {
            "Exchange":       exchange,
            "Token":          token,
            "TradingSymbol":  tsym,
            "Symbol":         symbol,
            "Underlying":     underlying,
            "LotSize":        lot_size,
            "Expiry":         expiry,     # DD-Mon-YYYY format
            "Instrument":     instrument,
            "OptionType":     option_type if option_type not in ("", "XX") else None,
            "StrikePrice":    strike if strike > 0 else None,
            "TickSize":       tick_sz,
        }

    return records


# ── Index builders ────────────────────────────────────────────────────────────

def _build_universal() -> None:
    """Build flat universal index from per-exchange data."""
    SHOONYA_UNIVERSAL.clear()
    for exch, data in SHOONYA_SCRIPTMASTER.items():
        for tok, rec in data.items():
            SHOONYA_UNIVERSAL[f"{exch}|{tok}"] = rec


def _build_expiry_calendar() -> None:
    """Build expiry calendar from current data."""
    cal: Dict[str, Dict[str, Dict[str, List[str]]]] = {}
    for exchange, data in SHOONYA_SCRIPTMASTER.items():
        fut_insts = FUTURE_INSTRUMENTS.get(exchange, set())
        opt_insts = OPTION_INSTRUMENTS.get(exchange, set())
        for rec in data.values():
            expiry = rec.get("Expiry")
            if not expiry:
                continue
            instrument = rec.get("Instrument", "")
            underlying = rec.get("Underlying") or rec.get("Symbol", "")
            if instrument in fut_insts:
                kind = "FUTURE"
            elif instrument in opt_insts:
                kind = "OPTION"
            else:
                continue
            (
                cal.setdefault(exchange, {})
                   .setdefault(underlying, {"FUTURE": [], "OPTION": []})
                   [kind].append(expiry)
            )
    # Deduplicate and sort
    for exch_map in cal.values():
        for sym_map in exch_map.values():
            for k in ("FUTURE", "OPTION"):
                try:
                    sym_map[k] = sorted(
                        set(sym_map[k]),
                        key=lambda x: datetime.strptime(x, "%d-%b-%Y"),
                    )
                except Exception:
                    sym_map[k] = sorted(set(sym_map[k]))

    EXPIRY_CALENDAR.clear()
    EXPIRY_CALENDAR.update(cal)


# ── Public API ────────────────────────────────────────────────────────────────

def refresh(force: bool = False) -> None:
    """Download all exchange ZIPs and rebuild in-memory indices."""
    global _last_refresh

    if not force and not _needs_refresh():
        logger.info("Shoonya scriptmaster already refreshed for current cycle — skipping download")
        return

    temp: Dict[str, Dict[str, Dict[str, Any]]] = {}
    with tempfile.TemporaryDirectory(prefix="smart_trader_shoonya_") as temp_dir:
        for exch, url in SCRIPTMASTER_URLS.items():
            try:
                logger.info("Downloading Shoonya scriptmaster: %s", exch)
                resp = requests.get(url, timeout=30)
                resp.raise_for_status()
                zip_path = Path(temp_dir) / f"{exch}.zip"
                zip_path.write_bytes(resp.content)
                records = _parse_zip(zip_path.read_bytes(), exch)
                temp[exch] = records
                logger.info("Loaded %d symbols from Shoonya %s", len(records), exch)
            except Exception as exc:
                logger.error("Failed to load Shoonya %s: %s", exch, exc)

    if not temp:
        logger.error("No Shoonya scriptmaster data downloaded")
        return

    with _LOCK:
        SHOONYA_SCRIPTMASTER.clear()
        SHOONYA_SCRIPTMASTER.update(temp)
        _build_universal()
        _build_expiry_calendar()
        _last_refresh = now_ist()

    logger.info("Shoonya scriptmaster refreshed: %d total symbols",
                sum(len(d) for d in temp.values()))


def _load_from_disk() -> None:
    """Persistent disk cache is disabled; runtime symbol lookups should use symbols_db."""
    logger.debug("Shoonya disk cache disabled; no persistent scriptmaster files are loaded")


# ── Query API ─────────────────────────────────────────────────────────────────

def search(query: str, exchange: Optional[str] = None) -> List[Dict[str, Any]]:
    """Search Shoonya scriptmaster by symbol / underlying / trading symbol."""
    q = query.upper()
    results = []
    with _LOCK:
        exchanges = [exchange.upper()] if exchange else list(SHOONYA_SCRIPTMASTER.keys())
        for exch in exchanges:
            for rec in SHOONYA_SCRIPTMASTER.get(exch, {}).values():
                sym = (rec.get("Symbol") or "").upper()
                underlying = (rec.get("Underlying") or "").upper()
                tsym = (rec.get("TradingSymbol") or "").upper()
                if q in sym or q in underlying or q in tsym:
                    results.append(rec)
                if len(results) >= 50:
                    return results
    return results


def get_futures(underlying: str, exchange: str = "NFO") -> List[Dict[str, Any]]:
    """Return all futures contracts for a given underlying, sorted by expiry."""
    u = underlying.upper()
    fut_insts = FUTURE_INSTRUMENTS.get(exchange.upper(), set())
    with _LOCK:
        matches = [
            r for r in SHOONYA_SCRIPTMASTER.get(exchange.upper(), {}).values()
            if (r.get("Underlying") == u or r.get("Symbol") == u)
            and r.get("Instrument") in fut_insts
            and r.get("Expiry")
        ]
    return sorted(matches, key=lambda r: _parse_expiry(r.get("Expiry", "")))


def get_options(
    underlying: str,
    exchange: str = "NFO",
    expiry: Optional[str] = None,
    strike: Optional[float] = None,
    option_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return options matching filters, sorted by (expiry, strike)."""
    u = underlying.upper()
    opt_insts = OPTION_INSTRUMENTS.get(exchange.upper(), set())
    with _LOCK:
        matches = [
            r for r in SHOONYA_SCRIPTMASTER.get(exchange.upper(), {}).values()
            if (r.get("Underlying") == u or r.get("Symbol") == u)
            and r.get("Instrument") in opt_insts
            and (expiry is None or r.get("Expiry") == expiry)
            and (strike is None or r.get("StrikePrice") == strike)
            and (option_type is None or (r.get("OptionType") or "").upper() == option_type.upper())
        ]
    return sorted(matches, key=lambda r: (_parse_expiry(r.get("Expiry", "")), r.get("StrikePrice") or 0))


def get_expiry_list(underlying: str, exchange: str = "NFO", kind: str = "OPTION") -> List[str]:
    """Return sorted expiry dates for an underlying."""
    with _LOCK:
        return EXPIRY_CALENDAR.get(exchange.upper(), {}).get(underlying.upper(), {}).get(kind.upper(), [])


def get_record_by_tsym(trading_symbol: str, exchange: str) -> Optional[Dict[str, Any]]:
    """Look up a Shoonya record by TradingSymbol + exchange."""
    ts = trading_symbol.upper()
    with _LOCK:
        for rec in SHOONYA_SCRIPTMASTER.get(exchange.upper(), {}).values():
            if rec.get("TradingSymbol") == ts:
                return rec
    return None


def get_record_by_token(token: str, exchange: str) -> Optional[Dict[str, Any]]:
    """Look up a Shoonya record by Token + exchange."""
    with _LOCK:
        return SHOONYA_SCRIPTMASTER.get(exchange.upper(), {}).get(str(token))


def requires_limit_order(exchange: str, token: Optional[str] = None,
                          tradingsymbol: Optional[str] = None) -> bool:
    """
    LIMIT orders required only for stock options (OPTSTK) and MCX options (OPTFUT).
    Index options (OPTIDX), futures, and equity allow MARKET orders.
    """
    rec = None
    with _LOCK:
        if token:
            rec = SHOONYA_UNIVERSAL.get(f"{exchange.upper()}|{token}")
        if rec is None and tradingsymbol:
            rec = get_record_by_tsym(tradingsymbol, exchange)
    if not rec:
        return False
    return rec.get("Instrument") in ("OPTSTK", "OPTFUT")


def get_total_count() -> int:
    """Return total loaded instruments count."""
    return len(SHOONYA_UNIVERSAL)


def _parse_expiry(expiry_str: str) -> datetime:
    """Parse DD-Mon-YYYY to datetime for sorting."""
    try:
        return datetime.strptime(expiry_str, "%d-%b-%Y")
    except (ValueError, TypeError):
        return datetime.max
