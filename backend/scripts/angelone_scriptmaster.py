"""
Angel One ScriptMaster — Instrument Master Loader
===================================================

Downloads and caches Angel One (SmartAPI) instrument data.

Data source (no auth required):
    https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json

Returns a JSON array:
    [
      {
        "token": "2885",
        "symbol": "RELIANCE-EQ",
        "name": "RELIANCE",
        "expiry": "",
        "strike": "-1.000000",
        "lotsize": "1",
        "instrumenttype": "",
        "exch_seg": "nse_cm",
        "tick_size": "5.000000"
      },
      ...
    ]

exch_seg values:  nse_cm, bse_cm, nfo, bfo, mcx_fo, cds_fo, bcd_fo, nse_index, bse_index
instrumenttype : "" (equity), FUTSTK, FUTIDX, OPTSTK, OPTIDX, FUTCOM, OPTFUT, FUTCUR, OPTCUR
"""
from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger("smart_trader.angelone_scriptmaster")

# ── Config ────────────────────────────────────────────────────────────────────

SCRIPTMASTER_VERSION = "1.0"
SCRIPTMASTER_URL = (
    "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
)

_HERE = Path(__file__).resolve().parent
DATA_DIR = _HERE / "data" / "angelone_scriptmaster"
DATA_DIR.mkdir(parents=True, exist_ok=True)
META_FILE = DATA_DIR / "metadata.json"
CACHE_FILE = DATA_DIR / "scripmaster.json"

# ── Exchange segment normalisation ────────────────────────────────────────────

_EXCH_SEG_MAP: Dict[str, str] = {
    "nse_cm":    "NSE",
    "bse_cm":    "BSE",
    "nfo":       "NFO",
    "bfo":       "BFO",
    "mcx_fo":    "MCX",
    "cds_fo":    "CDS",
    "bcd_fo":    "BCD",
    "nse_index": "NSE",
    "bse_index": "BSE",
}

# Instrument type → canonical
_INSTR_MAP: Dict[str, str] = {
    "":        "EQ",
    "FUTSTK":  "FUT",
    "FUTIDX":  "FUT",
    "OPTSTK":  "OPT",
    "OPTIDX":  "OPT",
    "FUTCOM":  "FUT",
    "OPTFUT":  "OPT",
    "OPTCOM":  "OPT",
    "FUTCUR":  "FUT",
    "OPTCUR":  "OPT",
    "FUTIRC":  "FUT",
    "AMXIDX":  "IDX",
}

# ── In-memory stores ─────────────────────────────────────────────────────────

# Flat index: ANGELONE_SCRIPTMASTER[token] = record
ANGELONE_SCRIPTMASTER: Dict[str, Dict[str, Any]] = {}

# Secondary index by symbol: SYMBOL_INDEX["RELIANCE-EQ"] = [record, ...]
SYMBOL_INDEX: Dict[str, List[Dict[str, Any]]] = {}

# Expiry calendar: EXPIRY_CALENDAR[exchange][underlying][kind] = [dates]
EXPIRY_CALENDAR: Dict[str, Dict[str, Dict[str, List[str]]]] = {}

_LOCK = threading.RLock()
_last_refresh: Optional[str] = None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _today_ist() -> str:
    ist = timezone(timedelta(hours=5, minutes=30))
    return datetime.now(ist).strftime("%Y-%m-%d")


def _needs_refresh() -> bool:
    global _last_refresh
    if _last_refresh == _today_ist():
        return False
    if META_FILE.exists():
        try:
            meta = json.loads(META_FILE.read_text())
            if meta.get("date") == _today_ist():
                _last_refresh = _today_ist()
                return False
        except Exception:
            pass
    return True


def _parse_expiry(expiry_str: str) -> str:
    """Normalise Angel One expiry strings to DD-Mon-YYYY."""
    if not expiry_str:
        return ""
    expiry_str = expiry_str.strip()
    # Angel One uses formats like "28APR2026", "28Apr2026", "2026-04-28" etc
    for fmt in ("%d%b%Y", "%d%B%Y", "%Y-%m-%d", "%d-%b-%Y", "%d-%B-%Y"):
        try:
            dt = datetime.strptime(expiry_str, fmt)
            return dt.strftime("%-d-%b-%Y")
        except ValueError:
            continue
    return expiry_str


def _extract_underlying(symbol: str, instrumenttype: str) -> str:
    """Extract underlying from Angel One symbol like 'NIFTY28APR26FUT'."""
    if not symbol:
        return ""
    if not instrumenttype:
        # Equity: strip suffix like -EQ, -BE, etc
        parts = symbol.split("-")
        return parts[0] if parts else symbol
    import re
    # Strip date/expiry/strike patterns to get underlying
    s = symbol.upper()
    s = re.sub(r"\d{2}[A-Z]{3}\d{2,4}.*$", "", s)
    s = re.sub(r"(FUT|CE|PE)$", "", s)
    return s or symbol


# ── Parsing ───────────────────────────────────────────────────────────────────

def _parse_json(raw_data: list) -> Dict[str, Dict[str, Any]]:
    """Parse Angel One JSON instrument list into record dict keyed by token."""
    records: Dict[str, Dict[str, Any]] = {}
    for item in raw_data:
        token = str(item.get("token", "")).strip()
        if not token:
            continue

        symbol = (item.get("symbol") or "").strip()
        name = (item.get("name") or "").strip()
        expiry_raw = (item.get("expiry") or "").strip()
        instrumenttype = (item.get("instrumenttype") or "").strip()
        exch_seg = (item.get("exch_seg") or "").strip().lower()

        try:
            strike = float(item.get("strike", "-1"))
        except (ValueError, TypeError):
            strike = -1.0

        try:
            lotsize = int(float(item.get("lotsize", "1")))
        except (ValueError, TypeError):
            lotsize = 1

        try:
            tick_size = float(item.get("tick_size", "5")) / 100.0  # Angel stores as paise
        except (ValueError, TypeError):
            tick_size = 0.05

        exchange = _EXCH_SEG_MAP.get(exch_seg, exch_seg.upper())
        canonical_instr = _INSTR_MAP.get(instrumenttype, instrumenttype)
        expiry = _parse_expiry(expiry_raw)
        underlying = _extract_underlying(symbol, instrumenttype)
        option_type = None
        if canonical_instr == "OPT":
            if symbol.endswith("CE"):
                option_type = "CE"
            elif symbol.endswith("PE"):
                option_type = "PE"

        records[token] = {
            "Token":          token,
            "Symbol":         symbol,
            "Name":           name,
            "Underlying":     underlying,
            "Exchange":       exchange,
            "ExchSeg":        exch_seg,
            "Instrument":     canonical_instr,
            "InstrumentType": instrumenttype,
            "Expiry":         expiry,
            "StrikePrice":    strike if strike > 0 else None,
            "OptionType":     option_type,
            "LotSize":        lotsize,
            "TickSize":       tick_size,
            "TradingSymbol":  symbol,
        }

    return records


def _build_indices() -> None:
    """Build secondary indices from ANGELONE_SCRIPTMASTER."""
    SYMBOL_INDEX.clear()
    for rec in ANGELONE_SCRIPTMASTER.values():
        sym = rec.get("Symbol", "")
        if sym:
            SYMBOL_INDEX.setdefault(sym, []).append(rec)

    # Expiry calendar
    cal: Dict[str, Dict[str, Dict[str, List[str]]]] = {}
    for rec in ANGELONE_SCRIPTMASTER.values():
        instr = rec.get("Instrument", "")
        if instr not in ("FUT", "OPT"):
            continue
        expiry = rec.get("Expiry")
        if not expiry:
            continue
        exchange = rec.get("Exchange", "")
        underlying = rec.get("Underlying", "")
        kind = "FUTURE" if instr == "FUT" else "OPTION"
        (
            cal.setdefault(exchange, {})
               .setdefault(underlying, {"FUTURE": [], "OPTION": []})
               [kind].append(expiry)
        )
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
    """Download Angel One instrument master and rebuild caches."""
    global _last_refresh

    if not force and not _needs_refresh():
        logger.info("Angel One scriptmaster up-to-date — loading from disk")
        _load_from_disk()
        return

    logger.info("Downloading Angel One scriptmaster from %s", SCRIPTMASTER_URL)
    try:
        resp = requests.get(SCRIPTMASTER_URL, timeout=60)
        resp.raise_for_status()
        raw_data = resp.json()
    except Exception as exc:
        logger.error("Failed to download Angel One scriptmaster: %s", exc)
        _load_from_disk()
        return

    records = _parse_json(raw_data)
    if not records:
        logger.error("Angel One scriptmaster returned empty data")
        return

    # Save raw JSON to disk
    CACHE_FILE.write_text(json.dumps(raw_data, default=str))

    with _LOCK:
        ANGELONE_SCRIPTMASTER.clear()
        ANGELONE_SCRIPTMASTER.update(records)
        _build_indices()
        _last_refresh = _today_ist()

    META_FILE.write_text(json.dumps({
        "date": _today_ist(),
        "version": SCRIPTMASTER_VERSION,
        "total_symbols": len(records),
    }, indent=2))
    logger.info("Angel One scriptmaster refreshed: %d symbols", len(records))


def _load_from_disk() -> None:
    """Load cached JSON from disk."""
    if not CACHE_FILE.exists():
        logger.warning("No cached Angel One scriptmaster found")
        return
    try:
        raw_data = json.loads(CACHE_FILE.read_text())
        records = _parse_json(raw_data)
        with _LOCK:
            ANGELONE_SCRIPTMASTER.clear()
            ANGELONE_SCRIPTMASTER.update(records)
            _build_indices()
        logger.info("Angel One scriptmaster loaded from disk: %d symbols", len(records))
    except Exception as exc:
        logger.warning("Could not load Angel One scriptmaster from disk: %s", exc)


def search(query: str, exchange: Optional[str] = None) -> List[Dict[str, Any]]:
    """Search Angel One scriptmaster by symbol/name/underlying."""
    q = query.upper()
    results = []
    with _LOCK:
        for rec in ANGELONE_SCRIPTMASTER.values():
            if exchange and rec.get("Exchange", "").upper() != exchange.upper():
                continue
            sym = (rec.get("Symbol") or "").upper()
            name = (rec.get("Name") or "").upper()
            underlying = (rec.get("Underlying") or "").upper()
            if q in sym or q in name or q in underlying:
                results.append(rec)
            if len(results) >= 50:
                break
    return results


def get_futures(underlying: str, exchange: str = "NFO") -> List[Dict[str, Any]]:
    """Return all futures for an underlying, sorted by expiry."""
    u = underlying.upper()
    with _LOCK:
        matches = [
            r for r in ANGELONE_SCRIPTMASTER.values()
            if r.get("Instrument") == "FUT"
            and r.get("Exchange", "").upper() == exchange.upper()
            and r.get("Underlying", "").upper() == u
            and r.get("Expiry")
        ]
    return sorted(matches, key=lambda r: _parse_expiry_sort(r.get("Expiry", "")))


def get_options(
    underlying: str,
    exchange: str = "NFO",
    expiry: Optional[str] = None,
    strike: Optional[float] = None,
    option_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return options matching filters."""
    u = underlying.upper()
    with _LOCK:
        matches = [
            r for r in ANGELONE_SCRIPTMASTER.values()
            if r.get("Instrument") == "OPT"
            and r.get("Exchange", "").upper() == exchange.upper()
            and r.get("Underlying", "").upper() == u
            and (expiry is None or r.get("Expiry") == expiry)
            and (strike is None or r.get("StrikePrice") == strike)
            and (option_type is None or (r.get("OptionType") or "").upper() == option_type.upper())
        ]
    return sorted(matches, key=lambda r: (_parse_expiry_sort(r.get("Expiry", "")), r.get("StrikePrice") or 0))


def get_expiry_list(underlying: str, exchange: str = "NFO", kind: str = "OPTION") -> List[str]:
    """Return sorted expiry dates for an underlying."""
    with _LOCK:
        return EXPIRY_CALENDAR.get(exchange.upper(), {}).get(underlying.upper(), {}).get(kind.upper(), [])


def get_record_by_token(token: str) -> Optional[Dict[str, Any]]:
    """Look up by token."""
    return ANGELONE_SCRIPTMASTER.get(str(token))


def get_record_by_symbol(symbol: str) -> Optional[Dict[str, Any]]:
    """Look up by trading symbol."""
    recs = SYMBOL_INDEX.get(symbol.upper(), [])
    return recs[0] if recs else None


def get_total_count() -> int:
    return len(ANGELONE_SCRIPTMASTER)


def _parse_expiry_sort(expiry_str: str) -> datetime:
    try:
        return datetime.strptime(expiry_str, "%d-%b-%Y")
    except (ValueError, TypeError):
        return datetime.max
