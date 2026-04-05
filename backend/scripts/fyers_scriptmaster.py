"""
Fyers ScriptMaster — Symbol Master Loader
==========================================

Downloads and caches Fyers instrument CSV files.
Mirrors the design of shoonya_platform/scripts/scriptmaster.py.

CSV Sources (no auth required):
    NSE_FO.csv  — NSE F&O  (futures + options)
    BSE_FO.csv  — BSE F&O
    NSE_CM.csv  — NSE equity
    BSE_CM.csv  — BSE equity
    MCX_COM.csv — MCX commodities (CRUDEOIL, GOLD, SILVER, etc.)
    NSE_CD.csv  — NSE currency derivatives (USDINR, EURINR, etc.)
    BSE_CD.csv  — BSE currency derivatives

Column layout (no headers in file):
    0  fyToken          — unique Fyers token (long int as str)
    1  description      — human name e.g. "NIFTY 28 Apr 26 FUT"
    2  instrument_code  — 11=futures, 10=equity, 12=options
    3  lot_size
    4  tick_size
    5  isin
    6  trading_hours
    7  last_updated_date
    8  expiry_epoch     — Unix timestamp (empty for equity)
    9  fyers_symbol     — "NSE:NIFTY26APRFUT"  (the key used in API calls)
    10 exchange_code    — 10=NSE, 11=BSE, 20=NFO, 21=BFO
    11 segment_code
    12 token            — short numeric token (e.g. 66691)
    13 underlying       — "NIFTY", "RELIANCE", etc.
    14 underlying_token — underlying instrument token
    15 strike_price     — -1.0 for non-options
    16 option_type      — CE / PE / XX (XX = futures / equity)
    17 …               — extra cols; ignored

SAFE TO USE IN PRODUCTION — download once per day, serve from disk cache.
"""
from __future__ import annotations

import csv
import io
import json
import logging
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger("smart_trader.fyers_scriptmaster")

# ── Config ────────────────────────────────────────────────────────────────────

SCRIPTMASTER_VERSION = "1.0"

BASE_URL = "https://public.fyers.in/sym_details"

SCRIPTMASTER_URLS: Dict[str, str] = {
    "NSE_FO":  f"{BASE_URL}/NSE_FO.csv",
    "BSE_FO":  f"{BASE_URL}/BSE_FO.csv",
    "NSE_CM":  f"{BASE_URL}/NSE_CM.csv",
    "BSE_CM":  f"{BASE_URL}/BSE_CM.csv",
    "MCX_COM": f"{BASE_URL}/MCX_COM.csv",
    "NSE_CD":  f"{BASE_URL}/NSE_CD.csv",
    "BSE_CD":  f"{BASE_URL}/BSE_CD.csv",
}

_HERE = Path(__file__).resolve().parent
DATA_DIR = _HERE / "data" / "fyers_scriptmaster"
DATA_DIR.mkdir(parents=True, exist_ok=True)
META_FILE = DATA_DIR / "metadata.json"

# ── Instrument type codes → human label ───────────────────────────────────────
# Fyers CSV instrument_code field (column 2):
#   FO segments: 11=index futures, 13=stock futures, 14=index options, 15=stock options
#   CM segments: 0/10/50=equity, 2=bond/NCD, 5=govt-sec, 8=MF, 9=ETF
_INSTR_CODE: Dict[str, str] = {
    "0":  "EQ",    # NSE/BSE equity shares
    "10": "EQ",    # NSE/BSE equity
    "11": "FUT",   # Index futures
    "13": "FUT",   # Stock futures
    "14": "OPT",   # Index options
    "15": "OPT",   # Stock options
    "16": "FUT",   # Currency futures
    "17": "OPT",   # Currency options
    "18": "FUT",   # Currency futures (weekly)
    "19": "OPT",   # Currency options (weekly)
    "23": "FUT",   # BSE currency futures
    "25": "OPT",   # BSE currency options
    "30": "FUT",   # Commodity futures (MCX)
    "31": "OPT",   # Commodity options (MCX)
    "50": "EQ",    # BSE equity
}

# ── Internal state ────────────────────────────────────────────────────────────
# Main store:  FYERS_SCRIPTMASTER[fyers_symbol] = record_dict
# e.g. FYERS_SCRIPTMASTER["NSE:NIFTY26APRFUT"] = {...}
FYERS_SCRIPTMASTER: Dict[str, Dict[str, Any]] = {}

# Lookup by short token:  TOKEN_INDEX["NSE_FO"]["66691"] = "NSE:NIFTY26APRFUT"
TOKEN_INDEX: Dict[str, Dict[str, str]] = {}

# Expiry calendar:  EXPIRY_CALENDAR["NSE"]["NIFTY"]["FUT"] = ["28-Apr-2026", ...]
EXPIRY_CALENDAR: Dict[str, Dict[str, Dict[str, List[str]]]] = {}

_LOCK = threading.RLock()
_last_refresh: Optional[str] = None  # YYYY-MM-DD


# ── Helpers ───────────────────────────────────────────────────────────────────

def _today_ist() -> str:
    """Current date in IST as YYYY-MM-DD."""
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


def _epoch_to_date(epoch_str: str) -> str:
    """Convert Unix timestamp string to 'DD-Mon-YYYY' for consistency with Shoonya."""
    try:
        ts = int(epoch_str)
        if ts <= 0:
            return ""
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt.strftime("%-d-%b-%Y")  # e.g. "28-Apr-2026"
    except (ValueError, TypeError):
        return ""


def _exchange_from_segment(segment_key: str) -> str:
    """Derive canonical exchange name from segment key (NSE_FO → NFO, NSE_CM → NSE)."""
    mapping = {
        "NSE_FO":  "NFO",
        "BSE_FO":  "BFO",
        "NSE_CM":  "NSE",
        "BSE_CM":  "BSE",
        "MCX_COM": "MCX",
        "NSE_CD":  "CDS",
        "BSE_CD":  "BCD",
    }
    return mapping.get(segment_key, segment_key)


# ── CSV parsing ───────────────────────────────────────────────────────────────

def _parse_csv(content: str, segment_key: str) -> tuple[Dict[str, Dict[str, Any]], Dict[str, str]]:
    """Parse a Fyers symbol master CSV (no header row)."""
    exchange = _exchange_from_segment(segment_key)
    records: Dict[str, Dict[str, Any]] = {}
    token_map: Dict[str, str] = {}

    for row in csv.reader(io.StringIO(content)):
        if len(row) < 14:
            continue
        try:
            fy_token    = row[0].strip()
            description = row[1].strip()
            instr_code  = row[2].strip()
            lot_size    = int(float(row[3])) if row[3] else 1
            tick_size   = float(row[4]) if row[4] else 0.05
            isin        = row[5].strip() if len(row) > 5 else ""
            expiry_epoch = row[8].strip() if len(row) > 8 else ""
            fyers_symbol = row[9].strip() if len(row) > 9 else ""
            short_token  = row[12].strip() if len(row) > 12 else ""
            underlying   = row[13].strip() if len(row) > 13 else ""
            strike_price = float(row[15]) if len(row) > 15 and row[15] else -1.0
            option_type  = row[16].strip() if len(row) > 16 else "XX"

            if not fyers_symbol:
                continue

            instrument = _INSTR_CODE.get(instr_code, "EQ")
            expiry_date = _epoch_to_date(expiry_epoch)

            record: Dict[str, Any] = {
                "FyersSymbol":   fyers_symbol,        # canonical key: "NSE:NIFTY26APRFUT"
                "FyToken":       fy_token,
                "Token":         short_token,
                "Exchange":      exchange,
                "SegmentKey":    segment_key,
                "Description":   description,
                "Instrument":    instrument,
                "Underlying":    underlying,
                "LotSize":       lot_size,
                "TickSize":      tick_size,
                "ISIN":          isin,
                "Expiry":        expiry_date,
                "ExpiryEpoch":   int(expiry_epoch) if expiry_epoch.isdigit() else 0,
                "StrikePrice":   strike_price if strike_price > 0 else None,
                "OptionType":    option_type if option_type not in ("XX", "") else None,
            }
            records[fyers_symbol] = record
            if short_token:
                token_map[short_token] = fyers_symbol

        except Exception as exc:
            logger.debug("Skipping row in %s: %s", segment_key, exc)
            continue

    return records, token_map


# ── Download + build ──────────────────────────────────────────────────────────

def _download(segment_key: str, url: str) -> str:
    logger.info("Downloading Fyers scriptmaster: %s", url)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    # Save raw
    raw_path = DATA_DIR / f"{segment_key}.csv"
    raw_path.write_bytes(resp.content)
    return resp.text


def _build_expiry_calendar() -> None:
    """Populate EXPIRY_CALENDAR from current FYERS_SCRIPTMASTER."""
    cal: Dict[str, Dict[str, Dict[str, List[str]]]] = {}
    for rec in FYERS_SCRIPTMASTER.values():
        if rec["Instrument"] not in ("FUT", "OPT"):
            continue
        expiry = rec.get("Expiry")
        if not expiry:
            continue
        exch = rec["Exchange"]
        underlying = rec.get("Underlying", "")
        kind = "FUTURE" if rec["Instrument"] == "FUT" else "OPTION"
        (
            cal.setdefault(exch, {})
               .setdefault(underlying, {"FUTURE": [], "OPTION": []})
               [kind]
               .append(expiry)
        )
    # Sort and de-duplicate
    for exch, sym_map in cal.items():
        for underlying, kinds in sym_map.items():
            for k in ("FUTURE", "OPTION"):
                try:
                    kinds[k] = sorted(
                        set(kinds[k]),
                        key=lambda x: datetime.strptime(x, "%d-%b-%Y"),
                    )
                except Exception:
                    kinds[k] = sorted(set(kinds[k]))
    EXPIRY_CALENDAR.clear()
    EXPIRY_CALENDAR.update(cal)


# ── Public API ────────────────────────────────────────────────────────────────

def refresh(force: bool = False) -> None:
    """Download all segment CSV files and rebuild in-memory indices."""
    global _last_refresh

    if not force and not _needs_refresh():
        logger.info("Fyers scriptmaster is up-to-date — loading from disk cache")
        _load_from_disk()
        return

    temp_sm: Dict[str, Dict[str, Any]] = {}
    temp_tok: Dict[str, Dict[str, str]] = {}

    for seg_key, url in SCRIPTMASTER_URLS.items():
        try:
            content = _download(seg_key, url)
            records, token_map = _parse_csv(content, seg_key)
            temp_sm.update(records)
            temp_tok[seg_key] = token_map
            logger.info("Loaded %d symbols from %s", len(records), seg_key)
        except Exception as exc:
            logger.error("Failed to load %s: %s", seg_key, exc)
            # Non-fatal: continue with other segments

    with _LOCK:
        FYERS_SCRIPTMASTER.clear()
        FYERS_SCRIPTMASTER.update(temp_sm)
        TOKEN_INDEX.clear()
        TOKEN_INDEX.update(temp_tok)
        _build_expiry_calendar()
        _last_refresh = _today_ist()

    META_FILE.write_text(json.dumps({
        "date": _today_ist(),
        "version": SCRIPTMASTER_VERSION,
        "total_symbols": len(temp_sm),
    }, indent=2))
    logger.info("Fyers scriptmaster refreshed: %d total symbols", len(temp_sm))


def _load_from_disk() -> None:
    """Reload previously downloaded CSVs from disk without re-downloading."""
    temp_sm: Dict[str, Dict[str, Any]] = {}
    temp_tok: Dict[str, Dict[str, str]] = {}
    for seg_key in SCRIPTMASTER_URLS:
        raw_path = DATA_DIR / f"{seg_key}.csv"
        if not raw_path.exists():
            continue
        try:
            records, token_map = _parse_csv(raw_path.read_text(), seg_key)
            temp_sm.update(records)
            temp_tok[seg_key] = token_map
        except Exception as exc:
            logger.warning("Could not load %s from disk: %s", seg_key, exc)

    if not temp_sm:
        logger.warning("No cached Fyers scriptmaster found — call refresh(force=True)")
        return

    with _LOCK:
        FYERS_SCRIPTMASTER.clear()
        FYERS_SCRIPTMASTER.update(temp_sm)
        TOKEN_INDEX.clear()
        TOKEN_INDEX.update(temp_tok)
        _build_expiry_calendar()
    logger.info("Fyers scriptmaster loaded from disk: %d symbols", len(temp_sm))


def get_symbol(fyers_symbol: str) -> Optional[Dict[str, Any]]:
    """Look up a symbol by its Fyers format e.g. 'NSE:NIFTY26APRFUT'."""
    return FYERS_SCRIPTMASTER.get(fyers_symbol)


def search(query: str, exchange: Optional[str] = None, instrument: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Full-text search across symbol descriptions and underlying names.

    Args:
        query:       Search term (case-insensitive)
        exchange:    Filter: 'NFO', 'NSE', 'BSE', 'BFO'
        instrument:  Filter: 'FUT', 'OPT', 'EQ'

    Returns list of matching records (max 50).
    """
    q = query.upper()
    results = []
    with _LOCK:
        for rec in FYERS_SCRIPTMASTER.values():
            if exchange and rec.get("Exchange", "").upper() != exchange.upper():
                continue
            if instrument and rec.get("Instrument", "").upper() != instrument.upper():
                continue
            sym = rec.get("FyersSymbol", "")
            underlying = rec.get("Underlying", "")
            desc = rec.get("Description", "")
            if q in sym.upper() or q in underlying.upper() or q in desc.upper():
                results.append(rec)
            if len(results) >= 50:
                break
    return results


def get_futures(underlying: str, exchange: str = "NFO") -> List[Dict[str, Any]]:
    """Return all futures contracts for a given underlying e.g. 'NIFTY', 'BANKNIFTY'."""
    u = underlying.upper()
    with _LOCK:
        return sorted(
            [
                r for r in FYERS_SCRIPTMASTER.values()
                if r.get("Instrument") == "FUT"
                and r.get("Exchange", "").upper() == exchange.upper()
                and r.get("Underlying", "").upper() == u
            ],
            key=lambda r: r.get("ExpiryEpoch", 0),
        )


def get_options(
    underlying: str,
    exchange: str = "NFO",
    expiry: Optional[str] = None,
    strike: Optional[float] = None,
    option_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Return options for a given underlying, with optional filters.

    Args:
        underlying:   e.g. "NIFTY"
        exchange:     e.g. "NFO"
        expiry:       e.g. "28-Apr-2026" (DD-Mon-YYYY)
        strike:       e.g. 24000.0
        option_type:  "CE" or "PE"
    """
    u = underlying.upper()
    with _LOCK:
        result = [
            r for r in FYERS_SCRIPTMASTER.values()
            if r.get("Instrument") == "OPT"
            and r.get("Exchange", "").upper() == exchange.upper()
            and r.get("Underlying", "").upper() == u
            and (expiry is None or r.get("Expiry") == expiry)
            and (strike is None or r.get("StrikePrice") == strike)
            and (option_type is None or r.get("OptionType", "").upper() == option_type.upper())
        ]
    return sorted(result, key=lambda r: (r.get("ExpiryEpoch", 0), r.get("StrikePrice", 0)))


def get_near_futures(underlying: str, exchange: str = "NFO") -> Optional[Dict[str, Any]]:
    """Return the nearest expiry futures contract for an underlying."""
    futs = get_futures(underlying, exchange)
    return futs[0] if futs else None


def get_expiry_list(underlying: str, exchange: str = "NFO", instrument: str = "FUTURE") -> List[str]:
    """Return sorted expiry date strings for an underlying."""
    with _LOCK:
        bucket = EXPIRY_CALENDAR.get(exchange.upper(), {})
        return bucket.get(underlying.upper(), {}).get(instrument.upper(), [])


def lot_size(fyers_symbol: str) -> int:
    """Return lot size for a Fyers symbol (1 for equity if not found)."""
    rec = FYERS_SCRIPTMASTER.get(fyers_symbol)
    return rec["LotSize"] if rec else 1


def tick_size(fyers_symbol: str) -> float:
    """Return tick size for a Fyers symbol."""
    rec = FYERS_SCRIPTMASTER.get(fyers_symbol)
    return rec["TickSize"] if rec else 0.05
