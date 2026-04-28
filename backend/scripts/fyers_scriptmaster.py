"""
Fyers ScriptMaster — Symbol Master Loader
==========================================

Downloads and caches Fyers instrument JSON files.
Mirrors the design of shoonya_platform/scripts/scriptmaster.py.

JSON Sources (no auth required):
    NSE_FO_sym_master.json  — NSE equity derivatives
    BSE_FO_sym_master.json  — BSE equity derivatives
    NSE_CM_sym_master.json  — NSE equity
    BSE_CM_sym_master.json  — BSE equity
    MCX_COM_sym_master.json — MCX commodities
    NSE_COM_sym_master.json — NSE commodities
    NSE_CD_sym_master.json  — NSE currency derivatives

SAFE TO USE IN PRODUCTION — download once per day, serve from disk cache.
"""
from __future__ import annotations

import json
import logging
import threading
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from core.daily_refresh import current_refresh_cycle_start, now_ist
from scripts.scriptmaster_export_utils import write_unified_csv

logger = logging.getLogger("smart_trader.fyers_scriptmaster")

# ── Config ────────────────────────────────────────────────────────────────────

SCRIPTMASTER_VERSION = "1.1"

BASE_URL = "https://public.fyers.in/sym_details"

SCRIPTMASTER_URLS: Dict[str, str] = {
    "NSE_FO":  f"{BASE_URL}/NSE_FO_sym_master.json",
    "NSE_CM":  f"{BASE_URL}/NSE_CM_sym_master.json",
    "BSE_CM":  f"{BASE_URL}/BSE_CM_sym_master.json",
    "BSE_FO":  f"{BASE_URL}/BSE_FO_sym_master.json",
    "MCX_COM": f"{BASE_URL}/MCX_COM_sym_master.json",
}

ALLOWED_CANONICAL_EXCHANGES = {"NSE", "NFO", "BSE", "BFO", "MCX"}

# ── Instrument type codes → human label ───────────────────────────────────────
# Fyers JSON field exInstType generally maps to these known values.
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
_last_refresh: Optional[datetime] = None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _today_ist() -> str:
    """Current date in IST as YYYY-MM-DD."""
    return now_ist().strftime("%Y-%m-%d")


def _needs_refresh() -> bool:
    global _last_refresh
    return not (_last_refresh and _last_refresh >= current_refresh_cycle_start())


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


def _expiry_to_date_and_epoch(expiry_value: Any) -> tuple[str, int]:
    """Normalize Fyers JSON expiryDate into display date and epoch seconds."""
    if expiry_value in (None, ""):
        return "", 0

    raw = str(expiry_value).strip()
    if not raw:
        return "", 0

    # Usually provided as unix timestamp (seconds/milliseconds).
    if raw.isdigit():
        try:
            ts = int(raw)
            if ts <= 0:
                return "", 0
            if ts > 10_000_000_000:
                ts //= 1000
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            return dt.strftime("%-d-%b-%Y"), ts
        except (ValueError, OverflowError):
            return "", 0

    # Fallback for non-epoch values.
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            dt = datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            return dt.strftime("%-d-%b-%Y"), int(dt.timestamp())
        except ValueError:
            continue
    return "", 0


def _exchange_from_segment(segment_key: str) -> str:
    """Derive canonical exchange name from segment key (NSE_FO → NFO, NSE_CM → NSE)."""
    mapping = {
        "NSE_FO":  "NFO",
        "BSE_FO":  "BFO",
        "NSE_CM":  "NSE",
        "BSE_CM":  "BSE",
        "MCX_COM": "MCX",
        "NSE_COM": "NSECOM",
        "NSE_CD":  "CDS",
    }
    return mapping.get(segment_key, segment_key)


# ── JSON parsing ──────────────────────────────────────────────────────────────

def _parse_json(content: str, segment_key: str) -> tuple[Dict[str, Dict[str, Any]], Dict[str, str]]:
    """Parse a Fyers symbol master JSON payload."""
    exchange = _exchange_from_segment(segment_key)
    records: Dict[str, Dict[str, Any]] = {}
    token_map: Dict[str, str] = {}

    payload = json.loads(content)
    if not isinstance(payload, dict):
        return records, token_map

    for ticker, item in payload.items():
        if not isinstance(item, dict):
            continue
        try:
            fy_token = str(item.get("fyToken", "")).strip()
            description = str(item.get("symDetails") or item.get("symbolDesc") or item.get("exSymName") or "").strip()
            instr_code = str(item.get("exInstType", "")).strip()
            lot_size = int(float(item.get("minLotSize", 1) or 1))
            tick_size = float(item.get("tickSize", 0.05) or 0.05)
            isin = str(item.get("isin", "")).strip()
            fyers_symbol = str(item.get("symTicker") or ticker or "").strip()
            short_token = str(item.get("exToken", "")).strip()
            underlying = str(item.get("underSym", "")).strip()
            strike_raw = item.get("strikePrice")
            strike_price = float(strike_raw) if strike_raw not in (None, "", "-1", -1, -1.0) else -1.0
            option_type = str(item.get("optType", "XX") or "XX").strip()

            if not fyers_symbol:
                continue

            instrument = _INSTR_CODE.get(instr_code, "EQ")
            expiry_date, expiry_epoch = _expiry_to_date_and_epoch(item.get("expiryDate"))

            if exchange not in ALLOWED_CANONICAL_EXCHANGES:
                continue

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
                "ExpiryEpoch":   expiry_epoch,
                "StrikePrice":   strike_price if strike_price > 0 else None,
                "OptionType":    option_type if option_type not in ("XX", "") else None,
            }
            records[fyers_symbol] = record
            if short_token:
                token_map[short_token] = fyers_symbol

        except Exception as exc:
            logger.debug("Skipping symbol %s in %s: %s", ticker, segment_key, exc)
            continue

    return records, token_map


# ── Download + build ──────────────────────────────────────────────────────────

def _download(segment_key: str, url: str, temp_dir: str) -> str:
    logger.info("Downloading Fyers scriptmaster: %s", url)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    raw_path = Path(temp_dir) / f"{segment_key}.json"
    raw_path.write_bytes(resp.content)
    return raw_path.read_text(encoding="utf-8")


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
    """Download all segment JSON files and rebuild in-memory indices."""
    global _last_refresh

    if not force and not _needs_refresh():
        logger.info("Fyers scriptmaster already refreshed for current cycle — skipping download")
        return

    temp_sm: Dict[str, Dict[str, Any]] = {}
    temp_tok: Dict[str, Dict[str, str]] = {}

    with tempfile.TemporaryDirectory(prefix="smart_trader_fyers_") as temp_dir:
        for seg_key, url in SCRIPTMASTER_URLS.items():
            try:
                content = _download(seg_key, url, temp_dir)
                records, token_map = _parse_json(content, seg_key)
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
        _last_refresh = now_ist()

    logger.info("Fyers scriptmaster refreshed: %d total symbols", len(temp_sm))


def _load_from_disk() -> None:
    """Persistent disk cache is disabled; startup/runtime should use symbols_db instead."""
    logger.debug("Fyers disk cache disabled; no persistent scriptmaster files are loaded")


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


def export_cleaned_csv(output_path: Optional[str] = None) -> str:
    """Export currently loaded Fyers records to a cleaned CSV file."""
    if not FYERS_SCRIPTMASTER:
        refresh()

    with _LOCK:
        rows = list(FYERS_SCRIPTMASTER.values())

    if not rows:
        raise RuntimeError("No Fyers scriptmaster data available to export")

    if output_path:
        out_path = Path(output_path)
    else:
        out_path = Path(__file__).resolve().parent / "fyers_scriptmaster_cleaned.csv"

    exported = write_unified_csv(rows, str(out_path), broker="fyers")

    logger.info("Exported Fyers cleaned CSV: %s (%d rows)", out_path, len(rows))
    return exported
