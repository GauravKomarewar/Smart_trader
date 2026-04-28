"""
Zerodha Kite ScriptMaster — Instrument Master Loader
======================================================

Downloads and caches Zerodha Kite Connect v3 instrument data.

Data source (requires auth for full dump, but exchange-specific endpoints may be public):
    Full:       https://api.kite.trade/instruments
    Per-exch:   https://api.kite.trade/instruments/{exchange}

CSV columns (comma-separated):
    instrument_token, exchange_token, tradingsymbol, name, last_price,
    expiry, strike, tick_size, lot_size, instrument_type, segment, exchange

instrument_type: EQ, FUT, CE, PE
segment: NSE, BSE, NFO-FUT, NFO-OPT, CDS-FUT, CDS-OPT, BCD-FUT, BCD-OPT, MCX
exchange: NSE, BSE, NFO, CDS, BCD, MCX

NOTE: The full instruments dump requires authentication via
      `Authorization: token api_key:access_token` header.
      We attempt unauthenticated first; if that fails, we try
      with credentials from the environment or skip silently.
"""
from __future__ import annotations

import csv
import io
import logging
import threading
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from core.daily_refresh import current_refresh_cycle_start, now_ist
from scripts.scriptmaster_export_utils import write_unified_csv

logger = logging.getLogger("smart_trader.kite_scriptmaster")

# ── Config ────────────────────────────────────────────────────────────────────

SCRIPTMASTER_VERSION = "1.0"
BASE_URL = "https://api.kite.trade/instruments"

EXCHANGES = ["NSE", "BSE", "NFO", "BFO", "MCX"]
ALLOWED_CANONICAL_EXCHANGES = {"NSE", "NFO", "BSE", "BFO", "MCX"}

# ── Instrument type → canonical ──────────────────────────────────────────────

_INSTR_MAP: Dict[str, str] = {
    "EQ":  "EQ",
    "FUT": "FUT",
    "CE":  "OPT",
    "PE":  "OPT",
}

# Segment → canonical exchange
_SEGMENT_TO_EXCHANGE: Dict[str, str] = {
    "NSE":     "NSE",
    "BSE":     "BSE",
    "NFO-FUT": "NFO",
    "NFO-OPT": "NFO",
    "CDS-FUT": "CDS",
    "CDS-OPT": "CDS",
    "BCD-FUT": "BCD",
    "BCD-OPT": "BCD",
    "MCX":     "MCX",
    "MCX-FUT": "MCX",
    "MCX-OPT": "MCX",
    "BFO-FUT": "BFO",
    "BFO-OPT": "BFO",
}

# ── In-memory stores ─────────────────────────────────────────────────────────

# KITE_SCRIPTMASTER["exchange:tradingsymbol"] = record
KITE_SCRIPTMASTER: Dict[str, Dict[str, Any]] = {}

# Token index: TOKEN_INDEX[instrument_token] = record
TOKEN_INDEX: Dict[str, Dict[str, Any]] = {}

# Expiry calendar
EXPIRY_CALENDAR: Dict[str, Dict[str, Dict[str, List[str]]]] = {}

_LOCK = threading.RLock()
_last_refresh: Optional[datetime] = None

# Optional credentials for authenticated download
_api_key: Optional[str] = None
_access_token: Optional[str] = None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _today_ist() -> str:
    return now_ist().strftime("%Y-%m-%d")


def _needs_refresh() -> bool:
    global _last_refresh
    return not (_last_refresh and _last_refresh >= current_refresh_cycle_start())


def set_credentials(api_key: str, access_token: str) -> None:
    """Set Kite API credentials for authenticated instrument download."""
    global _api_key, _access_token
    _api_key = api_key
    _access_token = access_token


def _normalise_expiry(raw: str) -> str:
    """Convert Kite expiry (YYYY-MM-DD) to DD-Mon-YYYY."""
    if not raw:
        return ""
    raw = raw.strip()
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d-%B-%Y"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.strftime("%-d-%b-%Y")
        except ValueError:
            continue
    return raw


def _extract_underlying(tradingsymbol: str, instrument_type: str, name: str) -> str:
    """Extract underlying from Kite tradingsymbol."""
    if not tradingsymbol:
        return name or ""
    if instrument_type == "EQ":
        return tradingsymbol
    import re
    s = tradingsymbol.upper()
    # Remove trailing strike+CE/PE for options, or FUT suffix
    s = re.sub(r"\d{2}[A-Z]{3}\d{2,4}.*$", "", s)
    s = re.sub(r"(FUT|CE|PE)$", "", s)
    return s or name or tradingsymbol


# ── CSV Parsing ───────────────────────────────────────────────────────────────

def _parse_csv(content: str) -> Dict[str, Dict[str, Any]]:
    """Parse Kite instruments CSV into record dict."""
    records: Dict[str, Dict[str, Any]] = {}
    reader = csv.DictReader(io.StringIO(content))
    if not reader.fieldnames:
        return {}

    for row in reader:
        instrument_token = (row.get("instrument_token") or "").strip()
        exchange_token = (row.get("exchange_token") or "").strip()
        tradingsymbol = (row.get("tradingsymbol") or "").strip()
        name = (row.get("name") or "").strip()
        expiry_raw = (row.get("expiry") or "").strip()
        instrument_type = (row.get("instrument_type") or "").strip().upper()
        segment = (row.get("segment") or "").strip()
        exchange = (row.get("exchange") or "").strip().upper()

        if not tradingsymbol or not exchange:
            continue

        try:
            strike = float(row.get("strike") or "0")
        except (ValueError, TypeError):
            strike = 0.0

        try:
            tick_size = float(row.get("tick_size") or "0.05")
        except (ValueError, TypeError):
            tick_size = 0.05

        try:
            lot_size = int(float(row.get("lot_size") or "1"))
        except (ValueError, TypeError):
            lot_size = 1

        canonical_instr = _INSTR_MAP.get(instrument_type, instrument_type)
        expiry = _normalise_expiry(expiry_raw)
        canonical_exchange = _SEGMENT_TO_EXCHANGE.get(segment, exchange)
        if canonical_exchange not in ALLOWED_CANONICAL_EXCHANGES:
            continue
        underlying = _extract_underlying(tradingsymbol, instrument_type, name)

        option_type = None
        if instrument_type in ("CE", "PE"):
            option_type = instrument_type

        key = f"{exchange}:{tradingsymbol}"
        record = {
            "InstrumentToken": instrument_token,
            "ExchangeToken":   exchange_token,
            "Token":           instrument_token,
            "TradingSymbol":   tradingsymbol,
            "Name":            name,
            "Underlying":      underlying,
            "Exchange":        canonical_exchange,
            "Segment":         segment,
            "Instrument":      canonical_instr,
            "InstrumentType":  instrument_type,
            "Expiry":          expiry,
            "StrikePrice":     strike if strike > 0 else None,
            "OptionType":      option_type,
            "LotSize":         lot_size,
            "TickSize":        tick_size,
            "KiteSymbol":      key,
        }
        records[key] = record
        TOKEN_INDEX[instrument_token] = record

    return records


def _build_expiry_calendar() -> None:
    """Build expiry calendar from KITE_SCRIPTMASTER."""
    cal: Dict[str, Dict[str, Dict[str, List[str]]]] = {}
    for rec in KITE_SCRIPTMASTER.values():
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


# ── Download ──────────────────────────────────────────────────────────────────

def _download(exchange: Optional[str] = None) -> str:
    """Download instruments CSV, with or without auth."""
    url = f"{BASE_URL}/{exchange}" if exchange else BASE_URL
    headers = {"X-Kite-Version": "3"}
    if _api_key and _access_token:
        headers["Authorization"] = f"token {_api_key}:{_access_token}"

    logger.info("Downloading Kite instruments: %s", url)
    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.text


# ── Public API ────────────────────────────────────────────────────────────────

def refresh(force: bool = False) -> None:
    """Download Kite instrument dump and rebuild caches."""
    global _last_refresh

    if not force and not _needs_refresh():
        logger.info("Kite scriptmaster already refreshed for current cycle — skipping download")
        return

    all_records: Dict[str, Dict[str, Any]] = {}

    with tempfile.TemporaryDirectory(prefix="smart_trader_kite_") as temp_dir:
        # Try full dump first
        try:
            content = _download()
            cache_path = Path(temp_dir) / "instruments.csv"
            cache_path.write_text(content)
            records = _parse_csv(cache_path.read_text())
            all_records.update(records)
            logger.info("Kite full dump: %d instruments", len(records))
        except Exception as exc:
            logger.warning("Kite full dump failed (%s), trying per-exchange", exc)
            # Fallback: download per exchange
            for exch in EXCHANGES:
                try:
                    content = _download(exch)
                    cache_path = Path(temp_dir) / f"{exch}.csv"
                    cache_path.write_text(content)
                    records = _parse_csv(cache_path.read_text())
                    all_records.update(records)
                    logger.info("Kite %s: %d instruments", exch, len(records))
                except Exception as e2:
                    logger.warning("Kite %s download failed: %s", exch, e2)

    if not all_records:
        logger.warning("No Kite instruments downloaded during refresh")
        return

    with _LOCK:
        KITE_SCRIPTMASTER.clear()
        KITE_SCRIPTMASTER.update(all_records)
        _build_expiry_calendar()
        _last_refresh = now_ist()

    logger.info("Kite scriptmaster refreshed: %d symbols", len(all_records))


def _load_from_disk() -> None:
    """Persistent disk cache is disabled; runtime symbol lookups should use symbols_db."""
    logger.debug("Kite disk cache disabled; no persistent scriptmaster files are loaded")


def search(query: str, exchange: Optional[str] = None) -> List[Dict[str, Any]]:
    """Search Kite scriptmaster."""
    q = query.upper()
    results = []
    with _LOCK:
        for rec in KITE_SCRIPTMASTER.values():
            if exchange and rec.get("Exchange", "").upper() != exchange.upper():
                continue
            tsym = (rec.get("TradingSymbol") or "").upper()
            name = (rec.get("Name") or "").upper()
            underlying = (rec.get("Underlying") or "").upper()
            if q in tsym or q in name or q in underlying:
                results.append(rec)
            if len(results) >= 50:
                break
    return results


def get_futures(underlying: str, exchange: str = "NFO") -> List[Dict[str, Any]]:
    u = underlying.upper()
    with _LOCK:
        matches = [
            r for r in KITE_SCRIPTMASTER.values()
            if r.get("Instrument") == "FUT"
            and r.get("Exchange", "").upper() == exchange.upper()
            and r.get("Underlying", "").upper() == u
        ]
    return sorted(matches, key=lambda r: _parse_expiry_sort(r.get("Expiry", "")))


def get_options(
    underlying: str,
    exchange: str = "NFO",
    expiry: Optional[str] = None,
    strike: Optional[float] = None,
    option_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    u = underlying.upper()
    with _LOCK:
        matches = [
            r for r in KITE_SCRIPTMASTER.values()
            if r.get("Instrument") == "OPT"
            and r.get("Exchange", "").upper() == exchange.upper()
            and r.get("Underlying", "").upper() == u
            and (expiry is None or r.get("Expiry") == expiry)
            and (strike is None or r.get("StrikePrice") == strike)
            and (option_type is None or (r.get("OptionType") or "").upper() == option_type.upper())
        ]
    return sorted(matches, key=lambda r: (_parse_expiry_sort(r.get("Expiry", "")), r.get("StrikePrice") or 0))


def get_expiry_list(underlying: str, exchange: str = "NFO", kind: str = "OPTION") -> List[str]:
    with _LOCK:
        return EXPIRY_CALENDAR.get(exchange.upper(), {}).get(underlying.upper(), {}).get(kind.upper(), [])


def get_record_by_kite_symbol(kite_symbol: str) -> Optional[Dict[str, Any]]:
    """Look up by 'EXCHANGE:TRADINGSYMBOL' key."""
    return KITE_SCRIPTMASTER.get(kite_symbol)


def get_record_by_token(instrument_token: str) -> Optional[Dict[str, Any]]:
    return TOKEN_INDEX.get(str(instrument_token))


def get_total_count() -> int:
    return len(KITE_SCRIPTMASTER)


def export_cleaned_csv(output_path: Optional[str] = None) -> str:
    """Export currently loaded Kite records to a cleaned CSV file."""
    if not KITE_SCRIPTMASTER:
        refresh()

    with _LOCK:
        rows = list(KITE_SCRIPTMASTER.values())

    if not rows:
        raise RuntimeError("No Kite scriptmaster data available to export")

    if output_path:
        out_path = Path(output_path)
    else:
        out_path = Path(__file__).resolve().parent / "kite_scriptmaster_cleaned.csv"

    exported = write_unified_csv(rows, str(out_path), broker="kite")

    logger.info("Exported Kite cleaned CSV: %s (%d rows)", out_path, len(rows))
    return exported


def _parse_expiry_sort(expiry_str: str) -> datetime:
    try:
        return datetime.strptime(expiry_str, "%d-%b-%Y")
    except (ValueError, TypeError):
        return datetime.max
