"""
Dhan ScriptMaster — Instrument Master Loader
==============================================

Downloads and caches Dhan instrument data.

Data sources (no auth required):
    Compact:  https://images.dhan.co/api-data/api-scrip-master.csv
    Detailed: https://images.dhan.co/api-data/api-scrip-master-detailed.csv

CSV columns (compact):
    SEM_EXM_EXCH_ID, SEM_SEGMENT, SEM_SMST_SECURITY_ID, SEM_INSTRUMENT_NAME,
    SEM_EXPIRY_CODE, SEM_EXPIRY_DATE, SEM_STRIKE_PRICE, SEM_OPTION_TYPE,
    SEM_TRADING_SYMBOL, SEM_LOT_UNITS, SEM_TICK_SIZE, SEM_CUSTOM_SYMBOL,
    SM_SYMBOL_NAME, SEM_EXCH_INSTRUMENT_TYPE, SEM_SERIES, ...

Exchange values: NSE, BSE, MCX
Segment values: E (Equity), D (Derivatives), C (Currency), M (Commodity)
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

logger = logging.getLogger("smart_trader.dhan_scriptmaster")

# ── Config ────────────────────────────────────────────────────────────────────

SCRIPTMASTER_VERSION = "1.0"
SCRIPTMASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

# ── Segment → Exchange mapping ────────────────────────────────────────────────

_SEGMENT_MAP: Dict[str, Dict[str, str]] = {
    "NSE": {"E": "NSE", "D": "NFO", "C": "CDS"},
    "BSE": {"E": "BSE", "D": "BFO", "C": "BCD"},
    "MCX": {"M": "MCX", "D": "MCX"},
}

ALLOWED_CANONICAL_EXCHANGES = {"NSE", "NFO", "BSE", "BFO", "MCX"}

# Instrument name → canonical
_INSTR_MAP: Dict[str, str] = {
    "EQUITY":   "EQ",
    "FUTIDX":   "FUT",
    "FUTSTK":   "FUT",
    "FUTCOM":   "FUT",
    "FUTCUR":   "FUT",
    "FUTIRC":   "FUT",
    "OPTIDX":   "OPT",
    "OPTSTK":   "OPT",
    "OPTFUT":   "OPT",
    "OPTCOM":   "OPT",
    "OPTCUR":   "OPT",
    "INDEX":    "IDX",
}

# ── In-memory stores ─────────────────────────────────────────────────────────

# DHAN_SCRIPTMASTER[security_id] = record
DHAN_SCRIPTMASTER: Dict[str, Dict[str, Any]] = {}

# SYMBOL_INDEX[display_name] = record
SYMBOL_INDEX: Dict[str, Dict[str, Any]] = {}

# Expiry calendar
EXPIRY_CALENDAR: Dict[str, Dict[str, Dict[str, List[str]]]] = {}

_LOCK = threading.RLock()
_last_refresh: Optional[datetime] = None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _today_ist() -> str:
    return now_ist().strftime("%Y-%m-%d")


def _needs_refresh() -> bool:
    global _last_refresh
    return not (_last_refresh and _last_refresh >= current_refresh_cycle_start())


def _normalise_expiry(raw: str) -> str:
    """Normalise Dhan expiry to DD-Mon-YYYY."""
    if not raw:
        return ""
    raw = raw.strip()
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d %H:%M:%S", "%d%b%Y"):
        try:
            dt = datetime.strptime(raw.split(" ")[0], fmt)
            return dt.strftime("%-d-%b-%Y")
        except ValueError:
            continue
    return raw


# ── Parsing ───────────────────────────────────────────────────────────────────

def _parse_csv(content: str) -> Dict[str, Dict[str, Any]]:
    """Parse Dhan CSV scripmaster into records keyed by security_id."""
    records: Dict[str, Dict[str, Any]] = {}
    reader = csv.DictReader(io.StringIO(content))
    if not reader.fieldnames:
        return {}

    for row in reader:
        # The column names may vary; try common patterns
        security_id = (
            row.get("SEM_SMST_SECURITY_ID")
            or row.get("SECURITY_ID")
            or row.get("SM_KEY_SECURITY_ID")
            or ""
        ).strip()
        if not security_id:
            continue

        exch_id = (row.get("SEM_EXM_EXCH_ID") or row.get("EXCH_ID") or "").strip().upper()
        segment = (row.get("SEM_SEGMENT") or row.get("SEGMENT") or "").strip().upper()
        instrument_name = (row.get("SEM_INSTRUMENT_NAME") or row.get("INSTRUMENT") or "").strip().upper()
        symbol_name = (row.get("SM_SYMBOL_NAME") or row.get("SYMBOL_NAME") or "").strip()
        trading_symbol = (row.get("SEM_TRADING_SYMBOL") or row.get("TRADING_SYMBOL") or "").strip()
        display_name = (row.get("SEM_CUSTOM_SYMBOL") or row.get("DISPLAY_NAME") or "").strip()
        instrument_type = (row.get("SEM_EXCH_INSTRUMENT_TYPE") or row.get("INSTRUMENT_TYPE") or "").strip()
        series = (row.get("SEM_SERIES") or row.get("SERIES") or "").strip()
        expiry_raw = (row.get("SEM_EXPIRY_DATE") or row.get("SM_EXPIRY_DATE") or "").strip()
        option_type = (row.get("SEM_OPTION_TYPE") or row.get("OPTION_TYPE") or "").strip().upper()

        try:
            strike = float(row.get("SEM_STRIKE_PRICE") or row.get("STRIKE_PRICE") or "0")
        except (ValueError, TypeError):
            strike = 0.0

        try:
            lot_size = int(float(row.get("SEM_LOT_UNITS") or row.get("LOT_SIZE") or "1"))
        except (ValueError, TypeError):
            lot_size = 1

        try:
            tick_size = float(row.get("SEM_TICK_SIZE") or row.get("TICK_SIZE") or "0.05")
        except (ValueError, TypeError):
            tick_size = 0.05

        # Derive canonical exchange
        exchange = _SEGMENT_MAP.get(exch_id, {}).get(segment, exch_id)
        if exchange not in ALLOWED_CANONICAL_EXCHANGES:
            continue
        canonical_instr = _INSTR_MAP.get(instrument_name, instrument_name)
        expiry = _normalise_expiry(expiry_raw)

        # Option type normalisation
        if option_type not in ("CE", "PE"):
            option_type = None
        else:
            option_type = option_type

        underlying = symbol_name or display_name.split(" ")[0] if display_name else ""

        records[security_id] = {
            "SecurityId":     security_id,
            "Token":          security_id,
            "Exchange":       exchange,
            "ExchId":         exch_id,
            "Segment":        segment,
            "Symbol":         symbol_name,
            "Underlying":     underlying,
            "TradingSymbol":  trading_symbol or display_name,
            "DisplayName":    display_name,
            "Instrument":     canonical_instr,
            "InstrumentName": instrument_name,
            "InstrumentType": instrument_type,
            "Series":         series,
            "Expiry":         expiry,
            "StrikePrice":    strike if strike > 0 else None,
            "OptionType":     option_type,
            "LotSize":        lot_size,
            "TickSize":       tick_size,
        }

    return records


def _build_indices() -> None:
    """Build secondary indices."""
    SYMBOL_INDEX.clear()
    for rec in DHAN_SCRIPTMASTER.values():
        for key_field in ("DisplayName", "TradingSymbol", "Symbol"):
            val = rec.get(key_field, "")
            if val:
                SYMBOL_INDEX[val.upper()] = rec

    cal: Dict[str, Dict[str, Dict[str, List[str]]]] = {}
    for rec in DHAN_SCRIPTMASTER.values():
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
    """Download Dhan scripmaster CSV and rebuild caches."""
    global _last_refresh

    if not force and not _needs_refresh():
        logger.info("Dhan scriptmaster already refreshed for current cycle — skipping download")
        return

    logger.info("Downloading Dhan scriptmaster from %s", SCRIPTMASTER_URL)
    try:
        resp = requests.get(SCRIPTMASTER_URL, timeout=60)
        resp.raise_for_status()
        with tempfile.TemporaryDirectory(prefix="smart_trader_dhan_") as temp_dir:
            cache_path = Path(temp_dir) / "scripmaster.csv"
            cache_path.write_text(resp.text)
            content = cache_path.read_text()
    except Exception as exc:
        logger.error("Failed to download Dhan scriptmaster: %s", exc)
        return

    records = _parse_csv(content)
    if not records:
        logger.error("Dhan scriptmaster returned empty data")
        return

    with _LOCK:
        DHAN_SCRIPTMASTER.clear()
        DHAN_SCRIPTMASTER.update(records)
        _build_indices()
        _last_refresh = now_ist()

    logger.info("Dhan scriptmaster refreshed: %d symbols", len(records))


def _load_from_disk() -> None:
    """Persistent disk cache is disabled; runtime symbol lookups should use symbols_db."""
    logger.debug("Dhan disk cache disabled; no persistent scriptmaster files are loaded")


def search(query: str, exchange: Optional[str] = None) -> List[Dict[str, Any]]:
    """Search Dhan scriptmaster."""
    q = query.upper()
    results = []
    with _LOCK:
        for rec in DHAN_SCRIPTMASTER.values():
            if exchange and rec.get("Exchange", "").upper() != exchange.upper():
                continue
            sym = (rec.get("Symbol") or "").upper()
            display = (rec.get("DisplayName") or "").upper()
            tsym = (rec.get("TradingSymbol") or "").upper()
            if q in sym or q in display or q in tsym:
                results.append(rec)
            if len(results) >= 50:
                break
    return results


def get_futures(underlying: str, exchange: str = "NFO") -> List[Dict[str, Any]]:
    u = underlying.upper()
    with _LOCK:
        matches = [
            r for r in DHAN_SCRIPTMASTER.values()
            if r.get("Instrument") == "FUT"
            and r.get("Exchange", "").upper() == exchange.upper()
            and (r.get("Underlying", "").upper() == u or r.get("Symbol", "").upper() == u)
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
            r for r in DHAN_SCRIPTMASTER.values()
            if r.get("Instrument") == "OPT"
            and r.get("Exchange", "").upper() == exchange.upper()
            and (r.get("Underlying", "").upper() == u or r.get("Symbol", "").upper() == u)
            and (expiry is None or r.get("Expiry") == expiry)
            and (strike is None or r.get("StrikePrice") == strike)
            and (option_type is None or (r.get("OptionType") or "").upper() == option_type.upper())
        ]
    return sorted(matches, key=lambda r: (_parse_expiry_sort(r.get("Expiry", "")), r.get("StrikePrice") or 0))


def get_expiry_list(underlying: str, exchange: str = "NFO", kind: str = "OPTION") -> List[str]:
    with _LOCK:
        return EXPIRY_CALENDAR.get(exchange.upper(), {}).get(underlying.upper(), {}).get(kind.upper(), [])


def get_record_by_security_id(security_id: str) -> Optional[Dict[str, Any]]:
    return DHAN_SCRIPTMASTER.get(str(security_id))


def get_total_count() -> int:
    return len(DHAN_SCRIPTMASTER)


def export_cleaned_csv(output_path: Optional[str] = None) -> str:
    """Export currently loaded Dhan records to a cleaned CSV file."""
    if not DHAN_SCRIPTMASTER:
        refresh()

    with _LOCK:
        rows = list(DHAN_SCRIPTMASTER.values())

    if not rows:
        raise RuntimeError("No Dhan scriptmaster data available to export")

    if output_path:
        out_path = Path(output_path)
    else:
        out_path = Path(__file__).resolve().parent / "dhan_scriptmaster_cleaned.csv"

    exported = write_unified_csv(rows, str(out_path), broker="dhan")

    logger.info("Exported Dhan cleaned CSV: %s (%d rows)", out_path, len(rows))
    return exported


def _parse_expiry_sort(expiry_str: str) -> datetime:
    try:
        return datetime.strptime(expiry_str, "%d-%b-%Y")
    except (ValueError, TypeError):
        return datetime.max
