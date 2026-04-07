"""
Groww ScriptMaster — Instrument Master Loader
===============================================

Downloads and caches Groww Trade API instrument data.

Data source (no auth required):
    https://growwapi-assets.groww.in/instruments/instrument.csv

CSV columns (comma-separated):
    exchange, exchange_token, trading_symbol, groww_symbol, name,
    instrument_type, segment, series, isin, underlying_symbol,
    underlying_exchange_token, expiry_date, strike_price, lot_size,
    tick_size, freeze_quantity, is_reserved, buy_allowed, sell_allowed

Segments: CASH, FNO, COMMODITY
Exchange: NSE, BSE, MCX
instrument_type: EQ, FUT, CE, PE, (empty for index, etc.)
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

logger = logging.getLogger("smart_trader.groww_scriptmaster")

# ── Config ────────────────────────────────────────────────────────────────────

SCRIPTMASTER_VERSION = "1.0"
SCRIPTMASTER_URL = "https://growwapi-assets.groww.in/instruments/instrument.csv"

# ── Segment → canonical exchange ──────────────────────────────────────────────

_SEGMENT_MAP: Dict[str, Dict[str, str]] = {
    "NSE": {"CASH": "NSE", "FNO": "NFO"},
    "BSE": {"CASH": "BSE", "FNO": "BFO"},
    "MCX": {"COMMODITY": "MCX", "FNO": "MCX"},
}

# Instrument type → canonical
_INSTR_MAP: Dict[str, str] = {
    "EQ":  "EQ",
    "FUT": "FUT",
    "CE":  "OPT",
    "PE":  "OPT",
    "":    "EQ",
}

# ── In-memory stores ─────────────────────────────────────────────────────────

# GROWW_SCRIPTMASTER["exchange:trading_symbol"] = record
GROWW_SCRIPTMASTER: Dict[str, Dict[str, Any]] = {}

# Token index: TOKEN_INDEX[exchange_token] = record
TOKEN_INDEX: Dict[str, Dict[str, Any]] = {}

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
    """Convert Groww expiry (YYYY-MM-DD) to DD-Mon-YYYY."""
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


def _extract_underlying(trading_symbol: str, underlying_symbol: str, instrument_type: str) -> str:
    """Extract underlying from Groww data."""
    if underlying_symbol and underlying_symbol.strip():
        return underlying_symbol.strip()
    if not trading_symbol:
        return ""
    if instrument_type in ("EQ", ""):
        return trading_symbol
    import re
    s = trading_symbol.upper()
    s = re.sub(r"\d{2}[A-Z]{3}\d{2,4}.*$", "", s)
    s = re.sub(r"(FUT|CE|PE)$", "", s)
    return s or trading_symbol


# ── Parsing ───────────────────────────────────────────────────────────────────

def _parse_csv(content: str) -> Dict[str, Dict[str, Any]]:
    """Parse Groww instrument CSV into records."""
    records: Dict[str, Dict[str, Any]] = {}

    # Groww CSV may have whitespace around column names
    reader = csv.DictReader(io.StringIO(content), skipinitialspace=True)
    if not reader.fieldnames:
        return {}

    # Clean field names
    reader.fieldnames = [f.strip() for f in reader.fieldnames]

    for row in reader:
        exchange = (row.get("exchange") or "").strip().upper()
        exchange_token = (row.get("exchange_token") or "").strip()
        trading_symbol = (row.get("trading_symbol") or "").strip()
        groww_symbol = (row.get("groww_symbol") or "").strip()
        name = (row.get("name") or "").strip()
        instrument_type = (row.get("instrument_type") or "").strip().upper()
        segment = (row.get("segment") or "").strip().upper()
        series = (row.get("series") or "").strip()
        isin = (row.get("isin") or "").strip()
        underlying_symbol = (row.get("underlying_symbol") or "").strip()
        underlying_exchange_token = (row.get("underlying_exchange_token") or "").strip()
        expiry_raw = (row.get("expiry_date") or "").strip()

        if not trading_symbol or not exchange:
            continue

        try:
            strike = float(row.get("strike_price") or "0")
        except (ValueError, TypeError):
            strike = 0.0

        try:
            lot_size = int(float(row.get("lot_size") or "1"))
        except (ValueError, TypeError):
            lot_size = 1

        try:
            tick_size = float(row.get("tick_size") or "0.05")
        except (ValueError, TypeError):
            tick_size = 0.05

        try:
            freeze_qty = int(float(row.get("freeze_quantity") or "0"))
        except (ValueError, TypeError):
            freeze_qty = 0

        buy_allowed = (row.get("buy_allowed") or "").strip() == "1"
        sell_allowed = (row.get("sell_allowed") or "").strip() == "1"

        canonical_exchange = _SEGMENT_MAP.get(exchange, {}).get(segment, exchange)
        canonical_instr = _INSTR_MAP.get(instrument_type, instrument_type)
        expiry = _normalise_expiry(expiry_raw)
        underlying = _extract_underlying(trading_symbol, underlying_symbol, instrument_type)

        option_type = None
        if instrument_type in ("CE", "PE"):
            option_type = instrument_type

        key = f"{exchange}:{trading_symbol}"
        record = {
            "ExchangeToken":           exchange_token,
            "Token":                   exchange_token,
            "TradingSymbol":           trading_symbol,
            "GrowwSymbol":             groww_symbol,
            "Name":                    name,
            "Underlying":              underlying,
            "UnderlyingExchangeToken": underlying_exchange_token,
            "Exchange":                canonical_exchange,
            "RawExchange":             exchange,
            "Segment":                 segment,
            "Series":                  series,
            "ISIN":                    isin,
            "Instrument":              canonical_instr,
            "InstrumentType":          instrument_type,
            "Expiry":                  expiry,
            "StrikePrice":             strike if strike > 0 else None,
            "OptionType":              option_type,
            "LotSize":                 lot_size,
            "TickSize":                tick_size,
            "FreezeQuantity":          freeze_qty,
            "BuyAllowed":              buy_allowed,
            "SellAllowed":             sell_allowed,
        }
        records[key] = record
        if exchange_token:
            TOKEN_INDEX[exchange_token] = record

    return records


def _build_expiry_calendar() -> None:
    """Build expiry calendar."""
    cal: Dict[str, Dict[str, Dict[str, List[str]]]] = {}
    for rec in GROWW_SCRIPTMASTER.values():
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
    """Download Groww instrument CSV and rebuild caches."""
    global _last_refresh

    if not force and not _needs_refresh():
        logger.info("Groww scriptmaster already refreshed for current cycle — skipping download")
        return

    logger.info("Downloading Groww scriptmaster from %s", SCRIPTMASTER_URL)
    try:
        resp = requests.get(SCRIPTMASTER_URL, timeout=60)
        resp.raise_for_status()
        with tempfile.TemporaryDirectory(prefix="smart_trader_groww_") as temp_dir:
            cache_path = Path(temp_dir) / "instrument.csv"
            cache_path.write_text(resp.text)
            content = cache_path.read_text()
    except Exception as exc:
        logger.error("Failed to download Groww scriptmaster: %s", exc)
        return

    records = _parse_csv(content)
    if not records:
        logger.error("Groww scriptmaster returned empty data")
        return

    with _LOCK:
        GROWW_SCRIPTMASTER.clear()
        GROWW_SCRIPTMASTER.update(records)
        _build_expiry_calendar()
        _last_refresh = now_ist()

    logger.info("Groww scriptmaster refreshed: %d symbols", len(records))


def _load_from_disk() -> None:
    """Persistent disk cache is disabled; runtime symbol lookups should use symbols_db."""
    logger.debug("Groww disk cache disabled; no persistent scriptmaster files are loaded")


def search(query: str, exchange: Optional[str] = None) -> List[Dict[str, Any]]:
    """Search Groww scriptmaster."""
    q = query.upper()
    results = []
    with _LOCK:
        for rec in GROWW_SCRIPTMASTER.values():
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
            r for r in GROWW_SCRIPTMASTER.values()
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
            r for r in GROWW_SCRIPTMASTER.values()
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


def get_record_by_groww_symbol(groww_symbol: str) -> Optional[Dict[str, Any]]:
    """Look up by Groww symbol e.g. 'NSE-BANKNIFTY-24Dec25-27000-PE'."""
    for rec in GROWW_SCRIPTMASTER.values():
        if rec.get("GrowwSymbol") == groww_symbol:
            return rec
    return None


def get_record_by_token(exchange_token: str) -> Optional[Dict[str, Any]]:
    return TOKEN_INDEX.get(str(exchange_token))


def get_total_count() -> int:
    return len(GROWW_SCRIPTMASTER)


def _parse_expiry_sort(expiry_str: str) -> datetime:
    try:
        return datetime.strptime(expiry_str, "%d-%b-%Y")
    except (ValueError, TypeError):
        return datetime.max
