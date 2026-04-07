"""
Upstox ScriptMaster — Instrument Master Loader
=================================================

Downloads and caches Upstox instrument data.

Data sources (no auth required, gzipped JSON):
    Complete: https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz
    NSE:      https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz
    BSE:      https://assets.upstox.com/market-quote/instruments/exchange/BSE.json.gz
    MCX:      https://assets.upstox.com/market-quote/instruments/exchange/MCX.json.gz

JSON record format:
    {
      "segment": "NSE_EQ",
      "name": "RELIANCE INDUSTRIES LTD",
      "exchange": "NSE",
      "isin": "INE002A01018",
      "instrument_type": "EQ",
      "instrument_key": "NSE_EQ|INE002A01018",
      "lot_size": 1,
      "freeze_quantity": 100000.0,
      "exchange_token": "2885",
      "tick_size": 5.0,
      "trading_symbol": "RELIANCE",
      "short_name": "RELIANCE",
      "security_type": "NORMAL"
    }

For derivatives, additional fields:
    "expiry": "2026-04-28",
    "strike_price": 24000.0,
    "option_type": "CE"

Segments: NSE_EQ, NSE_INDEX, NSE_FO, NCD_FO, BSE_EQ, BSE_INDEX, BSE_FO, BCD_FO, MCX_FO, NSE_COM
"""
from __future__ import annotations

import gzip
import json
import logging
import threading
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from core.daily_refresh import current_refresh_cycle_start, now_ist

logger = logging.getLogger("smart_trader.upstox_scriptmaster")

# ── Config ────────────────────────────────────────────────────────────────────

SCRIPTMASTER_VERSION = "1.0"
SCRIPTMASTER_URL = (
    "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
)

# ── Segment → canonical exchange ──────────────────────────────────────────────

_SEGMENT_MAP: Dict[str, str] = {
    "NSE_EQ":    "NSE",
    "NSE_INDEX": "NSE",
    "NSE_FO":    "NFO",
    "NCD_FO":    "CDS",
    "BSE_EQ":    "BSE",
    "BSE_INDEX": "BSE",
    "BSE_FO":    "BFO",
    "BCD_FO":    "BCD",
    "MCX_FO":    "MCX",
    "NSE_COM":   "MCX",
}

# Instrument type → canonical
_INSTR_MAP: Dict[str, str] = {
    "EQ":      "EQ",
    "FUTSTK":  "FUT",
    "FUTIDX":  "FUT",
    "FUTCOM":  "FUT",
    "FUTCUR":  "FUT",
    "FUTIRC":  "FUT",
    "OPTSTK":  "OPT",
    "OPTIDX":  "OPT",
    "OPTFUT":  "OPT",
    "OPTCOM":  "OPT",
    "OPTCUR":  "OPT",
    "INDEX":   "IDX",
    "CE":      "OPT",
    "PE":      "OPT",
    "FUT":     "FUT",
}

# ── In-memory stores ─────────────────────────────────────────────────────────

# UPSTOX_SCRIPTMASTER[instrument_key] = record
UPSTOX_SCRIPTMASTER: Dict[str, Dict[str, Any]] = {}

# Flat universal: UPSTOX_UNIVERSAL["exchange:trading_symbol"] = record
UPSTOX_UNIVERSAL: Dict[str, Dict[str, Any]] = {}

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
    """Convert Upstox expiry (YYYY-MM-DD) to DD-Mon-YYYY."""
    if not raw:
        return ""
    raw = str(raw).strip()
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d-%B-%Y"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.strftime("%-d-%b-%Y")
        except ValueError:
            continue
    return raw


def _extract_underlying(trading_symbol: str, instrument_type: str, name: str) -> str:
    """Extract underlying from Upstox trading symbol."""
    if not trading_symbol:
        return name or ""
    if instrument_type in ("EQ", "INDEX", ""):
        return trading_symbol
    import re
    s = trading_symbol.upper()
    s = re.sub(r"\d{2}[A-Z]{3}\d{2,4}.*$", "", s)
    s = re.sub(r"(FUT|CE|PE)$", "", s)
    return s or name or trading_symbol


# ── Parsing ───────────────────────────────────────────────────────────────────

def _parse_json(raw_data: list) -> Dict[str, Dict[str, Any]]:
    """Parse Upstox JSON instrument list."""
    records: Dict[str, Dict[str, Any]] = {}
    for item in raw_data:
        instrument_key = str(item.get("instrument_key") or "").strip()
        if not instrument_key:
            continue

        segment = str(item.get("segment") or "").strip()
        name = str(item.get("name") or "").strip()
        exchange = str(item.get("exchange") or "").strip().upper()
        isin = str(item.get("isin") or "").strip()
        instrument_type = str(item.get("instrument_type") or "").strip().upper()
        trading_symbol = str(item.get("trading_symbol") or "").strip()
        short_name = str(item.get("short_name") or "").strip()
        exchange_token = str(item.get("exchange_token") or "").strip()
        expiry_raw = str(item.get("expiry") or "").strip()
        option_type_raw = str(item.get("option_type") or "").strip().upper()

        try:
            lot_size = int(float(item.get("lot_size", 1)))
        except (ValueError, TypeError):
            lot_size = 1

        try:
            tick_size = float(item.get("tick_size", 5.0))
            # Upstox tick_size is in paise (500 = Rs 5.00)
            if tick_size > 1:
                tick_size = tick_size / 100.0
        except (ValueError, TypeError):
            tick_size = 0.05

        try:
            strike = float(item.get("strike_price") or item.get("strike") or 0)
        except (ValueError, TypeError):
            strike = 0.0

        try:
            freeze_qty = int(float(item.get("freeze_quantity", 0)))
        except (ValueError, TypeError):
            freeze_qty = 0

        canonical_exchange = _SEGMENT_MAP.get(segment, exchange)
        canonical_instr = _INSTR_MAP.get(instrument_type, instrument_type)
        expiry = _normalise_expiry(expiry_raw)
        underlying = _extract_underlying(trading_symbol, instrument_type, short_name or name)

        option_type = None
        if option_type_raw in ("CE", "PE"):
            option_type = option_type_raw
        elif instrument_type in ("CE", "PE"):
            option_type = instrument_type

        records[instrument_key] = {
            "InstrumentKey":  instrument_key,
            "Token":          exchange_token,
            "TradingSymbol":  trading_symbol,
            "ShortName":      short_name,
            "Name":           name,
            "Underlying":     underlying,
            "Exchange":       canonical_exchange,
            "Segment":        segment,
            "Instrument":     canonical_instr,
            "InstrumentType": instrument_type,
            "ISIN":           isin,
            "Expiry":         expiry,
            "StrikePrice":    strike if strike > 0 else None,
            "OptionType":     option_type,
            "LotSize":        lot_size,
            "TickSize":       tick_size,
            "FreezeQuantity": freeze_qty,
        }

    return records


def _build_indices() -> None:
    """Build secondary indices."""
    UPSTOX_UNIVERSAL.clear()
    for rec in UPSTOX_SCRIPTMASTER.values():
        exch = rec.get("Exchange", "")
        tsym = rec.get("TradingSymbol", "")
        if exch and tsym:
            UPSTOX_UNIVERSAL[f"{exch}:{tsym}"] = rec

    cal: Dict[str, Dict[str, Dict[str, List[str]]]] = {}
    for rec in UPSTOX_SCRIPTMASTER.values():
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
    """Download Upstox instrument JSON and rebuild caches."""
    global _last_refresh

    if not force and not _needs_refresh():
        logger.info("Upstox scriptmaster already refreshed for current cycle — skipping download")
        return

    logger.info("Downloading Upstox scriptmaster from %s", SCRIPTMASTER_URL)
    try:
        resp = requests.get(SCRIPTMASTER_URL, timeout=60)
        resp.raise_for_status()
        with tempfile.TemporaryDirectory(prefix="smart_trader_upstox_") as temp_dir:
            cache_path = Path(temp_dir) / "complete.json.gz"
            cache_path.write_bytes(resp.content)
            raw_data = json.loads(gzip.decompress(cache_path.read_bytes()))
    except Exception as exc:
        logger.error("Failed to download Upstox scriptmaster: %s", exc)
        return

    records = _parse_json(raw_data)
    if not records:
        logger.error("Upstox scriptmaster returned empty data")
        return

    with _LOCK:
        UPSTOX_SCRIPTMASTER.clear()
        UPSTOX_SCRIPTMASTER.update(records)
        _build_indices()
        _last_refresh = now_ist()

    logger.info("Upstox scriptmaster refreshed: %d symbols", len(records))


def _load_from_disk() -> None:
    """Persistent disk cache is disabled; runtime symbol lookups should use symbols_db."""
    logger.debug("Upstox disk cache disabled; no persistent scriptmaster files are loaded")


def search(query: str, exchange: Optional[str] = None) -> List[Dict[str, Any]]:
    """Search Upstox scriptmaster."""
    q = query.upper()
    results = []
    with _LOCK:
        for rec in UPSTOX_SCRIPTMASTER.values():
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
            r for r in UPSTOX_SCRIPTMASTER.values()
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
            r for r in UPSTOX_SCRIPTMASTER.values()
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


def get_record_by_instrument_key(instrument_key: str) -> Optional[Dict[str, Any]]:
    return UPSTOX_SCRIPTMASTER.get(instrument_key)


def get_total_count() -> int:
    return len(UPSTOX_SCRIPTMASTER)


def _parse_expiry_sort(expiry_str: str) -> datetime:
    try:
        return datetime.strptime(expiry_str, "%d-%b-%Y")
    except (ValueError, TypeError):
        return datetime.max
