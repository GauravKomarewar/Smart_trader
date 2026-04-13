"""
Smart Trader — Unified ScriptMaster
=====================================

Merges symbols from ALL broker-specific scriptmasters into a single interface
used throughout the system.

Every component — search, option chain, watchlist, order placement —
uses this module. Symbols are returned in a canonical NormalizedInstrument
format with per-broker trading_symbol mappings.

Architecture:
    scripts/fyers_scriptmaster.py     → Fyers symbols
    scripts/shoonya_scriptmaster.py   → Shoonya symbols
    scripts/angelone_scriptmaster.py  → Angel One symbols
    scripts/dhan_scriptmaster.py      → Dhan symbols
    scripts/kite_scriptmaster.py      → Zerodha Kite symbols
    scripts/upstox_scriptmaster.py    → Upstox symbols
    scripts/groww_scriptmaster.py     → Groww symbols
    scripts/unified_scriptmaster.py   → THIS FILE: combines, dedup, provides single API

Usage:
    from scripts.unified_scriptmaster import (
        refresh_all, search_all, get_expiries, get_options,
        resolve_broker_symbol, get_lot_size, requires_limit_order,
    )
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from core.daily_refresh import IST

logger = logging.getLogger("smart_trader.unified_scriptmaster")

_LOCK = threading.Lock()
_initialized = False

# ── Fallback constants ─────────────────────────────────────────────────────────

_STRIKE_GAPS: Dict[str, int] = {
    "NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50,
    "MIDCPNIFTY": 25, "NIFTYNXT50": 50, "SENSEX": 100, "BANKEX": 100, "SENSEX50": 50,
    "CRUDEOIL": 50, "GOLD": 100, "SILVER": 1000,
}

_INDEX_SYMBOLS: Dict[str, str] = {
    "NIFTY":      "NSE:NIFTY50-INDEX",
    "BANKNIFTY":  "NSE:NIFTYBANK-INDEX",
    "FINNIFTY":   "NSE:FINNIFTY-INDEX",
    "MIDCPNIFTY": "NSE:MIDCPNIFTY-INDEX",
    "NIFTYNXT50": "NSE:NIFTYNXT50-INDEX",
    "SENSEX":     "BSE:SENSEX-INDEX",
    "BANKEX":     "BSE:BANKEX-INDEX",
    "SENSEX50":   "BSE:SENSEX50-INDEX",
    "CRUDEOIL":   "MCX:CRUDEOIL-INDEX",
    "GOLD":       "MCX:GOLD-INDEX",
    "SILVER":     "MCX:SILVER-INDEX",
    "NATURALGAS": "MCX:NATURALGAS-INDEX",
    "COPPER":     "MCX:COPPER-INDEX",
    "USDINR":     "NSE:USDINR-INDEX",
}

# Exchange normalisation for cross-broker dedup
_EXCHANGE_NORM: Dict[str, str] = {
    "NSE": "NSE", "NFO": "NSE", "BSE": "BSE", "BFO": "BSE", "MCX": "MCX",
    "CDS": "CDS", "BCD": "BCD",
}


def _ensure_loaded() -> None:
    """Ensure runtime symbol stores are hydrated from the symbols DB."""
    global _initialized
    if _initialized:
        return
    with _LOCK:
        if _initialized:
            return
        counts = load_all_from_db()
        if sum(counts.values()) > 0:
            _initialized = True
        else:
            logger.warning("Unified ScriptMaster DB hydrate found no symbols; runtime stores remain empty")


def load_all_from_db() -> Dict[str, int]:
    """Hydrate broker runtime stores from the PostgreSQL symbols table."""
    global _initialized
    counts: Dict[str, int] = {
        "fyers": 0,
        "shoonya": 0,
        "angelone": 0,
        "dhan": 0,
        "kite": 0,
        "upstox": 0,
        "groww": 0,
    }

    try:
        import psycopg2.extras
        from db.trading_db import get_trading_conn

        conn = get_trading_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                """
                SELECT
                    exchange, exchange_token, symbol, trading_symbol, instrument_type,
                    lot_size, tick_size, isin, expiry, strike, option_type, description,
                    fyers_symbol, fyers_token, shoonya_token, shoonya_tsym,
                    angelone_token, angelone_tsym, dhan_security_id,
                    kite_instrument_token, kite_exchange_token, upstox_ikey, groww_token,
                    updated_at
                FROM symbols
                """
            )
            rows = [dict(row) for row in cur.fetchall()]
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("Unified ScriptMaster DB hydrate failed: %s", exc)
        _initialized = False
        return counts

    if not rows:
        _initialized = False
        return counts

    latest_refresh_at: Optional[datetime] = None
    fyers_records: Dict[str, Dict[str, Any]] = {}
    fyers_token_index: Dict[str, Dict[str, str]] = {}
    shoonya_records: Dict[str, Dict[str, Dict[str, Any]]] = {}
    angelone_records: Dict[str, Dict[str, Any]] = {}
    dhan_records: Dict[str, Dict[str, Any]] = {}
    kite_records: Dict[str, Dict[str, Any]] = {}
    kite_token_index: Dict[str, Dict[str, Any]] = {}
    upstox_records: Dict[str, Dict[str, Any]] = {}
    groww_records: Dict[str, Dict[str, Any]] = {}
    groww_token_index: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        exchange = str(row.get("exchange") or "").upper()
        trading_symbol = str(row.get("trading_symbol") or "").strip()
        if not exchange or not trading_symbol:
            continue

        latest_refresh_at = _max_refresh_time(latest_refresh_at, row.get("updated_at"))
        exchange_token = str(row.get("exchange_token") or "").strip()
        underlying = str(row.get("symbol") or trading_symbol).strip().upper()
        instrument = str(row.get("instrument_type") or "EQ").strip().upper()
        expiry = _db_expiry_to_broker(row.get("expiry"))
        expiry_epoch = _expiry_to_epoch(expiry)
        strike_val = float(row.get("strike") or 0) or 0.0
        option_type = str(row.get("option_type") or "").strip().upper() or None
        lot_size = int(row.get("lot_size") or 1)
        tick_size = float(row.get("tick_size") or 0.05)
        description = str(row.get("description") or "").strip()
        isin = str(row.get("isin") or "").strip()

        fyers_symbol = str(row.get("fyers_symbol") or "").strip()
        if not fyers_symbol:
            fyers_symbol = _default_fyers_symbol(exchange, trading_symbol, underlying)
        if fyers_symbol:
            segment_key = _fyers_segment_for_exchange(exchange)
            fyers_records[fyers_symbol] = {
                "FyersSymbol": fyers_symbol,
                "FyToken": str(row.get("fyers_token") or "").strip(),
                "Token": exchange_token,
                "Exchange": exchange,
                "SegmentKey": segment_key,
                "Description": description,
                "Instrument": instrument,
                "Underlying": underlying,
                "LotSize": lot_size,
                "TickSize": tick_size,
                "ISIN": isin,
                "Expiry": expiry,
                "ExpiryEpoch": expiry_epoch,
                "StrikePrice": strike_val if strike_val > 0 else None,
                "OptionType": option_type,
            }
            if exchange_token:
                fyers_token_index.setdefault(segment_key, {})[exchange_token] = fyers_symbol

        shoonya_token = str(row.get("shoonya_token") or exchange_token).strip()
        shoonya_tsym = str(row.get("shoonya_tsym") or trading_symbol).strip()
        shoonya_records.setdefault(exchange, {})[shoonya_token or exchange_token or trading_symbol] = {
            "Exchange": exchange,
            "Token": shoonya_token or exchange_token,
            "TradingSymbol": shoonya_tsym,
            "Symbol": underlying,
            "Underlying": underlying,
            "LotSize": lot_size,
            "Expiry": expiry,
            "Instrument": _shoonya_instrument_for_row(instrument, exchange, underlying),
            "OptionType": option_type,
            "StrikePrice": strike_val if strike_val > 0 else None,
            "TickSize": tick_size,
        }

        angelone_token = str(row.get("angelone_token") or "").strip()
        if angelone_token:
            angelone_symbol = str(row.get("angelone_tsym") or trading_symbol).strip()
            angelone_records[angelone_token] = {
                "Token": angelone_token,
                "Symbol": angelone_symbol,
                "Name": description or underlying,
                "Underlying": underlying,
                "Exchange": exchange,
                "ExchSeg": _angel_exchange_segment(exchange, instrument),
                "Instrument": instrument,
                "InstrumentType": _instrument_type_label(instrument, exchange, underlying),
                "Expiry": expiry,
                "StrikePrice": strike_val if strike_val > 0 else None,
                "OptionType": option_type,
                "LotSize": lot_size,
                "TickSize": tick_size,
                "TradingSymbol": angelone_symbol,
            }

        dhan_security_id = str(row.get("dhan_security_id") or "").strip()
        if dhan_security_id:
            dhan_records[dhan_security_id] = {
                "SecurityId": dhan_security_id,
                "Token": dhan_security_id,
                "Exchange": exchange,
                "ExchId": _dhan_exchange_id(exchange),
                "Segment": _dhan_segment(exchange),
                "Symbol": underlying,
                "Underlying": underlying,
                "TradingSymbol": trading_symbol,
                "DisplayName": description or trading_symbol,
                "Instrument": instrument,
                "InstrumentName": _instrument_type_label(instrument, exchange, underlying),
                "InstrumentType": _instrument_type_label(instrument, exchange, underlying),
                "Series": "",
                "Expiry": expiry,
                "StrikePrice": strike_val if strike_val > 0 else None,
                "OptionType": option_type,
                "LotSize": lot_size,
                "TickSize": tick_size,
            }

        kite_instrument_token = str(row.get("kite_instrument_token") or "").strip()
        kite_exchange_token = str(row.get("kite_exchange_token") or exchange_token).strip()
        if kite_instrument_token or kite_exchange_token:
            kite_key = f"{exchange}:{trading_symbol}"
            kite_record = {
                "InstrumentToken": kite_instrument_token,
                "ExchangeToken": kite_exchange_token,
                "Token": kite_instrument_token or kite_exchange_token,
                "TradingSymbol": trading_symbol,
                "Name": description or underlying,
                "Underlying": underlying,
                "Exchange": exchange,
                "Segment": _kite_segment(exchange, instrument),
                "Instrument": instrument,
                "InstrumentType": _kite_instrument_type(instrument, option_type),
                "Expiry": expiry,
                "StrikePrice": strike_val if strike_val > 0 else None,
                "OptionType": option_type,
                "LotSize": lot_size,
                "TickSize": tick_size,
                "KiteSymbol": kite_key,
            }
            kite_records[kite_key] = kite_record
            if kite_instrument_token:
                kite_token_index[kite_instrument_token] = kite_record

        upstox_ikey = str(row.get("upstox_ikey") or "").strip()
        if upstox_ikey:
            upstox_records[upstox_ikey] = {
                "InstrumentKey": upstox_ikey,
                "Token": exchange_token,
                "TradingSymbol": trading_symbol,
                "ShortName": underlying,
                "Name": description or underlying,
                "Underlying": underlying,
                "Exchange": exchange,
                "Segment": _upstox_segment(exchange, instrument),
                "Instrument": instrument,
                "InstrumentType": _upstox_instrument_type(instrument, option_type),
                "ISIN": isin,
                "Expiry": expiry,
                "StrikePrice": strike_val if strike_val > 0 else None,
                "OptionType": option_type,
                "LotSize": lot_size,
                "TickSize": tick_size,
                "FreezeQuantity": 0,
            }

        groww_token = str(row.get("groww_token") or exchange_token).strip()
        if groww_token:
            raw_exchange = _groww_raw_exchange(exchange)
            groww_key = f"{raw_exchange}:{trading_symbol}"
            groww_record = {
                "ExchangeToken": groww_token,
                "Token": groww_token,
                "TradingSymbol": trading_symbol,
                "GrowwSymbol": trading_symbol,
                "Name": description or underlying,
                "Underlying": underlying,
                "UnderlyingExchangeToken": "",
                "Exchange": exchange,
                "RawExchange": raw_exchange,
                "Segment": _groww_segment(exchange),
                "Series": "",
                "ISIN": isin,
                "Instrument": instrument,
                "InstrumentType": _upstox_instrument_type(instrument, option_type),
                "Expiry": expiry,
                "StrikePrice": strike_val if strike_val > 0 else None,
                "OptionType": option_type,
                "LotSize": lot_size,
                "TickSize": tick_size,
                "FreezeQuantity": 0,
                "BuyAllowed": True,
                "SellAllowed": True,
            }
            groww_records[groww_key] = groww_record
            groww_token_index[groww_token] = groww_record

    from scripts import angelone_scriptmaster as angelone_mod
    from scripts import dhan_scriptmaster as dhan_mod
    from scripts import fyers_scriptmaster as fyers_mod
    from scripts import groww_scriptmaster as groww_mod
    from scripts import kite_scriptmaster as kite_mod
    from scripts import shoonya_scriptmaster as shoonya_mod
    from scripts import upstox_scriptmaster as upstox_mod

    with fyers_mod._LOCK:
        fyers_mod.FYERS_SCRIPTMASTER.clear()
        fyers_mod.FYERS_SCRIPTMASTER.update(fyers_records)
        fyers_mod.TOKEN_INDEX.clear()
        fyers_mod.TOKEN_INDEX.update(fyers_token_index)
        fyers_mod._build_expiry_calendar()
        fyers_mod._last_refresh = latest_refresh_at
    counts["fyers"] = len(fyers_mod.FYERS_SCRIPTMASTER)

    with shoonya_mod._LOCK:
        shoonya_mod.SHOONYA_SCRIPTMASTER.clear()
        shoonya_mod.SHOONYA_SCRIPTMASTER.update(shoonya_records)
        shoonya_mod._build_universal()
        shoonya_mod._build_expiry_calendar()
        shoonya_mod._last_refresh = latest_refresh_at
    counts["shoonya"] = len(shoonya_mod.SHOONYA_UNIVERSAL)

    with angelone_mod._LOCK:
        angelone_mod.ANGELONE_SCRIPTMASTER.clear()
        angelone_mod.ANGELONE_SCRIPTMASTER.update(angelone_records)
        angelone_mod._build_indices()
        angelone_mod._last_refresh = latest_refresh_at
    counts["angelone"] = len(angelone_mod.ANGELONE_SCRIPTMASTER)

    with dhan_mod._LOCK:
        dhan_mod.DHAN_SCRIPTMASTER.clear()
        dhan_mod.DHAN_SCRIPTMASTER.update(dhan_records)
        dhan_mod._build_indices()
        dhan_mod._last_refresh = latest_refresh_at
    counts["dhan"] = len(dhan_mod.DHAN_SCRIPTMASTER)

    with kite_mod._LOCK:
        kite_mod.KITE_SCRIPTMASTER.clear()
        kite_mod.KITE_SCRIPTMASTER.update(kite_records)
        kite_mod.TOKEN_INDEX.clear()
        kite_mod.TOKEN_INDEX.update(kite_token_index)
        kite_mod._build_expiry_calendar()
        kite_mod._last_refresh = latest_refresh_at
    counts["kite"] = len(kite_mod.KITE_SCRIPTMASTER)

    with upstox_mod._LOCK:
        upstox_mod.UPSTOX_SCRIPTMASTER.clear()
        upstox_mod.UPSTOX_SCRIPTMASTER.update(upstox_records)
        upstox_mod._build_indices()
        upstox_mod._last_refresh = latest_refresh_at
    counts["upstox"] = len(upstox_mod.UPSTOX_SCRIPTMASTER)

    with groww_mod._LOCK:
        groww_mod.GROWW_SCRIPTMASTER.clear()
        groww_mod.GROWW_SCRIPTMASTER.update(groww_records)
        groww_mod.TOKEN_INDEX.clear()
        groww_mod.TOKEN_INDEX.update(groww_token_index)
        groww_mod._build_expiry_calendar()
        groww_mod._last_refresh = latest_refresh_at
    counts["groww"] = len(groww_mod.GROWW_SCRIPTMASTER)

    _initialized = sum(counts.values()) > 0
    if _initialized:
        logger.info("Unified ScriptMaster hydrated from symbols DB: %s", counts)
    return counts


def load_all_from_disk() -> Dict[str, int]:
    """Backward-compatible alias now backed by the symbols DB."""
    return load_all_from_db()


def _max_refresh_time(current: Optional[datetime], candidate: Optional[datetime]) -> Optional[datetime]:
    if candidate is None:
        return current
    if candidate.tzinfo is None:
        candidate = candidate.replace(tzinfo=timezone.utc)
    candidate = candidate.astimezone(IST)
    if current is None or candidate > current:
        return candidate
    return current


def _db_expiry_to_broker(value: Any) -> str:
    if not value:
        return ""
    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value)
        for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d-%B-%Y"):
            try:
                dt = datetime.strptime(raw, fmt)
                break
            except ValueError:
                continue
        else:
            return raw
    return dt.strftime("%-d-%b-%Y")


def _expiry_to_epoch(expiry: str) -> int:
    date_value = _parse_expiry_date(expiry)
    if not date_value:
        return 0
    return int(datetime.combine(date_value, datetime.min.time(), tzinfo=IST).timestamp())


def _fyers_segment_for_exchange(exchange: str) -> str:
    return {
        "NFO": "NSE_FO",
        "BFO": "BSE_FO",
        "NSE": "NSE_CM",
        "BSE": "BSE_CM",
        "MCX": "MCX_COM",
        "CDS": "NSE_CD",
        "BCD": "BSE_CD",
    }.get(exchange.upper(), exchange.upper())


def _default_fyers_symbol(exchange: str, trading_symbol: str, underlying: str) -> str:
    exchange = exchange.upper()
    if trading_symbol.endswith("-INDEX"):
        return f"{exchange}:{trading_symbol}"
    return _INDEX_SYMBOLS.get(underlying.upper(), f"{exchange}:{trading_symbol}")


def _is_index_underlying(symbol: str) -> bool:
    return symbol.upper() in {
        "NIFTY",
        "BANKNIFTY",
        "NIFTYBANK",
        "FINNIFTY",
        "MIDCPNIFTY",
        "SENSEX",
        "BANKEX",
    }


def _shoonya_instrument_for_row(instrument: str, exchange: str, symbol: str) -> str:
    instrument = instrument.upper()
    exchange = exchange.upper()
    if instrument == "FUT":
        if exchange in ("CDS", "BCD"):
            return "FUTCUR"
        if exchange == "MCX":
            return "FUTCOM"
        return "FUTIDX" if _is_index_underlying(symbol) else "FUTSTK"
    if instrument == "OPT":
        if exchange in ("CDS", "BCD"):
            return "OPTCUR"
        if exchange == "MCX":
            return "OPTFUT"
        return "OPTIDX" if _is_index_underlying(symbol) else "OPTSTK"
    return "EQ"


def _instrument_type_label(instrument: str, exchange: str, symbol: str) -> str:
    instrument = instrument.upper()
    if instrument in ("FUT", "OPT"):
        return _shoonya_instrument_for_row(instrument, exchange, symbol)
    if instrument == "IDX":
        return "INDEX"
    return "EQ"


def _angel_exchange_segment(exchange: str, instrument: str) -> str:
    instrument = instrument.upper()
    exchange = exchange.upper()
    if exchange == "NSE":
        return "nse_index" if instrument == "IDX" else "nse_cm"
    if exchange == "BSE":
        return "bse_index" if instrument == "IDX" else "bse_cm"
    return {
        "NFO": "nfo",
        "BFO": "bfo",
        "MCX": "mcx_fo",
        "CDS": "cds_fo",
        "BCD": "bcd_fo",
    }.get(exchange, exchange.lower())


def _dhan_exchange_id(exchange: str) -> str:
    return {
        "NFO": "NSE",
        "CDS": "NSE",
        "NSE": "NSE",
        "BFO": "BSE",
        "BCD": "BSE",
        "BSE": "BSE",
        "MCX": "MCX",
    }.get(exchange.upper(), exchange.upper())


def _dhan_segment(exchange: str) -> str:
    return {
        "NSE": "E",
        "BSE": "E",
        "NFO": "D",
        "BFO": "D",
        "CDS": "C",
        "BCD": "C",
        "MCX": "M",
    }.get(exchange.upper(), "E")


def _kite_segment(exchange: str, instrument: str) -> str:
    exchange = exchange.upper()
    instrument = instrument.upper()
    if exchange in ("NSE", "BSE"):
        return exchange
    if exchange == "MCX":
        return "MCX" if instrument == "EQ" else f"{exchange}-{'OPT' if instrument == 'OPT' else 'FUT'}"
    return f"{exchange}-{'OPT' if instrument == 'OPT' else 'FUT'}"


def _kite_instrument_type(instrument: str, option_type: Optional[str]) -> str:
    instrument = instrument.upper()
    if instrument == "OPT":
        return option_type or "CE"
    if instrument == "FUT":
        return "FUT"
    return "EQ"


def _upstox_segment(exchange: str, instrument: str) -> str:
    exchange = exchange.upper()
    instrument = instrument.upper()
    if exchange == "NSE":
        return "NSE_INDEX" if instrument == "IDX" else "NSE_EQ"
    if exchange == "BSE":
        return "BSE_INDEX" if instrument == "IDX" else "BSE_EQ"
    return {
        "NFO": "NSE_FO",
        "BFO": "BSE_FO",
        "CDS": "NCD_FO",
        "BCD": "BCD_FO",
        "MCX": "MCX_FO",
    }.get(exchange, exchange)


def _upstox_instrument_type(instrument: str, option_type: Optional[str]) -> str:
    instrument = instrument.upper()
    if instrument == "OPT":
        return option_type or "OPT"
    if instrument == "IDX":
        return "INDEX"
    return instrument


def _groww_raw_exchange(exchange: str) -> str:
    return {
        "NFO": "NSE",
        "CDS": "NSE",
        "NSE": "NSE",
        "BFO": "BSE",
        "BCD": "BSE",
        "BSE": "BSE",
        "MCX": "MCX",
    }.get(exchange.upper(), exchange.upper())


def _groww_segment(exchange: str) -> str:
    return {
        "NSE": "CASH",
        "BSE": "CASH",
        "NFO": "FNO",
        "BFO": "FNO",
        "MCX": "COMMODITY",
        "CDS": "FNO",
        "BCD": "FNO",
    }.get(exchange.upper(), "CASH")


def refresh_all(force: bool = False) -> Dict[str, int]:
    """
    Download/reload all broker scriptmasters.
    Returns dict of counts: {"fyers": N, "shoonya": M, "angelone": ..., ...}
    """
    global _initialized
    counts = {}
    clean_stats = {}

    from scripts.symbol_data_cleaning import (
        sanitize_angelone_scriptmaster,
        sanitize_dhan_scriptmaster,
        sanitize_fyers_scriptmaster,
        sanitize_kite_scriptmaster,
        sanitize_shoonya_scriptmaster,
    )

    try:
        from scripts.fyers_scriptmaster import refresh as fyers_refresh, FYERS_SCRIPTMASTER
        fyers_refresh(force=force)
        kept, dropped = sanitize_fyers_scriptmaster(FYERS_SCRIPTMASTER)
        clean_stats["fyers"] = {"kept": kept, "dropped": dropped}
        counts["fyers"] = len(FYERS_SCRIPTMASTER)
        logger.info("Fyers scriptmaster: %d symbols", counts["fyers"])
    except Exception as exc:
        logger.error("Fyers scriptmaster refresh failed: %s", exc)
        counts["fyers"] = 0

    try:
        from scripts.shoonya_scriptmaster import refresh as shoonya_refresh, SHOONYA_UNIVERSAL
        from scripts.shoonya_scriptmaster import SHOONYA_SCRIPTMASTER
        shoonya_refresh(force=force)
        kept, dropped = sanitize_shoonya_scriptmaster(SHOONYA_SCRIPTMASTER)
        clean_stats["shoonya"] = {"kept": kept, "dropped": dropped}
        # Rebuild universal map after cleaning
        SHOONYA_UNIVERSAL.clear()
        for exch, data in SHOONYA_SCRIPTMASTER.items():
            for tok, rec in data.items():
                SHOONYA_UNIVERSAL[f"{exch}|{tok}"] = rec
        counts["shoonya"] = len(SHOONYA_UNIVERSAL)
        logger.info("Shoonya scriptmaster: %d symbols", counts["shoonya"])
    except Exception as exc:
        logger.error("Shoonya scriptmaster refresh failed: %s", exc)
        counts["shoonya"] = 0

    try:
        from scripts.angelone_scriptmaster import refresh as angelone_refresh, ANGELONE_SCRIPTMASTER
        angelone_refresh(force=force)
        kept, dropped = sanitize_angelone_scriptmaster(ANGELONE_SCRIPTMASTER)
        clean_stats["angelone"] = {"kept": kept, "dropped": dropped}
        counts["angelone"] = len(ANGELONE_SCRIPTMASTER)
        logger.info("Angel One scriptmaster: %d symbols", counts["angelone"])
    except Exception as exc:
        logger.error("Angel One scriptmaster refresh failed: %s", exc)
        counts["angelone"] = 0

    try:
        from scripts.dhan_scriptmaster import refresh as dhan_refresh, DHAN_SCRIPTMASTER
        dhan_refresh(force=force)
        kept, dropped = sanitize_dhan_scriptmaster(DHAN_SCRIPTMASTER)
        clean_stats["dhan"] = {"kept": kept, "dropped": dropped}
        counts["dhan"] = len(DHAN_SCRIPTMASTER)
        logger.info("Dhan scriptmaster: %d symbols", counts["dhan"])
    except Exception as exc:
        logger.error("Dhan scriptmaster refresh failed: %s", exc)
        counts["dhan"] = 0

    try:
        from scripts.kite_scriptmaster import refresh as kite_refresh, KITE_SCRIPTMASTER, TOKEN_INDEX as KITE_TOKEN_INDEX
        kite_refresh(force=force)
        kept, dropped = sanitize_kite_scriptmaster(KITE_SCRIPTMASTER)
        clean_stats["kite"] = {"kept": kept, "dropped": dropped}
        KITE_TOKEN_INDEX.clear()
        for rec in KITE_SCRIPTMASTER.values():
            instrument_token = str(rec.get("InstrumentToken") or "").strip()
            if instrument_token:
                KITE_TOKEN_INDEX[instrument_token] = rec
        counts["kite"] = len(KITE_SCRIPTMASTER)
        logger.info("Kite scriptmaster: %d symbols", counts["kite"])
    except Exception as exc:
        logger.error("Kite scriptmaster refresh failed: %s", exc)
        counts["kite"] = 0

    try:
        from scripts.upstox_scriptmaster import refresh as upstox_refresh, UPSTOX_SCRIPTMASTER
        upstox_refresh(force=force)
        counts["upstox"] = len(UPSTOX_SCRIPTMASTER)
        logger.info("Upstox scriptmaster: %d symbols", counts["upstox"])
    except Exception as exc:
        logger.error("Upstox scriptmaster refresh failed: %s", exc)
        counts["upstox"] = 0

    try:
        from scripts.groww_scriptmaster import refresh as groww_refresh, GROWW_SCRIPTMASTER
        groww_refresh(force=force)
        counts["groww"] = len(GROWW_SCRIPTMASTER)
        logger.info("Groww scriptmaster: %d symbols", counts["groww"])
    except Exception as exc:
        logger.error("Groww scriptmaster refresh failed: %s", exc)
        counts["groww"] = 0

    _initialized = True
    if clean_stats:
        logger.info("Scriptmaster cleaning stats: %s", clean_stats)
    return counts


# ── Unified Search ────────────────────────────────────────────────────────────

def search_all(
    query: str,
    exchange: str = "",
    instrument_type: str = "",
    limit: int = 30,
) -> List[Dict[str, Any]]:
    """
    Search instruments across all broker scriptmasters.

    Returns unified records with both Fyers and Shoonya trading symbols
    where available. Results sorted by relevance.

    Each result has:
        symbol, exchange, instrument_type, trading_symbol (canonical, no prefix),
        fyers_symbol ("EXCH:SYM"), shoonya_symbol (plain "SYM"),
        angelone_symbol, dhan_symbol, kite_symbol, upstox_symbol, groww_symbol,
        lot_size, tick_size, expiry, strike, option_type, token
    """
    q = query.upper().strip()
    if not q:
        return []

    try:
        from db.symbols_db import search_symbols

        rows = search_symbols(query=query, exchange=exchange, instrument_type=instrument_type, limit=limit)
        results = [_db_row_to_unified_result(row) for row in rows]
        results.sort(key=lambda r: (
            _relevance_score(r, q),
            {"IDX": 0, "FUT": 1, "EQ": 2, "OPT": 3}.get(r.get("instrument_type", ""), 4),
        ))
        return results[:limit]
    except Exception as exc:
        logger.debug("search_all(%s, %s, %s) DB fallback failed: %s", query, exchange, instrument_type, exc)
        return []


def _db_row_to_unified_result(row: Dict[str, Any]) -> Dict[str, Any]:
    exchange = str(row.get("exchange") or "").upper()
    trading_symbol = str(row.get("trading_symbol") or "").strip()
    return {
        "symbol": row.get("symbol", ""),
        "trading_symbol": trading_symbol,
        "fyers_symbol": row.get("fyers_symbol") or _default_fyers_symbol(exchange, trading_symbol, row.get("symbol", "")),
        "shoonya_symbol": row.get("shoonya_tsym") or trading_symbol,
        "angelone_symbol": row.get("angelone_tsym") or trading_symbol,
        "dhan_symbol": row.get("dhan_security_id") or trading_symbol,
        "kite_symbol": trading_symbol,
        "upstox_symbol": row.get("upstox_ikey") or trading_symbol,
        "groww_symbol": trading_symbol,
        "exchange": exchange,
        "instrument_type": row.get("instrument_type", ""),
        "lot_size": int(row.get("lot_size") or 1),
        "tick_size": float(row.get("tick_size") or 0.05),
        "expiry": _to_iso(str(row.get("expiry") or "")),
        "strike": float(row.get("strike") or 0),
        "option_type": row.get("option_type") or "",
        "token": str(row.get("exchange_token", "")),
        "description": row.get("description") or "",
    }


def _db_row_to_standard(row: Dict[str, Any]) -> Dict[str, Any]:
    exchange = str(row.get("exchange") or "").upper()
    trading_symbol = str(row.get("trading_symbol") or "").strip()
    return {
        "TradingSymbol": trading_symbol,
        "FyersSymbol": row.get("fyers_symbol") or _default_fyers_symbol(exchange, trading_symbol, row.get("symbol", "")),
        "Symbol": row.get("symbol", ""),
        "Underlying": row.get("symbol", ""),
        "Exchange": exchange,
        "Token": str(row.get("exchange_token", "")),
        "FyToken": str(row.get("fyers_token", "")),
        "LotSize": int(row.get("lot_size") or 1),
        "TickSize": float(row.get("tick_size") or 0.05),
        "Expiry": _iso_to_broker(str(row.get("expiry") or "")) or "",
        "Instrument": row.get("instrument_type", ""),
        "OptionType": row.get("option_type") or "",
        "StrikePrice": float(row.get("strike") or 0) or None,
        "Description": row.get("description") or "",
        "tsym": trading_symbol,
        "exch": exchange,
        "ls": str(int(row.get("lot_size") or 1)),
    }


def lookup_by_trading_symbol(trading_symbol: str, exchange: str = "") -> Optional[Any]:
    """DB-backed lookup used by quote and websocket symbol resolution."""
    try:
        from types import SimpleNamespace
        from db.symbols_db import lookup_by_trading_symbol as db_lookup

        row = db_lookup(trading_symbol, exchange)
        if not row:
            return None

        expiry_value = row.get("expiry")
        expiry_iso = expiry_value.strftime("%Y-%m-%d") if hasattr(expiry_value, "strftime") else str(expiry_value or "")
        return SimpleNamespace(
            symbol=row.get("symbol", ""),
            trading_symbol=row.get("trading_symbol", ""),
            exchange=row.get("exchange", ""),
            instrument_type=row.get("instrument_type", ""),
            expiry=expiry_iso,
            strike=float(row.get("strike") or 0),
            option_type=row.get("option_type") or "",
            lot_size=int(row.get("lot_size") or 1),
            tick_size=float(row.get("tick_size") or 0.05),
            token=str(row.get("exchange_token", "")),
            fyers_symbol=row.get("fyers_symbol") or _default_fyers_symbol(row.get("exchange", ""), row.get("trading_symbol", ""), row.get("symbol", "")),
            description=row.get("description") or "",
        )
    except Exception as exc:
        logger.debug("lookup_by_trading_symbol(%s) failed: %s", trading_symbol, exc)
        return None


def _search_fyers(query: str, exchange: str, instrument_type: str, limit: int) -> List[Dict[str, Any]]:
    """Search Fyers scriptmaster and return unified format."""
    try:
        from scripts.fyers_scriptmaster import FYERS_SCRIPTMASTER
    except ImportError:
        return []

    results = []
    for key, rec in FYERS_SCRIPTMASTER.items():
        if exchange:
            exch_upper = exchange.upper()
            rec_exch = rec.get("Exchange", "").upper()
            if rec_exch != exch_upper:
                # Allow cross-matching between equivalent exchange names
                equiv_groups = [
                    {"NSE", "NFO"},
                    {"BSE", "BFO"},
                    {"CDS", "BCD"},
                ]
                matched = any(
                    exch_upper in grp and rec_exch in grp
                    for grp in equiv_groups
                )
                if not matched:
                    continue

        instr = rec.get("Instrument", "")
        if instrument_type and instr.upper() != instrument_type.upper():
            continue

        underlying = (rec.get("Underlying") or "").upper()
        fyers_sym = (rec.get("FyersSymbol") or "").upper()
        desc = (rec.get("Description") or "").upper()

        if query not in underlying and query not in fyers_sym and query not in desc:
            continue

        fyers_sym_full = rec.get("FyersSymbol", "")
        tsym = fyers_sym_full.split(":", 1)[-1] if ":" in fyers_sym_full else fyers_sym_full
        raw_expiry = rec.get("Expiry", "")

        results.append({
            "symbol":          rec.get("Underlying", "") or rec.get("Symbol", ""),
            "trading_symbol":  tsym,
            "fyers_symbol":    fyers_sym_full,
            "shoonya_symbol":  "",  # filled in merge
            "exchange":        rec.get("Exchange", ""),
            "instrument_type": instr,
            "lot_size":        int(rec.get("LotSize", 1) or 1),
            "tick_size":       float(rec.get("TickSize", 0.05) or 0.05),
            "expiry":          _to_iso(raw_expiry),
            "strike":          float(rec.get("StrikePrice") or 0),
            "option_type":     rec.get("OptionType") or "",
            "token":           str(rec.get("Token", "")),
            "description":     rec.get("Description", ""),
            "_source":         "fyers",
        })
        if len(results) >= limit:
            break
    return results


def _search_shoonya(query: str, exchange: str, instrument_type: str, limit: int) -> List[Dict[str, Any]]:
    """Search Shoonya scriptmaster and return unified format."""
    try:
        from scripts.shoonya_scriptmaster import SHOONYA_SCRIPTMASTER
    except ImportError:
        return []

    # Map Shoonya instrument names to canonical
    _instr_map = {
        "FUTIDX": "FUT", "FUTSTK": "FUT", "FUTCOM": "FUT",
        "FUTCUR": "FUT", "FUTIRC": "FUT",
        "OPTIDX": "OPT", "OPTSTK": "OPT", "OPTFUT": "OPT",
        "OPTCOM": "OPT", "OPTCUR": "OPT",
        "EQ": "EQ", "": "EQ",
    }

    results = []
    exchanges = [exchange.upper()] if exchange else list(SHOONYA_SCRIPTMASTER.keys())
    for exch in exchanges:
        for rec in SHOONYA_SCRIPTMASTER.get(exch, {}).values():
            raw_instr = rec.get("Instrument", "")
            canonical_instr = _instr_map.get(raw_instr, raw_instr)
            if instrument_type and canonical_instr != instrument_type.upper():
                continue

            sym = (rec.get("Symbol") or "").upper()
            underlying = (rec.get("Underlying") or "").upper()
            tsym = (rec.get("TradingSymbol") or "").upper()

            if query not in sym and query not in underlying and query not in tsym:
                continue

            raw_expiry = rec.get("Expiry", "")

            results.append({
                "symbol":          rec.get("Underlying", "") or rec.get("Symbol", ""),
                "trading_symbol":  rec.get("TradingSymbol", ""),
                "fyers_symbol":    "",  # filled in merge
                "shoonya_symbol":  rec.get("TradingSymbol", ""),
                "exchange":        exch,
                "instrument_type": canonical_instr,
                "lot_size":        int(rec.get("LotSize") or 1),
                "tick_size":       float(rec.get("TickSize") or 0.05),
                "expiry":          _to_iso(raw_expiry),
                "strike":          float(rec.get("StrikePrice") or 0),
                "option_type":     rec.get("OptionType") or "",
                "token":           str(rec.get("Token", "")),
                "description":     "",
                "shoonya_instrument": raw_instr,
                "_source":         "shoonya",
            })
            if len(results) >= limit:
                return results
    return results


def _search_other_brokers(query: str, exchange: str, instrument_type: str, limit: int) -> List[Dict[str, Any]]:
    """Search Angel One, Dhan, Kite, Upstox, Groww scriptmasters and return unified format."""
    results = []
    _instr_map = {
        "FUTIDX": "FUT", "FUTSTK": "FUT", "FUTCOM": "FUT",
        "FUTCUR": "FUT", "FUTIRC": "FUT",
        "OPTIDX": "OPT", "OPTSTK": "OPT", "OPTFUT": "OPT",
        "OPTCOM": "OPT", "OPTCUR": "OPT",
        "EQ": "EQ", "": "EQ", "IDX": "IDX",
    }

    # Helper to convert a generic broker record to unified format
    def _to_unified(rec: Dict, source: str) -> Dict[str, Any]:
        raw_instr = rec.get("Instrument", "") or rec.get("InstrumentType", "")
        canonical_instr = _instr_map.get(raw_instr, raw_instr) if raw_instr else "EQ"
        # Also check the canonical Instrument field directly
        if rec.get("Instrument") in ("FUT", "OPT", "EQ", "IDX"):
            canonical_instr = rec["Instrument"]

        raw_expiry = rec.get("Expiry", "")
        return {
            "symbol":          rec.get("Underlying", "") or rec.get("Symbol", ""),
            "trading_symbol":  rec.get("TradingSymbol", "") or rec.get("Symbol", ""),
            "fyers_symbol":    "",
            "shoonya_symbol":  "",
            "exchange":        rec.get("Exchange", ""),
            "instrument_type": canonical_instr,
            "lot_size":        int(rec.get("LotSize", 1) or 1),
            "tick_size":       float(rec.get("TickSize", 0.05) or 0.05),
            "expiry":          _to_iso(raw_expiry),
            "strike":          float(rec.get("StrikePrice") or 0),
            "option_type":     rec.get("OptionType") or "",
            "token":           str(rec.get("Token", "")),
            "description":     rec.get("Name", "") or rec.get("Description", ""),
            "_source":         source,
        }

    # Angel One
    try:
        from scripts.angelone_scriptmaster import ANGELONE_SCRIPTMASTER
        for rec in ANGELONE_SCRIPTMASTER.values():
            if exchange and rec.get("Exchange", "").upper() != exchange.upper():
                continue
            instr = rec.get("Instrument", "")
            if instrument_type and instr != instrument_type.upper():
                continue
            underlying = (rec.get("Underlying") or "").upper()
            sym = (rec.get("Symbol") or "").upper()
            if query not in underlying and query not in sym:
                continue
            results.append(_to_unified(rec, "angelone"))
            if len(results) >= limit:
                return results
    except Exception:
        pass

    # Dhan
    try:
        from scripts.dhan_scriptmaster import DHAN_SCRIPTMASTER
        for rec in DHAN_SCRIPTMASTER.values():
            if exchange and rec.get("Exchange", "").upper() != exchange.upper():
                continue
            instr = rec.get("Instrument", "")
            if instrument_type and instr != instrument_type.upper():
                continue
            underlying = (rec.get("Underlying") or "").upper()
            sym = (rec.get("Symbol") or "").upper()
            display = (rec.get("DisplayName") or "").upper()
            if query not in underlying and query not in sym and query not in display:
                continue
            results.append(_to_unified(rec, "dhan"))
            if len(results) >= limit:
                return results
    except Exception:
        pass

    # Kite
    try:
        from scripts.kite_scriptmaster import KITE_SCRIPTMASTER
        for rec in KITE_SCRIPTMASTER.values():
            if exchange and rec.get("Exchange", "").upper() != exchange.upper():
                continue
            instr = rec.get("Instrument", "")
            if instrument_type and instr != instrument_type.upper():
                continue
            underlying = (rec.get("Underlying") or "").upper()
            tsym = (rec.get("TradingSymbol") or "").upper()
            if query not in underlying and query not in tsym:
                continue
            results.append(_to_unified(rec, "kite"))
            if len(results) >= limit:
                return results
    except Exception:
        pass

    # Upstox
    try:
        from scripts.upstox_scriptmaster import UPSTOX_SCRIPTMASTER
        for rec in UPSTOX_SCRIPTMASTER.values():
            if exchange and rec.get("Exchange", "").upper() != exchange.upper():
                continue
            instr = rec.get("Instrument", "")
            if instrument_type and instr != instrument_type.upper():
                continue
            underlying = (rec.get("Underlying") or "").upper()
            tsym = (rec.get("TradingSymbol") or "").upper()
            if query not in underlying and query not in tsym:
                continue
            results.append(_to_unified(rec, "upstox"))
            if len(results) >= limit:
                return results
    except Exception:
        pass

    # Groww
    try:
        from scripts.groww_scriptmaster import GROWW_SCRIPTMASTER
        for rec in GROWW_SCRIPTMASTER.values():
            if exchange and rec.get("Exchange", "").upper() != exchange.upper():
                continue
            instr = rec.get("Instrument", "")
            if instrument_type and instr != instrument_type.upper():
                continue
            underlying = (rec.get("Underlying") or "").upper()
            tsym = (rec.get("TradingSymbol") or "").upper()
            if query not in underlying and query not in tsym:
                continue
            results.append(_to_unified(rec, "groww"))
            if len(results) >= limit:
                return results
    except Exception:
        pass

    return results


def _merge_results(fyers: List[Dict], shoonya: List[Dict], others: Optional[List[Dict]] = None) -> List[Dict]:
    """
    Merge Fyers, Shoonya, and other broker results, cross-linking broker symbols.

    Matching is by (exchange, underlying, instrument_type, expiry, strike, option_type).
    """
    # Index Shoonya records by matching key for cross-reference
    shoonya_idx: Dict[str, Dict] = {}
    for rec in shoonya:
        key = _match_key(rec)
        if key not in shoonya_idx:
            shoonya_idx[key] = rec

    # Index other broker records
    other_idx: Dict[str, List[Dict]] = {}
    for rec in (others or []):
        key = _match_key(rec)
        other_idx.setdefault(key, []).append(rec)

    merged = []
    seen_keys = set()

    # Process Fyers records first (they have richer data)
    for rec in fyers:
        key = _match_key(rec)
        seen_keys.add(key)
        # Cross-link Shoonya symbol
        shoonya_rec = shoonya_idx.get(key)
        if shoonya_rec:
            rec["shoonya_symbol"] = shoonya_rec.get("shoonya_symbol", "")
            if shoonya_rec.get("shoonya_instrument"):
                rec["shoonya_instrument"] = shoonya_rec["shoonya_instrument"]
        # Cross-link other broker symbols
        _cross_link_other(rec, other_idx.get(key, []))
        rec.pop("_source", None)
        merged.append(rec)

    # Add Shoonya-only records (MCX, etc.)
    for rec in shoonya:
        key = _match_key(rec)
        if key not in seen_keys:
            seen_keys.add(key)
            _cross_link_other(rec, other_idx.get(key, []))
            rec.pop("_source", None)
            merged.append(rec)

    # Add records from other brokers that weren't in Fyers or Shoonya
    for rec in (others or []):
        key = _match_key(rec)
        if key not in seen_keys:
            seen_keys.add(key)
            rec.pop("_source", None)
            merged.append(rec)

    return merged


def _cross_link_other(rec: Dict, other_recs: List[Dict]) -> None:
    """Cross-link other broker symbols into a merged record."""
    for orec in other_recs:
        source = orec.get("_source", "")
        if source == "angelone":
            rec["angelone_symbol"] = orec.get("trading_symbol", "")
            rec["angelone_token"] = orec.get("token", "")
        elif source == "dhan":
            rec["dhan_symbol"] = orec.get("trading_symbol", "")
            rec["dhan_security_id"] = orec.get("token", "")
        elif source == "kite":
            rec["kite_symbol"] = orec.get("trading_symbol", "")
            rec["kite_token"] = orec.get("token", "")
        elif source == "upstox":
            rec["upstox_symbol"] = orec.get("trading_symbol", "")
            rec["upstox_instrument_key"] = orec.get("token", "")
        elif source == "groww":
            rec["groww_symbol"] = orec.get("trading_symbol", "")
            rec["groww_token"] = orec.get("token", "")


def _match_key(rec: Dict) -> str:
    """Generate matching key for cross-broker dedup (exchange-normalised)."""
    raw_exch = rec.get("exchange", "").upper()
    norm_exch = _EXCHANGE_NORM.get(raw_exch, raw_exch)
    return (
        f"{norm_exch}|{rec.get('symbol', '').upper()}|"
        f"{rec.get('instrument_type', '')}|{rec.get('expiry', '')}|"
        f"{rec.get('strike', 0)}|{rec.get('option_type', '').upper()}"
    )


def _relevance_score(rec: Dict, query: str) -> int:
    """Lower = more relevant."""
    sym = (rec.get("symbol") or "").upper()
    if sym == query:
        return 0
    if sym.startswith(query):
        return 1
    if query in sym:
        return 2
    return 3


# ── Expiry and Lot Size ──────────────────────────────────────────────────────

def get_expiries(
    symbol: str,
    exchange: str = "NFO",
    kind: str = "OPTION",
) -> List[str]:
    """
    Get available expiry dates (ISO format) from all broker data.
    Returns sorted list of ISO date strings.
    """
    try:
        from db.symbols_db import get_expiry_list as db_get_expiry_list

        instrument_type = "FUT" if kind.upper().startswith("FUT") else "OPT"
        return db_get_expiry_list(symbol, exchange, instrument_type)
    except Exception as exc:
        logger.debug("get_expiries(%s, %s, %s) failed: %s", symbol, exchange, kind, exc)
        return []


def get_options(
    underlying: str,
    exchange: str = "NFO",
    expiry: Optional[str] = None,
    strike: Optional[float] = None,
    option_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Get options from all scriptmasters, returning unified records.
    Expiry can be ISO or DD-Mon-YYYY format.
    """
    try:
        from db.symbols_db import get_options as db_get_options

        rows = db_get_options(
            symbol=underlying,
            exchange=exchange,
            expiry=expiry,
            strike=strike,
            option_type=option_type,
        )
        return [_db_row_to_unified_result(row) for row in rows]
    except Exception as exc:
        logger.debug("get_options(%s, %s) failed: %s", underlying, exchange, exc)
        return []


def get_lot_size(symbol: str, exchange: str = "NFO") -> int:
    """Get lot size from the symbols DB."""
    try:
        from db.symbols_db import get_lot_size as db_get_lot_size
        return db_get_lot_size(symbol, exchange)
    except Exception as exc:
        logger.debug("get_lot_size(%s, %s) failed: %s", symbol, exchange, exc)
        return 1


def requires_limit_order(exchange: str = "", trading_symbol: str = "",
                          tradingsymbol: str = "", token: str = "",
                          broker: str = "", **_kw) -> bool:
    """
    Check if LIMIT order is required (stock options / MCX options).
    Accepts both trading_symbol and tradingsymbol for backwards compatibility.
    """
    ts = trading_symbol or tradingsymbol
    try:
        from db.symbols_db import lookup_by_trading_symbol as db_lookup

        rec = db_lookup(ts, exchange)
        if rec and rec.get("instrument_type") == "OPT":
            exch = str(rec.get("exchange") or exchange).upper()
            sym = str(rec.get("symbol") or "").upper()
            if exch == "MCX":
                return True
            if exch in ("NFO", "BFO"):
                return not _is_index_underlying(sym)
    except Exception:
        pass

    # Conservative default: options (CE/PE suffix) on F&O exchanges need LIMIT
    if ts:
        ts_upper = ts.upper()
        if "OPT" in ts_upper:
            return True
        if ts_upper.endswith("CE") or ts_upper.endswith("PE"):
            return exchange.upper() in ("NFO", "BFO", "MCX")
    return False


# ── Broker Symbol Resolution ─────────────────────────────────────────────────

def resolve_broker_symbol(
    trading_symbol: str,
    exchange: str,
    broker: str,
) -> str:
    """
    Convert canonical trading_symbol to broker-specific format.

    Args:
        trading_symbol: "NIFTY26APR24500CE" (no exchange prefix)
        exchange:       "NFO"
        broker:         "fyers" | "shoonya" | "angelone" | "dhan" | "kite" |
                        "upstox" | "groww" | "zerodha" | "paper"

    Returns:
        Fyers:    "NFO:NIFTY26APR24500CE"
        Kite:     "NIFTY26APR24500CE" (Kite uses exchange:tradingsymbol in API)
        Shoonya/Angel/Dhan/Paper: "NIFTY26APR24500CE"
        Upstox:   instrument_key if found, else trading_symbol
        Groww:    trading_symbol
    """
    clean = trading_symbol.split(":", 1)[-1] if ":" in trading_symbol else trading_symbol
    symbols = get_broker_symbols(clean, exchange)
    return symbols.get(broker, clean)


def resolve_from_broker(broker_symbol: str, broker: str) -> tuple:
    """
    Parse broker-specific symbol to (trading_symbol, exchange).
    """
    if ":" in broker_symbol:
        parts = broker_symbol.split(":", 1)
        return parts[1], parts[0]
    return broker_symbol, ""


# ── Cross-broker symbol lookup ────────────────────────────────────────────────

def get_broker_symbols(
    trading_symbol: str,
    exchange: str,
) -> Dict[str, str]:
    """
    Given a canonical trading_symbol, find the equivalent symbol for each broker.

    Returns:
        {"fyers": "NFO:NIFTY26APR24500CE", "shoonya": "NIFTY26APR24500CE",
         "angelone": "NIFTY26APR24500CE", "dhan": "NIFTY26APR24500CE",
         "kite": "NIFTY26APR24500CE", "upstox": "NSE_FO|...", "groww": "NIFTY26APR24500CE"}
    """
    try:
        from db.symbols_db import get_broker_symbols_by_trading_symbol
        return get_broker_symbols_by_trading_symbol(trading_symbol, exchange)
    except Exception:
        clean = trading_symbol.split(":", 1)[-1] if ":" in trading_symbol else trading_symbol
        return {
            "fyers": f"{exchange}:{clean}",
            "shoonya": clean,
            "angelone": clean,
            "dhan": clean,
            "kite": clean,
            "upstox": clean,
            "groww": clean,
        }


# ══════════════════════════════════════════════════════════════════════════════
# COMPATIBILITY API — same interface as old scriptmaster.py
# Used by: market_reader, entry_engine, strategy_executor_service, symbol_normalizer
# ══════════════════════════════════════════════════════════════════════════════

def _fyers_to_standard(rec: Dict[str, Any]) -> Dict[str, Any]:
    """Backward-compatible adapter that now accepts DB rows or legacy records."""
    if "exchange" in rec or "trading_symbol" in rec:
        return _db_row_to_standard(rec)
    fyers_sym = rec.get("FyersSymbol", "")
    plain_sym = fyers_sym.split(":", 1)[-1] if ":" in fyers_sym else fyers_sym
    return {
        "TradingSymbol": plain_sym,
        "FyersSymbol": fyers_sym,
        "Symbol": rec.get("Underlying", ""),
        "Underlying": rec.get("Underlying", ""),
        "Exchange": rec.get("Exchange", ""),
        "Token": rec.get("Token", ""),
        "FyToken": rec.get("FyToken", ""),
        "LotSize": rec.get("LotSize", 1),
        "TickSize": rec.get("TickSize", 0.05),
        "Expiry": rec.get("Expiry", ""),
        "Instrument": rec.get("Instrument", ""),
        "OptionType": rec.get("OptionType"),
        "StrikePrice": rec.get("StrikePrice"),
        "Description": rec.get("Description", ""),
        "tsym": plain_sym,
        "exch": rec.get("Exchange", ""),
        "ls": str(rec.get("LotSize", 1)),
    }


def _parse_expiry_date(expiry_str: str) -> Optional[Any]:
    """Parse DD-Mon-YYYY expiry string to date, or None."""
    if not expiry_str:
        return None
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(expiry_str, fmt).date()
        except ValueError:
            continue
    return None


def refresh(force: bool = False) -> int:
    """Download / reload instrument master data. Returns total symbol count."""
    counts = refresh_all(force=force)
    return sum(counts.values())


def get_future(
    symbol: str,
    exchange: str = "NFO",
    result: int = 0,
) -> Optional[Dict[str, Any]]:
    """
    Look up futures contract for a given underlying.
    Returns dict with TradingSymbol, LotSize, Expiry, Exchange, Token, etc.
    """
    try:
        from db.symbols_db import get_future as db_get_future

        rec = db_get_future(symbol, exchange, result)
        if rec:
            return _db_row_to_standard(rec)
    except Exception as exc:
        logger.debug("get_future(%s, %s) failed: %s", symbol, exchange, exc)

    lot = get_lot_size(symbol, exchange)
    return {
        "TradingSymbol": f"{symbol.upper()}FUT",
        "Symbol": symbol.upper(),
        "Underlying": symbol.upper(),
        "Exchange": exchange.upper(),
        "LotSize": lot,
        "Expiry": "",
        "Instrument": "FUT",
        "Token": "",
        "FyersSymbol": "",
        "tsym": f"{symbol.upper()}FUT",
        "exch": exchange.upper(),
        "ls": str(lot),
    }


def get_futures_list(
    symbol: str,
    exchange: str = "NFO",
) -> List[Dict[str, Any]]:
    """Return all futures contracts for a symbol, sorted by expiry."""
    try:
        from db.symbols_db import get_futures as db_get_futures

        rows = db_get_futures(symbol, exchange)
        if rows:
            return [_db_row_to_standard(row) for row in rows]
    except Exception as exc:
        logger.debug("get_futures_list(%s, %s) failed: %s", symbol, exchange, exc)
    fut = get_future(symbol, exchange)
    return [fut] if fut else []


def get_option(
    symbol: str,
    exchange: str = "NFO",
    expiry: Optional[str] = None,
    strike: Optional[float] = None,
    option_type: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Look up a specific option contract. Returns first match or None."""
    results = get_options_list(symbol, exchange, expiry, strike, option_type)
    return results[0] if results else None


def get_options_list(
    symbol: str,
    exchange: str = "NFO",
    expiry: Optional[str] = None,
    strike: Optional[float] = None,
    option_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return options matching filters, sorted by (expiry, strike)."""
    try:
        from db.symbols_db import get_options as db_get_options

        rows = db_get_options(
            symbol=symbol,
            exchange=exchange,
            expiry=expiry,
            strike=strike,
            option_type=option_type,
        )
        return [_db_row_to_standard(row) for row in rows]
    except Exception as exc:
        logger.debug("get_options_list error: %s", exc)
        return []


def universal_symbol_search(
    symbol: str,
    exchange: str = "NFO",
) -> List[Dict[str, Any]]:
    """
    Search instrument master for matching symbols/underlyings.
    Returns list of dicts with TradingSymbol, LotSize, Expiry, etc.
    """
    try:
        from db.symbols_db import search_symbols

        rows = search_symbols(symbol, exchange, limit=50)
        if rows:
            return [_db_row_to_standard(row) for row in rows]
    except Exception as exc:
        logger.debug("universal_symbol_search error: %s", exc)
    fut = get_future(symbol, exchange)
    return [fut] if fut else []


def options_expiry(
    symbol: str,
    exchange: str = "NFO",
    result: Optional[int] = None,
) -> Any:
    """
    Return option expiry dates for the symbol.
    result=None: full list, result=0: nearest, result=1: next, etc.
    Returns list[str] (DD-Mon-YYYY) or str.
    """
    expiries = [_iso_to_broker(exp) or "" for exp in get_expiries(symbol, exchange, "OPTION")]
    if result is not None:
        return expiries[result] if len(expiries) > result else ""
    return expiries


def fut_expiry(
    symbol: str,
    exchange: str = "NFO",
    result: Optional[int] = None,
) -> Any:
    """Return futures expiry dates for the symbol."""
    expiries = [_iso_to_broker(exp) or "" for exp in get_expiries(symbol, exchange, "FUTURE")]
    if result is not None:
        return expiries[result] if len(expiries) > result else ""
    return expiries


def get_strike_gap(symbol: str, exchange: str = "NFO") -> int:
    """Return strike gap for the underlying."""
    return _STRIKE_GAPS.get(symbol.upper(), 50)


def get_spot_symbol(symbol: str) -> Optional[str]:
    """Return the Fyers spot/index symbol for an underlying (for quotes)."""
    return _INDEX_SYMBOLS.get(symbol.upper())


def build_option_trading_symbol(
    symbol: str,
    expiry: str,
    strike: float,
    option_type: str,
    exchange: str = "NFO",
) -> str:
    """Build a Fyers-format option trading symbol using real scriptmaster data."""
    try:
        from db.symbols_db import get_options as db_get_options

        matches = db_get_options(
            symbol=symbol,
            exchange=exchange,
            expiry=expiry,
            strike=strike,
            option_type=option_type,
        )
        if matches:
            row = matches[0]
            return row.get("fyers_symbol") or _default_fyers_symbol(exchange, row.get("trading_symbol", ""), row.get("symbol", ""))
    except Exception:
        pass

    # Fallback: construct from convention
    try:
        dt = _parse_expiry_date(expiry)
        if dt:
            yymm = dt.strftime("%y%m")
            dd = dt.strftime("%d")
            prefix = "NSE" if exchange in ("NFO", "NSE") else "BSE"
            return f"{prefix}:{symbol.upper()}{yymm}{dd}{int(strike)}{option_type.upper()}"
    except Exception:
        pass
    return f"{symbol.upper()}{expiry}{int(strike)}{option_type.upper()}"


def get_instrument_count() -> int:
    try:
        from db.symbols_db import get_symbol_count
        return get_symbol_count()
    except Exception:
        return 0


# ── Date helpers ──────────────────────────────────────────────────────────────

_MONTH_MAP = {
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
    "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
    "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
}


def _to_iso(raw: str) -> str:
    """Convert any expiry string to ISO YYYY-MM-DD."""
    if not raw:
        return ""
    raw = raw.strip()
    if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
        return raw  # already ISO
    try:
        dt = datetime.strptime(raw, "%d-%b-%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass
    try:
        dt = datetime.strptime(raw, "%d-%B-%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return raw


def _iso_to_broker(iso: str) -> Optional[str]:
    """Convert ISO date to DD-Mon-YYYY for broker API."""
    if not iso:
        return None
    try:
        dt = datetime.strptime(iso, "%Y-%m-%d")
        return dt.strftime("%-d-%b-%Y")
    except ValueError:
        return iso
