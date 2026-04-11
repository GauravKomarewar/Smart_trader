"""
Symbol ScriptMaster Data Cleaning
=================================

Sanitizes broker scriptmaster dictionaries before symbols are persisted
to PostgreSQL. This module is intentionally conservative: it only performs
safe trimming, normalization, and required-field filtering.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Tuple


def _s(value: Any) -> str:
    return str(value or "").strip()


def _u(value: Any) -> str:
    return _s(value).upper()


def _expiry_dd_mon_yyyy(raw: Any) -> str:
    value = _s(raw)
    if not value:
        return ""
    for fmt in ("%d-%b-%Y", "%d-%b-%y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            dt = datetime.strptime(value, fmt)
            return dt.strftime("%d-%b-%Y")
        except ValueError:
            continue
    return value


def _option_type(raw: Any) -> str | None:
    v = _u(raw)
    return v if v in {"CE", "PE"} else None


def _instrument(raw: Any) -> str:
    v = _u(raw)
    if not v:
        return "EQ"
    if v in {"CE", "PE"}:
        return "OPT"
    if v.startswith("OPT"):
        return "OPT"
    if v.startswith("FUT"):
        return "FUT"
    if v in {"INDEX"}:
        return "IDX"
    return v


def sanitize_fyers_scriptmaster(records: Dict[str, Dict[str, Any]]) -> Tuple[int, int]:
    cleaned: Dict[str, Dict[str, Any]] = {}
    dropped = 0
    for rec in records.values():
        fyers_symbol = _s(rec.get("FyersSymbol"))
        if not fyers_symbol:
            dropped += 1
            continue
        exchange = _u(rec.get("Exchange"))
        if not exchange:
            dropped += 1
            continue

        tsym = fyers_symbol.split(":", 1)[-1]
        token = _s(rec.get("Token"))
        cleaned[fyers_symbol] = {
            **rec,
            "FyersSymbol": fyers_symbol,
            "Exchange": exchange,
            "Token": token,
            "Underlying": _u(rec.get("Underlying") or tsym),
            "Description": _s(rec.get("Description")),
            "Instrument": _instrument(rec.get("Instrument")),
            "OptionType": _option_type(rec.get("OptionType")),
            "Expiry": _expiry_dd_mon_yyyy(rec.get("Expiry")),
            "ISIN": _s(rec.get("ISIN")),
        }

    records.clear()
    records.update(cleaned)
    return len(cleaned), dropped


def sanitize_shoonya_scriptmaster(records: Dict[str, Dict[str, Dict[str, Any]]]) -> Tuple[int, int]:
    total = 0
    dropped = 0
    for exch, token_map in list(records.items()):
        exchange = _u(exch)
        cleaned_map: Dict[str, Dict[str, Any]] = {}
        for rec in token_map.values():
            token = _s(rec.get("Token"))
            tsym = _u(rec.get("TradingSymbol"))
            if not token or not tsym:
                dropped += 1
                continue
            cleaned_map[token] = {
                **rec,
                "Exchange": exchange,
                "Token": token,
                "TradingSymbol": tsym,
                "Symbol": _u(rec.get("Symbol") or tsym),
                "Underlying": _u(rec.get("Underlying") or rec.get("Symbol") or tsym),
                "Instrument": _instrument(rec.get("Instrument")),
                "OptionType": _option_type(rec.get("OptionType")),
                "Expiry": _expiry_dd_mon_yyyy(rec.get("Expiry")),
            }
        records[exchange] = cleaned_map
        total += len(cleaned_map)
    return total, dropped


def sanitize_angelone_scriptmaster(records: Dict[str, Dict[str, Any]]) -> Tuple[int, int]:
    cleaned: Dict[str, Dict[str, Any]] = {}
    dropped = 0
    for token_key, rec in records.items():
        token = _s(rec.get("Token") or token_key)
        exchange = _u(rec.get("Exchange"))
        tsym = _u(rec.get("TradingSymbol") or rec.get("Symbol"))
        if not token or not token.isdigit() or not exchange or not tsym:
            dropped += 1
            continue
        cleaned[token] = {
            **rec,
            "Token": token,
            "Exchange": exchange,
            "TradingSymbol": tsym,
            "Symbol": _u(rec.get("Symbol") or tsym),
            "Underlying": _u(rec.get("Underlying") or rec.get("Symbol") or tsym),
            "Instrument": _instrument(rec.get("Instrument") or rec.get("InstrumentType")),
            "OptionType": _option_type(rec.get("OptionType")),
            "Expiry": _expiry_dd_mon_yyyy(rec.get("Expiry")),
            "Name": _s(rec.get("Name")),
        }
    records.clear()
    records.update(cleaned)
    return len(cleaned), dropped


def sanitize_dhan_scriptmaster(records: Dict[str, Dict[str, Any]]) -> Tuple[int, int]:
    cleaned: Dict[str, Dict[str, Any]] = {}
    dropped = 0
    for key, rec in records.items():
        security_id = _s(rec.get("SecurityId") or rec.get("Token") or key)
        exchange = _u(rec.get("Exchange"))
        tsym = _u(rec.get("TradingSymbol") or rec.get("DisplayName") or rec.get("Symbol"))
        if not security_id or not exchange or not tsym:
            dropped += 1
            continue
        cleaned[security_id] = {
            **rec,
            "SecurityId": security_id,
            "Token": security_id,
            "Exchange": exchange,
            "TradingSymbol": tsym,
            "Symbol": _u(rec.get("Symbol") or tsym),
            "Underlying": _u(rec.get("Underlying") or rec.get("Symbol") or tsym),
            "Instrument": _instrument(rec.get("Instrument") or rec.get("InstrumentName")),
            "OptionType": _option_type(rec.get("OptionType")),
            "Expiry": _expiry_dd_mon_yyyy(rec.get("Expiry")),
            "DisplayName": _s(rec.get("DisplayName")),
        }
    records.clear()
    records.update(cleaned)
    return len(cleaned), dropped


def sanitize_kite_scriptmaster(records: Dict[str, Dict[str, Any]]) -> Tuple[int, int]:
    cleaned: Dict[str, Dict[str, Any]] = {}
    dropped = 0
    for rec in records.values():
        exchange = _u(rec.get("Exchange"))
        tsym = _u(rec.get("TradingSymbol"))
        if not exchange or not tsym:
            dropped += 1
            continue
        key = f"{exchange}:{tsym}"
        exchange_token = _s(rec.get("ExchangeToken"))
        instrument_token = _s(rec.get("InstrumentToken"))
        cleaned[key] = {
            **rec,
            "Exchange": exchange,
            "TradingSymbol": tsym,
            "ExchangeToken": exchange_token,
            "InstrumentToken": instrument_token,
            "Underlying": _u(rec.get("Underlying") or tsym),
            "Instrument": _instrument(rec.get("Instrument") or rec.get("InstrumentType")),
            "OptionType": _option_type(rec.get("OptionType") or rec.get("InstrumentType")),
            "Expiry": _expiry_dd_mon_yyyy(rec.get("Expiry")),
            "Name": _s(rec.get("Name")),
            "KiteSymbol": key,
        }
    records.clear()
    records.update(cleaned)
    return len(cleaned), dropped