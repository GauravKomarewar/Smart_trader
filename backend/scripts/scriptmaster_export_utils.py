"""Shared helpers for unified scriptmaster CSV exports."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Dict, Iterable, List

_COMMON_COLUMN_ORDER: List[str] = [
    "UniqueKey",
    "Broker",
    "Exchange",
    "Token",
    "TradingSymbol",
    "FyersSymbol",
    "KiteSymbol",
    "GrowwSymbol",
    "Symbol",
    "Name",
    "Description",
    "Underlying",
    "Instrument",
    "InstrumentType",
    "OptionType",
    "Expiry",
    "ExpiryEpoch",
    "StrikePrice",
    "LotSize",
    "TickSize",
    "ISIN",
    "Segment",
    "SegmentKey",
    "ExchangeToken",
    "InstrumentToken",
    "InstrumentKey",
    "SecurityId",
    "DisplayName",
    "ShortName",
    "Series",
    "ExchId",
    "ExchSeg",
    "UnderlyingExchangeToken",
    "FreezeQuantity",
    "BuyAllowed",
    "SellAllowed",
    "FyToken",
]

_TOKEN_FALLBACK_FIELDS: List[str] = [
    "Token",
    "ExchangeToken",
    "InstrumentToken",
    "SecurityId",
    "FyToken",
]


def _pick_token(row: Dict[str, Any]) -> str:
    for key in _TOKEN_FALLBACK_FIELDS:
        val = row.get(key)
        if val is not None and str(val).strip() != "":
            return str(val).strip()
    return ""


def _prepare_unified_rows(rows: Iterable[Dict[str, Any]], broker: str) -> List[Dict[str, Any]]:
    prepared: List[Dict[str, Any]] = []
    broker_name = broker.upper().strip()

    for src in rows:
        row = dict(src)
        exchange = str(row.get("Exchange") or "").strip().upper()
        token = _pick_token(row)

        row["Exchange"] = exchange
        row["Token"] = token
        row["Broker"] = broker_name
        row["UniqueKey"] = f"{exchange}|{token}" if exchange and token else ""

        prepared.append(row)

    return prepared


def _ordered_fieldnames(rows: Iterable[Dict[str, Any]]) -> List[str]:
    keyset = {k for row in rows for k in row.keys()}
    ordered = [col for col in _COMMON_COLUMN_ORDER if col in keyset]
    ordered_set = set(ordered)
    remaining = sorted(k for k in keyset if k not in ordered_set)
    return ordered + remaining


def write_unified_csv(
    rows: Iterable[Dict[str, Any]],
    output_path: str,
    broker: str,
) -> str:
    """Write broker rows into a unified-column CSV and return output path."""
    prepared_rows = _prepare_unified_rows(rows, broker)
    if not prepared_rows:
        raise RuntimeError(f"No {broker} scriptmaster data available to export")

    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = _ordered_fieldnames(prepared_rows)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(prepared_rows)

    return str(out_path)
