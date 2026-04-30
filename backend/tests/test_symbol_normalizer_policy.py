from __future__ import annotations

import sys
from pathlib import Path

# Ensure backend is on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from broker import symbol_normalizer as sn
from broker.symbol_normalizer import NormalizedInstrument


def test_resolve_symbol_for_broker_prefers_db_fyers_symbol(monkeypatch):
    inst = NormalizedInstrument(
        symbol="GOLDPETAL",
        trading_symbol="GOLDPETAL30APR26",
        exchange="MCX",
        instrument_type="FUT",
        fyers_symbol="MCX:GOLDPETAL26APRFUT",
    )

    monkeypatch.setattr(sn, "lookup_by_trading_symbol", lambda *args, **kwargs: inst)

    canonical, exchange, broker_symbol = sn.resolve_symbol_for_broker(
        "GOLDPETAL30APR26", "MCX", "fyers"
    )

    assert canonical == "GOLDPETAL30APR26"
    assert exchange == "MCX"
    assert broker_symbol == "MCX:GOLDPETAL26APRFUT"


def test_resolve_symbol_for_broker_handles_exchange_prefixed_input(monkeypatch):
    inst = NormalizedInstrument(
        symbol="CRUDEOILM",
        trading_symbol="CRUDEOILM16APR26C4850",
        exchange="MCX",
        instrument_type="OPT",
        option_type="CE",
        strike=4850.0,
        fyers_symbol="MCX:CRUDEOILM26APR4850CE",
    )

    def fake_lookup(symbol, exchange=""):
        if symbol in ("MCX:CRUDEOILM16APR26C4850", "CRUDEOILM16APR26C4850"):
            return inst
        return None

    monkeypatch.setattr(sn, "lookup_by_trading_symbol", fake_lookup)

    canonical, exchange, broker_symbol = sn.resolve_symbol_for_broker(
        "MCX:CRUDEOILM16APR26C4850", "", "fyers"
    )

    assert canonical == "CRUDEOILM16APR26C4850"
    assert exchange == "MCX"
    assert broker_symbol == "MCX:CRUDEOILM26APR4850CE"


def test_resolve_symbol_for_broker_fallback_when_db_unavailable(monkeypatch):
    monkeypatch.setattr(sn, "lookup_by_trading_symbol", lambda *args, **kwargs: None)

    canonical, exchange, broker_symbol = sn.resolve_symbol_for_broker(
        "SBIN", "NSE", "fyers"
    )

    assert canonical == "SBIN"
    assert exchange == "NSE"
    assert broker_symbol == "NSE:SBIN-EQ"


def test_resolve_symbol_for_broker_ignores_wrong_exchange_hint_when_db_has_match(monkeypatch):
    inst = NormalizedInstrument(
        symbol="GOLDPETAL",
        trading_symbol="GOLDPETAL30APR26",
        exchange="MCX",
        instrument_type="FUT",
        fyers_symbol="MCX:GOLDPETAL26APRFUT",
    )

    calls = []

    def fake_lookup(symbol, exchange=""):
        calls.append((symbol, exchange))
        if exchange == "" and symbol in ("GOLDPETAL30APR26", "MCX:GOLDPETAL30APR26"):
            return inst
        return None

    monkeypatch.setattr(sn, "lookup_by_trading_symbol", fake_lookup)

    canonical, exchange, broker_symbol = sn.resolve_symbol_for_broker(
        "GOLDPETAL30APR26", "NSE", "fyers"
    )

    assert canonical == "GOLDPETAL30APR26"
    assert exchange == "MCX"
    assert broker_symbol == "MCX:GOLDPETAL26APRFUT"
    assert any(ex == "" for _, ex in calls)
