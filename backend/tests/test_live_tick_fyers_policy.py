from __future__ import annotations

import sys
from pathlib import Path

# Ensure backend is on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from broker.live_tick_service import LiveTickService


def test_to_fyers_sym_uses_nse_for_nifty_option_without_exchange_hint():
    sym = LiveTickService._to_fyers_sym("NIFTY2642124500CE")
    assert sym == "NSE:NIFTY2642124500CE"


def test_to_fyers_sym_uses_bse_for_sensex_option_without_exchange_hint():
    sym = LiveTickService._to_fyers_sym("SENSEX2642275000CE")
    assert sym == "BSE:SENSEX2642275000CE"


def test_to_fyers_sym_keeps_mcx_for_commodity_option():
    sym = LiveTickService._to_fyers_sym("GOLDPETAL30APR26CE")
    assert sym == "MCX:GOLDPETAL30APR26CE"


def test_to_fyers_sym_keeps_mcx_for_crudeoilm_spot_root():
    sym = LiveTickService._to_fyers_sym("CRUDEOILM")
    assert sym == "MCX:CRUDEOILM"


def test_to_fyers_sym_keeps_mcx_for_naturalgas_eq_variant():
    sym = LiveTickService._to_fyers_sym("NATURALGAS-EQ")
    assert sym == "MCX:NATURALGAS"


def test_to_fyers_sym_exchange_hint_nse_still_forces_mcx_for_commodity():
    sym = LiveTickService._to_fyers_sym("NSE:CRUDEOILM")
    assert sym == "MCX:CRUDEOILM"


def test_to_fyers_sym_ignores_cross_underlying_db_fallback(monkeypatch):
    from db import symbols_db as sdb

    monkeypatch.setattr(
        sdb,
        "resolve_broker_symbol",
        lambda *_args, **_kwargs: {
            "symbol": "NSE:NIFTYNXT5026APR56300PE",
            "token": "",
            "exchange": "NSE",
            "tick_size": "0.05",
            "lot_size": "25",
        },
    )

    sym = LiveTickService._to_fyers_sym("BANKNIFTY26APR56300PE")
    assert sym == "NSE:BANKNIFTY26APR56300PE"


def test_fyers_auth_error_classifier_rejects_invalid_symbol_error():
    assert not LiveTickService._is_fyers_auth_error(
        code="-300",
        err_str="please provide a valid symbol",
        invalid_symbols=["NFO:NIFTY2642124500CE"],
    )


def test_fyers_auth_error_classifier_accepts_token_expired_error():
    assert LiveTickService._is_fyers_auth_error(
        code="401",
        err_str="token expired",
        invalid_symbols=[],
    )
