import sys
from pathlib import Path

# Ensure backend is on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.symbol_data_cleaning import (
    sanitize_angelone_scriptmaster,
    sanitize_dhan_scriptmaster,
    sanitize_fyers_scriptmaster,
    sanitize_kite_scriptmaster,
    sanitize_shoonya_scriptmaster,
)


def test_sanitize_fyers_scriptmaster_filters_invalid_and_normalizes_fields():
    data = {
        "bad": {"Exchange": "NSE"},
        "ok": {
            "FyersSymbol": "NSE:reliance-eq",
            "Exchange": "nse",
            "Token": " 2885 ",
            "Instrument": "eq",
            "OptionType": "xx",
            "Expiry": "2026-04-30",
        },
    }
    kept, dropped = sanitize_fyers_scriptmaster(data)
    assert kept == 1
    assert dropped == 1
    rec = data["NSE:reliance-eq"]
    assert rec["Exchange"] == "NSE"
    assert rec["Instrument"] == "EQ"
    assert rec["OptionType"] is None
    assert rec["Expiry"] == "30-Apr-2026"


def test_sanitize_shoonya_scriptmaster_uppercases_tradingsymbol_and_exchange():
    data = {
        "nse": {
            "2885": {
                "Token": "2885",
                "TradingSymbol": "reliance-eq",
                "Symbol": "reliance",
                "Instrument": "eq",
            }
        }
    }
    kept, dropped = sanitize_shoonya_scriptmaster(data)
    assert kept == 1
    assert dropped == 0
    assert "NSE" in data
    rec = data["NSE"]["2885"]
    assert rec["TradingSymbol"] == "RELIANCE-EQ"
    assert rec["Underlying"] == "RELIANCE"


def test_sanitize_angelone_scriptmaster_requires_token_exchange_and_symbol():
    data = {
        "x": {"Token": "", "Exchange": "NSE", "TradingSymbol": "SBIN-EQ"},
        "y": {"Token": "3045", "Exchange": "nse", "TradingSymbol": "sbin-eq", "Instrument": "eq"},
    }
    kept, dropped = sanitize_angelone_scriptmaster(data)
    assert kept == 1
    assert dropped == 1
    rec = data["3045"]
    assert rec["Exchange"] == "NSE"
    assert rec["TradingSymbol"] == "SBIN-EQ"


def test_sanitize_dhan_scriptmaster_uses_security_id_as_primary_key():
    data = {
        "tmp": {
            "SecurityId": "3045",
            "Exchange": "nse",
            "TradingSymbol": "sbin-eq",
            "Instrument": "EQ",
        }
    }
    kept, dropped = sanitize_dhan_scriptmaster(data)
    assert kept == 1
    assert dropped == 0
    assert "3045" in data
    assert data["3045"]["Token"] == "3045"


def test_sanitize_kite_scriptmaster_rekeys_on_exchange_and_tradingsymbol():
    data = {
        "old": {
            "Exchange": "nse",
            "TradingSymbol": "reliance-eq",
            "Instrument": "EQ",
            "ExchangeToken": "2885",
            "InstrumentToken": "738561",
        }
    }
    kept, dropped = sanitize_kite_scriptmaster(data)
    assert kept == 1
    assert dropped == 0
    assert "NSE:RELIANCE-EQ" in data
    rec = data["NSE:RELIANCE-EQ"]
    assert rec["KiteSymbol"] == "NSE:RELIANCE-EQ"