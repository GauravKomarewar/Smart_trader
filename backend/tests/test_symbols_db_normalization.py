from db import symbols_db as sdb


def test_normalize_trading_symbol_value_strips_equity_suffixes():
    assert sdb._normalize_trading_symbol_value("RELIANCE-EQ", "EQ") == "RELIANCE"
    assert sdb._normalize_trading_symbol_value("NSE:RELIANCE-EQ", "EQ") == "RELIANCE"
    assert sdb._normalize_trading_symbol_value("NIFTY 50-INDEX", "IDX") == "NIFTY 50"


def test_normalize_trading_symbol_value_preserves_derivative_symbols():
    assert sdb._normalize_trading_symbol_value("RELIANCE26APR1300CE", "OPT") == "RELIANCE26APR1300CE"
    assert sdb._normalize_trading_symbol_value("MCX:GOLDPETAL26APRFUT", "FUT") == "GOLDPETAL26APRFUT"


def test_lookup_symbol_variants_include_common_aliases():
    variants = sdb._lookup_symbol_variants("RELIANCE")
    assert variants == ["RELIANCE", "RELIANCE-EQ", "RELIANCE-INDEX"]

    eq_variants = sdb._lookup_symbol_variants("NSE:RELIANCE-EQ")
    assert eq_variants == ["RELIANCE-EQ", "RELIANCE", "RELIANCE-INDEX"]


def test_broker_coverage_score_prefers_richer_rows():
    weak = {
        "kite_exchange_token": "738561",
        "kite_instrument_token": "738561",
        "description": "",
    }
    rich = {
        "fyers_symbol": "NSE:RELIANCE-EQ",
        "fyers_token": "10100000002885",
        "shoonya_token": "2885",
        "shoonya_tsym": "RELIANCE-EQ",
        "angelone_token": "2885",
        "angelone_tsym": "RELIANCE-EQ",
        "dhan_security_id": "2885",
        "description": "RELIANCE INDUSTRIES LTD",
    }

    assert sdb._broker_coverage_score(rich) > sdb._broker_coverage_score(weak)