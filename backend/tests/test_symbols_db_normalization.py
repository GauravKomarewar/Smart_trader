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


def test_resolve_broker_symbol_partner_row_requires_same_underlying(monkeypatch):
    class _FakeCursor:
        def __init__(self):
            self._params = None

        def execute(self, _query, params):
            self._params = params

        def fetchone(self):
            if self._params and self._params[-1] == "BANKNIFTY":
                return {
                    "shoonya_tsym": "BANKNIFTY26APR56300PE",
                    "shoonya_token": "12345",
                    "exchange": "NFO",
                    "tick_size": 0.05,
                    "lot_size": 30,
                    "normalized_underlying": "BANKNIFTY",
                }
            return {
                "shoonya_tsym": "NIFTYNXT5026APR56300PE",
                "shoonya_token": "99999",
                "exchange": "NFO",
                "tick_size": 0.05,
                "lot_size": 75,
                "normalized_underlying": "NIFTYNXT50",
            }

    class _FakeConn:
        def __init__(self):
            self._cursor = _FakeCursor()

        def cursor(self, cursor_factory=None):
            return self._cursor

        def close(self):
            return None

    monkeypatch.setattr(sdb, "get_trading_conn", lambda: _FakeConn())
    monkeypatch.setattr(
        sdb,
        "lookup_by_trading_symbol",
        lambda *_args, **_kwargs: {
            "expiry": "2026-04-28",
            "strike": 56300,
            "option_type": "PE",
            "exchange": "NFO",
            "normalized_underlying": "BANKNIFTY",
            "symbol": "BANKNIFTY",
            "shoonya_tsym": "",
        },
    )

    resolved = sdb.resolve_broker_symbol("BANKNIFTY26APR56300PE", "shoonya", "NFO")
    assert resolved["symbol"] == "BANKNIFTY26APR56300PE"