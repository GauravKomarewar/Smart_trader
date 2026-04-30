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


# ── _clean_underlying tests ───────────────────────────────────────────────────

def test_clean_underlying_equity_unchanged():
    assert sdb._clean_underlying("RELIANCE") == "RELIANCE"
    assert sdb._clean_underlying("INFY") == "INFY"
    assert sdb._clean_underlying("360ONE") == "360ONE"


def test_clean_underlying_strips_eq_suffix():
    assert sdb._clean_underlying("RELIANCE-EQ") == "RELIANCE"
    assert sdb._clean_underlying("SBIN-BE") == "SBIN"


def test_clean_underlying_strips_index_suffix():
    assert sdb._clean_underlying("NIFTY-INDEX") == "NIFTY"
    assert sdb._clean_underlying("SENSEX-INDEX") == "SENSEX"


def test_clean_underlying_removes_trailing_digits():
    # Some MCX broker feeds append a contract suffix digit
    assert sdb._clean_underlying("CRUDEOIL1") == "CRUDEOIL"
    assert sdb._clean_underlying("GOLDPETAL2") == "GOLDPETAL"


def test_clean_underlying_strips_broker_month_suffix():
    # Shoonya MCX/CDS feeds: underlying field contains SYMBOL+YYMON
    assert sdb._clean_underlying("ALUMINI26APR") == "ALUMINI"
    assert sdb._clean_underlying("CRUDEOIL26MAY") == "CRUDEOIL"
    assert sdb._clean_underlying("USDINR26APR") == "USDINR"
    assert sdb._clean_underlying("EURINR26JUN") == "EURINR"


def test_clean_underlying_mcx_long_form_keeps_first_word():
    assert sdb._clean_underlying("GOLDPETAL FUT 30 APR 26") == "GOLDPETAL"
    assert sdb._clean_underlying("CRUDEOIL FUT 17 APR 26") == "CRUDEOIL"
    assert sdb._clean_underlying("NATURALGAS FUT 28 APR 26") == "NATURALGAS"


def test_clean_underlying_cds_long_form_keeps_first_word():
    assert sdb._clean_underlying("EURINR FUT 26 MAY 26") == "EURINR"
    assert sdb._clean_underlying("USDINR FUT 28 APR 26") == "USDINR"


def test_clean_underlying_lowercases_and_strips_whitespace():
    assert sdb._clean_underlying("  reliance  ") == "RELIANCE"
    assert sdb._clean_underlying("goldpetal fut 30 apr 26") == "GOLDPETAL"


def test_clean_underlying_empty_returns_empty():
    assert sdb._clean_underlying("") == ""
    assert sdb._clean_underlying(None) == ""


def test_extract_underlying_hint_handles_360one_option_formats():
    assert sdb._extract_underlying_hint("360ONE 1000 CE 26 MAY 26") == "360ONE"
    assert sdb._extract_underlying_hint("BFO:360ONE26MAY261000CE") == "360ONE"


def test_extract_underlying_hint_handles_numeric_cash_aliases():
    assert sdb._extract_underlying_hint("12 MFLS2") == "12"
    assert sdb._extract_underlying_hint("505196") == "505196"


def test_clean_underlying_index_names_preserved():
    assert sdb._clean_underlying("NIFTY") == "NIFTY"
    assert sdb._clean_underlying("BANKNIFTY") == "BANKNIFTY"
    assert sdb._clean_underlying("MIDCPNIFTY") == "MIDCPNIFTY"


# ── SymbolNormalizer new methods smoke tests ──────────────────────────────────

def test_normalizer_search_by_normalized_returns_none_when_db_unavailable(monkeypatch):
    monkeypatch.setattr(
        sdb, "lookup_by_smart_trader_name", lambda _name: None
    )
    from broker.symbol_normalizer import get_normalizer
    norm = get_normalizer()
    assert norm.search_by_normalized("NFO-NIFTY-FUT-28Apr26") is None


def test_normalizer_get_all_broker_symbols_empty_when_not_found(monkeypatch):
    monkeypatch.setattr(
        sdb, "lookup_by_smart_trader_name", lambda _name: None
    )
    from broker.symbol_normalizer import get_normalizer
    norm = get_normalizer()
    assert norm.get_all_broker_symbols("NFO-NIFTY-FUT-28Apr26") == {}


def test_normalizer_get_all_broker_symbols_returns_per_broker_dict(monkeypatch):
    fake_row = {
        "fyers_symbol": "NFO:NIFTY26APRFUT",
        "fyers_token": "abc",
        "shoonya_tsym": "NIFTY26APRFUT",
        "shoonya_token": "12345",
        "angelone_tsym": "NIFTY26APRFUT",
        "angelone_token": "999",
        "dhan_security_id": "dhan123",
        "kite_instrument_token": "kite789",
        "upstox_ikey": "upstox_key",
        "groww_token": "groww_tok",
        "trading_symbol": "NIFTY26APRFUT",
    }
    monkeypatch.setattr(sdb, "lookup_by_smart_trader_name", lambda _name: fake_row)
    from broker.symbol_normalizer import get_normalizer
    norm = get_normalizer()
    result = norm.get_all_broker_symbols("NFO-NIFTY-FUT-28Apr26")
    assert result["fyers"]["symbol"] == "NFO:NIFTY26APRFUT"
    assert result["shoonya"]["symbol"] == "NIFTY26APRFUT"
    assert result["shoonya"]["token"] == "12345"
    assert result["angelone"]["token"] == "999"
    assert result["dhan"]["symbol"] == "dhan123"


def test_normalizer_search_returns_smart_trader_name(monkeypatch):
    fake_rows = [
        {
            "smart_trader_name": "NFO-NIFTY-FUT-28Apr26",
            "exchange": "NFO",
            "symbol": "NIFTY",
            "instrument_type": "FUT",
            "lot_size": 75,
            "tick_size": 0.05,
        }
    ]
    monkeypatch.setattr(sdb, "search_symbols", lambda *_a, **_kw: fake_rows)
    from broker.symbol_normalizer import get_normalizer
    norm = get_normalizer()
    results = norm.search("NIFTY", exchange="NFO")
    assert len(results) == 1
    assert results[0]["smart_trader_name"] == "NFO-NIFTY-FUT-28Apr26"
    assert results[0]["lot_size"] == 75


def test_broker_symbol_candidates_shoonya_option_format():
    from broker.symbol_normalizer import broker_symbol_candidates

    cands = broker_symbol_candidates(
        broker="shoonya",
        exchange="NFO",
        trading_symbol="NIFTY05MAY2624000CE",
        underlying="NIFTY",
        instrument_type="OPT",
        expiry="2026-05-05",
        strike=24000,
        option_type="CE",
    )
    assert "NIFTY05MAY26C24000" in cands


def test_broker_symbol_candidates_shoonya_future_short_f_format():
    from broker.symbol_normalizer import broker_symbol_candidates

    cands = broker_symbol_candidates(
        broker="shoonya",
        exchange="NFO",
        trading_symbol="SBIN26MAY26FUT",
        underlying="SBIN",
        instrument_type="FUT",
        expiry="2026-05-26",
        strike=0,
        option_type="",
    )
    assert cands[0] == "SBIN26MAY26F"


def test_to_broker_symbol_equity_uses_eq_suffix():
    from broker.symbol_normalizer import to_broker_symbol

    assert to_broker_symbol("SBIN", "NSE", "shoonya", underlying="SBIN", instrument_type="EQ") == "SBIN-EQ"
    assert to_broker_symbol("SBIN", "NSE", "fyers", underlying="SBIN", instrument_type="EQ") == "NSE:SBIN-EQ"
    assert to_broker_symbol("SBIN", "BSE", "shoonya", underlying="SBIN", instrument_type="EQ") == "SBIN"
    assert to_broker_symbol("SBIN", "BSE", "fyers", underlying="SBIN", instrument_type="EQ") == "BSE:SBIN"


def test_to_broker_symbol_fyers_monthly_derivative_without_year():
    from broker.symbol_normalizer import to_broker_symbol

    fut = to_broker_symbol(
        trading_symbol="SBIN26MAY26FUT",
        exchange="NFO",
        broker="fyers",
        underlying="SBIN",
        instrument_type="FUT",
        expiry="2026-05-26",
    )
    opt = to_broker_symbol(
        trading_symbol="SBIN26MAY261000CE",
        exchange="NFO",
        broker="fyers",
        underlying="SBIN",
        instrument_type="OPT",
        expiry="2026-05-26",
        strike=1000,
        option_type="CE",
    )
    assert fut == "NFO:SBIN26MAYFUT"
    assert opt == "NFO:SBIN26MAY1000CE"


def test_to_broker_symbol_kite_uses_plain_eq_and_ddmon_derivatives():
    from broker.symbol_normalizer import to_broker_symbol

    eq = to_broker_symbol("SBIN", "NSE", "kite", underlying="SBIN", instrument_type="EQ")
    fut = to_broker_symbol(
        trading_symbol="SBIN26MAY26FUT",
        exchange="NFO",
        broker="kite",
        underlying="SBIN",
        instrument_type="FUT",
        expiry="2026-05-26",
    )
    opt = to_broker_symbol(
        trading_symbol="SBIN26MAY261000CE",
        exchange="NFO",
        broker="kite",
        underlying="SBIN",
        instrument_type="OPT",
        expiry="2026-05-26",
        strike=1000,
        option_type="CE",
    )
    assert eq == "SBIN"
    assert fut == "SBIN26MAYFUT"
    assert opt == "SBIN26MAY1000CE"


def test_to_broker_symbol_angelone_uses_ddmonyy_derivatives():
    from broker.symbol_normalizer import to_broker_symbol

    fut = to_broker_symbol(
        trading_symbol="SBIN26MAY26FUT",
        exchange="NFO",
        broker="angelone",
        underlying="SBIN",
        instrument_type="FUT",
        expiry="2026-05-26",
    )
    opt = to_broker_symbol(
        trading_symbol="SBIN26MAY261000CE",
        exchange="NFO",
        broker="angelone",
        underlying="SBIN",
        instrument_type="OPT",
        expiry="2026-05-26",
        strike=1000,
        option_type="CE",
    )
    assert fut == "SBIN26MAY26FUT"
    assert opt == "SBIN26MAY261000CE"


def test_to_broker_symbol_token_based_brokers_do_not_fake_text_keys():
    from broker.symbol_normalizer import to_broker_symbol

    assert to_broker_symbol("SBIN", "NSE", "dhan", underlying="SBIN", instrument_type="EQ") == ""
    assert to_broker_symbol("SBIN", "NSE", "upstox", underlying="SBIN", instrument_type="EQ") == ""


def test_resolve_broker_symbol_uses_formula_fallback(monkeypatch):
    class _FakeCursor:
        def execute(self, _query, _params):
            return None

        def fetchone(self):
            return None

    class _FakeConn:
        def cursor(self, cursor_factory=None):
            return _FakeCursor()

        def close(self):
            return None

    monkeypatch.setattr(sdb, "get_trading_conn", lambda: _FakeConn())
    monkeypatch.setattr(
        sdb,
        "lookup_by_trading_symbol",
        lambda *_args, **_kwargs: {
            "trading_symbol": "NIFTY05MAY2624000CE",
            "exchange": "NFO",
            "instrument_type": "OPT",
            "expiry": "2026-05-05",
            "strike": 24000,
            "option_type": "CE",
            "symbol": "NIFTY",
            "lot_size": 75,
            "tick_size": 0.05,
            "shoonya_tsym": "",
        },
    )

    resolved = sdb.resolve_broker_symbol("NIFTY05MAY2624000CE", "shoonya", "NFO")
    assert resolved["symbol"] in {"NIFTY05MAY2624000CE", "NIFTY05MAY26C24000"}