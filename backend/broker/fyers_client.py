"""
Smart Trader — Fyers Data Provider
====================================
Provides live market data (quotes, indices, option chain, screener) via
the Fyers API v3. Used as the global data source for all users.

The client operates in two modes:
  • configured + token valid  → live mode (real data from Fyers)
  • else                      → offline (empty / caller falls back to demo)

Token loading order:
  1. Path in FYERS_TOKEN_FILE env var / config
  2. /home/ubuntu/option_trading_system_fyers/fyers_token.json  (legacy path)
"""
from __future__ import annotations

import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List

logger = logging.getLogger("smart_trader.fyers")

# ── Static Fyers symbol tables ─────────────────────────────────────────────────

# These symbols update monthly. The client generates current-month expiry strings
# dynamically so they stay valid.
_INDEX_SYMBOLS: Dict[str, str] = {
    "NSE:NIFTY50-INDEX":     "NIFTY",
    "NSE:NIFTYBANK-INDEX":   "BANKNIFTY",
    "NSE:FINNIFTY-INDEX":    "FINNIFTY",
    "NSE:MIDCPNIFTY-INDEX":  "MIDCPNIFTY",
    "BSE:SENSEX-INDEX":      "SENSEX",
}

# Fyers option chain underlying symbol map
OC_SYMBOL_MAP: Dict[str, str] = {
    "NIFTY":       "NSE:NIFTY50-INDEX",
    "BANKNIFTY":   "NSE:NIFTYBANK-INDEX",
    "FINNIFTY":    "NSE:FINNIFTY-INDEX",
    "MIDCPNIFTY":  "NSE:MIDCPNIFTY-INDEX",
    "SENSEX":      "BSE:SENSEX-INDEX",
}


def _near_month_suffix() -> str:
    """Return '26APR' style suffix for the current near-month contract."""
    now = datetime.now()
    return now.strftime("%y%b").upper()


def _next_month_suffix() -> str:
    """Return '26MAY' style suffix for the next-month contract."""
    now = datetime.now()
    if now.month == 12:
        nxt = datetime(now.year + 1, 1, 1)
    else:
        nxt = datetime(now.year, now.month + 1, 1)
    return nxt.strftime("%y%b").upper()


def _month_suffix(offset: int) -> str:
    """Return '26JUN' style suffix for current_month + offset."""
    now = datetime.now()
    month = now.month + offset
    year = now.year
    while month > 12:
        month -= 12
        year += 1
    return datetime(year, month, 1).strftime("%y%b").upper()


# MCX symbols that use futures contracts as option chain underlying
_MCX_SYMBOLS = {
    "CRUDEOIL", "GOLD", "GOLDM", "SILVER", "SILVERM", "SILVERMIC",
    "NATURALGAS", "COPPER", "ALUMINIUM", "ZINC", "LEAD", "NICKEL",
    "GOLDPETAL",
}


def _resolve_oc_symbol(underlying: str, exchange: str = "NFO") -> list[str]:
    """
    Resolve the Fyers optionchain API symbol for a given underlying+exchange.
    Returns a list of candidate symbols to try (first match wins).

    - Indices:       NSE:NIFTY50-INDEX  (static map)
    - MCX commodity: MCX:{SYM}{YYMM}FUT  (try current thru +4 months ahead)
    - NFO/BFO stock: NSE:{SYM}-EQ
    """
    sym = underlying.upper()
    exch = exchange.upper()

    # 1. Static index map (highest priority)
    if sym in OC_SYMBOL_MAP:
        return [OC_SYMBOL_MAP[sym]]

    # 2. MCX commodities — underlying is the futures contract
    #    Try current month through +4 ahead (covers GOLD bi-monthly cycles)
    if exch == "MCX" or sym in _MCX_SYMBOLS:
        return [f"MCX:{sym}{_month_suffix(i)}FUT" for i in range(5)]

    # 3. NFO / BFO stocks — underlying is the equity symbol
    return [f"NSE:{sym}-EQ"]


# ── Fyers Client ───────────────────────────────────────────────────────────────

class FyersDataClient:
    """Wrapper around fyers_apiv3.FyersModel for data-only usage."""

    def __init__(self) -> None:
        self._client: Optional[Any] = None
        self._lock   = threading.Lock()
        self._app_id = ""
        self._loaded = False

    # ── init / reload ──────────────────────────────────────────────────────────

    def _try_load(self) -> bool:
        """Try to initialise the Fyers client from config + token file."""
        if self._loaded:
            return self._client is not None

        from core.config import config

        app_id = config.FYERS_APP_ID
        token_paths = [
            Path(config.FYERS_TOKEN_FILE),
            Path("/home/ubuntu/option_trading_system_fyers/fyers_token.json"),
        ]

        access_token: Optional[str] = None
        for tp in token_paths:
            if tp.exists():
                try:
                    data = json.loads(tp.read_text())
                    access_token = data.get("access_token")
                    if not app_id:
                        # Try reading app_id embedded in credentials next to token
                        creds_path = tp.parent / "config" / "credentials.env"
                        if creds_path.exists():
                            for line in creds_path.read_text().splitlines():
                                if line.startswith("APP_ID="):
                                    app_id = line.split("=", 1)[1].strip()
                    if access_token:
                        break
                except Exception as e:
                    logger.debug("fyers token load from %s: %s", tp, e)

        if not app_id or not access_token:
            logger.info("Fyers: no app_id / access_token — running without live data")
            self._loaded = True
            return False

        try:
            from fyers_apiv3 import fyersModel
            self._client = fyersModel.FyersModel(
                client_id=app_id,
                token=access_token,
                log_path="/tmp/",
                is_async=False,
            )
            self._app_id = app_id
            self._loaded = True
            logger.info("Fyers client initialised with app_id=%s", app_id)
            return True
        except Exception as e:
            logger.warning("Fyers client init failed: %s", e)
            self._loaded = True
            return False

    def reload(self) -> bool:
        """Force reload — call after credentials are updated in admin."""
        with self._lock:
            self._loaded = False
            self._client = None
        return self._try_load()

    def inject_token(self, app_id: str, access_token: str) -> bool:
        """Inject a valid token from a broker adapter session.

        Called by FyersAdapter._build_client() so the global data client
        always uses the freshest token.
        """
        try:
            from fyers_apiv3 import fyersModel
            with self._lock:
                self._client = fyersModel.FyersModel(
                    client_id=app_id,
                    token=access_token,
                    log_path="/tmp/",
                    is_async=False,
                )
                self._app_id = app_id
                self._loaded = True
            logger.info("Fyers data client updated with injected token (app=%s)", app_id)
            return True
        except Exception as e:
            logger.warning("Failed to inject Fyers token into data client: %s", e)
            return False

    @property
    def is_live(self) -> bool:
        """True when the Fyers client is available."""
        with self._lock:
            if not self._loaded:
                self._try_load()
            return self._client is not None

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _get(self) -> Optional[Any]:
        """Return the fyers model, loading lazily."""
        with self._lock:
            if not self._loaded:
                self._try_load()
            return self._client

    _refresh_attempted = False          # one-shot guard per process

    def _try_refresh_from_broker(self) -> bool:
        """Grab a fresh Fyers token from any active broker session.

        Called automatically when a history/quotes call gets an auth error.
        Returns True if a new token was successfully injected.
        """
        if self._refresh_attempted:
            return False                # don't loop
        self._refresh_attempted = True
        try:
            from broker.multi_broker import registry
            # _sessions is {user_id: {config_id: BrokerAccountSession}}
            for _uid, user_map in list(getattr(registry, '_sessions', {}).items()):
                for _cid, sess in list(user_map.items()):
                    if getattr(sess, 'broker_id', '') != 'fyers':
                        continue
                    adapter = getattr(sess, '_adapter', None)
                    if adapter is None:
                        continue
                    client = getattr(adapter, '_client', None)
                    if client is None:
                        continue
                    # Extract app_id and token from the adapter's FyersModel
                    app_id = getattr(client, 'client_id', '') or getattr(adapter, '_app_id', '')
                    token  = getattr(client, 'token', '')
                    if app_id and token:
                        ok = self.inject_token(app_id, token)
                        if ok:
                            logger.info("Refreshed Fyers data client token from broker session")
                            return True
        except Exception as e:
            logger.debug("_try_refresh_from_broker failed: %s", e)
        return False

    def _quotes(self, symbols: List[str]) -> List[Dict]:
        """Call fyers.quotes for a list of symbols; returns list of 'v' dicts."""
        f = self._get()
        if not f:
            return []
        try:
            sym_str = ",".join(symbols)
            resp = f.quotes({"symbols": sym_str})
            if resp.get("code") != 200:
                return []
            results = []
            for item in resp.get("d", []):
                v = item.get("v", {})
                if v.get("s") == "error" or v.get("code", 0) == -300:
                    continue
                v["_sym"] = item.get("n", "")
                results.append(v)
            return results
        except Exception as e:
            logger.debug("Fyers quotes error: %s", e)
            return []

    # ── Public data methods ────────────────────────────────────────────────────

    def get_indices(self) -> List[Dict]:
        """Return major Indian indices."""
        fyers_syms = list(_INDEX_SYMBOLS.keys())
        raw = self._quotes(fyers_syms)
        out = []
        for v in raw:
            sym = _INDEX_SYMBOLS.get(v["_sym"], v.get("short_name", v["_sym"]))
            out.append({
                "symbol":    sym,
                "token":     v["_sym"],
                "exchange":  v.get("exchange", "NSE"),
                "ltp":       v.get("lp", 0),
                "change":    v.get("ch", 0),
                "changePct": v.get("chp", 0),
                "open":      v.get("open_price", 0),
                "high":      v.get("high_price", 0),
                "low":       v.get("low_price", 0),
                "close":     v.get("prev_close_price", 0),
                "volume":    v.get("volume", 0),
                "advances":  0,
                "declines":  0,
                "source":    "fyers",
            })
        return out

    def get_global_markets(self) -> List[Dict]:
        """Return global market data: world indices, commodities, forex."""
        m = _near_month_suffix()  # e.g. "26APR"

        world_indices = [
            # Commodities (MCX)
            {"sym": f"MCX:GOLD{m}FUT",        "name": "Gold",          "category": "commodity", "unit": "₹/10g"},
            {"sym": f"MCX:GOLDM{m}FUT",       "name": "Gold Mini",     "category": "commodity", "unit": "₹/10g"},
            {"sym": f"MCX:SILVER{m}FUT",       "name": "Silver",        "category": "commodity", "unit": "₹/kg"},
            {"sym": f"MCX:CRUDEOIL{m}FUT",     "name": "Crude Oil",     "category": "commodity", "unit": "₹/bbl"},
            {"sym": f"MCX:NATURALGAS{m}FUT",   "name": "Natural Gas",   "category": "commodity", "unit": "₹/mmbtu"},
            {"sym": f"MCX:COPPER{m}FUT",       "name": "Copper",        "category": "commodity", "unit": "₹/kg"},
            {"sym": f"MCX:ALUMINIUM{m}FUT",    "name": "Aluminium",     "category": "commodity", "unit": "₹/kg"},
            # Currency Futures (NSE-CDS)
            {"sym": f"NSE:USDINR{m}FUT",       "name": "USD/INR",       "category": "forex",     "unit": "₹"},
            {"sym": f"NSE:EURINR{m}FUT",       "name": "EUR/INR",       "category": "forex",     "unit": "₹"},
            {"sym": f"NSE:GBPINR{m}FUT",       "name": "GBP/INR",       "category": "forex",     "unit": "₹"},
            {"sym": f"NSE:JPYINR{m}FUT",       "name": "JPY/INR (×100)","category": "forex",     "unit": "₹"},
        ]

        # Next month for silver (if near-month is Apr, silver uses May)
        now = datetime.now()
        next_m = datetime(now.year if now.month < 12 else now.year + 1,
                          now.month + 1 if now.month < 12 else 1, 1).strftime("%y%b").upper()
        for item in world_indices:
            if "SILVER" in item["sym"] and item["sym"].endswith(f"{m}FUT"):
                item["sym"] = f"MCX:SILVER{next_m}FUT"

        fyers_syms = [i["sym"] for i in world_indices]
        raw = self._quotes(fyers_syms)
        raw_map = {v["_sym"]: v for v in raw}

        out = []
        for item in world_indices:
            v = raw_map.get(item["sym"])
            if not v:
                continue
            out.append({
                "symbol":    item["sym"],
                "name":      item["name"],
                "category":  item["category"],
                "unit":      item["unit"],
                "ltp":       v.get("lp", 0),
                "change":    v.get("ch", 0),
                "changePct": v.get("chp", 0),
                "open":      v.get("open_price", 0),
                "high":      v.get("high_price", 0),
                "low":       v.get("low_price", 0),
                "close":     v.get("prev_close_price", 0),
                "volume":    v.get("volume", 0),
                "source":    "fyers",
            })
        return out

    def get_quote(self, fyers_symbol: str) -> Optional[Dict]:
        """Get a single quote by Fyers symbol (e.g. 'NSE:RELIANCE-EQ')."""
        items = self._quotes([fyers_symbol])
        if not items:
            return None
        v = items[0]
        sym = fyers_symbol.split(":")[-1]
        return {
            "symbol":    sym,
            "ltp":       v.get("lp", 0),
            "change":    v.get("ch", 0),
            "changePct": v.get("chp", 0),
            "open":      v.get("open_price", 0),
            "high":      v.get("high_price", 0),
            "low":       v.get("low_price", 0),
            "close":     v.get("prev_close_price", 0),
            "volume":    v.get("volume", 0),
            "source":    "fyers",
        }

    def get_depth(self, symbol: str, exchange: str = "NSE") -> Optional[Dict]:
        """Get market depth (best 5 bid/ask levels) via Fyers depth API."""
        f = self._get()
        if not f:
            return None
        # Build Fyers symbol
        fyers_sym = OC_SYMBOL_MAP.get(symbol.upper(), None)
        if not fyers_sym:
            inst = None
            try:
                from broker.symbol_normalizer import lookup_by_trading_symbol
                inst = lookup_by_trading_symbol(symbol.upper())
                if inst and inst.fyers_symbol:
                    fyers_sym = inst.fyers_symbol
            except Exception:
                pass
            if not fyers_sym:
                if exchange in ("NFO", "MCX", "BFO"):
                    fyers_sym = f"{exchange}:{symbol.upper().replace(' ', '')}"
                else:
                    fyers_sym = f"{exchange}:{symbol.upper().replace(' ', '')}-EQ"
        try:
            resp = f.depth({"symbol": fyers_sym, "ohlcv_flag": 1})
            if resp.get("code") != 200:
                logger.debug("Fyers depth error: %s", resp)
                return None
            d = resp.get("d", {}).get(fyers_sym, resp.get("d", {}))
            # Normalize the response
            bids = []
            asks = []
            for b in d.get("bids", []):
                bids.append({"price": b.get("price", 0), "qty": b.get("volume", b.get("quantity", 0)), "orders": b.get("number", 0)})
            for a in d.get("ask", d.get("asks", [])):
                asks.append({"price": a.get("price", 0), "qty": a.get("volume", a.get("quantity", 0)), "orders": a.get("number", 0)})
            return {
                "symbol": symbol.upper(),
                "exchange": exchange,
                "bids": bids,
                "asks": asks,
                "ltp": d.get("lp", d.get("ltp", 0)),
                "open": d.get("open_price", d.get("open", 0)),
                "high": d.get("high_price", d.get("high", 0)),
                "low": d.get("low_price", d.get("low", 0)),
                "close": d.get("prev_close_price", d.get("close", 0)),
                "volume": d.get("volume", d.get("vol_traded_today", 0)),
                "oi": d.get("oi", 0),
                "total_buy_qty": d.get("totalbuyqty", d.get("total_buy_qty", 0)),
                "total_sell_qty": d.get("totalsellqty", d.get("total_sell_qty", 0)),
                "upper_circuit": d.get("upper_ckt", d.get("upper_circuit", 0)),
                "lower_circuit": d.get("lower_ckt", d.get("lower_circuit", 0)),
            }
        except Exception as e:
            logger.debug("Fyers depth error: %s", e)
            return None

    def get_option_chain(self, underlying: str, expiry_ts: str = "", exchange: str = "NFO") -> Optional[Dict]:
        """
        Fetch option chain from Fyers.
        underlying: e.g. "NIFTY", "BANKNIFTY", "CRUDEOIL", "RELIANCE"
        expiry_ts:  Fyers timestamp string; empty = nearest expiry
        exchange:   "NFO", "MCX", "BFO" — used to resolve the Fyers symbol
        """
        f = self._get()
        if not f:
            return None
        candidates = _resolve_oc_symbol(underlying, exchange)
        for fyers_sym in candidates:
            try:
                resp = f.optionchain({
                    "symbol":      fyers_sym,
                    "strikecount": 10,
                    "timestamp":   expiry_ts,
                })
                if resp.get("code") == 200 and resp.get("data", {}).get("optionsChain"):
                    return resp["data"]
                logger.debug("Fyers OC %s: code=%s", fyers_sym, resp.get("code"))
            except Exception as e:
                logger.debug("Fyers get_option_chain %s error: %s", fyers_sym, e)
        return None

    def get_screener(self, symbols: List[str]) -> List[Dict]:
        """Batch fetch quotes for a list of NSE symbols and return screener rows."""
        fyers_syms = [f"NSE:{s}-EQ" for s in symbols]
        # Fyers allows max 50 symbols per call
        rows = []
        for i in range(0, len(fyers_syms), 50):
            batch = fyers_syms[i:i+50]
            rows.extend(self._quotes(batch))
        out = []
        for v in rows:
            sym_raw = v["_sym"].replace("NSE:", "").replace("-EQ", "")
            out.append({
                "symbol":    sym_raw,
                "name":      sym_raw,
                "tradingsymbol": sym_raw,
                "exchange":  "NSE",
                "ltp":       v.get("lp", 0),
                "change":    v.get("ch", 0),
                "changePct": v.get("chp", 0),
                "volume":    v.get("volume", 0),
                "open":      v.get("open_price", 0),
                "high":      v.get("high_price", 0),
                "low":       v.get("low_price", 0),
                "source":    "fyers",
            })
        return out

    # ── Historical candle data ─────────────────────────────────────────────────

    _TF_RESOLUTION = {
        "1m": "1", "3m": "3", "5m": "5", "10m": "10",
        "15m": "15", "30m": "30", "1h": "60", "2h": "120",
        "4h": "240", "D": "1D", "W": "7D", "M": "30D",
    }

    def get_history(
        self,
        fyers_symbol: str,
        resolution: str = "5m",
        days_back: int = 30,
    ) -> List[Dict]:
        """
        Fetch OHLCV candle history from Fyers.

        Args:
            fyers_symbol: e.g. "NSE:NIFTY50-INDEX", "NSE:RELIANCE-EQ"
            resolution:   "1m" | "5m" | "15m" | "30m" | "1h" | "D" etc.
            days_back:    number of calendar days to look back

        Returns:
            List of {time, open, high, low, close, volume} dicts (ascending).
        """
        f = self._get()
        if not f:
            return []

        fyres_res = self._TF_RESOLUTION.get(resolution, "5")
        from datetime import datetime, timedelta
        now = datetime.now()
        range_from = (now - timedelta(days=days_back)).strftime("%Y-%m-%d")
        range_to = now.strftime("%Y-%m-%d")

        try:
            resp = f.history({
                "symbol":      fyers_symbol,
                "resolution":  fyres_res,
                "date_format":  1,
                "range_from":  range_from,
                "range_to":    range_to,
                "cont_flag":   1,
            })
            if resp.get("code") != 200 or resp.get("s") != "ok":
                logger.debug("Fyers history error: %s", resp.get("message", resp))
                # Auth failure → try refreshing token from active broker session
                if resp.get("code") in (-16, -17):
                    if self._try_refresh_from_broker():
                        return self.get_history(fyers_symbol, resolution, days_back)
                return []
            candles_raw = resp.get("candles", [])
            # Fyers returns: [[epoch, O, H, L, C, V], ...]
            candles = []
            for c in candles_raw:
                if len(c) >= 6:
                    candles.append({
                        "time":   int(c[0]),
                        "open":   c[1],
                        "high":   c[2],
                        "low":    c[3],
                        "close":  c[4],
                        "volume": int(c[5]),
                    })
            return candles
        except Exception as e:
            logger.debug("Fyers get_history error: %s", e)
            return []


# ── Singleton ──────────────────────────────────────────────────────────────────
_fyers_client: Optional[FyersDataClient] = None
_fyers_lock   = threading.Lock()


def get_fyers_client() -> FyersDataClient:
    """Return the singleton FyersDataClient, creating it on first call."""
    global _fyers_client
    with _fyers_lock:
        if _fyers_client is None:
            _fyers_client = FyersDataClient()
    return _fyers_client
