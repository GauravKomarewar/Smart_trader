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

    def get_option_chain(self, underlying: str, expiry_ts: str = "") -> Optional[Dict]:
        """
        Fetch option chain from Fyers.
        underlying: e.g. "NIFTY", "BANKNIFTY"
        expiry_ts:  Fyers timestamp string; empty = nearest expiry
        """
        f = self._get()
        if not f:
            return None
        fyers_sym = OC_SYMBOL_MAP.get(underlying.upper())
        if not fyers_sym:
            return None
        try:
            resp = f.optionchain({
                "symbol":      fyers_sym,
                "strikecount": 10,
                "timestamp":   expiry_ts,
            })
            if resp.get("code") != 200:
                logger.debug("Fyers OC error: %s", resp)
                return None
            return resp["data"]
        except Exception as e:
            logger.debug("Fyers get_option_chain error: %s", e)
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
