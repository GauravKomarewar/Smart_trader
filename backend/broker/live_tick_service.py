"""
Smart Trader — Live Tick Service
==================================
Central hub for all real-time market data.

Architecture:
  • Fyers WebSocket  (primary)   — `FyersDataSocket` from fyers_apiv3
  • Shoonya WebSocket (parallel) — `NorenApi.start_websocket()` via shoonya_client

Both pipelines push ticks into a shared in-memory store AND persist to
the `market_ticks` PostgreSQL table.  A 1-minute OHLCV aggregator runs
as a background thread and updates `market_ohlcv`.

Broadcast:
  Registered async callbacks (added by the WebSocket router) receive each
  tick so connected frontend clients get real-time updates.

Usage:
  from broker.live_tick_service import tick_service
  tick_service.subscribe(["NSE:NIFTY50-INDEX", "NSE:RELIANCE-EQ"])
  tick_service.add_broadcast_callback(async_callback)
"""
from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Set

logger = logging.getLogger("smart_trader.live_tick")


def _normalize_sym(s: str) -> str:
    """Normalize a symbol for comparison: strip exchange prefix, -EQ/-INDEX suffixes and spaces."""
    s = str(s).upper().split(":")[-1]
    for sfx in ("-INDEX", "-EQ", "-BE", "-SM", "-IL"):
        if s.endswith(sfx):
            s = s[: -len(sfx)]
    return s.replace(" ", "")


import re as _re

# MCX commodity keywords for exchange detection
_MCX_KEYWORDS = {"GOLD", "SILVER", "CRUDE", "NATURALGAS", "COPPER", "ZINC", "LEAD",
                 "ALUMINIUM", "NICKEL", "GOLDPETAL", "SILVERMIC", "GOLDGUINEA",
                 "CRUDEOIL", "NGAS", "COTTONCNDY", "MENTHAOIL"}


def _detect_exchange_from_symbol(sym: str) -> str:
    """Detect exchange from symbol pattern. Used as fallback when ScriptMaster lookup fails."""
    upper = sym.upper().strip()
    # Options: digits followed by CE or PE
    if _re.search(r'\d{3,}(CE|PE)$', upper):
        # MCX options
        for kw in _MCX_KEYWORDS:
            if upper.startswith(kw):
                return "MCX"
        return "NFO"
    # Futures: end with FUT
    if _re.search(r'\d+FUT$', upper):
        for kw in _MCX_KEYWORDS:
            if upper.startswith(kw):
                return "MCX"
        return "NFO"
    # MCX commodity spot names
    for kw in _MCX_KEYWORDS:
        if upper.startswith(kw):
            return "MCX"
    # BSE instruments
    if upper.startswith("SENSEX"):
        return "BFO"
    return "NSE"


# ── Tick schema ────────────────────────────────────────────────────────────────

def _norm_tick(
    symbol: str,
    ltp: float,
    *,
    exchange: str = "NSE",
    token: str = "",
    bid: float = 0.0,
    ask: float = 0.0,
    volume: int = 0,
    oi: int = 0,
    open_price: float = 0.0,
    high_price: float = 0.0,
    low_price: float = 0.0,
    close_price: float = 0.0,
    tick_time: Optional[datetime] = None,
    source: str = "",
) -> dict:
    if tick_time is None:
        tick_time = datetime.now(timezone.utc)
    return {
        "symbol":      symbol,
        "exchange":    exchange,
        "token":       token,
        "ltp":         float(ltp),
        "bid":         float(bid),
        "ask":         float(ask),
        "volume":      int(volume),
        "oi":          int(oi),
        "open":        float(open_price),
        "high":        float(high_price),
        "low":         float(low_price),
        "close":       float(close_price),
        "change":      float(ltp) - float(close_price),
        "changePct":   ((float(ltp) - float(close_price)) / float(close_price) * 100)
                       if close_price else 0.0,
        "tick_time":   tick_time.isoformat(),
        "source":      source,
    }


# ── OHLCV aggregator ───────────────────────────────────────────────────────────

class OhlcvAggregator:
    """
    In-memory 1-minute OHLCV bar builder.
    Flushes closed bars to PostgreSQL `market_ohlcv`.
    """

    def __init__(self) -> None:
        # symbol → {"bar_time": dt, "open":, "high":, "low":, "close":, "volume":, "oi":}
        self._bars: Dict[str, dict] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _bar_start(ts: datetime) -> datetime:
        """Truncate to 1-minute boundary (UTC)."""
        return ts.replace(second=0, microsecond=0)

    def push(self, tick: dict) -> None:
        sym = tick["symbol"]
        ltp = tick["ltp"]
        vol = tick["volume"]
        oi  = tick["oi"]
        try:
            ts = datetime.fromisoformat(tick["tick_time"].replace("Z", "+00:00"))
        except Exception:
            ts = datetime.now(timezone.utc)
        bar_start = self._bar_start(ts)

        with self._lock:
            cur = self._bars.get(sym)
            if cur is None or cur["bar_time"] != bar_start:
                if cur is not None:
                    self._flush_bar(sym, cur)
                self._bars[sym] = {
                    "symbol":    sym,
                    "exchange":  tick.get("exchange", "NSE"),
                    "bar_time":  bar_start,
                    "open":      ltp,
                    "high":      ltp,
                    "low":       ltp,
                    "close":     ltp,
                    "volume":    vol,
                    "oi":        oi,
                }
            else:
                cur["high"]   = max(cur["high"], ltp)
                cur["low"]    = min(cur["low"],  ltp)
                cur["close"]  = ltp
                cur["volume"] += vol
                cur["oi"]     = oi

    def _flush_bar(self, sym: str, bar: dict) -> None:
        """Upsert a completed bar into market_ohlcv."""
        try:
            from db.trading_db import trading_cursor
            with trading_cursor() as cur:
                cur.execute("""
                    INSERT INTO market_ohlcv
                        (symbol, exchange, timeframe, bar_time, open, high, low, close, volume, oi)
                    VALUES (%s,%s,'1m',%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (symbol, exchange, timeframe, bar_time) DO UPDATE SET
                        high   = GREATEST(market_ohlcv.high,   EXCLUDED.high),
                        low    = LEAST   (market_ohlcv.low,    EXCLUDED.low),
                        close  = EXCLUDED.close,
                        volume = market_ohlcv.volume + EXCLUDED.volume,
                        oi     = EXCLUDED.oi
                """, (
                    bar["symbol"], bar["exchange"], bar["bar_time"],
                    bar["open"], bar["high"], bar["low"], bar["close"],
                    bar["volume"], bar["oi"],
                ))
        except Exception as e:
            logger.debug("OHLCV flush error for %s: %s", sym, e)


_ohlcv_agg = OhlcvAggregator()


# ── Tick persistence ───────────────────────────────────────────────────────────

_TICK_BUFFER: List[dict] = []
_TICK_BUFFER_LOCK = threading.Lock()
_TICK_FLUSH_INTERVAL = 5  # seconds


def _buffer_tick(tick: dict) -> None:
    with _TICK_BUFFER_LOCK:
        _TICK_BUFFER.append(tick)


def _flush_ticks() -> None:
    with _TICK_BUFFER_LOCK:
        if not _TICK_BUFFER:
            return
        batch = _TICK_BUFFER[:]
        _TICK_BUFFER.clear()

    try:
        from db.trading_db import trading_cursor
        with trading_cursor() as cur:
            cur.executemany("""
                INSERT INTO market_ticks
                    (symbol, exchange, token, ltp, bid, ask,
                     volume, oi, open_price, high_price, low_price, close_price,
                     tick_time, source)
                VALUES (%(symbol)s, %(exchange)s, %(token)s, %(ltp)s, %(bid)s, %(ask)s,
                        %(volume)s, %(oi)s, %(open)s, %(high)s, %(low)s, %(close)s,
                        %(tick_time)s, %(source)s)
            """, batch)
    except Exception as e:
        logger.debug("Tick DB flush error: %s", e)


def _tick_flush_loop() -> None:
    while True:
        time.sleep(_TICK_FLUSH_INTERVAL)
        try:
            _flush_ticks()
        except Exception as e:
            logger.debug("Tick flush loop error: %s", e)


# ── LiveTickService ────────────────────────────────────────────────────────────

class LiveTickService:
    """
    Manages both Fyers and Shoonya WebSocket subscriptions.
    Dispatches ticks to registered async broadcast callbacks.
    """

    def __init__(self) -> None:
        self._subscribed: Set[str] = set()      # canonical "EXCHANGE:SYMBOL" keys
        self._tick_store: Dict[str, dict] = {}  # latest tick per symbol
        self._callbacks: List[Callable]  = []   # async broadcast callbacks
        self._lock = threading.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        # Map normalized symbol → original subscribed symbol (e.g. "NIFTY50" → "NIFTY 50")
        self._sym_alias: Dict[str, str] = {}

        # Fyers WS
        self._fyers_ws  = None
        self._fyers_running = False
        self._fyers_ws_fail_time: float = 0   # monotonic time of last WS auth failure
        self._FYERS_WS_RETRY_SECS = 30        # retry WS init after this many seconds

        # Shoonya WS
        self._shoonya_ws_running = False

        # Background flush thread
        self._flush_thread = threading.Thread(
            target=_tick_flush_loop, daemon=True, name="tick-flush"
        )
        self._flush_thread.start()
        logger.info("LiveTickService initialised")

    # ── Public API ─────────────────────────────────────────────────────────────

    def set_event_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def add_broadcast_callback(self, cb: Callable) -> None:
        with self._lock:
            if cb not in self._callbacks:
                self._callbacks.append(cb)

    def remove_broadcast_callback(self, cb: Callable) -> None:
        with self._lock:
            self._callbacks = [c for c in self._callbacks if c is not cb]

    def subscribe(self, symbols: List[str]) -> None:
        """
        Subscribe to real-time ticks for a list of symbols.
        Symbols may be in any of these formats:
          • "NSE:NIFTY50-INDEX"  (Fyers format)
          • "NIFTY"              (plain)
          • "NSE:RELIANCE-EQ"
        """
        new_syms = [s for s in symbols if s not in self._subscribed]
        if not new_syms:
            return
        with self._lock:
            self._subscribed.update(new_syms)
            for s in new_syms:
                self._sym_alias[_normalize_sym(s)] = s
        logger.info("Subscribing %d new symbols: %s", len(new_syms), new_syms[:5])
        self._subscribe_fyers(new_syms)
        self._subscribe_shoonya(new_syms)

    def unsubscribe(self, symbols: List[str]) -> None:
        with self._lock:
            for s in symbols:
                self._subscribed.discard(s)

    def get_latest(self, symbol: str) -> Optional[dict]:
        # Try exact match first, then normalized alias lookup
        tick = self._tick_store.get(symbol)
        if tick:
            return tick
        norm = _normalize_sym(symbol)
        aliased = self._sym_alias.get(norm, norm)
        return self._tick_store.get(aliased) or self._tick_store.get(norm)

    def get_all_latest(self) -> Dict[str, dict]:
        return dict(self._tick_store)

    # ── Internal tick handler ──────────────────────────────────────────────────

    def _on_tick(self, tick: dict) -> None:
        """Called from any thread when a tick arrives (Fyers or Shoonya)."""
        sym = tick.get("symbol", "")
        if not sym:
            return
        self._tick_store[sym] = tick
        _ohlcv_agg.push(tick)
        _buffer_tick(tick)
        self._broadcast(tick)

    def _broadcast(self, tick: dict) -> None:
        """Schedule broadcast to all WS clients from the event loop."""
        if not self._callbacks:
            return
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        msg = {"type": "tick", "data": tick}
        for cb in list(self._callbacks):
            try:
                asyncio.run_coroutine_threadsafe(cb(msg), loop)
            except Exception as e:
                logger.debug("Broadcast error: %s", e)

    # ── Fyers WebSocket ────────────────────────────────────────────────────────

    def _subscribe_fyers(self, symbols: List[str]) -> None:
        """Subscribe given symbols via Fyers WebSocket."""
        thread = threading.Thread(
            target=self._fyers_subscribe_thread,
            args=(symbols,),
            daemon=True,
            name="fyers-ws",
        )
        thread.start()

    def _fyers_subscribe_thread(self, symbols: List[str]) -> None:
        try:
            self._init_fyers_ws()
            if self._fyers_ws is None:
                return
            # Symbols must be in Fyers format e.g. "NSE:RELIANCE-EQ"
            fyers_syms = [self._to_fyers_sym(s) for s in symbols]
            fyers_syms = [s for s in fyers_syms if s]
            if fyers_syms:
                self._fyers_ws.subscribe(symbols=fyers_syms, data_type="SymbolUpdate")
                logger.info("Fyers WS subscribed: %s", fyers_syms[:5])
        except Exception as e:
            logger.warning("Fyers WS subscribe error: %s", e)

    def _init_fyers_ws(self) -> None:
        if self._fyers_ws is not None:
            return
        # Don't retry too quickly after a failure
        if self._fyers_ws_fail_time and (time.monotonic() - self._fyers_ws_fail_time) < self._FYERS_WS_RETRY_SECS:
            return
        try:
            from broker.fyers_client import get_fyers_client
            from core.config import config
            import json
            from pathlib import Path

            fc = get_fyers_client()
            if not fc.is_live:
                logger.info("Fyers not live — skipping WS init")
                return

            # Load access token
            token_paths = [
                Path(config.FYERS_TOKEN_FILE),
                Path("/home/ubuntu/option_trading_system_fyers/fyers_token.json"),
            ]
            access_token = ""
            app_id = config.FYERS_APP_ID
            for tp in token_paths:
                if tp.exists():
                    data = json.loads(tp.read_text())
                    access_token = data.get("access_token", "")
                    if not app_id:
                        creds = tp.parent / "config" / "credentials.env"
                        if creds.exists():
                            for line in creds.read_text().splitlines():
                                if line.startswith("APP_ID="):
                                    app_id = line.split("=", 1)[1].strip()
                    if access_token:
                        break

            if not access_token or not app_id:
                logger.warning("Fyers: cannot init WS — no access token")
                return

            from fyers_apiv3.FyersWebsocket.data_ws import FyersDataSocket

            def _on_ws_error(e):
                err_str = str(e).lower()
                logger.warning("Fyers WS error: %s", e)
                # Detect auth failures → reset WS so next subscribe re-inits with fresh token
                if any(kw in err_str for kw in ("token", "expired", "unauthorized", "auth", "invalid")):
                    logger.warning("Fyers WS auth failure detected — resetting for re-init")
                    self._fyers_ws = None
                    self._fyers_ws_fail_time = time.monotonic()

            self._fyers_ws = FyersDataSocket(
                access_token=f"{app_id}:{access_token}",
                write_to_file=False,
                log_path="/tmp/",
                litemode=True,
                reconnect=True,
                on_message=self._on_fyers_message,
                on_error=_on_ws_error,
                on_connect=lambda: logger.info("Fyers WS connected"),
                on_close=lambda msg: logger.info("Fyers WS closed: %s", msg),
                reconnect_retry=10,
            )
            self._fyers_ws.connect()
            self._fyers_ws_fail_time = 0  # reset on successful init
            logger.info("Fyers WebSocket initialised")
        except Exception as e:
            logger.warning("Fyers WS init failed: %s", e)
            self._fyers_ws = None
            self._fyers_ws_fail_time = time.monotonic()

    def _on_fyers_message(self, msg: dict) -> None:
        """Handle incoming Fyers tick message."""
        try:
            if not isinstance(msg, dict):
                return
            # Fyers sends list of dicts in litemode
            if isinstance(msg, list):
                for item in msg:
                    self._on_fyers_message(item)
                return
            sym = msg.get("symbol", msg.get("n", ""))
            if not sym:
                return
            ltp = float(msg.get("ltp", msg.get("lp", 0)) or 0)
            if ltp <= 0:
                return
            parts = sym.split(":")
            exchange = parts[0] if len(parts) == 2 else "NSE"
            clean_sym = (parts[1] if len(parts) == 2 else sym).replace("-EQ", "").replace("-INDEX", "")
            # Map back to the user-subscribed symbol (e.g. "NIFTY50" → "NIFTY 50")
            norm = _normalize_sym(clean_sym)
            subscribed_sym = self._sym_alias.get(norm, clean_sym)
            tick = _norm_tick(
                subscribed_sym,
                ltp,
                exchange=exchange,
                bid=float(msg.get("bid", 0) or 0),
                ask=float(msg.get("ask", 0) or 0),
                volume=int(msg.get("vol_traded_today", msg.get("volume", 0)) or 0),
                oi=int(msg.get("oi", 0) or 0),
                open_price=float(msg.get("open_price", 0) or 0),
                high_price=float(msg.get("high_price", 0) or 0),
                low_price=float(msg.get("low_price", 0) or 0),
                close_price=float(msg.get("prev_close_price", msg.get("close_price", 0)) or 0),
                source="fyers",
            )
            # Store under multiple keys: Fyers key, clean_sym, and subscribed alias
            self._tick_store[sym] = tick          # "NSE:NIFTY50-INDEX"
            self._tick_store[clean_sym] = tick    # "NIFTY50"
            self._tick_store[subscribed_sym] = tick  # "NIFTY 50" (user-subscribed)
            self._on_tick(tick)
        except Exception as e:
            logger.debug("Fyers message parse error: %s", e)

    @staticmethod
    def _to_fyers_sym(symbol: str) -> str:
        """Convert a generic symbol to Fyers format.
        Handles indices, equities, options (CE/PE), and futures (FUT).
        """
        if ":" in symbol:
            return symbol  # Already in Fyers format
        upper = symbol.upper().replace(" ", "")
        # Indices
        _idx = {
            "NIFTY": "NSE:NIFTY50-INDEX",
            "NIFTY50": "NSE:NIFTY50-INDEX",
            "BANKNIFTY": "NSE:NIFTYBANK-INDEX",
            "NIFTYBANK": "NSE:NIFTYBANK-INDEX",
            "FINNIFTY": "NSE:FINNIFTY-INDEX",
            "MIDCPNIFTY": "NSE:MIDCPNIFTY-INDEX",
            "SENSEX": "BSE:SENSEX-INDEX",
        }
        # Also check original with spaces for "NIFTY 50", "NIFTY BANK"
        upper_orig = symbol.upper().strip()
        if upper in _idx:
            return _idx[upper]
        if upper_orig in {"NIFTY 50"}: return "NSE:NIFTY50-INDEX"
        if upper_orig in {"NIFTY BANK", "BANK NIFTY"}: return "NSE:NIFTYBANK-INDEX"
        if upper_orig in {"FIN NIFTY"}: return "NSE:FINNIFTY-INDEX"

        # Option symbols: contain CE or PE preceded by digits (e.g. NIFTY24APR22500CE, NIFTY2290030JUN26CE)
        import re
        if re.search(r'\d{3,}(CE|PE)', upper):
            return f"NFO:{upper}"
        # Futures: end with FUT and contain digits (e.g. NIFTY24APRFUT)
        if re.search(r'\d+FUT$', upper):
            return f"NFO:{upper}"
        # Try ScriptMaster lookup for accurate exchange detection
        try:
            from scripts.unified_scriptmaster import lookup_by_trading_symbol
            inst = lookup_by_trading_symbol(upper)
            if inst and inst.fyers_symbol:
                return inst.fyers_symbol
        except Exception:
            pass
        # Default: assume NSE equity
        return f"NSE:{upper}-EQ"

    # ── Shoonya WebSocket ──────────────────────────────────────────────────────

    def _subscribe_shoonya(self, symbols: List[str]) -> None:
        """Subscribe given symbols via Shoonya WebSocket (runs in a thread)."""
        thread = threading.Thread(
            target=self._shoonya_subscribe_thread,
            args=(symbols,),
            daemon=True,
            name="shoonya-ws",
        )
        thread.start()

    def _shoonya_subscribe_thread(self, symbols: List[str]) -> None:
        try:
            from broker.multi_broker import registry
            sessions = registry.get_all_sessions()
            shoonya_sessions = [s for s in sessions if s.is_live and hasattr(s, "_client") and s._client]
            if not shoonya_sessions:
                logger.debug("No live Shoonya sessions for WS subscription")
                return

            session = shoonya_sessions[0]
            tokens = self._resolve_shoonya_tokens(symbols)
            if not tokens:
                return

            session.subscribe_websocket(
                tokens,
                on_tick=self._on_shoonya_tick,
            )
            logger.info("Shoonya WS subscribed %d tokens", len(tokens))
        except Exception as e:
            logger.debug("Shoonya WS subscribe error: %s", e)

    def _on_shoonya_tick(self, tick_data: dict) -> None:
        """Handle incoming Shoonya tick."""
        try:
            if not isinstance(tick_data, dict):
                return
            # Shoonya format: {"t": "tf", "e": "NSE", "tk": "26000", "ts": "NIFTY",
            #                   "lp": "22500.00", "c": "22450.00", ...}
            t_type = tick_data.get("t", "")
            if t_type not in ("tf", "tk"):
                return
            sym  = tick_data.get("ts", tick_data.get("e", ""))
            ltp  = float(tick_data.get("lp", 0) or 0)
            if not sym or ltp <= 0:
                return
            exchange = tick_data.get("e", "NSE")
            # Map Shoonya symbol to the user-subscribed symbol
            norm = _normalize_sym(sym)
            subscribed_sym = self._sym_alias.get(norm)
            if subscribed_sym is None:
                # Try via Fyers format (e.g. "NIFTY" → "NSE:NIFTY50-INDEX" → "NIFTY50")
                fyers_parts = self._to_fyers_sym(sym).split(":")
                if len(fyers_parts) == 2:
                    fyers_clean = _normalize_sym(fyers_parts[1])
                    subscribed_sym = self._sym_alias.get(fyers_clean, sym)
                else:
                    subscribed_sym = sym
            tick = _norm_tick(
                subscribed_sym,
                ltp,
                exchange=exchange,
                token=tick_data.get("tk", ""),
                volume=int(tick_data.get("v", 0) or 0),
                oi=int(tick_data.get("oi", 0) or 0),
                open_price=float(tick_data.get("o", 0) or 0),
                high_price=float(tick_data.get("h", 0) or 0),
                low_price=float(tick_data.get("lo", 0) or 0),
                close_price=float(tick_data.get("c", 0) or 0),
                source="shoonya",
            )
            # Only broadcast if Fyers didn't already handle this symbol
            # (deduplication: prefer Fyers; use Shoonya as fallback)
            fyers_key = self._to_fyers_sym(sym)
            if fyers_key not in self._tick_store:
                self._on_tick(tick)
            else:
                # Still buffer for DB persistence (secondary source)
                _buffer_tick(tick)
        except Exception as e:
            logger.debug("Shoonya tick parse error: %s", e)

    @staticmethod
    def _resolve_shoonya_tokens(symbols: List[str]) -> List[str]:
        """
        Map symbol names to Shoonya subscriber tokens ("EXCHANGE|TOKEN").
        Falls back to detecting exchange from symbol pattern if lookup fails.
        """
        import re
        tokens: List[str] = []
        try:
            from broker.symbol_normalizer import lookup_by_trading_symbol
            for sym in symbols:
                clean = sym.split(":")[-1].replace("-EQ", "").replace("-INDEX", "")
                inst = lookup_by_trading_symbol(clean)
                if inst and inst.token:
                    tokens.append(f"{inst.exchange}|{inst.token}")
                else:
                    # Detect exchange from symbol pattern
                    exch = _detect_exchange_from_symbol(clean)
                    tokens.append(f"{exch}|{clean}")
        except Exception as e:
            logger.debug("Token resolution error: %s", e)
            for sym in symbols:
                clean = sym.split(":")[-1].replace("-EQ", "").replace("-INDEX", "")
                exch = _detect_exchange_from_symbol(clean)
                tokens.append(f"{exch}|{clean}")
        return tokens


# ── Singleton ──────────────────────────────────────────────────────────────────

_service: Optional[LiveTickService] = None
_service_lock = threading.Lock()


def get_tick_service() -> LiveTickService:
    global _service
    with _service_lock:
        if _service is None:
            _service = LiveTickService()
    return _service
