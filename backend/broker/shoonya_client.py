"""
Smart Trader — Shoonya Broker Client Wrapper
============================================

Self-contained Shoonya broker integration for Smart Trader.
No external project dependencies — uses local oauth_login + NorenApi.

Architecture:
  - Uses broker.oauth_login for daily SEBI-compliant OAuth token
  - Uses NorenRestApiPy (NorenApi) for REST + WebSocket
  - Falls back to DEMO mode when credentials are not configured
  - Thread-safe singleton per session
"""

import os
import logging
import threading
import time
from typing import Optional, Any
from pathlib import Path

logger = logging.getLogger("smart_trader.broker")

# ── Import local OAuth login ─────────────────────────────────────────────────
try:
    from broker.oauth_login import run_oauth_login
    _SHOONYA_AVAILABLE = True
    logger.info("Smart Trader OAuth login module loaded")
except ImportError as e:
    _SHOONYA_AVAILABLE = False
    run_oauth_login = None  # type: ignore[assignment]
    logger.warning("OAuth login not available: %s — running in DEMO mode", e)

try:
    from NorenRestApiPy.NorenApi import NorenApi  # type: ignore[import]
    _NOREN_AVAILABLE = True
except ImportError:
    _NOREN_AVAILABLE = False
    logger.warning("NorenRestApiPy not installed — live trading disabled")


# ── Minimal config adapter for ShoonyaClient ─────────────────────────────────
# ShoonyaClient(config) only reads config.shoonya_host + config.shoonya_websocket
# in __init__, and config.get_shoonya_credentials() in login().
# This adapter lets us construct ShoonyaClient without needing a real .env file.
class _MinimalShoonyaConfig:
    """Duck-type compatible with shoonya_platform.core.config.Config."""

    def __init__(
        self,
        user_id: str = "",
        password: str = "",
        totp_key: str = "",
        vendor_code: str = "",
        api_secret: str = "",
        imei: str = "mac",
        host: str = "https://trade.shoonya.com/NorenWClientAPI/",
        websocket: str = "wss://trade.shoonya.com/NorenWSTP/",
    ):
        self.user_id = user_id
        self.password = password
        self.totp_key = totp_key
        self.vendor_code = vendor_code
        self.api_secret = api_secret
        self.imei = imei
        self.shoonya_host = host
        self.shoonya_websocket = websocket
        # ShoonyaClient.get_positions() checks getattr(self._config, "bot", None)
        self.bot = None

    def is_shoonya_configured(self) -> bool:
        """Duck-type: always True because credentials were explicitly provided."""
        return bool(self.user_id)

    def get_shoonya_credentials(self) -> dict:
        return {
            "user_id": self.user_id,
            "password": self.password,
            "totp_key": self.totp_key,
            "vendor_code": self.vendor_code,
            "api_secret": self.api_secret,
            "imei": self.imei,
            "host": self.shoonya_host,
            "websocket": self.shoonya_websocket,
        }


# ── Demo market data ─────────────────────────────────────────────────────────
import random

def _make_demo_quote(symbol: str) -> dict:
    base_prices = {
        "NIFTY": 23500, "BANKNIFTY": 51000, "FINNIFTY": 22300,
        "MIDCPNIFTY": 11500, "SENSEX": 76000,
    }
    base = base_prices.get(symbol, 1000 + hash(symbol) % 9000)
    ltp = base + random.uniform(-base * 0.01, base * 0.01)
    prev = ltp - random.uniform(-base * 0.005, base * 0.005)
    chg_pct = ((ltp - prev) / prev) * 100
    return {
        "symbol": symbol,
        "ltp": round(ltp, 2),
        "close": round(prev, 2),
        "change": round(ltp - prev, 2),
        "changePct": round(chg_pct, 4),
        "open": round(base + random.uniform(-50, 50), 2),
        "high": round(ltp + random.uniform(0, base * 0.005), 2),
        "low":  round(ltp - random.uniform(0, base * 0.005), 2),
        "volume": random.randint(1_000_000, 50_000_000),
        "oi": 0,
        "source": "demo",
    }


# ── BrokerSession: manages the Shoonya connection ────────────────────────────
class BrokerSession:
    """
    Thread-safe Shoonya session manager.

    Lifecycle:
        1. login()      — runs OAuth via Selenium (or reads cached token)
        2. subscribe()  — starts WebSocket + registers tick handler
        3. get_ltp()    — returns latest tick from in-memory store
        4. logout()     — disconnects WebSocket
    """

    def __init__(self, cfg):
        self._cfg = cfg
        self._lock = threading.RLock()
        self._client: Optional[Any] = None
        self._session_token: Optional[str] = None
        self._demo_mode: bool = not cfg.is_shoonya_configured()
        self._tick_store: dict[str, dict] = {}
        self._ws_running: bool = False
        self._subscribed: set[str] = set()

        if self._demo_mode:
            logger.info("BrokerSession: DEMO mode (credentials not configured)")
        else:
            logger.info("BrokerSession: LIVE mode (Shoonya credentials loaded)")

    # ── Authentication ────────────────────────────────────────────────────────

    def login(self) -> bool:
        """
        Authenticate with Shoonya using OAuth.

        Returns True on success, False in demo mode (always succeeds).

        OAuth flow:
          1. Checks if cached token is still valid (< 8 hours old)
          2. Runs headless Firefox OAuth via run_oauth_login()
          3. Stores token and initializes ShoonyaClient
        """
        with self._lock:
            if self._demo_mode:
                logger.info("DEMO mode: skipping OAuth login")
                return True

            if not _SHOONYA_AVAILABLE:
                logger.error("OAuth login module not available — cannot login")
                return False

            if not _NOREN_AVAILABLE:
                logger.error("NorenRestApiPy not installed — cannot login")
                return False

            logger.info("Starting Shoonya OAuth login for user=%s", self._cfg.SHOONYA_USER_ID)

            # Set env vars expected by run_oauth_login
            os.environ["USER_ID"]      = self._cfg.SHOONYA_USER_ID
            os.environ["PASSWORD"]     = self._cfg.SHOONYA_PASSWORD
            os.environ["TOKEN"]        = self._cfg.SHOONYA_TOTP_KEY
            os.environ["VC"]           = self._cfg.SHOONYA_VENDOR_CODE
            os.environ["OAUTH_SECRET"] = self._cfg.SHOONYA_OAUTH_SECRET

            try:
                token = run_oauth_login()
            except Exception as e:
                logger.exception("OAuth login exception: %s", e)
                return False

            if not token:
                logger.error("OAuth login returned no token")
                return False

            self._session_token = token
            logger.info("OAuth login successful — token acquired")

            # Initialize NorenApi client with the injected token
            try:
                cfg = _MinimalShoonyaConfig(
                    user_id=self._cfg.SHOONYA_USER_ID,
                    password=self._cfg.SHOONYA_PASSWORD,
                    totp_key=self._cfg.SHOONYA_TOTP_KEY,
                    vendor_code=self._cfg.SHOONYA_VENDOR_CODE,
                    api_secret=self._cfg.SHOONYA_OAUTH_SECRET,
                )
                client = NorenApi(
                    host=cfg.shoonya_host,
                    websocket=cfg.shoonya_websocket,
                )
                client.set_session(
                    userid=self._cfg.SHOONYA_USER_ID,
                    password="",
                    usertoken=token,
                )
                self._client = client
                logger.info("NorenApi client initialized with OAuth token")
            except Exception as e:
                logger.exception("Failed to initialize ShoonyaClient: %s", e)
                return False

            return True

    def logout(self) -> None:
        with self._lock:
            if self._client:
                try:
                    self._client.close_websocket()
                except Exception:
                    pass
            self._client = None
            self._session_token = None
            self._ws_running = False
            logger.info("Session terminated")

    @property
    def is_logged_in(self) -> bool:
        if self._demo_mode:
            return True
        return self._session_token is not None

    @property
    def is_demo(self) -> bool:
        return self._demo_mode

    # ── Market Data ───────────────────────────────────────────────────────────

    def get_ltp(self, exchange: str, symbol: str) -> Optional[dict]:
        """Get latest tick data for a symbol."""
        key = f"{exchange}:{symbol}"
        with self._lock:
            if key in self._tick_store:
                return self._tick_store[key]

        if self._demo_mode:
            quote = _make_demo_quote(symbol)
            with self._lock:
                self._tick_store[key] = quote
            return quote

        if not self._client:
            return None

        try:
            resp = self._client.get_quotes(exchange=exchange, token=symbol)
            if resp:
                tick = {
                    "symbol": symbol,
                    "ltp": float(resp.get("lp", 0)),
                    "close": float(resp.get("c", 0)),
                    "change": float(resp.get("lp", 0)) - float(resp.get("c", 0)),
                    "changePct": float(resp.get("pc", 0)),
                    "open": float(resp.get("o", 0)),
                    "high": float(resp.get("h", 0)),
                    "low": float(resp.get("l", 0)),
                    "volume": int(resp.get("v", 0)),
                    "oi": int(resp.get("oi", 0)),
                    "source": "live",
                }
                with self._lock:
                    self._tick_store[key] = tick
                return tick
        except Exception as e:
            logger.error("get_ltp error for %s: %s", key, e)

        return None

    def get_option_chain(
        self, exchange: str, symbol: str, strike_count: int = 10
    ) -> Optional[dict]:
        """Fetch option chain for an underlying."""
        if self._demo_mode:
            return _make_demo_option_chain(symbol, strike_count)

        if not self._client:
            return None

        try:
            # ShoonyaClient.get_option_chain(exchange, tradingsymbol, strikeprice, count)
            # strikeprice is the ATM strike (string).  We pass "0" to let the broker
            # return strikes centred around the current price when the exact LTP is unknown.
            resp = self._client.get_option_chain(
                exchange=exchange,
                tradingsymbol=symbol,
                strikeprice="0",
                count=str(strike_count * 2),
            )
            if resp:
                return resp
        except Exception as e:
            logger.error("get_option_chain error for %s: %s", symbol, e)

        return None

    def get_positions(self) -> list:
        """Get current open positions."""
        if self._demo_mode:
            return []

        if not self._client:
            return []

        try:
            return self._client.get_positions() or []
        except Exception as e:
            logger.error("get_positions error: %s", e)
            return []

    def get_order_book(self) -> list:
        """Get today's orders."""
        if self._demo_mode:
            return []

        if not self._client:
            return []

        try:
            return self._client.get_order_book() or []
        except Exception as e:
            logger.error("get_order_book error: %s", e)
            return []

    def get_limits(self) -> dict:
        """Get account funds / margin limits from broker."""
        if self._demo_mode or not self._client:
            return {}
        try:
            raw = self._client.get_limits()
            if not raw:
                return {}
            # Normalise Shoonya's field names to common schema
            return {
                "cash":             float(raw.get("cash")   or raw.get("brkcsh")  or 0),
                "payin":            float(raw.get("payin")  or 0),
                "payout":           float(raw.get("payout") or 0),
                "marginUsed":       float(raw.get("marginused") or raw.get("culshldmrgn") or 0),
                "marginAvailable":  float(raw.get("marginavailable") or raw.get("net")  or 0),
                "realizedPnl":      float(raw.get("rpnl")   or 0),
                "unrealizedPnl":    float(raw.get("urpnl")  or 0),
                "totalBalance":     float(raw.get("collateral") or raw.get("brkcsh") or raw.get("cash") or 0),
                "raw":              raw,
            }
        except Exception as e:
            logger.error("get_limits error: %s", e)
            return {}

    def place_order(self, order: dict) -> dict:
        """Place a buy/sell order."""
        logger.info("place_order: %s", order)

        if self._demo_mode:
            import time as _t
            return {
                "success": True,
                "orderId": f"DEMO-{int(_t.time())}",
                "message": "Order placed in DEMO mode",
            }

        if not self._client:
            return {"success": False, "message": "Not logged in"}

        try:
            # ShoonyaClient.place_order() takes order_params: Union[dict, Any]
            # Pass a dict — ShoonyaClient._normalize_order_params() handles it.
            order_params = {
                "buy_or_sell":    order["transactionType"],
                "product_type":   order["productType"],
                "exchange":       order["exchange"],
                "tradingsymbol":  order["symbol"],
                "quantity":       int(order["quantity"]),
                "discloseqty":    0,
                "price_type":     order["orderType"],
                "price":          float(order.get("price", 0)),
                "retention":      "DAY",
                "remarks":        order.get("tag", "Smart Trader"),
            }
            trig = float(order.get("triggerPrice", 0) or 0)
            if trig:
                order_params["trigger_price"] = trig

            result = self._client.place_order(order_params)
            # ShoonyaClient.place_order() returns an OrderResult dataclass
            if hasattr(result, "success"):
                return {
                    "success": result.success,
                    "orderId": getattr(result, "order_id", ""),
                    "message": getattr(result, "error_message", "") or "Order placed",
                }
            # Fallback: raw dict from older API
            if result and isinstance(result, dict):
                ok = result.get("stat") == "Ok"
                return {
                    "success": ok,
                    "orderId": result.get("norenordno", ""),
                    "message": result.get("emsg", "Order placed") if not ok else "Order placed",
                }
            return {"success": False, "message": "Unexpected broker response"}
        except Exception as e:
            logger.exception("place_order failed: %s", e)
            return {"success": False, "message": str(e)}

    def subscribe_websocket(self, symbols: list[str], on_tick) -> None:
        """Subscribe to live ticks via WebSocket."""
        if self._demo_mode or not self._client:
            return
        try:
            # ShoonyaClient.start_websocket uses on_tick/on_order_update/on_open/on_close
            self._client.start_websocket(
                on_tick=on_tick,
                on_order_update=lambda msg: logger.debug("order update: %s", msg),
                on_open=lambda: logger.info("WebSocket connected"),
                on_close=lambda: logger.warning("WebSocket closed"),
            )
            tokens = [f"NSE|{s}" for s in symbols]
            self._client.subscribe(tokens)
            self._subscribed.update(symbols)
        except Exception as e:
            logger.error("subscribe_websocket error: %s", e)

    # ── Token injection (for broker_sessions.py flow) ─────────────────────────

    def inject_token(self, creds: dict, token: str) -> bool:
        """
        Inject a pre-acquired OAuth token and switch to live mode.
        Called by broker_sessions.py after a successful UI-driven OAuth flow.

        Uses _MinimalShoonyaConfig so no .env file is required; only the
        credentials dict passed in by the caller is used.
        """
        if not _NOREN_AVAILABLE:
            logger.warning("inject_token: NorenRestApiPy not available")
            return False

        with self._lock:
            try:
                cfg = _MinimalShoonyaConfig(
                    user_id=creds.get("USER_ID", ""),
                    password=creds.get("PASSWORD", ""),
                    totp_key=creds.get("TOKEN", ""),
                    vendor_code=creds.get("VC", ""),
                    api_secret=creds.get("APP_KEY", ""),
                    imei=creds.get("IMEI", "mac"),
                )
                client = NorenApi(
                    host=cfg.shoonya_host,
                    websocket=cfg.shoonya_websocket,
                )
                client.set_session(
                    userid=creds.get("USER_ID", ""),
                    password="",
                    usertoken=token,
                )

                self._client = client
                self._session_token = token
                self._demo_mode = False
                logger.info(
                    "BrokerSession: token injected for user=%s — live mode active",
                    creds.get("USER_ID", "?"),
                )
                return True
            except Exception as e:
                logger.warning("inject_token failed: %s", e)
                return False

    def reset_to_demo(self) -> None:
        """Switch back to demo mode (called on disconnect)."""
        with self._lock:
            if self._client:
                try:
                    self._client.close_websocket()
                except Exception:
                    pass
            self._client        = None
            self._session_token = None
            self._demo_mode     = True
            self._tick_store.clear()
            logger.info("BrokerSession: reset to DEMO mode")


# ── Demo option chain ─────────────────────────────────────────────────────────

def _make_demo_option_chain(symbol: str, strikes: int = 10) -> dict:
    base = {"NIFTY": 23500, "BANKNIFTY": 51000, "FINNIFTY": 22300}.get(symbol, 20000)
    atm = round(base / 50) * 50
    rows = []
    for i in range(-strikes, strikes + 1):
        strike = atm + i * 50
        call_iv = max(0.05, 0.2 - abs(i) * 0.01 + random.uniform(-0.02, 0.02))
        put_iv  = max(0.05, 0.2 - abs(i) * 0.01 + random.uniform(-0.02, 0.02))
        call_ltp = max(0.5, (base - strike) if strike < base else (base * call_iv * 0.3))
        put_ltp  = max(0.5, (strike - base) if strike > base else (base * put_iv  * 0.3))
        base_oi  = random.randint(100, 5000) * 75
        # Greeks approximations
        call_delta = round(max(0.01, min(0.99, 0.5 - i * 0.05)), 4)
        put_delta  = round(max(-0.99, min(-0.01, -0.5 + i * 0.05)), 4)
        # Gamma peaks at ATM, decays away from it
        gamma_val  = round(max(0.0001, 0.002 * (1 - abs(i) * 0.08)), 6)
        # Theta is negative (time decay), larger for ATM
        call_theta = round(-max(0.1, call_ltp * 0.01 * (1 - abs(i) * 0.04)), 4)
        put_theta  = round(-max(0.1, put_ltp  * 0.01 * (1 - abs(i) * 0.04)), 4)
        # Vega: higher for ATM
        vega_val   = round(max(0.5, 15 * (1 - abs(i) * 0.07)), 4)
        rows.append({
            "strike": strike,
            "call": {
                "ltp": round(call_ltp, 2),
                "iv": round(call_iv * 100, 2),
                "delta": call_delta,
                "gamma": gamma_val,
                "theta": call_theta,
                "vega": vega_val,
                "oi": base_oi + random.randint(-500, 500) * 75,
                "oiChange": random.randint(-200, 200) * 75,
                "volume": random.randint(1000, 100000),
                "bid": round(call_ltp * 0.995, 2),
                "ask": round(call_ltp * 1.005, 2),
            },
            "put": {
                "ltp": round(put_ltp, 2),
                "iv": round(put_iv * 100, 2),
                "delta": put_delta,
                "gamma": gamma_val,
                "theta": put_theta,
                "vega": vega_val,
                "oi": base_oi + random.randint(-500, 500) * 75,
                "oiChange": random.randint(-200, 200) * 75,
                "volume": random.randint(1000, 100000),
                "bid": round(put_ltp * 0.995, 2),
                "ask": round(put_ltp * 1.005, 2),
            },
        })
    total_ce_oi = sum(r["call"]["oi"] for r in rows)
    total_pe_oi = sum(r["put"]["oi"]  for r in rows)
    pcr = round(total_pe_oi / max(total_ce_oi, 1), 4)
    return {
        "underlying": symbol,
        "underlyingLtp": base + random.uniform(-50, 50),
        "expiry": "28-NOV-2024",
        "expiries": ["28-NOV-2024", "05-DEC-2024", "19-DEC-2024"],
        "pcr": pcr,
        "maxPainStrike": atm,
        "rows": rows,
        "source": "demo",
    }


# ── Global session singleton ──────────────────────────────────────────────────
_session: Optional[BrokerSession] = None
_session_lock = threading.Lock()


def get_session() -> BrokerSession:
    """Return (or create) the global broker session singleton."""
    global _session
    with _session_lock:
        if _session is None:
            from core.config import config
            _session = BrokerSession(config)
        return _session


# ── Per-user live session registry ────────────────────────────────────────────
# Maps  user_id (JWT sub)  →  BrokerSession in live mode
# Used by orders/positions endpoints to provide per-user isolation.
_user_sessions: dict[str, BrokerSession] = {}
_user_sessions_lock = threading.Lock()


def register_user_session(user_id: str, creds: dict, token: str) -> bool:
    """
    Create (or update) a live BrokerSession for a specific user.
    Called by broker_sessions.py after a successful OAuth connect.
    Also syncs the global singleton so single-user flows keep working.
    """
    from core.config import config
    with _user_sessions_lock:
        sess = _user_sessions.get(user_id)
        if sess is None:
            sess = BrokerSession(config)
            _user_sessions[user_id] = sess
        ok = sess.inject_token(creds, token)

    # Keep the global singleton in sync for backward-compat
    get_session().inject_token(creds, token)
    return ok


def deregister_user_session(user_id: str) -> None:
    """
    Disconnect a specific user's live session and fall back to demo.
    Also resets the global singleton.
    """
    with _user_sessions_lock:
        sess = _user_sessions.pop(user_id, None)
        if sess:
            sess.reset_to_demo()

    get_session().reset_to_demo()


def get_user_session(user_id: str) -> Optional[BrokerSession]:
    """
    Return the live BrokerSession for a user, or None if not connected.
    Callers fall back to get_session() for demo / global session.
    """
    with _user_sessions_lock:
        return _user_sessions.get(user_id)


def get_best_session(user_id: Optional[str] = None) -> BrokerSession:
    """
    Return the best available session for a user:
      1. Per-user live session (if connected via UI)
      2. Global singleton (live if configured via env vars, otherwise demo)
    """
    if user_id:
        user_sess = get_user_session(user_id)
        if user_sess and not user_sess.is_demo:
            return user_sess
    return get_session()
