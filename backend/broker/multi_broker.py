"""
Smart Trader — Multi-Broker Account Registry
=============================================

Each subscribed user can add multiple broker accounts.
This module manages ALL active broker sessions in memory:

    user_id  →  {config_id  →  BrokerAccountSession}

Supported account types:
  • shoonya    — Finvasia / Shoonya (live REST + WebSocket via NorenApi)
  • paper      — Paper trading (local OMS)
  • fyers      — Fyers (TODO: trading, currently market data only)
  • demo       — Read-only demo (no login required)

Usage:
    from broker.multi_broker import registry

    # After successful broker connect:
    registry.register(user_id, config_id, broker_id, creds, token)

    # Get session for specific account:
    sess = registry.get_session(user_id, config_id)
    positions = sess.get_positions()

    # Get all live accounts for a user:
    accounts = registry.list_sessions(user_id)

    # Route an order to the right broker account:
    result = registry.execute_order(user_id, config_id, order_dict)
"""

import threading
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any

logger = logging.getLogger("smart_trader.broker.registry")



# ── Broker-aware logger adapter ───────────────────────────────────────────────

class _BrokerLog(logging.LoggerAdapter):
    """Prefixes every log line with [user=... broker=... client=...] context."""

    def process(self, msg, kwargs):
        extra = self.extra or {}
        prefix = (
            f"[user={str(extra.get('user_id','?'))[:8]} "
            f"broker={str(extra.get('broker_id','?'))} "
            f"client={str(extra.get('client_id','?'))}]"
        )
        return f"{prefix} {msg}", kwargs


def _make_log(user_id: str, broker_id: str, client_id: str) -> logging.LoggerAdapter:
    return _BrokerLog(
        logging.getLogger("smart_trader.broker.session"),
        {"user_id": user_id, "broker_id": broker_id, "client_id": client_id},
    )


# ── BrokerAccountSession ──────────────────────────────────────────────────────

class BrokerAccountSession:
    """
    Represents one active broker account session for a user.

    Abstracts over different broker clients so order routing and
    data fetching are broker-agnostic from the rest of the app.
    """

    def __init__(
        self,
        user_id: str,
        config_id: str,
        broker_id: str,
        client_id: str,
        mode: str = "demo",    # "live" | "paper" | "demo"
    ):
        self.user_id   = user_id
        self.config_id = config_id
        self.broker_id = broker_id
        self.client_id = client_id
        self.mode      = mode
        self.connected_at: Optional[datetime] = None
        self.last_heartbeat: Optional[datetime] = None
        self.error: Optional[str] = None

        self._lock    = threading.RLock()
        self._client  = None        # ShoonyaClient | None
        self._adapter = None        # BrokerAdapter instance (normalised API)
        self._log     = _make_log(user_id, broker_id, client_id)
        self._last_relogin_at: Optional[datetime] = None
        self._relogin_in_progress = False

    # ── Connection ────────────────────────────────────────────────────────────

    def inject_shoonya_token(self, creds: dict, token: str) -> bool:
        """
        Restore a Shoonya session from a previously-obtained OAuth token.
        Called after successful OAuth login in broker_sessions.py.
        """
        try:
            from NorenRestApiPy.NorenApi import NorenApi  # type: ignore[import]

            from broker.shoonya_client import _MinimalShoonyaConfig
            min_cfg = _MinimalShoonyaConfig(
                user_id=creds.get("USER_ID", ""),
                password=creds.get("PASSWORD", ""),
                totp_key=creds.get("TOKEN", ""),
                vendor_code=creds.get("VC", ""),
                api_secret=creds.get("APP_KEY") or creds.get("OAUTH_SECRET", ""),
                imei=creds.get("IMEI", "mac"),
            )

            with self._lock:
                if self._client is None:
                    self._client = NorenApi(
                        host=min_cfg.shoonya_host,
                        websocket=min_cfg.shoonya_websocket,
                    )
                self._client.set_session(
                    userid=creds.get("USER_ID", ""),
                    password="",
                    usertoken=token,
                )
                self.mode          = "live"
                self.connected_at  = datetime.now(timezone.utc)
                self.last_heartbeat = self.connected_at
                self.error         = None

            # Build adapter over the injected session
            try:
                from broker.shoonya_client import BrokerSession as _BS, _MinimalShoonyaConfig
                from broker.adapters.shoonya_adapter import ShoonyaAdapter
                _dummy_cfg = _MinimalShoonyaConfig(
                    user_id=creds.get("USER_ID", ""),
                    password=creds.get("PASSWORD", ""),
                    totp_key=creds.get("TOKEN", ""),
                    vendor_code=creds.get("VC", ""),
                    api_secret=creds.get("APP_KEY") or creds.get("OAUTH_SECRET", ""),
                )
                shoonya_bs = _BS(_dummy_cfg)
                shoonya_bs._client     = self._client
                shoonya_bs._demo_mode  = False
                shoonya_bs._session_token = token
                self._adapter = ShoonyaAdapter(shoonya_bs, self.config_id, self.client_id)
            except Exception as _ae:
                self._log.warning("ShoonyaAdapter init failed (non-fatal): %s", _ae)

            self._log.info("inject_shoonya_token succeeded — mode=live")
            return True
        except Exception as e:
            self.error = str(e)
            self._log.error("inject_shoonya_token failed: %s", e, exc_info=True)
            return False

    def activate_paper(self) -> None:
        self.mode = "paper"
        self.connected_at  = datetime.now(timezone.utc)
        self.last_heartbeat = self.connected_at
        self.error = None
        try:
            from broker.adapters.paper_adapter import PaperAdapter
            self._adapter = PaperAdapter(self.config_id, self.client_id)
        except Exception as _ae:
            self._log.warning("PaperAdapter init failed: %s", _ae)
        self._log.info("Paper trading session activated")

    def inject_fyers_token(self, creds: dict, token: str) -> bool:
        """
        Register a live Fyers session using a pre-acquired access token.
        Creates a FyersAdapter that wraps the Fyers API v3 client.
        """
        try:
            from broker.adapters.fyers_adapter import FyersAdapter
            adapter = FyersAdapter(creds, token, self.config_id, self.client_id)
            if not adapter.is_connected():
                self.error = "Fyers token injection failed — client not connected"
                self._log.error("inject_fyers_token: FyersAdapter not connected")
                return False
            with self._lock:
                self._adapter         = adapter
                self.mode             = "live"
                self.connected_at     = datetime.now(timezone.utc)
                self.last_heartbeat   = self.connected_at
                self.error            = None
            self._log.info("Fyers session injected — mode=live")
            return True
        except Exception as e:
            self.error = str(e)
            self._log.error("inject_fyers_token failed: %s", e, exc_info=True)
            return False

    def inject_angelone_token(self, creds: dict, jwt_token: str) -> bool:
        """
        Register a live Angel One session using a pre-acquired JWT token.
        Creates an AngelOneAdapter that wraps the Angel One SmartAPI.
        """
        try:
            from broker.adapters.angelone_adapter import AngelOneAdapter
            adapter = AngelOneAdapter(creds, jwt_token, self.config_id, self.client_id)
            if not adapter.is_connected():
                self.error = "Angel One token injection failed — missing API key or token"
                self._log.error("inject_angelone_token: AngelOneAdapter not connected")
                return False
            with self._lock:
                self._adapter         = adapter
                self.mode             = "live"
                self.connected_at     = datetime.now(timezone.utc)
                self.last_heartbeat   = self.connected_at
                self.error            = None
            self._log.info("Angel One session injected — mode=live")
            return True
        except Exception as e:
            self.error = str(e)
            self._log.error("inject_angelone_token failed: %s", e, exc_info=True)
            return False

    def inject_dhan_token(self, creds: dict, access_token: str) -> bool:
        """
        Register a live Dhan session using a pre-acquired access token.
        Creates a DhanAdapter that wraps the Dhan HQ API v2.
        """
        try:
            from broker.adapters.dhan_adapter import DhanAdapter
            adapter = DhanAdapter(creds, access_token, self.config_id, self.client_id)
            if not adapter.is_connected():
                self.error = "Dhan token injection failed — missing access token"
                self._log.error("inject_dhan_token: DhanAdapter not connected")
                return False
            with self._lock:
                self._adapter         = adapter
                self.mode             = "live"
                self.connected_at     = datetime.now(timezone.utc)
                self.last_heartbeat   = self.connected_at
                self.error            = None
            self._log.info("Dhan session injected — mode=live")
            return True
        except Exception as e:
            self.error = str(e)
            self._log.error("inject_dhan_token failed: %s", e, exc_info=True)
            return False

    def inject_groww_token(self, creds: dict, access_token: str) -> bool:
        """
        Register a live Groww session using a pre-acquired access token.
        Creates a GrowwAdapter that wraps the Groww Trade API.
        """
        try:
            from broker.adapters.groww_adapter import GrowwAdapter
            adapter = GrowwAdapter(creds, access_token, self.config_id, self.client_id)
            if not adapter.is_connected():
                self.error = "Groww token injection failed — missing access token"
                self._log.error("inject_groww_token: GrowwAdapter not connected")
                return False
            with self._lock:
                self._adapter         = adapter
                self.mode             = "live"
                self.connected_at     = datetime.now(timezone.utc)
                self.last_heartbeat   = self.connected_at
                self.error            = None
            self._log.info("Groww session injected — mode=live")
            return True
        except Exception as e:
            self.error = str(e)
            self._log.error("inject_groww_token failed: %s", e, exc_info=True)
            return False

    def inject_upstox_token(self, creds: dict, access_token: str) -> bool:
        """
        Register a live Upstox session using a pre-acquired OAuth access token.
        Creates an UpstoxAdapter that wraps the Upstox API v3.
        """
        try:
            from broker.adapters.upstox_adapter import UpstoxAdapter
            adapter = UpstoxAdapter(creds, access_token, self.config_id, self.client_id)
            if not adapter.is_connected():
                self.error = "Upstox token injection failed — missing access token"
                self._log.error("inject_upstox_token: UpstoxAdapter not connected")
                return False
            with self._lock:
                self._adapter         = adapter
                self.mode             = "live"
                self.connected_at     = datetime.now(timezone.utc)
                self.last_heartbeat   = self.connected_at
                self.error            = None
            self._log.info("Upstox session injected — mode=live")
            return True
        except Exception as e:
            self.error = str(e)
            self._log.error("inject_upstox_token failed: %s", e, exc_info=True)
            return False

    def inject_kite_token(self, creds: dict, api_key: str, access_token: str) -> bool:
        """
        Register a live Zerodha Kite session using api_key and access_token.
        Creates a KiteAdapter that wraps the Kite Connect v3 API.
        """
        try:
            from broker.adapters.kite_adapter import KiteAdapter
            adapter = KiteAdapter(creds, api_key, access_token, self.config_id, self.client_id)
            if not adapter.is_connected():
                self.error = "Kite token injection failed — missing API key or access token"
                self._log.error("inject_kite_token: KiteAdapter not connected")
                return False
            with self._lock:
                self._adapter         = adapter
                self.mode             = "live"
                self.connected_at     = datetime.now(timezone.utc)
                self.last_heartbeat   = self.connected_at
                self.error            = None
            self._log.info("Kite session injected — mode=live")
            return True
        except Exception as e:
            self.error = str(e)
            self._log.error("inject_kite_token failed: %s", e, exc_info=True)
            return False

    def disconnect(self) -> None:
        with self._lock:
            self._client  = None
            self._adapter = None
            self.mode = "demo"
            self.error = None
        self._log.info("Session disconnected — reset to demo")

    # ── Status ────────────────────────────────────────────────────────────────

    @property
    def is_live(self) -> bool:
        return self.mode == "live"

    @property
    def is_paper(self) -> bool:
        return self.mode == "paper"

    @property
    def is_demo(self) -> bool:
        return self.mode == "demo"

    def heartbeat(self) -> bool:
        """Ping broker to confirm session is alive. If stale, attempt relogin."""
        if self.is_demo:
            return True
        if self.is_paper:
            self.last_heartbeat = datetime.now(timezone.utc)
            return True
        with self._lock:
            if self._client is None:
                return False
            try:
                # Lightweight validation call — doesn't modify state
                result = self._client._NorenApi__susertoken  # check token present
                alive = bool(result)
                if alive:
                    self.last_heartbeat = datetime.now(timezone.utc)
                else:
                    # Token missing — attempt relogin
                    self._log.warning("Heartbeat: token missing — triggering relogin")
                    if self._relogin():
                        self.last_heartbeat = datetime.now(timezone.utc)
                        return True
                return alive
            except Exception as e:
                self._log.warning("Heartbeat failed: %s — triggering relogin", e)
                if self._relogin():
                    self.last_heartbeat = datetime.now(timezone.utc)
                    return True
                return False

    def to_dict(self) -> dict:
        return {
            "user_id":       self.user_id,
            "config_id":     self.config_id,
            "broker_id":     self.broker_id,
            "client_id":     self.client_id,
            "mode":          self.mode,
            "is_live":       self.is_live,
            "connected_at":  self.connected_at.isoformat() if self.connected_at else None,
            "last_heartbeat": self.last_heartbeat.isoformat() if self.last_heartbeat else None,
            "error":         self.error,
        }

    # ── Ensure-Login (Auto-Reconnect) ────────────────────────────────────────

    _RELOGIN_COOLDOWN_SECS = 60
    _SESSION_ERROR_PATTERNS = [
        "session expired", "token expired", "invalid token",
        "unauthorized", "not logged in", "session not found",
        "invalid session", "please login", "authentication failed",
        "session is expired", "token is invalid", "access denied",
        "not authorized", "login required", "not_ok", "request token",
        "invalid_token", "token_expired", "ag8001", "ab1010",
        "expecting value",  # JSON parse fail = empty response = session gone
    ]

    def _is_session_error(self, exc: Exception) -> bool:
        """Check if an exception indicates broker session expiry."""
        if self.mode != "live":
            return False
        err_text = str(exc).lower()
        return any(pat in err_text for pat in self._SESSION_ERROR_PATTERNS)

    def _is_session_error_response(self, result) -> bool:
        """Check if a broker API response indicates session expiry."""
        if self.mode != "live":
            return False
        if result is None and self.broker_id == "shoonya":
            return True
        if isinstance(result, dict):
            # Shoonya: stat != "Ok" with session-related emsg
            emsg = str(result.get("emsg", "")).lower()
            if emsg and any(pat in emsg for pat in self._SESSION_ERROR_PATTERNS):
                return True
            # Fyers: s != "ok" with session-related message
            msg = str(result.get("message", "")).lower()
            if msg and any(pat in msg for pat in self._SESSION_ERROR_PATTERNS):
                return True
            # Angel One: status == False with session-related message
            if result.get("status") is False:
                msg2 = str(result.get("message", "")).lower()
                if any(pat in msg2 for pat in self._SESSION_ERROR_PATTERNS):
                    return True
        return False

    def _relogin(self) -> bool:
        """
        Re-authenticate this broker session using stored credentials.
        Returns True if relogin succeeded and adapter was refreshed.
        Enforces a cooldown to prevent relogin storms.
        """
        now = datetime.now(timezone.utc)

        # Cooldown: don't retry relogin within _RELOGIN_COOLDOWN_SECS
        if self._last_relogin_at and (now - self._last_relogin_at).total_seconds() < self._RELOGIN_COOLDOWN_SECS:
            self._log.warning("Relogin skipped — cooldown active (last attempt %ss ago)",
                              int((now - self._last_relogin_at).total_seconds()))
            return False

        # Prevent concurrent relogin attempts
        with self._lock:
            if self._relogin_in_progress:
                self._log.warning("Relogin already in progress — skipping")
                return False
            self._relogin_in_progress = True

        self._last_relogin_at = now
        self._log.info("ENSURE_LOGIN: attempting relogin for broker=%s", self.broker_id)

        try:
            from db.database import SessionLocal, BrokerConfig
            from core.security import decrypt_credentials

            db = SessionLocal()
            try:
                cfg = db.query(BrokerConfig).filter(BrokerConfig.id == self.config_id).first()
                if not cfg:
                    self._log.error("Relogin: BrokerConfig not found for config_id=%s", self.config_id)
                    return False

                try:
                    creds = decrypt_credentials(str(cfg.credentials))
                except Exception as e:
                    self._log.error("Relogin: cannot decrypt credentials: %s", e)
                    return False

                if self.broker_id == "shoonya":
                    return self._relogin_shoonya(creds, cfg, db)
                elif self.broker_id == "fyers":
                    return self._relogin_fyers(creds, cfg, db)
                elif self.broker_id == "angel":
                    return self._relogin_angelone(creds, cfg, db)
                else:
                    self._log.warning("Relogin: no auto-relogin for broker_id=%s", self.broker_id)
                    return False
            finally:
                db.close()
        except Exception as e:
            self._log.error("Relogin failed: %s", e, exc_info=True)
            return False
        finally:
            with self._lock:
                self._relogin_in_progress = False

    def _relogin_shoonya(self, creds: dict, cfg, db) -> bool:
        """Re-authenticate Shoonya via OAuth (headless Firefox)."""
        import os as _os

        required = ["USER_ID", "PASSWORD", "TOKEN", "VC", "APP_KEY", "OAUTH_SECRET"]
        missing = [f for f in required if not creds.get(f)]
        if missing:
            self._log.error("Relogin shoonya: missing fields %s", missing)
            return False

        env_map = {
            "USER_ID":      creds.get("USER_ID", ""),
            "PASSWORD":     creds.get("PASSWORD", ""),
            "TOKEN":        creds.get("TOKEN", ""),
            "VC":           creds.get("VC", ""),
            "APP_KEY":      creds.get("APP_KEY", ""),
            "OAUTH_SECRET": creds.get("OAUTH_SECRET", ""),
            "IMEI":         creds.get("IMEI", "abc1234"),
        }
        old_env = {k: _os.environ.get(k) for k in env_map}
        for k, v in env_map.items():
            _os.environ[k] = v

        try:
            from broker.oauth_login import run_oauth_login
            token = run_oauth_login()
        finally:
            for k, old_v in old_env.items():
                if old_v is None:
                    _os.environ.pop(k, None)
                else:
                    _os.environ[k] = old_v

        if not token:
            self._log.error("Relogin shoonya: OAuth returned empty token")
            return False

        ok = self.inject_shoonya_token(creds, token)
        if ok:
            self._log.info("ENSURE_LOGIN: Shoonya relogin SUCCESS — token length=%d", len(token))
            self._persist_token(cfg, db, token)
        return ok

    def _relogin_fyers(self, creds: dict, cfg, db) -> bool:
        """Re-authenticate Fyers via direct API login."""
        required = ["CLIENT_ID", "APP_ID", "SECRET_KEY", "TOTP_KEY", "PIN"]
        missing = [f for f in required if not creds.get(f)]
        if missing:
            self._log.error("Relogin fyers: missing fields %s", missing)
            return False

        from routers.broker_sessions import _fyers_direct_login
        token = _fyers_direct_login(creds, self._log)
        if not token or len(token) < 10:
            self._log.error("Relogin fyers: login returned empty token")
            return False

        ok = self.inject_fyers_token(creds, token)
        if ok:
            self._log.info("ENSURE_LOGIN: Fyers relogin SUCCESS — token length=%d", len(token))
            self._persist_token(cfg, db, token)
        return ok

    def _relogin_angelone(self, creds: dict, cfg, db) -> bool:
        """Re-authenticate Angel One via SmartAPI."""
        required = ["CLIENT_ID", "PIN", "API_KEY", "TOTP_SECRET"]
        missing = [f for f in required if not creds.get(f)]
        if missing:
            self._log.error("Relogin angel: missing fields %s", missing)
            return False

        from routers.broker_sessions import _angelone_direct_login
        jwt_token = _angelone_direct_login(
            creds["CLIENT_ID"], creds["PIN"], creds["API_KEY"], creds["TOTP_SECRET"], self._log,
        )
        if not jwt_token or len(jwt_token) < 10:
            self._log.error("Relogin angel: login returned empty token")
            return False

        ok = self.inject_angelone_token(creds, jwt_token)
        if ok:
            self._log.info("ENSURE_LOGIN: Angel One relogin SUCCESS — token length=%d", len(jwt_token))
            self._persist_token(cfg, db, jwt_token)
        return ok

    def _persist_token(self, cfg, db, token: str):
        """Save the refreshed token to the DB session row."""
        try:
            from routers.broker_sessions import _upsert_session
            _upsert_session(db, cfg, self.user_id, mode="live", token=token, logged_in=True)
        except Exception as e:
            self._log.warning("Failed to persist relogin token to DB: %s", e)

    def _call_with_relogin(self, fn, *args, **kwargs):
        """
        Call fn(*args, **kwargs).
        If it raises a session-expired error, relogin and retry once.
        """
        try:
            result = fn(*args, **kwargs)
            return result
        except Exception as first_err:
            if not self._is_session_error(first_err):
                raise
            self._log.warning(
                "ENSURE_LOGIN: session error in %s: %s — attempting relogin",
                getattr(fn, '__name__', str(fn)), first_err,
            )
            if not self._relogin():
                raise
            self._log.info("ENSURE_LOGIN: relogin OK — retrying %s", getattr(fn, '__name__', str(fn)))
            return fn(*args, **kwargs)

    # ── Data Methods (with ensure_login auto-reconnect) ─────────────────────

    def get_positions(self) -> list:
        if self._adapter is not None:
            try:
                return [p.to_dict() for p in self._call_with_relogin(self._adapter.get_positions)]
            except Exception as e:
                self._log.error("get_positions (adapter) error: %s", e)
                return []
        if not self.is_live or self._client is None:
            return []
        try:
            result = self._call_with_relogin(self._client.get_positions)
            if isinstance(result, list):
                return result
            if isinstance(result, dict):
                return result.get("data", [])
            return []
        except Exception as e:
            self._log.error("get_positions error: %s", e)
            return []

    def get_order_book(self) -> list:
        if self._adapter is not None:
            try:
                return [o.to_dict() for o in self._call_with_relogin(self._adapter.get_order_book)]
            except Exception as e:
                self._log.error("get_order_book (adapter) error: %s", e)
                return []
        if not self.is_live or self._client is None:
            return []
        try:
            result = self._call_with_relogin(self._client.get_order_book)
            if isinstance(result, list):
                return result
            if isinstance(result, dict):
                return result.get("data", [])
            return []
        except Exception as e:
            self._log.error("get_order_book error: %s", e)
            return []

    def get_limits(self) -> dict:
        if self._adapter is not None:
            try:
                return self._call_with_relogin(self._adapter.get_funds).to_dict()
            except Exception as e:
                self._log.error("get_limits (adapter) error: %s", e)
                return {}
        if not self.is_live or self._client is None:
            return {}
        try:
            result = self._call_with_relogin(self._client.get_limits)
            if isinstance(result, dict):
                return result
            return {}
        except Exception as e:
            self._log.error("get_limits error: %s", e)
            return {}

    def get_holdings(self) -> list:
        if self._adapter is not None:
            try:
                return [h.to_dict() for h in self._call_with_relogin(self._adapter.get_holdings)]
            except Exception as e:
                self._log.error("get_holdings (adapter) error: %s", e)
                return []
        if not self.is_live or self._client is None:
            return []
        try:
            result = self._call_with_relogin(self._client.get_holdings)
            if isinstance(result, list):
                return result
            if isinstance(result, dict):
                return result.get("data", [])
            return []
        except Exception as e:
            self._log.error("get_holdings error: %s", e)
            return []

    def get_tradebook(self) -> list:
        if self._adapter is not None:
            try:
                return [t.to_dict() for t in self._call_with_relogin(self._adapter.get_tradebook)]
            except Exception as e:
                self._log.error("get_tradebook (adapter) error: %s", e)
                return []
        return []  # No tradebook without adapter

    def place_order(self, order: dict) -> dict:
        """
        Route an order to the correct broker via adapter.
        Accepts the canonical Smart Trader order dict:
            symbol, exchange, side (BUY/SELL), product (MIS/NRML/CNC),
            order_type (MARKET/LIMIT/SL/SL-M), qty, price, trigger_price
        Also accepts legacy Shoonya-format keys for backward compat.
        Uses ensure_login: if session expired, relogin and retry once.
        """
        if self._adapter is not None:
            # ── Normalise to canonical keys ───────────────────────────────────
            _SIDE = {"B": "BUY", "S": "SELL", "BUY": "BUY", "SELL": "SELL"}
            _PRD  = {"I": "MIS", "C": "CNC", "M": "NRML",
                     "MIS": "MIS", "CNC": "CNC", "NRML": "NRML",
                     "INTRADAY": "MIS", "MARGIN": "NRML"}
            _OTYPE = {"MKT": "MARKET", "LMT": "LIMIT", "SL-LMT": "SL", "SL-MKT": "SL-M",
                      "MARKET": "MARKET", "LIMIT": "LIMIT", "SL": "SL", "SL-M": "SL-M"}

            raw_side = (order.get("side") or order.get("transaction_type", "B")).upper()
            raw_prd  = (order.get("product") or order.get("product_type", "MIS")).upper()
            raw_otype = (order.get("order_type") or order.get("price_type", "MARKET")).upper()

            normalised = {
                "symbol":        order.get("symbol") or order.get("tradingsymbol", ""),
                "exchange":      order.get("exchange", "NSE"),
                "side":          _SIDE.get(raw_side, "BUY"),
                "product":       _PRD.get(raw_prd, raw_prd),
                "order_type":    _OTYPE.get(raw_otype, raw_otype),
                "qty":           int(order.get("qty") or order.get("quantity", 0)),
                "price":         float(order.get("price", 0) or 0),
                "trigger_price": float(order.get("trigger_price", 0) or 0),
                "tag":           order.get("tag") or order.get("remarks") or "smart_trader",
                "retention":     order.get("retention") or "DAY",
            }

            # Attempt via adapter with ensure_login
            try:
                result = self._call_with_relogin(self._adapter.place_order, normalised)
                # Check if response itself indicates session expiry
                if self._is_session_error_response(result):
                    self._log.warning("ENSURE_LOGIN: place_order response indicates session error — attempting relogin")
                    if self._relogin():
                        self._log.info("ENSURE_LOGIN: relogin OK — retrying place_order")
                        result = self._adapter.place_order(normalised)
                return result
            except Exception as e:
                self._log.error("place_order (adapter) error: %s", e, exc_info=True)
                return {"success": False, "message": str(e)}

        if self.is_paper:
            return self._paper_order(order)
        if self.is_demo:
            return {"success": False, "message": "Demo mode — connect a broker first"}
        if self._client is None:
            return {"success": False, "message": "No active client — please reconnect"}
        try:
            order_params = {
                "buy_or_sell":   order.get("transaction_type", "B"),
                "product_type":  order.get("product_type", "M"),
                "exchange":      order.get("exchange", "NSE"),
                "tradingsymbol": order.get("symbol", ""),
                "quantity":      int(order.get("quantity", 0)),
                "discloseqty":   0,
                "price_type":    order.get("price_type", "MKT"),
                "price":         float(order.get("price", 0)),
                "trigger_price": float(order.get("trigger_price", 0)),
                "retention":     order.get("retention", "DAY"),
                "remarks":       order.get("tag", "smart_trader"),
            }
            self._log.info(
                "Placing order: %s %s qty=%s px=%s",
                order_params["buy_or_sell"],
                order_params["tradingsymbol"],
                order_params["quantity"],
                order_params["price"],
            )
            result = self._call_with_relogin(self._client.place_order, order_params)
            # Check response for session expiry
            if self._is_session_error_response(result):
                self._log.warning("ENSURE_LOGIN: legacy place_order response indicates session error — attempting relogin")
                if self._relogin():
                    result = self._client.place_order(order_params)
            # result may be an OrderResult dataclass or dict
            if hasattr(result, "order_id"):
                return {"success": True, "order_id": result.order_id, "message": "Order placed"}
            if isinstance(result, dict):
                oid = result.get("norenordno") or result.get("order_id") or ""
                return {"success": bool(oid), "order_id": oid, "raw": result,
                        "message": "Order placed" if oid else result.get("emsg", "Order failed")}
            return {"success": False, "message": str(result)}
        except Exception as e:
            self._log.error("place_order error: %s", e, exc_info=True)
            return {"success": False, "message": str(e)}

    def _paper_order(self, order: dict) -> dict:
        """Simulate order for paper trading."""
        import uuid
        paper_id = f"PAPER-{uuid.uuid4().hex[:8].upper()}"
        self._log.info(
            "Paper order: %s %s qty=%s",
            order.get("transaction_type", "B"),
            order.get("symbol", ""),
            order.get("quantity", 0),
        )
        return {"success": True, "order_id": paper_id, "mode": "paper",
                "message": "Paper order simulated"}

    def modify_order(self, order_id: str, params: dict) -> dict:
        if not self.is_live or self._client is None:
            return {"success": False, "message": "Not connected"}
        try:
            result = self._call_with_relogin(
                self._client.modify_order, {**params, "norenordno": order_id}
            )
            return result if isinstance(result, dict) else {"success": True}
        except Exception as e:
            self._log.error("modify_order error: %s", e)
            return {"success": False, "message": str(e)}

    def cancel_order(self, order_id: str) -> dict:
        if not self.is_live or self._client is None:
            return {"success": False, "message": "Not connected"}
        try:
            result = self._call_with_relogin(self._client.cancel_order, order_id)
            return result if isinstance(result, dict) else {"success": True}
        except Exception as e:
            self._log.error("cancel_order error: %s", e)
            return {"success": False, "message": str(e)}


# ── MultiAccountRegistry ──────────────────────────────────────────────────────

class MultiAccountRegistry:
    """
    Thread-safe registry of all active broker account sessions.

    Layout:  _sessions[user_id][config_id] = BrokerAccountSession
    """

    def __init__(self):
        self._sessions: Dict[str, Dict[str, BrokerAccountSession]] = {}
        self._lock     = threading.Lock()

    # ── Registration ──────────────────────────────────────────────────────────

    def register_shoonya(
        self,
        user_id:   str,
        config_id: str,
        client_id: str,
        creds:     dict,
        token:     str,
    ) -> BrokerAccountSession:
        """
        Register (or refresh) a live Shoonya session.
        Called by connect_broker after successful OAuth.
        """
        log = _make_log(user_id, "shoonya", client_id)
        with self._lock:
            user_map = self._sessions.setdefault(user_id, {})
            sess = user_map.get(config_id)
            if sess is None:
                sess = BrokerAccountSession(user_id, config_id, "shoonya", client_id)
                user_map[config_id] = sess
                log.info("New BrokerAccountSession created")
            else:
                log.info("Refreshing existing BrokerAccountSession")

        ok = sess.inject_shoonya_token(creds, token)
        if ok:
            log.info("Shoonya session registered for config=%s", config_id)
        else:
            log.error("Shoonya session injection FAILED for config=%s", config_id)
            sess.error = "Token injection failed"
        return sess

    def register_paper(
        self,
        user_id:   str,
        config_id: str,
        client_id: str = "PAPER",
    ) -> BrokerAccountSession:
        """Register a paper trading session."""
        with self._lock:
            user_map = self._sessions.setdefault(user_id, {})
            sess = BrokerAccountSession(user_id, config_id, "paper_trade", client_id)
            user_map[config_id] = sess
            sess.activate_paper()
        _make_log(user_id, "paper", client_id).info("Paper session registered config=%s", config_id)
        return sess

    def register_fyers(
        self,
        user_id:   str,
        config_id: str,
        client_id: str,
        creds:     dict,
        token:     str,
    ) -> BrokerAccountSession:
        """Register (or refresh) a live Fyers session."""
        log = _make_log(user_id, "fyers", client_id)
        with self._lock:
            user_map = self._sessions.setdefault(user_id, {})
            sess = user_map.get(config_id)
            if sess is None:
                sess = BrokerAccountSession(user_id, config_id, "fyers", client_id)
                user_map[config_id] = sess
                log.info("New Fyers BrokerAccountSession created")
            else:
                log.info("Refreshing existing Fyers BrokerAccountSession")

        ok = sess.inject_fyers_token(creds, token)
        if ok:
            log.info("Fyers session registered for config=%s", config_id)
        else:
            log.error("Fyers session injection FAILED for config=%s — %s", config_id, sess.error)
        return sess

    def register_angelone(
        self,
        user_id:   str,
        config_id: str,
        client_id: str,
        creds:     dict,
        jwt_token: str,
    ) -> BrokerAccountSession:
        """Register (or refresh) a live Angel One session."""
        log = _make_log(user_id, "angel", client_id)
        with self._lock:
            user_map = self._sessions.setdefault(user_id, {})
            sess = user_map.get(config_id)
            if sess is None:
                sess = BrokerAccountSession(user_id, config_id, "angel", client_id)
                user_map[config_id] = sess
                log.info("New Angel One BrokerAccountSession created")
            else:
                log.info("Refreshing existing Angel One BrokerAccountSession")

        ok = sess.inject_angelone_token(creds, jwt_token)
        if ok:
            log.info("Angel One session registered for config=%s", config_id)
        else:
            log.error("Angel One session injection FAILED for config=%s — %s", config_id, sess.error)
        return sess

    def register_dhan(
        self,
        user_id:   str,
        config_id: str,
        client_id: str,
        creds:     dict,
        access_token: str,
    ) -> BrokerAccountSession:
        """Register (or refresh) a live Dhan session."""
        log = _make_log(user_id, "dhan", client_id)
        with self._lock:
            user_map = self._sessions.setdefault(user_id, {})
            sess = user_map.get(config_id)
            if sess is None:
                sess = BrokerAccountSession(user_id, config_id, "dhan", client_id)
                user_map[config_id] = sess
                log.info("New Dhan BrokerAccountSession created")
            else:
                log.info("Refreshing existing Dhan BrokerAccountSession")

        ok = sess.inject_dhan_token(creds, access_token)
        if ok:
            log.info("Dhan session registered for config=%s", config_id)
        else:
            log.error("Dhan session injection FAILED for config=%s — %s", config_id, sess.error)
        return sess

    def register_groww(
        self,
        user_id:   str,
        config_id: str,
        client_id: str,
        creds:     dict,
        access_token: str,
    ) -> BrokerAccountSession:
        """Register (or refresh) a live Groww session."""
        log = _make_log(user_id, "groww", client_id)
        with self._lock:
            user_map = self._sessions.setdefault(user_id, {})
            sess = user_map.get(config_id)
            if sess is None:
                sess = BrokerAccountSession(user_id, config_id, "groww", client_id)
                user_map[config_id] = sess
                log.info("New Groww BrokerAccountSession created")
            else:
                log.info("Refreshing existing Groww BrokerAccountSession")

        ok = sess.inject_groww_token(creds, access_token)
        if ok:
            log.info("Groww session registered for config=%s", config_id)
        else:
            log.error("Groww session injection FAILED for config=%s — %s", config_id, sess.error)
        return sess

    def register_upstox(
        self,
        user_id:   str,
        config_id: str,
        client_id: str,
        creds:     dict,
        access_token: str,
    ) -> BrokerAccountSession:
        """Register (or refresh) a live Upstox session."""
        log = _make_log(user_id, "upstox", client_id)
        with self._lock:
            user_map = self._sessions.setdefault(user_id, {})
            sess = user_map.get(config_id)
            if sess is None:
                sess = BrokerAccountSession(user_id, config_id, "upstox", client_id)
                user_map[config_id] = sess
                log.info("New Upstox BrokerAccountSession created")
            else:
                log.info("Refreshing existing Upstox BrokerAccountSession")

        ok = sess.inject_upstox_token(creds, access_token)
        if ok:
            log.info("Upstox session registered for config=%s", config_id)
        else:
            log.error("Upstox session injection FAILED for config=%s — %s", config_id, sess.error)
        return sess

    def register_kite(
        self,
        user_id:   str,
        config_id: str,
        client_id: str,
        creds:     dict,
        api_key:   str,
        access_token: str,
    ) -> BrokerAccountSession:
        """Register (or refresh) a live Zerodha Kite session."""
        log = _make_log(user_id, "zerodha", client_id)
        with self._lock:
            user_map = self._sessions.setdefault(user_id, {})
            sess = user_map.get(config_id)
            if sess is None:
                sess = BrokerAccountSession(user_id, config_id, "zerodha", client_id)
                user_map[config_id] = sess
                log.info("New Kite BrokerAccountSession created")
            else:
                log.info("Refreshing existing Kite BrokerAccountSession")

        ok = sess.inject_kite_token(creds, api_key, access_token)
        if ok:
            log.info("Kite session registered for config=%s", config_id)
        else:
            log.error("Kite session injection FAILED for config=%s — %s", config_id, sess.error)
        return sess

    def deregister(self, user_id: str, config_id: str) -> None:
        """Disconnect and remove a specific account session."""
        with self._lock:
            user_map = self._sessions.get(user_id, {})
            sess = user_map.pop(config_id, None)
        if sess:
            sess.disconnect()
            _make_log(user_id, sess.broker_id, sess.client_id).info(
                "Session deregistered config=%s", config_id
            )

    def deregister_all(self, user_id: str) -> None:
        """Disconnect all sessions for a user (e.g., user logout)."""
        with self._lock:
            user_map = self._sessions.pop(user_id, {})
        for sess in user_map.values():
            sess.disconnect()

    # ── Lookup ────────────────────────────────────────────────────────────────

    def get_session(
        self, user_id: str, config_id: str
    ) -> Optional[BrokerAccountSession]:
        with self._lock:
            return self._sessions.get(user_id, {}).get(config_id)

    def list_sessions(self, user_id: str) -> List[BrokerAccountSession]:
        """All active sessions for a user (any mode)."""
        with self._lock:
            return list(self._sessions.get(user_id, {}).values())

    def list_live_sessions(self, user_id: str) -> List[BrokerAccountSession]:
        """Only live/paper sessions (not demo)."""
        return [s for s in self.list_sessions(user_id) if not s.is_demo]

    def get_primary_session(self, user_id: str) -> Optional[BrokerAccountSession]:
        """
        Return the first live session for a user, or None.
        Used when no specific config_id is given (e.g., legacy API calls).
        """
        for sess in self.list_sessions(user_id):
            if sess.is_live or sess.is_paper:
                return sess
        return None

    def get_all_sessions(self) -> List[BrokerAccountSession]:
        """All sessions across all users (for admin monitoring)."""
        result = []
        with self._lock:
            for user_map in self._sessions.values():
                result.extend(user_map.values())
        return result

    # ── Aggregated data ───────────────────────────────────────────────────────

    def get_positions(self, user_id: str, config_id: Optional[str] = None) -> list:
        """
        Get positions for one account (config_id given) or ALL accounts.
        Each position row has 'account_id' and 'broker_id' added.
        """
        if config_id:
            sess = self.get_session(user_id, config_id)
            if not sess:
                return []
            rows = sess.get_positions()
            return [dict(r, account_id=config_id, broker_id=sess.broker_id,
                         client_id=sess.client_id) for r in rows]

        # Aggregate across all live accounts
        all_positions = []
        for sess in self.list_live_sessions(user_id):
            try:
                rows = sess.get_positions()
                all_positions.extend(
                    dict(r, account_id=sess.config_id, broker_id=sess.broker_id,
                         client_id=sess.client_id)
                    for r in rows
                )
            except Exception as e:
                logger.warning(
                    "get_positions failed for config=%s: %s", sess.config_id, e
                )
        return all_positions

    def get_order_book(self, user_id: str, config_id: Optional[str] = None) -> list:
        if config_id:
            sess = self.get_session(user_id, config_id)
            if not sess:
                return []
            return sess.get_order_book()

        all_orders = []
        for sess in self.list_live_sessions(user_id):
            try:
                rows = sess.get_order_book()
                all_orders.extend(
                    dict(r, account_id=sess.config_id, broker_id=sess.broker_id)
                    for r in rows
                )
            except Exception as e:
                logger.warning("get_order_book failed for config=%s: %s", sess.config_id, e)
        return all_orders

    def get_funds(self, user_id: str, config_id: Optional[str] = None) -> list:
        """Return funds/margin for one or all accounts."""
        sessions = (
            [self.get_session(user_id, config_id)]
            if config_id
            else self.list_live_sessions(user_id)
        )
        result = []
        for sess in sessions:
            if sess is None:
                continue
            try:
                funds = sess.get_limits()
                if funds:
                    result.append(dict(funds, account_id=sess.config_id,
                                       broker_id=sess.broker_id, client_id=sess.client_id))
            except Exception as e:
                logger.warning("get_funds failed for config=%s: %s", sess.config_id, e)
        return result

    def get_holdings(self, user_id: str, config_id: Optional[str] = None) -> list:
        """Return holdings for one or all accounts."""
        sessions = (
            [self.get_session(user_id, config_id)]
            if config_id
            else self.list_live_sessions(user_id)
        )
        result = []
        for sess in sessions:
            if sess is None:
                continue
            try:
                rows = sess.get_holdings()
                result.extend(
                    dict(r, account_id=sess.config_id, broker_id=sess.broker_id,
                         client_id=sess.client_id)
                    for r in rows
                )
            except Exception as e:
                logger.warning("get_holdings failed for config=%s: %s", sess.config_id, e)
        return result

    def get_tradebook(self, user_id: str, config_id: Optional[str] = None) -> list:
        """Return tradebook entries for one or all accounts."""
        sessions = (
            [self.get_session(user_id, config_id)]
            if config_id
            else self.list_live_sessions(user_id)
        )
        result = []
        for sess in sessions:
            if sess is None:
                continue
            try:
                rows = sess.get_tradebook()
                result.extend(
                    dict(r, account_id=sess.config_id, broker_id=sess.broker_id,
                         client_id=sess.client_id)
                    for r in rows
                )
            except Exception as e:
                logger.warning("get_tradebook failed for config=%s: %s", sess.config_id, e)
        return result

    # ── Order execution ───────────────────────────────────────────────────────

    def execute_order(
        self,
        user_id:   str,
        config_id: str,
        order:     dict,
    ) -> dict:
        """Route an order to the specified broker account."""
        sess = self.get_session(user_id, config_id)
        if not sess:
            return {
                "success": False,
                "message": f"No active session for account {config_id}. Please connect first.",
            }
        return sess.place_order(order)


# ── Module-level singleton ────────────────────────────────────────────────────
registry = MultiAccountRegistry()
