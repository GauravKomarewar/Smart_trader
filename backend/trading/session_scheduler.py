"""
Smart Trader — Daily Session Scheduler (SEBI Compliance)
=========================================================

SEBI guideline: broker sessions must be freshly authenticated daily.

Schedule:
  08:45 IST — Auto-login: connect ALL active broker configs for ALL users
  23:55 IST — Auto-logout: disconnect ALL live sessions for ALL users

Auto-login is supported for brokers with TOTP-based credentials:
  • Shoonya — OAuth via headless Firefox + TOTP
  • Fyers   — Direct API login + TOTP + PIN
  • Angel One — SmartAPI REST + TOTP

Brokers with pre-generated tokens (Dhan, Groww, Upstox, Zerodha) are also
re-verified during auto-login — if the stored token still works, the session
is restored; otherwise it is marked as needing manual re-auth.

Paper trading sessions are always restored immediately.
"""

import logging
import os
import threading
import time
from datetime import date, datetime, timezone, timedelta

logger = logging.getLogger("smart_trader.session_scheduler")

# IST offset (UTC+5:30)
_IST = timezone(timedelta(hours=5, minutes=30))

# Schedule times (IST)
_LOGIN_HOUR, _LOGIN_MIN = 8, 45
_LOGOUT_HOUR, _LOGOUT_MIN = 23, 55

# ── Standalone helpers for 3-tier Shoonya login ───────────────────────────────

_TOKEN_CACHE_FILE = "/home/ubuntu/.shoonya_token.json"
_TOKEN_MAX_AGE_HOURS = 5


def _load_shoonya_cached_token(user_id: str) -> str | None:
    """Load a previously-saved Shoonya token from the shared cache file."""
    import json as _json
    try:
        if not os.path.exists(_TOKEN_CACHE_FILE):
            return None
        with open(_TOKEN_CACHE_FILE) as f:
            data = _json.load(f)
        if data.get("user_id") != user_id:
            return None
        saved_at = datetime.fromisoformat(data["timestamp"])
        age = datetime.now() - saved_at
        if age > timedelta(hours=_TOKEN_MAX_AGE_HOURS):
            logger.info("Cached Shoonya token expired (%.1fh old)", age.total_seconds() / 3600)
            return None
        token = data.get("token", "")
        if not token:
            return None
        # Validate with lightweight API call
        if _validate_shoonya_token(user_id, token):
            logger.info("Cached Shoonya token is VALID (%.1fh old, source=%s)",
                        age.total_seconds() / 3600, data.get("source", "unknown"))
            return token
        logger.info("Cached Shoonya token INVALID (session expired)")
        return None
    except Exception as e:
        logger.debug("Could not load cached Shoonya token: %s", e)
        return None


def _validate_shoonya_token(user_id: str, token: str) -> bool:
    """Quick call to Limits endpoint to check if session token is alive."""
    import json as _json, requests
    try:
        payload = _json.dumps({"uid": user_id, "actid": user_id})
        for host in ["https://api.shoonya.com/NorenWClientAPI", "https://trade.shoonya.com/NorenWClientAPI"]:
            try:
                resp = requests.post(
                    f"{host}/Limits",
                    data="jData=" + payload + f"&jKey={token}",
                    timeout=8,
                )
                if resp.status_code == 200:
                    result = _json.loads(resp.text)
                    if result.get("stat") == "Ok":
                        return True
                    if "session" in str(result.get("emsg", "")).lower():
                        return False
                    return result.get("stat") != "Not_Ok"
            except Exception:
                continue
        return False
    except Exception:
        return False


def _save_shoonya_cached_token(user_id: str, token: str, source: str = "unknown"):
    """Save a Shoonya token to shared cache file for cross-platform use."""
    import json as _json
    data = {
        "user_id": user_id,
        "token": token,
        "timestamp": datetime.now().isoformat(),
        "source": source,
    }
    with open(_TOKEN_CACHE_FILE, "w") as f:
        _json.dump(data, f)


def _shoonya_quick_auth(creds: dict, log) -> str | None:
    """Direct REST API login via QuickAuth (no browser needed)."""
    import hashlib, json as _json, requests, pyotp

    user_id   = creds["USER_ID"]
    password  = creds["PASSWORD"]
    totp_key  = creds["TOKEN"]
    vc        = creds["VC"]
    app_key   = creds["APP_KEY"]
    imei      = creds.get("IMEI", "abc1234")

    pwd_hash  = hashlib.sha256(password.encode()).hexdigest()
    appkey_hash = hashlib.sha256(f"{user_id}|{app_key}".encode()).hexdigest()

    hosts = [
        "https://api.shoonya.com/NorenWClientAPI",
        "https://trade.shoonya.com/NorenWClientAPI",
    ]

    for attempt in range(1, 3):
        otp_val = pyotp.TOTP(totp_key).now()
        payload = {
            "source": "API",
            "apkversion": "1.0.0",
            "uid": user_id,
            "pwd": pwd_hash,
            "factor2": otp_val,
            "vc": vc,
            "appkey": appkey_hash,
            "imei": imei,
        }
        for host in hosts:
            url = f"{host}/QuickAuth"
            try:
                resp = requests.post(url, data="jData=" + _json.dumps(payload), timeout=15)
                if resp.status_code == 200:
                    result = _json.loads(resp.text)
                    if result.get("stat") == "Ok":
                        return result.get("susertoken", "")
                    emsg = result.get("emsg", "")
                    if any(k in emsg.lower() for k in ["invalid", "credential", "blocked"]):
                        log.warning("QuickAuth rejected: %s", emsg)
                        return None
            except Exception:
                continue
        if attempt < 2:
            time.sleep(5)
    return None


class SessionScheduler:
    """
    Daemon thread that runs auto-login at 08:45 IST and auto-logout at 23:55 IST.
    Uses minute-granularity polling (checks every 30s).
    """

    def __init__(self, poll_seconds: float = 30.0):
        self._poll = poll_seconds
        self._running = False
        self._thread: threading.Thread | None = None
        self._last_login_date: date | None = None
        self._last_logout_date: date | None = None

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="SessionScheduler"
        )
        self._thread.start()
        logger.info(
            "SessionScheduler started — login@%02d:%02d IST, logout@%02d:%02d IST",
            _LOGIN_HOUR, _LOGIN_MIN, _LOGOUT_HOUR, _LOGOUT_MIN,
        )

    def stop(self):
        self._running = False

    def _now_ist(self) -> datetime:
        return datetime.now(_IST)

    def _loop(self):
        while self._running:
            try:
                self._tick()
            except Exception:
                logger.exception("SessionScheduler tick error")
            time.sleep(self._poll)

    def _tick(self):
        now = self._now_ist()
        today = now.date()

        # Auto-login at 08:45 IST (or catch-up if restarted after 08:45)
        login_time_passed = (
            now.hour > _LOGIN_HOUR
            or (now.hour == _LOGIN_HOUR and now.minute >= _LOGIN_MIN)
        )
        logout_time_passed = (
            now.hour > _LOGOUT_HOUR
            or (now.hour == _LOGOUT_HOUR and now.minute >= _LOGOUT_MIN)
        )
        if (
            self._last_login_date != today
            and login_time_passed
            and not logout_time_passed
        ):
            logger.info("DAILY AUTO-LOGIN triggered at %s IST", now.strftime("%H:%M:%S"))
            try:
                self._auto_login_all()
            except Exception:
                logger.exception("Auto-login failed")
            self._last_login_date = today

        # Auto-logout at 23:55 IST
        if (
            self._last_logout_date != today
            and now.hour == _LOGOUT_HOUR
            and now.minute >= _LOGOUT_MIN
        ):
            logger.info("DAILY AUTO-LOGOUT triggered at %s IST", now.strftime("%H:%M:%S"))
            try:
                self._auto_logout_all()
            except Exception:
                logger.exception("Auto-logout failed")
            self._last_logout_date = today

    # ── Auto-Login ────────────────────────────────────────────────────────────

    def _auto_login_all(self):
        """
        For every user with active broker configs, attempt auto-login.
        Runs sequentially per config to avoid resource contention (Firefox etc.).
        """
        from db.database import SessionLocal, BrokerConfig, BrokerSession
        from core.security import decrypt_credentials

        db = SessionLocal()
        success_count = 0
        fail_count = 0
        skip_count = 0

        try:
            configs = (
                db.query(BrokerConfig)
                .filter(BrokerConfig.is_active == True)
                .all()
            )
            logger.info("Auto-login: found %d active broker configs", len(configs))

            for cfg in configs:
                broker_id = cfg.broker_id
                client_id = cfg.client_id or "?"
                user_id = cfg.user_id
                log = _make_log(user_id, broker_id, client_id)

                try:
                    creds = decrypt_credentials(str(cfg.credentials))
                except Exception as e:
                    log.warning("Cannot decrypt creds: %s — skipping", e)
                    fail_count += 1
                    continue

                try:
                    if broker_id == "shoonya":
                        self._login_shoonya(cfg, creds, user_id, db, log)
                        success_count += 1
                    elif broker_id == "fyers":
                        self._login_fyers(cfg, creds, user_id, db, log)
                        success_count += 1
                    elif broker_id == "angel":
                        self._login_angelone(cfg, creds, user_id, db, log)
                        success_count += 1
                    elif broker_id == "paper_trade":
                        self._login_paper(cfg, creds, user_id, db, log)
                        success_count += 1
                    elif broker_id in ("dhan", "groww", "upstox", "zerodha"):
                        self._login_token_broker(cfg, creds, user_id, db, log)
                        success_count += 1
                    else:
                        log.info("No auto-login for broker_id=%s — skipping", broker_id)
                        skip_count += 1
                except Exception as e:
                    log.error("Auto-login FAILED: %s", e, exc_info=True)
                    fail_count += 1

            logger.info(
                "Auto-login complete: %d success, %d failed, %d skipped (of %d total)",
                success_count, fail_count, skip_count, len(configs),
            )
        finally:
            db.close()

    def _login_shoonya(self, cfg, creds: dict, user_id: str, db, log):
        """Shoonya: 3-tier login — cached token → direct API → OAuth browser."""
        import os as _os

        client_id = creds.get("USER_ID", cfg.client_id or "")

        required = ["USER_ID", "PASSWORD", "TOKEN", "VC", "APP_KEY", "OAUTH_SECRET"]
        missing = [f for f in required if not creds.get(f)]
        if missing:
            raise RuntimeError(f"Missing Shoonya fields: {', '.join(missing)}")

        token = None

        # ── Tier 0: Try cached token from shared file ────────────────────────
        try:
            cached = _load_shoonya_cached_token(creds["USER_ID"])
            if cached:
                log.info("Using cached Shoonya token from shared file")
                token = cached
        except Exception as e:
            log.debug("Cached token not available: %s", e)

        # ── Tier 1: Try direct QuickAuth API ─────────────────────────────────
        if not token:
            try:
                direct = _shoonya_quick_auth(creds, log)
                if direct:
                    log.info("Shoonya direct API login OK")
                    token = direct
            except Exception as e:
                log.warning("Direct API login failed: %s — falling back to OAuth", e)

        # ── Tier 2: Full OAuth browser login (last resort) ───────────────────
        if not token:
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
            raise RuntimeError("Shoonya login failed — all 3 methods exhausted")

        log.info("Shoonya auto-login OK — token length=%d", len(token))
        self._register_and_persist(
            cfg, creds, user_id, db, log,
            broker_id="shoonya", client_id=client_id, token=token,
        )
        # Save to shared cache file for cross-platform token sharing
        try:
            _save_shoonya_cached_token(client_id, token, source="session_scheduler")
            log.info("Saved Shoonya token to shared cache file")
        except Exception as e:
            log.debug("Could not save Shoonya token to cache: %s", e)

    def _login_fyers(self, cfg, creds: dict, user_id: str, db, log):
        """Fyers: Direct API TOTP login."""
        from routers.broker_sessions import _fyers_direct_login

        client_id = creds.get("CLIENT_ID", cfg.client_id or "")

        required = ["CLIENT_ID", "APP_ID", "SECRET_KEY", "TOTP_KEY", "PIN"]
        missing = [f for f in required if not creds.get(f)]
        if missing:
            raise RuntimeError(f"Missing Fyers fields: {', '.join(missing)}")

        token = _fyers_direct_login(creds, log)
        if not token or len(token) < 10:
            raise RuntimeError("Fyers login returned empty token")

        log.info("Fyers auto-login OK — token length=%d", len(token))
        self._register_and_persist(
            cfg, creds, user_id, db, log,
            broker_id="fyers", client_id=client_id, token=token,
        )

    def _login_angelone(self, cfg, creds: dict, user_id: str, db, log):
        """Angel One: SmartAPI REST TOTP login."""
        from routers.broker_sessions import _angelone_direct_login

        client_id = creds.get("CLIENT_ID", cfg.client_id or "")
        pin = creds.get("PIN", "")
        api_key = creds.get("API_KEY", "")
        totp_secret = creds.get("TOTP_SECRET", "")

        required = ["CLIENT_ID", "PIN", "API_KEY", "TOTP_SECRET"]
        missing = [f for f in required if not creds.get(f)]
        if missing:
            raise RuntimeError(f"Missing Angel One fields: {', '.join(missing)}")

        jwt_token = _angelone_direct_login(client_id, pin, api_key, totp_secret, log)
        if not jwt_token or len(jwt_token) < 10:
            raise RuntimeError("Angel One login returned empty token")

        log.info("Angel One auto-login OK — token length=%d", len(jwt_token))
        self._register_and_persist(
            cfg, creds, user_id, db, log,
            broker_id="angel", client_id=client_id, token=jwt_token,
        )

    def _login_paper(self, cfg, creds: dict, user_id: str, db, log):
        """Paper trading: always succeeds."""
        from broker.multi_broker import registry
        from routers.broker_sessions import _upsert_session

        registry.register_paper(user_id=user_id, config_id=cfg.id, client_id="PAPER")
        _upsert_session(db, cfg, user_id, mode="paper", token=None, logged_in=True)
        log.info("Paper trading auto-login OK")

    def _login_token_broker(self, cfg, creds: dict, user_id: str, db, log):
        """
        Dhan / Groww / Upstox / Zerodha: restore from stored ACCESS_TOKEN.
        These brokers use pre-generated tokens that we can't auto-renew.
        We verify the token still works and restore the session.
        """
        from db.database import BrokerSession as DBSession

        broker_id = cfg.broker_id
        client_id = creds.get("CLIENT_ID", cfg.client_id or "")
        access_token = creds.get("ACCESS_TOKEN", "")

        # For Zerodha, also need API_KEY
        api_key = creds.get("API_KEY", "")

        if not access_token:
            # Check for stored session token from DB
            db_sess = db.query(DBSession).filter(DBSession.config_id == cfg.id).first()
            if db_sess and db_sess.session_token:
                access_token = db_sess.session_token
            else:
                raise RuntimeError(f"No ACCESS_TOKEN for {broker_id} — requires manual auth")

        # Register in multi-broker registry
        self._register_and_persist(
            cfg, creds, user_id, db, log,
            broker_id=broker_id, client_id=client_id, token=access_token,
            api_key=api_key if broker_id == "zerodha" else None,
        )
        log.info("%s auto-login OK (token restored)", broker_id)

    def _register_and_persist(
        self, cfg, creds: dict, user_id: str, db, log,
        broker_id: str, client_id: str, token: str,
        api_key: str | None = None,
    ):
        """Register session in registry, persist to DB, init risk manager."""
        from broker.multi_broker import registry
        from routers.broker_sessions import _upsert_session
        from broker.account_risk import get_account_risk

        if broker_id == "shoonya":
            sess = registry.register_shoonya(
                user_id=user_id, config_id=cfg.id,
                client_id=client_id, creds=creds, token=token,
            )
        elif broker_id == "fyers":
            sess = registry.register_fyers(
                user_id=user_id, config_id=cfg.id,
                client_id=client_id, creds=creds, token=token,
            )
        elif broker_id == "angel":
            sess = registry.register_angelone(
                user_id=user_id, config_id=cfg.id,
                client_id=client_id, creds=creds, jwt_token=token,
            )
        elif broker_id == "dhan":
            sess = registry.register_dhan(
                user_id=user_id, config_id=cfg.id,
                client_id=client_id, creds=creds, access_token=token,
            )
        elif broker_id == "groww":
            sess = registry.register_groww(
                user_id=user_id, config_id=cfg.id,
                client_id=client_id, creds=creds, access_token=token,
            )
        elif broker_id == "upstox":
            sess = registry.register_upstox(
                user_id=user_id, config_id=cfg.id,
                client_id=client_id, creds=creds, access_token=token,
            )
        elif broker_id == "zerodha":
            sess = registry.register_kite(
                user_id=user_id, config_id=cfg.id,
                client_id=client_id, creds=creds,
                api_key=api_key or creds.get("API_KEY", ""),
                access_token=token,
            )
        else:
            raise RuntimeError(f"Unknown broker_id for register: {broker_id}")

        if hasattr(sess, "error") and sess.error:
            raise RuntimeError(f"Session registration failed: {sess.error}")

        _upsert_session(db, cfg, user_id, mode="live", token=token, logged_in=True)

        try:
            get_account_risk(
                user_id=user_id, config_id=cfg.id,
                broker_id=broker_id, client_id=client_id,
            )
        except Exception as e:
            log.warning("Risk manager init (non-fatal): %s", e)

    # ── Auto-Logout ───────────────────────────────────────────────────────────

    def _auto_logout_all(self):
        """
        Disconnect ALL live sessions for ALL users.
        Clears in-memory registry and marks DB sessions as logged out.
        """
        from db.database import SessionLocal, BrokerSession as DBSession
        from broker.multi_broker import registry

        # 1. Clear all in-memory sessions
        all_sessions = registry.get_all_sessions()
        deregistered = 0
        for sess in all_sessions:
            try:
                registry.deregister(sess.user_id, sess.config_id)
                deregistered += 1
            except Exception as e:
                logger.warning(
                    "Deregister failed user=%s config=%s: %s",
                    sess.user_id[:8], sess.config_id[:8], e,
                )

        # 2. Also deregister from legacy singleton
        try:
            from broker.shoonya_client import deregister_user_session
            # Clear all known users
            for sess in all_sessions:
                try:
                    deregister_user_session(sess.user_id)
                except Exception:
                    pass
        except Exception:
            pass

        # 3. Mark ALL active sessions as logged out in DB
        db = SessionLocal()
        try:
            active = db.query(DBSession).filter(DBSession.is_logged_in == True).all()
            for db_sess in active:
                db_sess.is_logged_in = False
                db_sess.session_token = None
                db_sess.last_heartbeat = datetime.now(timezone.utc)
            db.commit()
            logger.info(
                "Auto-logout complete: %d deregistered from registry, %d marked logged-out in DB",
                deregistered, len(active),
            )
        finally:
            db.close()

    # ── Manual trigger (for admin API) ────────────────────────────────────────

    def trigger_login(self):
        """Manually trigger auto-login (e.g. from admin endpoint)."""
        logger.info("Manual auto-login triggered")
        self._auto_login_all()

    def trigger_logout(self):
        """Manually trigger auto-logout (e.g. from admin endpoint)."""
        logger.info("Manual auto-logout triggered")
        self._auto_logout_all()


def _make_log(user_id: str, broker_id: str, client_id: str) -> logging.LoggerAdapter:
    return logging.LoggerAdapter(
        logger,
        {"user_id": user_id[:8], "broker": broker_id, "client": client_id},
    )


# Module-level singleton
session_scheduler = SessionScheduler()
