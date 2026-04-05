"""
Smart Trader — Broker Session Router
======================================

Manages per-user broker accounts:
  • Add / update / delete broker configurations (credentials encrypted at rest)
  • Connect / disconnect from a broker (OAuth login → live session)
  • List all broker sessions with real-time status

Each user can add MULTIPLE broker accounts (Shoonya, Zerodha, Fyers, etc.).
Credentials are stored encrypted using Fernet AES-256-GCM.
Live sessions are managed by broker.multi_broker.MultiAccountRegistry.
Per-account risk limits are managed by broker.account_risk.AccountRiskManager.

Connect flow (Shoonya):
  1. Decrypt credentials from DB (Fernet)
  2. Temporarily export creds to env vars
  3. Call broker.oauth_login.run_oauth_login() (headless Firefox TOTP login)
  4. Receive session token
  5. Register in MultiAccountRegistry (in-memory)
  6. Save token + status to BrokerSession DB row
  7. Initialise AccountRiskManager for this account

Error handling:
  • CREDENTIALS_STALE → 422 with clear message to re-save
  • OAuth failure      → 502 with exact broker error message
  • All steps logged with [user=... broker=... client=...] context
"""

import logging
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from db.database import get_db, BrokerConfig, BrokerSession, AuditLog
from core.security import encrypt_credentials, decrypt_credentials, generate_id
from core.deps import current_user

logger = logging.getLogger("smart_trader.broker_sessions")
router = APIRouter(prefix="/broker", tags=["broker"])


# ── Broker-contextual logger ──────────────────────────────────────────────────

def _blog(user_id: str, broker_id: str, client_id: str = "?") -> logging.LoggerAdapter:
    """Return a logger prefixed with [user=... broker=... client=...] context."""
    return logging.LoggerAdapter(
        logging.getLogger("smart_trader.broker.connect"),
        {"user_id": user_id[:8], "broker": broker_id, "client": client_id},
    )


# ── Broker field definitions ───────────────────────────────────────────────────

BROKER_FIELD_DEFS = {
    "shoonya": [
        {"key": "USER_ID",      "label": "User ID / Client ID",    "type": "text",     "required": True},
        {"key": "PASSWORD",     "label": "Login Password",          "type": "password", "required": True},
        {"key": "TOKEN",        "label": "TOTP Secret Key",         "type": "password", "required": True,  "hint": "Base32 TOTP secret from Shoonya backoffice"},
        {"key": "VC",           "label": "Vendor Code",             "type": "text",     "required": True,  "hint": "e.g. FA14667_U"},
        {"key": "APP_KEY",      "label": "App Key (API Key)",       "type": "password", "required": True},
        {"key": "OAUTH_SECRET", "label": "OAuth Secret",            "type": "password", "required": True},
        {"key": "IMEI",         "label": "IMEI / Device ID",        "type": "text",     "required": False, "default": "abc1234"},
    ],
    "zerodha": [
        {"key": "CLIENT_ID",    "label": "Client ID",               "type": "text",     "required": True},
        {"key": "API_KEY",      "label": "API Key",                  "type": "password", "required": True},
        {"key": "API_SECRET",   "label": "API Secret",               "type": "password", "required": True},
        {"key": "TOTP_SECRET",  "label": "TOTP Secret",              "type": "password", "required": False},
    ],
    "fyers": [
        {"key": "CLIENT_ID",    "label": "Client ID",               "type": "text",     "required": True},
        {"key": "APP_ID",       "label": "App ID",                   "type": "text",     "required": True},
        {"key": "SECRET_KEY",   "label": "Secret Key",               "type": "password", "required": True},
        {"key": "TOTP_KEY",     "label": "TOTP Secret",              "type": "password", "required": False},
        {"key": "PIN",          "label": "PIN",                      "type": "password", "required": False},
    ],
    "angel": [
        {"key": "CLIENT_ID",    "label": "Client ID",               "type": "text",     "required": True},
        {"key": "API_KEY",      "label": "API Key",                  "type": "password", "required": True},
        {"key": "TOTP_SECRET",  "label": "TOTP Secret",              "type": "password", "required": True},
    ],
    "upstox": [
        {"key": "CLIENT_ID",    "label": "Client ID",               "type": "text",     "required": True},
        {"key": "API_KEY",      "label": "API Key",                  "type": "password", "required": True},
        {"key": "API_SECRET",   "label": "API Secret",               "type": "password", "required": True},
    ],
    "dhan": [
        {"key": "CLIENT_ID",    "label": "Client ID",               "type": "text",     "required": True},
        {"key": "ACCESS_TOKEN", "label": "Access Token",             "type": "password", "required": True},
    ],
    "paper_trade": [
        {"key": "STARTING_CAPITAL", "label": "Starting Capital (₹)", "type": "number", "required": True, "default": "1000000"},
    ],
}

BROKER_NAMES = {
    "shoonya":     "Shoonya / Finvasia",
    "zerodha":     "Zerodha",
    "fyers":       "Fyers",
    "angel":       "Angel One",
    "upstox":      "Upstox",
    "dhan":        "Dhan",
    "paper_trade": "Paper Trade",
}


# ── Pydantic schemas ───────────────────────────────────────────────────────────

class AddBrokerRequest(BaseModel):
    broker_id:   str
    credentials: dict     # raw key/value dict; encrypted server-side


class UpdateBrokerRequest(BaseModel):
    credentials: dict


class RiskConfigUpdate(BaseModel):
    max_daily_loss:        Optional[float] = None
    max_order_value:       Optional[float] = None
    max_qty_per_order:     Optional[int]   = None
    max_open_positions:    Optional[int]   = None
    warning_threshold_pct: Optional[float] = None


# ── Helpers ────────────────────────────────────────────────────────────────────

def _config_to_out(cfg: BrokerConfig) -> dict:
    sess_data = None
    if cfg.session:
        s = cfg.session
        sess_data = {
            "is_logged_in":  s.is_logged_in,
            "mode":          s.mode,
            "login_at":      s.login_at.isoformat() if s.login_at else None,
            "last_heartbeat": s.last_heartbeat.isoformat() if s.last_heartbeat else None,
        }
    return {
        "id":          cfg.id,
        "broker_id":   cfg.broker_id,
        "broker_name": cfg.broker_name,
        "client_id":   cfg.client_id,
        "is_active":   cfg.is_active,
        "created_at":  cfg.created_at.isoformat() if cfg.created_at is not None else None,
        "session":     sess_data,
    }


def _safe_decrypt(cfg: BrokerConfig, db: Optional[Session] = None) -> dict:
    """
    Decrypt broker credentials.

    * If credentials decrypt successfully but are in the old XOR format,
      they are automatically re-encrypted with Fernet and saved to DB so the
      user never needs to manually re-save (transparent migration).
    * Raises HTTP 422 only if decryption truly fails with ALL known keys.
    """
    try:
        creds = decrypt_credentials(str(cfg.credentials))
    except ValueError as e:
        if "CREDENTIALS_STALE" in str(e):
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Broker credentials for {cfg.broker_name} ({cfg.client_id}) "
                    "could not be decrypted with any known key. "
                    "Please click Edit and re-save your credentials."
                ),
            )
        raise HTTPException(status_code=500, detail=f"Failed to read credentials: {e}")

    # Auto-migrate: if stored in old XOR format, silently re-encrypt with Fernet
    if db is not None and not cfg.credentials.startswith("fernet:"):
        try:
            from core.security import encrypt_credentials
            cfg.credentials = encrypt_credentials(creds)
            db.commit()
            logger.debug("Auto-migrated credentials to Fernet for config_id=%s", cfg.id[:8])
        except Exception as _me:
            logger.warning("Auto-migration of credentials failed: %s", _me)

    return creds


def _audit(db: Session, user_id: str, action: str, resource: str,
           status: str = "success", details: str = None):
    """Write a row to audit_log for this sensitive operation."""
    try:
        row = AuditLog(
            user_id=user_id,
            action=action,
            resource=resource,
            status=status,
            details=details,
        )
        db.add(row)
        db.commit()
    except Exception as e:
        logger.warning("audit log write failed: %s", e)


# ── Routes ─────────────────────────────────────────────────────────────────────

@router.get("/fields/{broker_id}")
def get_broker_fields(broker_id: str):
    """Return credential field definitions for a broker."""
    fields = BROKER_FIELD_DEFS.get(broker_id)
    if not fields:
        raise HTTPException(404, f"Unknown broker: {broker_id}")
    return {"broker_id": broker_id, "broker_name": BROKER_NAMES.get(broker_id, broker_id),
            "fields": fields}


@router.get("/supported")
def list_supported_brokers():
    return [
        {"id": bid, "name": name, "fields": len(BROKER_FIELD_DEFS.get(bid, []))}
        for bid, name in BROKER_NAMES.items()
    ]


@router.get("/configs")
def list_broker_configs(
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """List all broker configs for the current user (with live session status)."""
    user_id = payload["sub"]
    configs = db.query(BrokerConfig).filter(BrokerConfig.user_id == user_id).all()

    # Enrich with in-memory live session status
    from broker.multi_broker import registry
    result = []
    for cfg in configs:
        out = _config_to_out(cfg)
        live_sess = registry.get_session(user_id, cfg.id)
        if live_sess:
            out["live_session"] = live_sess.to_dict()
        result.append(out)
    return result


@router.post("/configs")
def add_broker_config(
    body: AddBrokerRequest,
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """Add a new broker configuration for the current user."""
    user_id = payload["sub"]
    log = _blog(user_id, body.broker_id)

    if body.broker_id not in BROKER_NAMES:
        raise HTTPException(400, f"Unsupported broker: {body.broker_id}")

    # Check duplicate
    existing = db.query(BrokerConfig).filter(
        BrokerConfig.user_id == user_id,
        BrokerConfig.broker_id == body.broker_id,
    ).first()
    if existing:
        raise HTTPException(
            409,
            f"You already have a {body.broker_id} configuration. "
            "Update it or delete it before adding a new one.",
        )

    # Validate required fields
    required = [f["key"] for f in BROKER_FIELD_DEFS.get(body.broker_id, []) if f.get("required")]
    missing = [k for k in required if not body.credentials.get(k)]
    if missing:
        raise HTTPException(422, f"Missing required fields: {', '.join(missing)}")

    client_id = (
        body.credentials.get("USER_ID")
        or body.credentials.get("CLIENT_ID")
        or "N/A"
    )

    cfg = BrokerConfig(
        id          = generate_id(),
        user_id     = user_id,
        broker_id   = body.broker_id,
        broker_name = BROKER_NAMES[body.broker_id],
        client_id   = client_id,
        credentials = encrypt_credentials(body.credentials),
    )
    db.add(cfg)
    db.commit()
    db.refresh(cfg)

    log.info(
        "Broker config added — config_id=%s client=%s",
        cfg.id[:8], client_id,
    )
    _audit(db, user_id, "broker_config_add",
           f"{body.broker_id}:{client_id}",
           details=f"config_id={cfg.id}")

    return _config_to_out(cfg)


@router.put("/configs/{config_id}")
def update_broker_config(
    config_id: str,
    body: UpdateBrokerRequest,
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    user_id = payload["sub"]
    cfg = db.query(BrokerConfig).filter(
        BrokerConfig.id == config_id,
        BrokerConfig.user_id == user_id,
    ).first()
    if not cfg:
        raise HTTPException(404, "Broker config not found")

    old_client = cfg.client_id
    client_id = (
        body.credentials.get("USER_ID")
        or body.credentials.get("CLIENT_ID")
        or cfg.client_id
    )
    cfg.credentials = encrypt_credentials(body.credentials)
    cfg.client_id   = client_id
    cfg.updated_at  = datetime.now(timezone.utc)
    db.commit()
    db.refresh(cfg)

    _blog(user_id, cfg.broker_id, client_id).info(
        "Broker config updated — config_id=%s (was client=%s now client=%s)",
        config_id[:8], old_client, client_id,
    )
    return _config_to_out(cfg)


@router.delete("/configs/{config_id}")
def delete_broker_config(
    config_id: str,
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    user_id = payload["sub"]
    cfg = db.query(BrokerConfig).filter(
        BrokerConfig.id == config_id,
        BrokerConfig.user_id == user_id,
    ).first()
    if not cfg:
        raise HTTPException(404, "Broker config not found")

    log = _blog(user_id, cfg.broker_id, cfg.client_id)

    # Deregister from multi-broker registry
    try:
        from broker.multi_broker import registry
        registry.deregister(user_id, config_id)
        log.info("Live session deregistered before delete")
    except Exception as e:
        log.warning("Could not deregister live session: %s", e)

    db.delete(cfg)
    db.commit()

    log.info("Broker config deleted — config_id=%s", config_id[:8])
    _audit(db, user_id, "broker_config_delete", f"{cfg.broker_id}:{cfg.client_id}",
           details=f"config_id={config_id}")
    return {"success": True}


@router.post("/configs/{config_id}/connect")
def connect_broker(
    config_id: str,
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """
    Connect a broker account.

    Shoonya:    Full TOTP-based OAuth login via headless Firefox.
    Paper:      Activate paper trading immediately.
    Others:     Saved for manual token injection (future implementation).

    All steps are logged with full user/broker/client context.
    """
    user_id = payload["sub"]
    cfg = db.query(BrokerConfig).filter(
        BrokerConfig.id == config_id,
        BrokerConfig.user_id == user_id,
    ).first()
    if not cfg:
        raise HTTPException(404, "Broker config not found")

    # Decrypt credentials — auto-migrates XOR→Fernet transparently
    creds = _safe_decrypt(cfg, db)

    log = _blog(user_id, cfg.broker_id, cfg.client_id)
    log.info("Connect requested for config_id=%s", config_id[:8])

    if cfg.broker_id == "shoonya":
        return _connect_shoonya(cfg, creds, user_id, db, log)
    elif cfg.broker_id == "fyers":
        return _connect_fyers(cfg, creds, user_id, db, log)
    elif cfg.broker_id == "paper_trade":
        return _connect_paper(cfg, creds, user_id, db, log)
    else:
        # Placeholder for future broker implementations
        _upsert_session(db, cfg, user_id, mode="pending", token=None, logged_in=False)
        log.info("Broker %s saved — manual token required", cfg.broker_id)
        return {
            "success": True,
            "mode":    "pending",
            "message": (
                f"{cfg.broker_name} configuration saved. "
                "Automated login for this broker is not yet available — "
                "contact admin to configure session token manually."
            ),
        }


@router.post("/configs/{config_id}/disconnect")
def disconnect_broker(
    config_id: str,
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    user_id = payload["sub"]
    cfg = db.query(BrokerConfig).filter(
        BrokerConfig.id == config_id,
        BrokerConfig.user_id == user_id,
    ).first()
    if not cfg:
        raise HTTPException(404, "Broker config not found")

    log = _blog(user_id, cfg.broker_id, cfg.client_id)
    log.info("Disconnect requested for config_id=%s", config_id[:8])

    # 1. Remove from in-memory multi-broker registry
    try:
        from broker.multi_broker import registry
        registry.deregister(user_id, config_id)
        log.info("Removed from multi-broker registry")
    except Exception as e:
        log.warning("Multi-broker deregister error: %s", e)

    # 2. Remove from legacy singleton (backward compat)
    try:
        from broker.shoonya_client import deregister_user_session
        deregister_user_session(user_id)
    except Exception as e:
        log.debug("Legacy deregister_user_session: %s", e)

    # 3. Mark session as logged-out in DB
    sess = db.query(BrokerSession).filter(BrokerSession.config_id == config_id).first()
    if sess:
        sess.is_logged_in   = False
        sess.session_token  = None
        sess.last_heartbeat = datetime.now(timezone.utc)
        db.commit()

    log.info("Broker disconnected — config_id=%s", config_id[:8])
    _audit(db, user_id, "broker_disconnect", f"{cfg.broker_id}:{cfg.client_id}",
           details=f"config_id={config_id}")

    return {"success": True, "message": f"{cfg.broker_name} disconnected"}


@router.get("/configs/{config_id}/status")
def get_broker_status(
    config_id: str,
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """Get detailed session status for a single broker account."""
    user_id = payload["sub"]
    cfg = db.query(BrokerConfig).filter(
        BrokerConfig.id == config_id,
        BrokerConfig.user_id == user_id,
    ).first()
    if not cfg:
        raise HTTPException(404, "Broker config not found")

    from broker.multi_broker import registry
    from broker.account_risk import get_account_risk

    live_sess   = registry.get_session(user_id, config_id)
    db_sess     = db.query(BrokerSession).filter(BrokerSession.config_id == config_id).first()
    risk_status = get_account_risk(
        user_id   = user_id,
        config_id = config_id,
        broker_id = cfg.broker_id,
        client_id = cfg.client_id,
    ).get_status()

    return {
        "config":     _config_to_out(cfg),
        "live":       live_sess.to_dict() if live_sess else None,
        "db_session": {
            "is_logged_in":  db_sess.is_logged_in if db_sess else False,
            "mode":          db_sess.mode if db_sess else "demo",
            "login_at":      db_sess.login_at.isoformat() if db_sess and db_sess.login_at else None,
        } if db_sess else None,
        "risk":       risk_status,
    }


@router.get("/configs/{config_id}/risk")
def get_account_risk_status(
    config_id: str,
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """Get risk management status for a specific broker account."""
    user_id = payload["sub"]
    cfg = db.query(BrokerConfig).filter(
        BrokerConfig.id == config_id,
        BrokerConfig.user_id == user_id,
    ).first()
    if not cfg:
        raise HTTPException(404, "Broker config not found")

    from broker.account_risk import get_account_risk
    rm = get_account_risk(
        user_id=user_id, config_id=config_id,
        broker_id=cfg.broker_id, client_id=cfg.client_id
    )
    return rm.get_status()


@router.patch("/configs/{config_id}/risk")
def update_account_risk_config(
    config_id: str,
    body: RiskConfigUpdate,
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """
    Update risk parameters for a specific broker account.
    Only the fields provided in the request body are changed.
    """
    user_id = payload["sub"]
    cfg = db.query(BrokerConfig).filter(
        BrokerConfig.id == config_id,
        BrokerConfig.user_id == user_id,
    ).first()
    if not cfg:
        raise HTTPException(404, "Broker config not found")

    from broker.account_risk import get_account_risk, AccountRiskConfig
    rm = get_account_risk(
        user_id=user_id, config_id=config_id,
        broker_id=cfg.broker_id, client_id=cfg.client_id
    )
    # Apply only non-None fields
    current = rm._config
    updates = body.dict(exclude_none=True)
    if not updates:
        return rm.get_status()

    new_cfg = AccountRiskConfig(
        max_daily_loss        = updates.get("max_daily_loss",        current.max_daily_loss),
        max_order_value       = updates.get("max_order_value",       current.max_order_value),
        max_qty_per_order     = updates.get("max_qty_per_order",     current.max_qty_per_order),
        max_open_positions    = updates.get("max_open_positions",    current.max_open_positions),
        warning_threshold_pct = updates.get("warning_threshold_pct", current.warning_threshold_pct),
        trail_step            = current.trail_step,
        allowed_exchanges     = current.allowed_exchanges,
        state_dir             = current.state_dir,
    )
    rm.update_config(new_cfg)

    _blog(user_id, cfg.broker_id, cfg.client_id).info(
        "Risk config updated: %s", updates
    )
    return rm.get_status()


@router.get("/all-sessions")
def list_all_active_sessions(payload: dict = Depends(current_user)):
    """List all in-memory live sessions for the current user."""
    user_id = payload["sub"]
    from broker.multi_broker import registry
    sessions = registry.list_sessions(user_id)
    return [s.to_dict() for s in sessions]


# ── Broker Diagnostics ────────────────────────────────────────────────────────

_DIAG_CALLS = {
    "profile":    "profile info",
    "positions":  "open positions",
    "orderbook":  "order book",
    "funds":      "funds / margin",
    "holdings":   "holdings",
    "tradebook":  "today's trades",
}


@router.post("/configs/{config_id}/diagnose")
def diagnose_broker(
    config_id: str,
    call: str = "funds",
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """
    Run a single broker API call and return the **raw** broker response.
    Supported calls: profile, positions, orderbook, funds, holdings, tradebook.
    Used by the Diagnostics page in Settings.
    """
    import time as _time
    user_id = payload["sub"]

    if call not in _DIAG_CALLS:
        raise HTTPException(400, f"Unknown call '{call}'. Supported: {list(_DIAG_CALLS)}")

    # Verify config belongs to this user
    cfg = db.query(BrokerConfig).filter(
        BrokerConfig.id == config_id,
        BrokerConfig.user_id == user_id,
    ).first()
    if not cfg:
        raise HTTPException(404, "Broker config not found")

    from broker.multi_broker import registry
    sess = registry.get_session(user_id, config_id)
    if not sess:
        raise HTTPException(503, "Broker not connected — connect first via Settings → Broker Accounts")

    t0 = _time.perf_counter()
    try:
        if call == "profile":
            # Try raw client profile if adapter doesn't support it
            raw = None
            if sess._client is not None and hasattr(sess._client, "get_user_profile"):
                raw = sess._client.get_user_profile()
            elif sess._adapter is not None and hasattr(sess._adapter._client, "_fyers_client"):
                fyers_client = sess._adapter._client._fyers_client
                if hasattr(fyers_client, "fyers") and fyers_client.fyers is not None:
                    raw = fyers_client.fyers.get_profile()
            result = raw if raw else {"note": "profile not available for this broker"}
        elif call == "positions":
            result = sess.get_positions()
        elif call == "orderbook":
            result = sess.get_order_book()
        elif call == "funds":
            result = sess.get_limits()
        elif call == "holdings":
            result = sess.get_holdings()
        elif call == "tradebook":
            result = sess.get_tradebook()
        else:
            result = {}

        elapsed_ms = round((_time.perf_counter() - t0) * 1000, 1)
        return {
            "call":       call,
            "broker_id":  sess.broker_id,
            "client_id":  sess.client_id,
            "mode":       sess.mode,
            "elapsed_ms": elapsed_ms,
            "ok":         True,
            "data":       result,
        }
    except Exception as exc:
        elapsed_ms = round((_time.perf_counter() - t0) * 1000, 1)
        return {
            "call":       call,
            "broker_id":  sess.broker_id,
            "client_id":  sess.client_id,
            "mode":       sess.mode,
            "elapsed_ms": elapsed_ms,
            "ok":         False,
            "error":      str(exc),
            "data":       None,
        }


@router.get("/configs/{config_id}/env-preview")
def env_preview(
    config_id: str,
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """Return a sanitized preview of the .env content for download."""
    user_id = payload["sub"]
    cfg = db.query(BrokerConfig).filter(
        BrokerConfig.id == config_id,
        BrokerConfig.user_id == user_id,
    ).first()
    if not cfg:
        raise HTTPException(404, "Broker config not found")

    creds = _safe_decrypt(cfg, db)
    lines = [
        f"# Smart Trader — {cfg.broker_name} credentials",
        f"# Generated: {datetime.now().isoformat()}",
        f"# Account: {cfg.client_id}",
        "",
    ]
    # Map Smart Trader cred keys back to env var names
    for field_def in BROKER_FIELD_DEFS.get(cfg.broker_id, []):
        key   = field_def["key"]
        val   = creds.get(key, "")
        label = field_def["label"]
        # Mask secrets in preview — show first 2 chars + asterisks
        is_secret = field_def.get("type") == "password"
        display   = (val[:2] + "***") if (is_secret and len(val) > 2) else val
        lines.append(f"{key}={display}   # {label}")

    return {"preview": "\n".join(lines), "broker_id": cfg.broker_id,
            "client_id": cfg.client_id}


# ── Connect implementations ────────────────────────────────────────────────────

def _connect_shoonya(
    cfg: BrokerConfig,
    creds: dict,
    user_id: str,
    db: Session,
    log: logging.LoggerAdapter,
) -> dict:
    """
    Full Shoonya OAuth login flow.

    Steps:
      1. Validate all required credential fields
      2. Set environment variables (run_oauth_login reads from env)
      3. Run headless Firefox Selenium login with TOTP
      4. Obtain session token
      5. Register in MultiAccountRegistry
      6. Register in legacy BrokerSession singleton (backward-compat)
      7. Persist session token and status to DB
      8. Initialise per-account risk manager

    All steps logged with [user=... broker=shoonya client=...] context.
    """
    import os

    client_id = creds.get("USER_ID", cfg.client_id)
    log = _blog(user_id, "shoonya", client_id)

    # ── Step 1: Validate fields ───────────────────────────────────────────────
    required_fields = ["USER_ID", "PASSWORD", "TOKEN", "VC", "APP_KEY", "OAUTH_SECRET"]
    missing = [f for f in required_fields if not creds.get(f)]
    if missing:
        msg = f"Missing required Shoonya credential fields: {', '.join(missing)}"
        log.error("Connect aborted — %s", msg)
        raise HTTPException(422, msg)

    log.info(
        "Starting Shoonya OAuth login — client=%s vendor=%s",
        client_id, creds.get("VC", "?"),
    )

    # ── Step 2: Temporarily export env vars for run_oauth_login ──────────────
    env_map = {
        "USER_ID":      creds.get("USER_ID", ""),
        "PASSWORD":     creds.get("PASSWORD", ""),
        "TOKEN":        creds.get("TOKEN", ""),
        "VC":           creds.get("VC", ""),
        "APP_KEY":      creds.get("APP_KEY", ""),
        "OAUTH_SECRET": creds.get("OAUTH_SECRET", ""),
        "IMEI":         creds.get("IMEI", "abc1234"),
    }
    old_env = {k: os.environ.get(k) for k in env_map}
    log.info("Exporting %d credential env vars for OAuth", len(env_map))

    for k, v in env_map.items():
        os.environ[k] = v

    token = None
    try:
        # ── Step 3: Run OAuth login ───────────────────────────────────────────
        log.info("Launching headless Firefox for Shoonya OAuth (TOTP auto-fill)...")
        try:
            from broker.oauth_login import run_oauth_login
        except ImportError as ie:
            log.error("OAuth login module not available: %s", ie)
            raise HTTPException(
                503,
                "OAuth login module is not available on this server."
            )

        try:
            token = run_oauth_login()
        except RuntimeError as rte:
            msg = str(rte)
            log.error("OAuth runtime error: %s", msg)
            raise HTTPException(502, f"Shoonya login failed: {msg}")
        except Exception as e:
            log.exception("Unexpected error during OAuth login: %s", e)
            raise HTTPException(502, f"Shoonya login error: {type(e).__name__}: {e}")

        # ── Step 4: Validate token ────────────────────────────────────────────
        if not token:
            log.error("OAuth completed but returned empty token for client=%s", client_id)
            raise HTTPException(
                502,
                f"Shoonya OAuth returned no token for {client_id}. "
                "Check: (1) TOTP secret is correct, (2) password is current, "
                "(3) vendor code matches your app key."
            )
        log.info("OAuth token obtained — length=%d chars", len(token))

    finally:
        # ── Restore environment variables ─────────────────────────────────────
        for k, old_v in old_env.items():
            if old_v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = old_v
        log.info("Credential env vars restored")

    # ── Step 5: Register in MultiAccountRegistry ─────────────────────────────
    try:
        from broker.multi_broker import registry
        sess = registry.register_shoonya(
            user_id   = user_id,
            config_id = cfg.id,
            client_id = client_id,
            creds     = creds,
            token     = token,
        )
        log.info(
            "Multi-broker session registered — mode=%s is_live=%s",
            sess.mode, sess.is_live,
        )
    except Exception as e:
        # Non-fatal: token is valid, in-memory registration failed
        log.error("Multi-broker registration failed (non-fatal): %s", e)

    # ── Step 6: Register in legacy singleton (backward-compat with orders.py) ─
    try:
        from broker.shoonya_client import register_user_session
        register_user_session(user_id, creds, token)
        log.info("Legacy broker singleton updated")
    except Exception as e:
        log.warning("Legacy register_user_session failed: %s", e)

    # ── Step 7: Persist to DB ─────────────────────────────────────────────────
    _upsert_session(db, cfg, user_id, mode="live", token=token, logged_in=True)
    log.info("Session token persisted to DB for config_id=%s", cfg.id[:8])

    # ── Step 8: Initialise per-account risk manager ───────────────────────────
    try:
        from broker.account_risk import get_account_risk
        rm = get_account_risk(
            user_id   = user_id,
            config_id = cfg.id,
            broker_id = cfg.broker_id,
            client_id = client_id,
        )
        log.info("Account risk manager ready — max_loss=%s", rm._config.max_daily_loss)
    except Exception as e:
        log.warning("Could not init account risk manager: %s", e)

    _audit(db, user_id, "broker_connect",
           f"shoonya:{client_id}",
           details=f"config_id={cfg.id} token_len={len(token)}")

    log.info(
        "Shoonya connect COMPLETE — client=%s config=%s",
        client_id, cfg.id[:8],
    )
    return {
        "success":  True,
        "mode":     "live",
        "userId":   client_id,
        "broker":   "Shoonya / Finvasia",
        "message":  f"✓ Connected to Shoonya as {client_id}",
    }


def _connect_paper(
    cfg: BrokerConfig,
    creds: dict,
    user_id: str,
    db: Session,
    log: logging.LoggerAdapter,
) -> dict:
    """Activate paper trading immediately — no API call needed."""
    capital = creds.get("STARTING_CAPITAL", "1000000")

    from broker.multi_broker import registry
    registry.register_paper(user_id=user_id, config_id=cfg.id, client_id="PAPER")

    _upsert_session(db, cfg, user_id, mode="paper", token=None, logged_in=True)

    from broker.account_risk import get_account_risk
    get_account_risk(user_id=user_id, config_id=cfg.id,
                     broker_id="paper_trade", client_id="PAPER")

    log.info("Paper trading activated — starting_capital=%s", capital)
    _audit(db, user_id, "broker_connect", "paper_trade:PAPER",
           details=f"config_id={cfg.id} capital={capital}")

    return {
        "success": True,
        "mode":    "paper",
        "userId":  "PAPER",
        "broker":  "Paper Trade",
        "message": f"✓ Paper trading activated (starting capital: ₹{capital})",
    }


def _upsert_session(
    db: Session,
    cfg: BrokerConfig,
    user_id: str,
    mode: str,
    token: Optional[str],
    logged_in: bool,
) -> BrokerSession:
    """Create or update the BrokerSession DB row for a config."""
    sess = db.query(BrokerSession).filter(BrokerSession.config_id == cfg.id).first()
    now  = datetime.now(timezone.utc)
    if sess:
        sess.is_logged_in   = logged_in
        sess.mode           = mode
        sess.session_token  = token
        sess.login_at       = now
        sess.last_heartbeat = now
    else:
        sess = BrokerSession(
            id            = generate_id(),
            user_id       = user_id,
            config_id     = cfg.id,
            broker_id     = cfg.broker_id,
            session_token = token,
            mode          = mode,
            is_logged_in  = logged_in,
            login_at      = now,
            last_heartbeat = now,
        )
        db.add(sess)
    db.commit()
    return sess


# ── Fyers connect ─────────────────────────────────────────────────────────────

def _connect_fyers(
    cfg,
    creds: dict,
    user_id: str,
    db,
    log: logging.LoggerAdapter,
) -> dict:
    """
    Fyers OAuth2 login flow.

    Steps:
      1. Validate required credential fields
      2. Run Fyers authcode login via fyers_apiv3 (direct)
      3. Obtain access token
      4. Register in MultiAccountRegistry
      5. Persist session to DB
      6. Initialise per-account risk manager
      7. Write audit log
    """
    import os

    client_id = creds.get("CLIENT_ID", cfg.client_id or "")
    app_id    = creds.get("APP_ID", "")

    log.info("Step 1: Validating Fyers credentials for client=%s", client_id)

    required = ["CLIENT_ID", "APP_ID", "SECRET_KEY", "TOTP_KEY", "PIN"]
    missing  = [f for f in required if not creds.get(f)]
    if missing:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Fyers credentials incomplete. Missing fields: {', '.join(missing)}. "
                "Click Edit and fill in all required fields."
            ),
        )

    log.info("Step 2: Starting Fyers OAuth login")

    # ── Fyers direct login via fyers_apiv3 ───────────────────────────────────────
    token: str = ""
    try:
        token = _fyers_direct_login(creds, log)
    except Exception as e:
        log.error("Fyers login failed: %s", e, exc_info=True)
        raise HTTPException(
            status_code=502,
            detail=f"Fyers login failed: {e}",
        )

    if not token or len(token) < 10:
        raise HTTPException(
            status_code=502,
            detail="Fyers login returned empty token. Check APP_ID, SECRET_KEY and TOTP_KEY.",
        )

    log.info("Step 4: Registering Fyers session in registry")

    from broker.multi_broker import registry

    sess_obj = registry.register_fyers(
        user_id=user_id,
        config_id=cfg.id,
        client_id=client_id,
        creds=creds,
        token=token,
    )

    if sess_obj.error:
        raise HTTPException(
            status_code=502,
            detail=f"Fyers session registration failed: {sess_obj.error}",
        )

    log.info("Step 5: Persisting Fyers session to DB")
    _upsert_session(db, cfg, user_id, mode="live", token=token, logged_in=True)

    log.info("Step 6: Initialising Fyers risk manager")
    try:
        from broker.account_risk import AccountRiskManager
        risk = AccountRiskManager(user_id=user_id, config_id=cfg.id, broker_id="fyers")
        risk.activate()
    except Exception as _re:
        log.warning("AccountRiskManager init failed (non-fatal): %s", _re)

    log.info("Step 7: Writing audit log")
    _audit(db, user_id, "fyers_connect", cfg.id,
           f"Fyers client={client_id} connected successfully")

    log.info("Fyers connect complete — client=%s", client_id)
    return {
        "success":   True,
        "mode":      "live",
        "broker_id": "fyers",
        "userId":    client_id,
        "message":   f"Fyers account {client_id} connected successfully",
        "config_id": cfg.id,
    }


def _fyers_direct_login(creds: dict, log) -> str:
    """
    Perform Fyers TOTP-based login using the v2 API endpoints.
    Flow: send_login_otp_v2 → verify_otp (TOTP) → verify_pin_v2 → token exchange.
    Returns the access token string.
    """
    import base64
    import pyotp
    import requests as _req
    from urllib.parse import parse_qs, urlparse
    from datetime import datetime

    client_id  = creds["CLIENT_ID"]
    app_id     = creds["APP_ID"]
    secret_key = creds["SECRET_KEY"]
    totp_key   = creds["TOTP_KEY"]
    pin        = creds["PIN"]
    redirect_url = creds.get("REDIRECT_URL", "https://trade.fyers.in/api-login/redirect-uri/index.html")

    def _b64(s):
        return base64.b64encode(str(s).encode()).decode()

    log.info("Fyers direct login: client_id=%s app_id=%s", client_id, app_id)

    try:
        from fyers_apiv3 import fyersModel as _fyersModel
    except ImportError:
        raise RuntimeError(
            "fyers_apiv3 not installed. Run: pip install fyers-apiv3"
        )

    verify_url = "https://api-t2.fyers.in/vagator/v2"

    # Step 1: Send login OTP (v2 endpoint, base64-encoded fy_id)
    r1 = _req.post(
        f"{verify_url}/send_login_otp_v2",
        json={"fy_id": _b64(client_id), "app_id": "2"},
        timeout=15,
    )
    r1.raise_for_status()
    j1 = r1.json()
    request_key1 = j1.get("request_key", "")
    if not request_key1:
        raise RuntimeError(f"Fyers send_login_otp_v2 failed: {j1}")
    log.debug("Fyers Step 1 OK: OTP sent")

    # Wait if near TOTP boundary to avoid stale code
    if datetime.now().second % 30 > 27:
        import time
        time.sleep(5)

    # Step 2: Verify OTP with TOTP code
    totp_code = pyotp.TOTP(totp_key).now()
    r2 = _req.post(
        f"{verify_url}/verify_otp",
        json={"request_key": request_key1, "otp": totp_code},
        timeout=15,
    )
    r2.raise_for_status()
    j2 = r2.json()
    request_key2 = j2.get("request_key", "")
    if not request_key2:
        raise RuntimeError(f"Fyers verify_otp failed: {j2}")
    log.debug("Fyers Step 2 OK: TOTP verified")

    # Step 3: Verify PIN (v2 endpoint, base64-encoded pin)
    r3 = _req.post(
        f"{verify_url}/verify_pin_v2",
        json={"request_key": request_key2, "identity_type": "pin", "identifier": _b64(pin)},
        timeout=15,
    )
    r3.raise_for_status()
    j3 = r3.json()
    bearer_token = j3.get("data", {}).get("access_token", "")
    if not bearer_token:
        raise RuntimeError(f"Fyers verify_pin_v2 failed: {j3}")
    log.debug("Fyers Step 3 OK: PIN verified")

    # Step 4: Exchange bearer for auth_code via token endpoint
    ses = _req.Session()
    ses.headers.update({"authorization": f"Bearer {bearer_token}"})
    r4 = ses.post(
        "https://api-t1.fyers.in/api/v3/token",
        json={
            "fyers_id": client_id,
            "app_id": app_id[:-4],  # strip "-100" suffix
            "redirect_uri": redirect_url,
            "appType": "100",
            "code_challenge": "",
            "state": "None",
            "scope": "",
            "nonce": "",
            "response_type": "code",
            "create_cookie": True,
        },
        timeout=15,
    )
    r4.raise_for_status()
    j4 = r4.json()
    token_url = j4.get("Url", "")
    if not token_url:
        raise RuntimeError(f"Fyers token endpoint failed: {j4}")
    auth_code = parse_qs(urlparse(token_url).query).get("auth_code", [""])[0]
    if not auth_code:
        raise RuntimeError(f"No auth_code in Fyers redirect URL: {token_url}")
    log.debug("Fyers Step 4 OK: got auth_code")

    # Step 5: Generate access token via SessionModel
    session_model = _fyersModel.SessionModel(
        client_id=app_id,
        secret_key=secret_key,
        redirect_uri=redirect_url,
        response_type="code",
        grant_type="authorization_code",
    )
    session_model.set_token(auth_code)
    token_resp = session_model.generate_token()
    access_token = token_resp.get("access_token", "")
    if not access_token:
        raise RuntimeError(f"Fyers token exchange failed: {token_resp}")

    log.info("Fyers direct login succeeded, token length=%d", len(access_token))
    return access_token
