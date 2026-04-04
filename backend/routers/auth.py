"""
Auth router — email/password login, registration, JWT tokens.
Manages platform users (not broker sessions — those are in broker_sessions router).
"""
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, field_validator
from sqlalchemy.orm import Session

from db.database import get_db, User, AuditLog
from core.security import (
    hash_password, verify_password,
    create_access_token, generate_id,
)
from core.deps import current_user, require_admin
from broker.shoonya_client import get_session

logger = logging.getLogger("smart_trader.auth")
router = APIRouter(prefix="/auth", tags=["auth"])


# ── Schemas ────────────────────────────────────────────────────────────────────

class RegisterRequest(BaseModel):
    email: str
    name: str
    password: str
    phone: Optional[str] = None

    @field_validator("password")
    @classmethod
    def pw_strength(cls, v):
        if len(v) < 6:
            raise ValueError("Password must be at least 6 characters")
        return v


class LoginRequest(BaseModel):
    email: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: dict


class MeResponse(BaseModel):
    id: str
    email: str
    name: str
    role: str
    is_active: bool


class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str


def _audit(db, user_id, action, request, status="success", details=None):
    db.add(AuditLog(
        user_id=user_id,
        action=action,
        ip_address=request.client.host if request.client else None,
        user_agent=request.headers.get("user-agent", "")[:500],
        status=status,
        details=details,
    ))
    db.commit()


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.post("/register", response_model=TokenResponse)
def register(body: RegisterRequest, request: Request, db: Session = Depends(get_db)):
    """Register a new user. First user becomes admin."""
    existing = db.query(User).filter(User.email == body.email.lower()).first()
    if existing:
        raise HTTPException(400, "Email already registered")

    is_first = db.query(User).count() == 0
    uid = generate_id()
    user = User(
        id=uid,
        email=body.email.lower().strip(),
        name=body.name.strip(),
        phone=body.phone,
        password_hash=hash_password(body.password),
        role="admin" if is_first else "user",
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    token = create_access_token(user.id, user.email, user.role)
    _audit(db, uid, "register", request)
    logger.info("New user registered: %s role=%s", user.email, user.role)
    return TokenResponse(
        access_token=token,
        user={"id": user.id, "email": user.email, "name": user.name, "role": user.role},
    )


@router.post("/login", response_model=TokenResponse)
def login(body: LoginRequest, request: Request, db: Session = Depends(get_db)):
    """Email + password login."""
    user = db.query(User).filter(User.email == body.email.lower()).first()
    if not user or not verify_password(body.password, user.password_hash):
        _audit(db, None, "login_failed", request, status="failure", details=body.email)
        raise HTTPException(401, "Invalid email or password")
    if not user.is_active:
        raise HTTPException(403, "Account disabled — contact admin")

    user.last_login = datetime.now(timezone.utc)
    db.commit()

    token = create_access_token(user.id, user.email, user.role)
    _audit(db, user.id, "login", request)
    logger.info("Login: %s role=%s", user.email, user.role)
    return TokenResponse(
        access_token=token,
        user={"id": user.id, "email": user.email, "name": user.name, "role": user.role},
    )


@router.get("/me", response_model=MeResponse)
def me(payload: dict = Depends(current_user), db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == payload["sub"]).first()
    if not user:
        raise HTTPException(404, "User not found")
    return MeResponse(
        id=user.id, email=user.email, name=user.name,
        role=user.role, is_active=user.is_active,
    )


@router.post("/logout")
def logout(payload: dict = Depends(current_user)):
    return {"success": True, "message": "Logged out"}


@router.post("/change-password")
def change_password(
    body: ChangePasswordRequest,
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    user = db.query(User).filter(User.id == payload["sub"]).first()
    if not user or not verify_password(body.current_password, user.password_hash):
        raise HTTPException(400, "Current password incorrect")
    user.password_hash = hash_password(body.new_password)
    db.commit()
    return {"success": True}


# ── Shoonya OAuth status (backward-compat) ─────────────────────────────────

@router.get("/status")
def shoonya_status():
    session = get_session()
    from core.config import config
    return {
        "loggedIn": session.is_logged_in,
        "mode": "demo" if session.is_demo else "live",
        "userId": config.SHOONYA_USER_ID if not session.is_demo else "DEMO_USER",
        "configured": config.is_shoonya_configured(),
    }


@router.get("/broker-status")
def user_broker_status(
    payload: dict = Depends(current_user),
    db: Session = Depends(get_db),
):
    """
    Return this user's broker connection status.
    Checks:
      1. Multi-broker registry (any live/paper session)
      2. Legacy in-memory per-user Shoonya session
      3. DB BrokerSession table (persisted across restarts)
      4. Global singleton (env-var-configured Shoonya)
    """
    from broker.shoonya_client import get_user_session, get_session as _global_sess
    from broker.multi_broker import registry
    from db.database import BrokerSession as DBSession, BrokerConfig

    user_id = payload["sub"]

    # 1. Multi-broker registry — covers Fyers, Shoonya, Paper
    live_sessions = registry.list_live_sessions(user_id)
    if live_sessions:
        brokers = list({s.broker_id for s in live_sessions})
        primary = live_sessions[0]
        return {
            "isLive": True,
            "broker": primary.broker_id,
            "brokers": brokers,
            "mode": "paper" if primary.is_paper else "live",
            "clientId": primary.client_id,
            "loginAt": None,
        }

    # 2. Legacy in-memory Shoonya session
    live = get_user_session(user_id)
    if live and not live.is_demo:
        return {"isLive": True, "broker": "shoonya", "brokers": ["shoonya"],
                "mode": "live", "clientId": None, "loginAt": None}

    # 3. DB session (persisted; try to restore in-memory session from it)
    db_sessions = (
        db.query(DBSession)
        .filter(DBSession.user_id == user_id, DBSession.is_logged_in == True)
        .order_by(DBSession.login_at.desc())
        .all()
    )
    for ds in db_sessions:
        cfg = db.query(BrokerConfig).filter(BrokerConfig.id == ds.config_id).first()
        if not cfg:
            continue
        if ds.session_token and ds.broker_id == "shoonya":
            try:
                from broker.shoonya_client import register_user_session
                mini_creds = {"USER_ID": cfg.client_id}
                register_user_session(user_id, mini_creds, ds.session_token)
                logger.info("Restored Shoonya session from DB for user=%s", user_id)
            except Exception as e:
                logger.debug("Session restore failed: %s", e)
        return {
            "isLive": True,
            "broker": ds.broker_id,
            "brokers": [ds.broker_id],
            "clientId": cfg.client_id,
            "mode": ds.mode,
            "loginAt": ds.login_at.isoformat() if ds.login_at else None,
        }

    # 4. Global singleton (env-var Shoonya config)
    g = _global_sess()
    if not g.is_demo and g.is_logged_in:
        from core.config import config
        return {"isLive": True, "broker": "shoonya", "brokers": ["shoonya"],
                "mode": "live", "clientId": config.SHOONYA_USER_ID, "loginAt": None}

    return {"isLive": False, "broker": None, "brokers": [], "mode": "demo",
            "clientId": None, "loginAt": None}


@router.post("/shoonya-connect")
def shoonya_connect():
    """Trigger Shoonya OAuth — used from broker settings."""
    session = get_session()
    if session.is_demo:
        return {"success": True, "mode": "demo", "userId": "DEMO_USER",
                "message": "Running in DEMO mode"}
    if session.is_logged_in:
        from core.config import config
        return {"success": True, "mode": "live",
                "userId": config.SHOONYA_USER_ID, "message": "Already logged in"}
    ok = session.login()
    if not ok:
        raise HTTPException(401, "Shoonya login failed — check credentials and logs")
    from core.config import config
    return {"success": True, "mode": "live",
            "userId": config.SHOONYA_USER_ID, "message": "Login successful"}


@router.post("/shoonya-disconnect")
def shoonya_disconnect():
    session = get_session()
    session.logout()
    return {"success": True, "message": "Disconnected"}
