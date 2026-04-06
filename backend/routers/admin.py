"""
Admin router — platform management (admin role required).

Endpoints:
  GET  /admin/users          — list all users
  GET  /admin/users/{id}     — user detail
  POST /admin/users          — create user (by admin)
  PUT  /admin/users/{id}     — update user (role, active)
  DELETE /admin/users/{id}   — delete user
  GET  /admin/stats          — platform statistics
  GET  /admin/audit-log      — audit log
  GET  /admin/broker-sessions— all active broker sessions
  POST /admin/users/{id}/reset-password — reset password
  POST /admin/session-scheduler/login   — trigger auto-login now
  POST /admin/session-scheduler/logout  — trigger auto-logout now
  GET  /admin/session-scheduler/status  — scheduler status
"""
import logging
import secrets
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from db.database import get_db, User, BrokerConfig, BrokerSession, AuditLog
from core.security import hash_password, generate_id
from core.deps import require_admin

logger = logging.getLogger("smart_trader.admin")
router = APIRouter(prefix="/admin", tags=["admin"])


class CreateUserRequest(BaseModel):
    email: str
    name: str
    password: str
    role: str = "user"
    phone: Optional[str] = None


class UpdateUserRequest(BaseModel):
    name: Optional[str] = None
    role: Optional[str] = None
    is_active: Optional[bool] = None
    phone: Optional[str] = None


class ResetPasswordRequest(BaseModel):
    new_password: str


@router.get("/stats")
def get_stats(
    _: dict = Depends(require_admin),
    db: Session = Depends(get_db),
):
    total_users   = db.query(User).count()
    active_users  = db.query(User).filter(User.is_active == True).count()
    admin_users   = db.query(User).filter(User.role == "admin").count()
    broker_cfgs   = db.query(BrokerConfig).count()
    active_sess   = db.query(BrokerSession).filter(BrokerSession.is_logged_in == True).count()
    recent_logins = db.query(AuditLog).filter(AuditLog.action == "login").order_by(
        AuditLog.created_at.desc()
    ).limit(5).all()

    return {
        "total_users":       total_users,
        "active_users":      active_users,
        "admin_users":       admin_users,
        "broker_configs":    broker_cfgs,
        "active_sessions":   active_sess,
        "recent_logins": [
            {
                "user_id":    l.user_id,
                "ip":         l.ip_address,
                "status":     l.status,
                "created_at": l.created_at.isoformat() if l.created_at else None,
            }
            for l in recent_logins
        ],
    }


@router.get("/users")
def list_users(
    skip: int = 0,
    limit: int = 100,
    _: dict = Depends(require_admin),
    db: Session = Depends(get_db),
):
    users = db.query(User).order_by(User.created_at.desc()).offset(skip).limit(limit).all()
    return [_user_out(u, db) for u in users]


@router.get("/users/{user_id}")
def get_user(
    user_id: str,
    _: dict = Depends(require_admin),
    db: Session = Depends(get_db),
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(404, "User not found")
    return _user_out(user, db)


@router.post("/users", status_code=201)
def create_user(
    body: CreateUserRequest,
    _: dict = Depends(require_admin),
    db: Session = Depends(get_db),
):
    if db.query(User).filter(User.email == body.email.lower()).first():
        raise HTTPException(409, "Email already exists")
    if body.role not in ("admin", "user", "viewer"):
        raise HTTPException(422, "role must be admin|user|viewer")

    user = User(
        id=generate_id(),
        email=body.email.lower().strip(),
        name=body.name.strip(),
        phone=body.phone,
        password_hash=hash_password(body.password),
        role=body.role,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    logger.info("Admin created user: %s", user.email)
    return _user_out(user, db)


@router.put("/users/{user_id}")
def update_user(
    user_id: str,
    body: UpdateUserRequest,
    _: dict = Depends(require_admin),
    db: Session = Depends(get_db),
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(404, "User not found")
    if body.name      is not None: user.name      = body.name
    if body.role      is not None: user.role      = body.role
    if body.is_active is not None: user.is_active = body.is_active
    if body.phone     is not None: user.phone     = body.phone
    user.updated_at = datetime.now(timezone.utc)
    db.commit()
    return _user_out(user, db)


@router.delete("/users/{user_id}")
def delete_user(
    user_id: str,
    admin_payload: dict = Depends(require_admin),
    db: Session = Depends(get_db),
):
    if user_id == admin_payload["sub"]:
        raise HTTPException(400, "Cannot delete your own account")
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(404, "User not found")
    db.delete(user)
    db.commit()
    return {"success": True}


@router.post("/users/{user_id}/reset-password")
def reset_password(
    user_id: str,
    body: ResetPasswordRequest,
    _: dict = Depends(require_admin),
    db: Session = Depends(get_db),
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(404, "User not found")
    user.password_hash = hash_password(body.new_password)
    user.updated_at = datetime.now(timezone.utc)
    db.commit()
    return {"success": True, "message": "Password reset"}


@router.get("/audit-log")
def get_audit_log(
    skip: int = 0,
    limit: int = 200,
    user_id: Optional[str] = Query(None),
    action: Optional[str] = Query(None),
    _: dict = Depends(require_admin),
    db: Session = Depends(get_db),
):
    q = db.query(AuditLog).order_by(AuditLog.created_at.desc())
    if user_id:
        q = q.filter(AuditLog.user_id == user_id)
    if action:
        q = q.filter(AuditLog.action == action)
    logs = q.offset(skip).limit(limit).all()
    return [
        {
            "id":         l.id,
            "user_id":    l.user_id,
            "action":     l.action,
            "resource":   l.resource,
            "ip_address": l.ip_address,
            "status":     l.status,
            "details":    l.details,
            "created_at": l.created_at.isoformat() if l.created_at else None,
        }
        for l in logs
    ]


@router.get("/broker-sessions")
def get_all_broker_sessions(
    _: dict = Depends(require_admin),
    db: Session = Depends(get_db),
):
    sessions = db.query(BrokerSession).order_by(BrokerSession.login_at.desc()).all()
    return [
        {
            "id":             s.id,
            "user_id":        s.user_id,
            "broker_id":      s.broker_id,
            "mode":           s.mode,
            "is_logged_in":   s.is_logged_in,
            "login_at":       s.login_at.isoformat() if s.login_at else None,
            "last_heartbeat": s.last_heartbeat.isoformat() if s.last_heartbeat else None,
        }
        for s in sessions
    ]


def _user_out(user: User, db: Session) -> dict:
    broker_count = db.query(BrokerConfig).filter(BrokerConfig.user_id == user.id).count()
    return {
        "id":           user.id,
        "email":        user.email,
        "name":         user.name,
        "phone":        user.phone,
        "role":         user.role,
        "is_active":    user.is_active,
        "broker_count": broker_count,
        "created_at":   user.created_at.isoformat() if user.created_at else None,
        "last_login":   user.last_login.isoformat() if user.last_login else None,
    }


# ── Session Scheduler admin endpoints ─────────────────────────────────────────

@router.post("/session-scheduler/login")
def admin_trigger_login(payload: dict = Depends(require_admin)):
    """Manually trigger daily auto-login for all broker configs."""
    import threading
    from trading.session_scheduler import session_scheduler

    def _run():
        try:
            session_scheduler.trigger_login()
        except Exception as e:
            logging.getLogger("smart_trader.admin").error("Manual login trigger failed: %s", e)

    threading.Thread(target=_run, daemon=True, name="ManualLogin").start()
    return {"success": True, "message": "Auto-login triggered in background. Check logs for progress."}


@router.post("/session-scheduler/logout")
def admin_trigger_logout(payload: dict = Depends(require_admin)):
    """Manually trigger daily auto-logout for all sessions."""
    from trading.session_scheduler import session_scheduler

    session_scheduler.trigger_logout()
    return {"success": True, "message": "All sessions logged out."}


@router.get("/session-scheduler/status")
def admin_scheduler_status(payload: dict = Depends(require_admin)):
    """Get session scheduler status."""
    from trading.session_scheduler import session_scheduler, _LOGIN_HOUR, _LOGIN_MIN, _LOGOUT_HOUR, _LOGOUT_MIN

    return {
        "running":          session_scheduler._running,
        "login_schedule":   f"{_LOGIN_HOUR:02d}:{_LOGIN_MIN:02d} IST",
        "logout_schedule":  f"{_LOGOUT_HOUR:02d}:{_LOGOUT_MIN:02d} IST",
        "last_login_date":  str(session_scheduler._last_login_date) if session_scheduler._last_login_date else None,
        "last_logout_date": str(session_scheduler._last_logout_date) if session_scheduler._last_logout_date else None,
    }
