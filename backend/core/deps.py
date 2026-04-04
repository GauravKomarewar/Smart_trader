"""FastAPI dependency: require authenticated user (any role) or admin."""
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError

from core.security import decode_token

bearer = HTTPBearer(auto_error=False)


def _get_payload(credentials: HTTPAuthorizationCredentials):
    if not credentials:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        return decode_token(credentials.credentials)
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


def current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer),
) -> dict:
    """Returns decoded JWT payload dict for any authenticated user."""
    return _get_payload(credentials)


def require_admin(
    credentials: HTTPAuthorizationCredentials = Depends(bearer),
) -> dict:
    """Returns decoded JWT payload dict, raises 403 if not admin."""
    payload = _get_payload(credentials)
    if payload.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return payload
