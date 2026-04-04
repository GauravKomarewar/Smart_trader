"""JWT + password hashing + credential encryption utilities for Smart Trader."""
import uuid
import hashlib
import base64
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from jose import jwt, JWTError
from passlib.context import CryptContext

pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Import config lazily to avoid circular imports
def _cfg():
    from core.config import config
    return config


# ── Encryption key derivation ─────────────────────────────────────────────────

def _derive_fernet_key() -> bytes:
    """
    Derive a 32-byte Fernet-compatible key from ENCRYPTION_KEY env var.
    Falls back to JWT_SECRET so existing deployments don't need extra config.
    Key is SHA-256 of the secret → base64url-encoded (valid Fernet key format).
    """
    secret = os.getenv("ENCRYPTION_KEY") or _cfg().JWT_SECRET
    return base64.urlsafe_b64encode(hashlib.sha256(secret.encode()).digest())


# ── Password helpers ──────────────────────────────────────────────────────────

def hash_password(password: str) -> str:
    return pwd_ctx.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_ctx.verify(plain, hashed)


# ── JWT helpers ───────────────────────────────────────────────────────────────

def create_access_token(
    user_id: str,
    email: str,
    role: str,
    expires_hours: Optional[int] = None,
) -> str:
    cfg = _cfg()
    exp = expires_hours or cfg.JWT_EXPIRE_HOURS
    payload = {
        "sub":   user_id,
        "email": email,
        "role":  role,
        "iat":   datetime.now(timezone.utc),
        "exp":   datetime.now(timezone.utc) + timedelta(hours=exp),
        "jti":   str(uuid.uuid4()),
    }
    return jwt.encode(payload, cfg.JWT_SECRET, algorithm="HS256")


def decode_token(token: str) -> dict:
    """Raises JWTError on invalid/expired token."""
    cfg = _cfg()
    return jwt.decode(token, cfg.JWT_SECRET, algorithms=["HS256"])


# ── Credential encryption (AES-256-GCM via Fernet) ────────────────────────────

def encrypt_credentials(data: dict) -> str:
    """
    Encrypt broker credentials with Fernet (AES-128-CBC + HMAC-SHA256).
    Returns a URL-safe base64 string prefixed with 'fernet:' for versioning.
    """
    import json
    from cryptography.fernet import Fernet
    f = Fernet(_derive_fernet_key())
    token = f.encrypt(json.dumps(data).encode()).decode()
    return "fernet:" + token


def decrypt_credentials(data: str) -> dict:
    """
    Decrypt broker credentials.

    Supports two formats (for migration):
      1. Fernet (new) — prefixed with 'fernet:'
      2. Legacy XOR   — all other base64 strings

    Raises ValueError("CREDENTIALS_STALE") if both methods fail so callers
    can ask the user to re-save credentials rather than crashing with 500.
    """
    import json

    # ── Fernet decrypt (new format) ───────────────────────────────────────────
    if data.startswith("fernet:"):
        from cryptography.fernet import Fernet, InvalidToken
        try:
            f = Fernet(_derive_fernet_key())
            return json.loads(f.decrypt(data[7:].encode()).decode())
        except InvalidToken:
            raise ValueError("CREDENTIALS_STALE")
        except Exception as e:
            raise ValueError(f"CREDENTIALS_STALE: {e}")

    # ── Legacy XOR decode (migration path) ────────────────────────────────────
    # Try known keys in priority order:
    #   1. Current JWT_SECRET (may have been rotated)
    #   2. Hard-coded shipped default (first-run installs that never changed)
    #   3. ENCRYPTION_KEY itself (if different from JWT_SECRET)
    _xor_candidates = [
        _cfg().JWT_SECRET,
        "changeme_use_long_random_secret",          # shipped default
        "smart_trader_jwt_secret_2026_change_in_production",  # common env default
    ]
    # Also add ENCRYPTION_KEY if set separately
    _enc_key = os.getenv("ENCRYPTION_KEY", "")
    if _enc_key and _enc_key not in _xor_candidates:
        _xor_candidates.append(_enc_key)

    for _candidate in _xor_candidates:
        try:
            raw       = base64.urlsafe_b64decode(data.encode() + b"==")
            key       = hashlib.sha256(_candidate.encode()).digest()
            decrypted = bytes(b ^ key[i % len(key)] for i, b in enumerate(raw))
            result    = json.loads(decrypted.decode("utf-8"))
            return result
        except Exception:
            continue

    # Also try Fernet with ENCRYPTION_KEY if it differs from JWT_SECRET
    if _enc_key:
        try:
            from cryptography.fernet import Fernet, InvalidToken as _IT
            _alt_key = base64.urlsafe_b64encode(hashlib.sha256(_enc_key.encode()).digest())
            _f = Fernet(_alt_key)
            # data here doesn't start with 'fernet:' but try raw token
            return json.loads(_f.decrypt(data.encode()).decode())
        except Exception:
            pass

    # All methods failed — credentials saved with an unknown key
    raise ValueError("CREDENTIALS_STALE")


def generate_id() -> str:
    return str(uuid.uuid4())
