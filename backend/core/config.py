"""Smart Trader Backend — Configuration via .env file."""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from backend directory
_here = Path(__file__).parent
_env_file = _here / ".env"
if _env_file.exists():
    load_dotenv(str(_env_file))
else:
    load_dotenv()  # fallback: current working directory


class Config:
    # ── Server ──────────────────────────────────────────
    HOST: str            = os.getenv("BACKEND_HOST", "0.0.0.0")
    PORT: int            = int(os.getenv("BACKEND_PORT", "8001"))
    DEBUG: bool          = os.getenv("DEBUG", "false").lower() == "true"
    CORS_ORIGINS: list   = os.getenv(
        "CORS_ORIGINS",
        "http://localhost:5173,http://localhost:3000"
    ).split(",")

    # ── Shoonya Credentials ─────────────────────────────
    SHOONYA_USER_ID: str       = os.getenv("SHOONYA_USER_ID", "")
    SHOONYA_PASSWORD: str      = os.getenv("SHOONYA_PASSWORD", "")
    SHOONYA_TOTP_KEY: str      = os.getenv("SHOONYA_TOTP_KEY", "")
    SHOONYA_VENDOR_CODE: str   = os.getenv("SHOONYA_VENDOR_CODE", "")
    SHOONYA_OAUTH_SECRET: str  = os.getenv("SHOONYA_OAUTH_SECRET", "")
    SHOONYA_HOST: str          = os.getenv(
        "SHOONYA_HOST",
        "https://api.shoonya.com/NorenWClientAPI"
    )
    SHOONYA_WEBSOCKET: str     = os.getenv(
        "SHOONYA_WEBSOCKET",
        "wss://api.shoonya.com/NorenWS"
    )

    # ── Fyers Data API Credentials ───────────────────────
    # Used as the global market data provider (quotes, option chain, indices).
    # Admin can configure these via the admin panel -> Data Sources section.
    FYERS_APP_ID: str          = os.getenv("FYERS_APP_ID", "")       # e.g. IQOURN2NSJ-100
    FYERS_SECRET_ID: str       = os.getenv("FYERS_SECRET_ID", "")
    FYERS_CLIENT_ID: str       = os.getenv("FYERS_CLIENT_ID", "")    # e.g. FG0158
    FYERS_TOTP_KEY: str        = os.getenv("FYERS_TOTP_KEY", "")
    FYERS_PIN: str             = os.getenv("FYERS_PIN", "")
    FYERS_REDIRECT_URL: str    = os.getenv("FYERS_REDIRECT_URL", "http://localhost/")
    # Path to the token cache file (persists access tokens between restarts)
    FYERS_TOKEN_FILE: str      = os.getenv("FYERS_TOKEN_FILE",
                                           str(Path(__file__).parent.parent / "data" / "fyers_token.json"))

    # ── Logging ─────────────────────────────────────────
    LOG_DIR: str          = os.getenv("LOG_DIR", "logs")
    LOG_LEVEL: str        = os.getenv("LOG_LEVEL", "INFO")
    LOG_MAX_BYTES: int    = int(os.getenv("LOG_MAX_BYTES", str(10 * 1024 * 1024)))
    LOG_BACKUP_COUNT: int = int(os.getenv("LOG_BACKUP_COUNT", "5"))

    # ── JWT ─────────────────────────────────────────────
    JWT_SECRET: str        = os.getenv("JWT_SECRET", "changeme_use_long_random_secret")
    JWT_EXPIRE_HOURS: int  = int(os.getenv("JWT_EXPIRE_HOURS", "24"))

    # ── Credential encryption ─────────────────────────
    # Separate stable key for Fernet-encrypting broker credentials.
    # If blank, falls back to JWT_SECRET (legacy behaviour).
    ENCRYPTION_KEY: str    = os.getenv("ENCRYPTION_KEY", "")

    # ── Admin ─────────────────────────────────────────────
    ADMIN_EMAIL: str       = os.getenv("ADMIN_EMAIL", "gktradeslog@gmail.com")
    ADMIN_PASSWORD: str    = os.getenv("ADMIN_PASSWORD", "Admin@1234")
    ADMIN_NAME: str        = os.getenv("ADMIN_NAME", "Gaurav Komarewar")

    # ── Database ──────────────────────────────────────────
    DATABASE_URL: str      = os.getenv("DATABASE_URL", "")  # empty = auto SQLite

    def is_shoonya_configured(self) -> bool:
        return bool(
            self.SHOONYA_USER_ID
            and self.SHOONYA_PASSWORD
            and self.SHOONYA_VENDOR_CODE
            and self.SHOONYA_OAUTH_SECRET
        )

    def is_fyers_configured(self) -> bool:
        return bool(self.FYERS_APP_ID and self.FYERS_CLIENT_ID)


config = Config()
