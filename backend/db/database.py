"""
Smart Trader — SQLite database with SQLAlchemy async engine.

Tables:
  users          — platform users (email/password, role)
  broker_configs — per-user broker credentials (encrypted .env)
  broker_sessions— live session tokens & state
  audit_log      — login/action audit trail
"""

import os
from pathlib import Path
from datetime import datetime, timezone

from sqlalchemy import (
    Column, String, Boolean, Integer, Float, Text,
    DateTime, ForeignKey, JSON, UniqueConstraint,
    create_engine, event,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, Session

_HERE = Path(__file__).parent.parent
DB_PATH = os.getenv("DATABASE_URL", f"sqlite:///{_HERE}/data/smarttrader.db")

# Ensure data dir exists
(Path(_HERE) / "data").mkdir(exist_ok=True)

engine = create_engine(
    DB_PATH,
    connect_args={"check_same_thread": False},
    echo=False,
)

# Enable WAL mode for better concurrent reads
@event.listens_for(engine, "connect")
def _set_wal(dbapi_conn, _):
    dbapi_conn.execute("PRAGMA journal_mode=WAL")
    dbapi_conn.execute("PRAGMA foreign_keys=ON")

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def utcnow():
    return datetime.now(timezone.utc)


# ── Models ────────────────────────────────────────────────────────────────────

class User(Base):
    __tablename__ = "users"

    id           = Column(String(36), primary_key=True)        # UUID
    email        = Column(String(255), unique=True, nullable=False, index=True)
    name         = Column(String(255), nullable=False)
    phone        = Column(String(20), nullable=True)
    password_hash= Column(String(255), nullable=False)
    role         = Column(String(20), nullable=False, default="user")  # admin|user|viewer
    is_active    = Column(Boolean, default=True)
    api_key      = Column(String(64), unique=True, nullable=True, index=True)  # webhook/alert auth key
    created_at   = Column(DateTime, default=utcnow)
    updated_at   = Column(DateTime, default=utcnow, onupdate=utcnow)
    last_login   = Column(DateTime, nullable=True)

    broker_configs  = relationship("BrokerConfig",  back_populates="user", cascade="all, delete-orphan")
    broker_sessions = relationship("BrokerSession", back_populates="user", cascade="all, delete-orphan")
    audit_logs      = relationship("AuditLog",      back_populates="user", cascade="all, delete-orphan")


class BrokerConfig(Base):
    """Stores encrypted broker credentials as key-value pairs."""
    __tablename__ = "broker_configs"
    __table_args__ = (
        UniqueConstraint("user_id", "broker_id", name="uq_user_broker"),
    )

    id           = Column(String(36), primary_key=True)
    user_id      = Column(String(36), ForeignKey("users.id"), nullable=False)
    broker_id    = Column(String(50), nullable=False)    # shoonya | zerodha | fyers …
    broker_name  = Column(String(100), nullable=False)
    client_id    = Column(String(100), nullable=False)   # broker client ID
    credentials  = Column(Text, nullable=False)          # JSON encrypted creds
    is_active    = Column(Boolean, default=True)
    created_at   = Column(DateTime, default=utcnow)
    updated_at   = Column(DateTime, default=utcnow, onupdate=utcnow)

    user    = relationship("User", back_populates="broker_configs")
    session = relationship("BrokerSession", back_populates="config",
                           uselist=False, cascade="all, delete-orphan")


class BrokerSession(Base):
    """Active broker session state."""
    __tablename__ = "broker_sessions"

    id             = Column(String(36), primary_key=True)
    user_id        = Column(String(36), ForeignKey("users.id"), nullable=False)
    config_id      = Column(String(36), ForeignKey("broker_configs.id"), nullable=True)
    broker_id      = Column(String(50), nullable=False)
    session_token  = Column(Text, nullable=True)         # raw session token (encrypted)
    mode           = Column(String(20), default="demo")  # demo|live
    is_logged_in   = Column(Boolean, default=False)
    login_at       = Column(DateTime, nullable=True)
    expires_at     = Column(DateTime, nullable=True)
    last_heartbeat = Column(DateTime, nullable=True)
    extra          = Column(JSON, default=dict)         # broker-specific metadata

    user   = relationship("User", back_populates="broker_sessions")
    config = relationship("BrokerConfig", back_populates="session")


class AuditLog(Base):
    __tablename__ = "audit_log"

    id         = Column(Integer, primary_key=True, autoincrement=True)
    user_id    = Column(String(36), ForeignKey("users.id"), nullable=True)
    action     = Column(String(100), nullable=False)
    resource   = Column(String(200), nullable=True)
    ip_address = Column(String(50), nullable=True)
    user_agent = Column(String(500), nullable=True)
    status     = Column(String(20), default="success")   # success|failure
    details    = Column(Text, nullable=True)
    created_at = Column(DateTime, default=utcnow)

    user = relationship("User", back_populates="audit_logs")


# ── DB helpers ─────────────────────────────────────────────────────────────────

def create_tables():
    Base.metadata.create_all(bind=engine)


def get_db() -> Session:
    """FastAPI dependency — yields a database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
