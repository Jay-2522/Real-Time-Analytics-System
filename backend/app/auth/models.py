"""Authentication tables are separate from historical security-event models."""
from sqlalchemy import Boolean, CheckConstraint, ForeignKey, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class AuthBase(DeclarativeBase):
    pass


class User(AuthBase):
    __tablename__ = 'auth_users'
    __table_args__ = (CheckConstraint("role IN ('viewer','analyst','administrator')"),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    username: Mapped[str] = mapped_column(String(64), unique=True)
    password_hash: Mapped[str] = mapped_column(Text)
    role: Mapped[str] = mapped_column(String(20))
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[int] = mapped_column(Integer)


class LoginSession(AuthBase):
    __tablename__ = 'auth_sessions'
    token_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[str] = mapped_column(ForeignKey('auth_users.id'), index=True)
    created_at: Mapped[int] = mapped_column(Integer)
    expires_at: Mapped[int] = mapped_column(Integer, index=True)
    last_seen: Mapped[int] = mapped_column(Integer)
    revoked: Mapped[bool] = mapped_column(Boolean, default=False)


class Audit(AuthBase):
    __tablename__ = 'auth_audit'
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    timestamp: Mapped[int] = mapped_column(Integer, index=True)
    actor_id: Mapped[str | None] = mapped_column(ForeignKey('auth_users.id'), nullable=True)
    action: Mapped[str] = mapped_column(String(64))
    target: Mapped[str] = mapped_column(String(200))
    outcome: Mapped[str] = mapped_column(String(32))


class LoginThrottle(AuthBase):
    __tablename__ = 'auth_login_throttle'
    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    window_start: Mapped[int] = mapped_column(Integer)
    attempts: Mapped[int] = mapped_column(Integer)


class Control(AuthBase):
    __tablename__ = 'auth_control'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    generation: Mapped[int] = mapped_column(Integer, default=0)
    bootstrapped: Mapped[bool] = mapped_column(Boolean, default=False)
