import hashlib
import hmac
import re
import secrets
import time
from uuid import uuid4
from argon2 import PasswordHasher
from argon2.exceptions import VerificationError, InvalidHashError
from sqlalchemy import select, update
from app.auth.models import Audit, Control, LoginSession, LoginThrottle, User
from app.core.settings import settings

HASHER = PasswordHasher()  # argon2-cffi's maintained Argon2id defaults.
DUMMY_HASH = HASHER.hash(secrets.token_urlsafe(32))
ROLES = {'viewer', 'analyst', 'administrator'}


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def csrf_token(token):
    return hmac.new(token.encode(), b'soc-session-csrf-v1', hashlib.sha256).hexdigest()


def username(value):
    value = value.strip().lower()
    if not re.fullmatch(r'[a-z0-9][a-z0-9._-]{2,63}', value):
        raise ValueError('Username must be 3–64 letters, numbers, dots, underscores or hyphens')
    return value


def password_hash(password):
    if len(password) < 15 or len(password) > 128 or len(set(password)) < 8:
        raise ValueError('Use a unique password of 15–128 characters with at least 8 different characters')
    return HASHER.hash(password)


def audit(db, action, target='', outcome='success', actor=None):
    db.add(Audit(id=str(uuid4()), timestamp=int(time.time()), actor_id=actor,
                 action=action, target=str(target)[:200], outcome=outcome))


def lock_accounts(db):
    if db.execute(update(Control).where(Control.id == 1).values(generation=Control.generation + 1)).rowcount != 1:
        raise RuntimeError('Authentication migration is required')


def create_user(db, name, password, role, actor=None, initial=False):
    if role not in ROLES:
        raise ValueError('Invalid role')
    name = username(name)
    hashed = password_hash(password)
    lock_accounts(db)
    control = db.get(Control, 1)
    if initial and (control.bootstrapped or db.scalar(select(User.id).limit(1))):
        raise ValueError('Initial administrator has already been provisioned')
    user = User(id=str(uuid4()), username=name, password_hash=hashed, role=role,
                active=True, created_at=int(time.time()))
    db.add(user); db.flush()
    if initial:
        control.bootstrapped = True
    audit(db, 'initial_administrator_created' if initial else 'user_created', user.id, actor=actor)
    return user


def principal(db, token, touch=True):
    if not token or not re.fullmatch(r'[A-Za-z0-9_-]{43}', token):
        return None
    now = int(time.time())
    row = db.get(LoginSession, digest(token))
    if not row or row.revoked or row.expires_at <= now or row.last_seen <= now - settings.session_idle_seconds:
        return None
    user = db.get(User, row.user_id)
    if not user or not user.active or user.role not in ROLES:
        return None
    if touch:
        row.last_seen = now
    return {'id': user.id, 'username': user.username, 'role': user.role, 'session_hash': row.token_hash,
            'expires_at': row.expires_at}


def throttle(db, name, client):
    """Atomic shared-DB admission. Commit before password hashing; survives failed attempts."""
    now = int(time.time())
    insert = __import__('sqlalchemy.dialects.' + db.bind.dialect.name, fromlist=['insert']).insert
    allowed = True
    for value, maximum in sorted([(f'account:{name}', 10), (f'client:{client}', 50)]):
        key = digest(value)
        db.execute(insert(LoginThrottle).values(key=key, window_start=now, attempts=0).on_conflict_do_nothing())
        # A write lock also serializes SQLite test connections; PostgreSQL locks only this row.
        db.execute(update(LoginThrottle).where(LoginThrottle.key == key).values(attempts=LoginThrottle.attempts + 1))
        row = db.get(LoginThrottle, key)
        if row.window_start <= now - 900:
            row.window_start, row.attempts = now, 1
        if row.attempts > maximum:
            allowed = False
    return allowed


def verify_password(user, password):
    try:
        valid = HASHER.verify(user.password_hash if user else DUMMY_HASH, password)
        return bool(valid and user and user.active and user.role in ROLES)
    except (VerificationError, InvalidHashError):
        return False


def authorize_write(db, identity, required):
    """Serialize privileged writes with role changes and recheck their session."""
    from fastapi import HTTPException
    lock_accounts(db)
    user = db.get(User, identity['id'])
    session = db.get(LoginSession, identity['session_hash'])
    now = int(time.time())
    ranks = {'viewer': 0, 'analyst': 1, 'administrator': 2}
    if (not user or not user.active or ranks.get(user.role, -1) < ranks[required]
            or not session or session.revoked or session.expires_at <= now
            or session.last_seen <= now - settings.session_idle_seconds):
        audit(db, 'modification_denied', required, 'authorization_changed', identity['id'])
        db.commit()
        raise HTTPException(403, 'Authorization changed; sign in again')
