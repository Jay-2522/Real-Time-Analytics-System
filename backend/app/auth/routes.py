import secrets
import time
from typing import Literal
from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field, ConfigDict
from sqlalchemy import select, update, func
from sqlalchemy.exc import IntegrityError
from app.auth.models import User, LoginSession, Audit
from app.auth.service import create_user, csrf_token, digest, audit, throttle, verify_password, authorize_write
from app.core.settings import settings
from app.db.session import SessionLocal

router = APIRouter(prefix='/auth', tags=['authentication'])


class Credentials(BaseModel):
    model_config = ConfigDict(extra='forbid')
    username: str = Field(min_length=3, max_length=64)
    password: str = Field(min_length=1, max_length=128, repr=False)


class NewUser(Credentials):
    role: Literal['viewer', 'analyst', 'administrator']


class UserChange(BaseModel):
    model_config = ConfigDict(extra='forbid')
    role: Literal['viewer', 'analyst', 'administrator']
    active: bool


def public_user(user):
    return {'id':user.id, 'username':user.username, 'role':user.role, 'active':user.active}


@router.post('/login')
def login(body: Credentials, request: Request, response: Response):
    response.headers['Cache-Control'] = 'no-store'
    name = body.username.strip().lower()
    client = request.client.host if request.client else 'unknown'
    with SessionLocal() as db, db.begin():
        admitted = throttle(db, name, client)
        if not admitted:
            audit(db, 'login', digest(name), 'rate_limited')
    if not admitted:
        raise HTTPException(429, 'Too many login attempts; try again later', headers={'Retry-After':'900'})
    with SessionLocal() as db, db.begin():
        user = db.scalar(select(User).where(User.username == name).with_for_update())
        if not verify_password(user, body.password):
            audit(db, 'login', digest(name), 'invalid_credentials')
            failure = True
        else:
            failure = False
            token = secrets.token_urlsafe(32)
            now = int(time.time())
            # Login never adopts an existing browser-supplied identifier (no fixation).
            prior = request.cookies.get(settings.session_cookie)
            if prior:
                db.execute(update(LoginSession).where(LoginSession.token_hash == digest(prior)).values(revoked=True))
            db.add(LoginSession(token_hash=digest(token), user_id=user.id, created_at=now,
                                expires_at=now+settings.session_seconds, last_seen=now, revoked=False))
            audit(db, 'login', user.id, actor=user.id)
            result = public_user(user)
    if failure:
        raise HTTPException(401, 'Invalid username or password')
    response.set_cookie(settings.session_cookie, token, max_age=settings.session_seconds,
                        secure=not settings.auth_local_http, httponly=True, samesite='strict', path='/')
    return {'user':result, 'csrf_token':csrf_token(token), 'expires_at':now+settings.session_seconds}


@router.get('/me')
def me(request: Request):
    user = request.state.principal
    return {'user':{k:user[k] for k in ('id','username','role')},
            'csrf_token':csrf_token(request.cookies[settings.session_cookie]), 'expires_at':user['expires_at']}


@router.post('/logout', status_code=204)
def logout(request: Request, response: Response):
    user = request.state.principal
    with SessionLocal() as db, db.begin():
        db.execute(update(LoginSession).where(LoginSession.token_hash == user['session_hash']).values(revoked=True))
        audit(db, 'logout', user['id'], actor=user['id'])
    response.delete_cookie(settings.session_cookie, path='/', secure=not settings.auth_local_http, httponly=True, samesite='strict')


@router.get('/users')
def users():
    with SessionLocal() as db:
        return [public_user(u) for u in db.scalars(select(User).order_by(User.username))]


@router.post('/users', status_code=201)
def add_user(body: NewUser, request: Request):
    try:
        with SessionLocal() as db, db.begin():
            authorize_write(db, request.state.principal, 'administrator')
            user = create_user(db, body.username, body.password, body.role, request.state.principal['id'])
            return public_user(user)
    except (ValueError, IntegrityError):
        raise HTTPException(400, 'Invalid credentials or username already exists')


@router.patch('/users/{user_id}')
def change_user(user_id: str, body: UserChange, request: Request):
    actor = request.state.principal['id']
    with SessionLocal() as db, db.begin():
        authorize_write(db, request.state.principal, 'administrator')
        user = db.get(User, user_id)
        if not user:
            raise HTTPException(404, 'User not found')
        if actor == user_id:
            audit(db, 'modification_denied', user_id, 'self_role_change', actor)
            db.commit()
            raise HTTPException(403, 'Self role/activation changes are not permitted')
        if user.active and user.role == 'administrator' and (not body.active or body.role != 'administrator'):
            count = db.scalar(select(func.count()).select_from(User).where(User.active == True, User.role == 'administrator'))
            if count <= 1:
                raise HTTPException(409, 'Cannot remove the final active administrator')
        user.role, user.active = body.role, body.active
        db.execute(update(LoginSession).where(LoginSession.user_id == user.id).values(revoked=True))
        audit(db, 'user_role_or_activation_changed', user.id, actor=actor)
        audit(db, 'user_sessions_revoked', user.id, actor=actor)
        return public_user(user)


@router.get('/audit')
def audit_log():
    with SessionLocal() as db:
        return [{'id':a.id,'timestamp':a.timestamp,'actor_id':a.actor_id,'action':a.action,'target':a.target,'outcome':a.outcome}
                for a in db.scalars(select(Audit).order_by(Audit.timestamp.desc()).limit(200))]
