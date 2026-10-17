from pathlib import Path
import secrets
from starlette.concurrency import run_in_threadpool
from starlette.requests import HTTPConnection
from starlette.responses import JSONResponse
from starlette.websockets import WebSocketDisconnect
from app.auth.policy import permission
from app.auth.service import audit, csrf_token, principal
from app.core.settings import settings
from app.db.session import SessionLocal


def lookup(token, touch=True):
    with SessionLocal() as db, db.begin():
        return principal(db, token, touch)


def record_denial(actor, path, outcome):
    with SessionLocal() as db, db.begin():
        audit(db, 'modification_denied', path, outcome, actor)


class AccessControlMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope['type'] not in ('http', 'websocket'):
            return await self.app(scope, receive, send)
        connection = HTTPConnection(scope)
        path = scope['path']
        origin = connection.headers.get('origin')
        token = connection.cookies.get(settings.session_cookie, '')
        if scope['type'] == 'websocket':
            allowed = path in {settings.api_v1_prefix + '/ws/' + p for p in ('dashboard', 'alerts', 'threat-intel')}
            user = await run_in_threadpool(lookup, token, False) if allowed and origin in settings.cors_origins else None
            if not user:
                return await send({'type':'websocket.close', 'code':4401})
            async def checked_send(message):
                if message['type'] in ('websocket.accept', 'websocket.send'):
                    if not await run_in_threadpool(lookup, token, False):
                        await send({'type':'websocket.close', 'code':4401})
                        raise WebSocketDisconnect(4401)
                await send(message)
            scope.setdefault('state', {})['principal'] = user
            return await self.app(scope, receive, checked_send)
        method = scope['method']
        # CORS middleware handles preflights without returning protected content.
        if method == 'OPTIONS':
            return await self.app(scope, receive, send)
        async def deny(code, detail):
            await JSONResponse({'detail':detail}, status_code=code, headers={'Cache-Control':'no-store'})(scope, receive, send)
        if path == '/metrics':
            try:
                expected = Path(settings.metrics_token_file).read_text().strip()
            except (OSError, ValueError):
                return await deny(503, 'Metrics authentication unavailable')
            if method != 'GET' or len(expected) < 32 or not secrets.compare_digest(connection.headers.get('authorization','').encode(), ('Bearer ' + expected).encode()):
                return await deny(401, 'Service authentication required')
            return await self.app(scope, receive, send)
        if method == 'GET' and path == settings.api_v1_prefix + '/health':
            return await self.app(scope, receive, send)
        if method == 'POST' and path == settings.api_v1_prefix + '/auth/login':
            if origin not in settings.cors_origins or connection.headers.get('x-requested-with') != 'SOC':
                return await deny(403, 'Invalid login origin or CSRF header')
            if settings.auth_local_http:
                if connection.url.hostname not in ('localhost', '127.0.0.1', '::1'):
                    return await deny(403, 'Local HTTP authentication requires a loopback host')
            elif connection.url.scheme != 'https':
                return await deny(400, 'HTTPS is required')
            return await self.app(scope, receive, send)
        user = await run_in_threadpool(lookup, token)
        if not user:
            if method not in ('GET', 'HEAD'):
                await run_in_threadpool(record_denial, None, path, 'unauthenticated')
            return await deny(401, 'Authentication required')
        scope.setdefault('state', {})['principal'] = user
        required = permission(path, method, settings.api_v1_prefix)
        ranks = {'viewer':0, 'analyst':1, 'administrator':2}
        if required is None or ranks[user['role']] < ranks[required]:
            if method not in ('GET', 'HEAD'):
                await run_in_threadpool(record_denial, user['id'], path, 'forbidden')
            return await deny(403, 'Permission denied')
        if method not in ('GET', 'HEAD'):
            if origin not in settings.cors_origins or not secrets.compare_digest(connection.headers.get('x-csrf-token', '').encode(), csrf_token(token).encode()):
                await run_in_threadpool(record_denial, user['id'], path, 'csrf_rejected')
                return await deny(403, 'CSRF validation failed')
        async def private_send(message):
            if message['type'] == 'http.response.start':
                message['headers'] = list(message.get('headers', [])) + [(b'cache-control', b'no-store')]
            await send(message)
        return await self.app(scope, receive, private_send)
