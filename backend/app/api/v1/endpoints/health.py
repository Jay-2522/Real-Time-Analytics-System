from fastapi import APIRouter
from fastapi.responses import JSONResponse
from app.services.health_service import is_ready

router = APIRouter()


@router.get('')
@router.get('/live')
def liveness():
    return {'status': 'alive'}


@router.get('/ready')
def readiness():
    ready = is_ready()
    return JSONResponse({'status': 'ready' if ready else 'not_ready'}, status_code=200 if ready else 503,
                        headers={'Cache-Control': 'no-store'})
