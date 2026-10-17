from fastapi.testclient import TestClient
from test_api_contracts import load_backend_app


def test_liveness_is_independent_and_readiness_checks_database(tmp_path, monkeypatch):
    app = load_backend_app(tmp_path)
    from app.services import health_service
    with TestClient(app) as client:
        assert client.get('/api/v1/health/ready').status_code == 200
        def unavailable(): raise RuntimeError('secret database topology must not leak')
        monkeypatch.setattr(health_service, 'readiness_engine', unavailable)
        result = client.get('/api/v1/health/ready')
        assert result.status_code == 503 and result.json() == {'status':'not_ready'}
        assert client.get('/api/v1/health/live').json() == {'status':'alive'}
        assert client.get('/api/v1/monitoring/services').status_code == 401
