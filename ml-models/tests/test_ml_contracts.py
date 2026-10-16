import sys
from pathlib import Path
from uuid import UUID


def load_ml_module(module: str):
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    return __import__(module, fromlist=["*"])


def test_feature_mapper_maps_login_failures():
    from test_feature_windows import payload
    mapper = load_ml_module("app.features.feature_mapper").FeatureMapper()
    window = payload()
    window["source_event_ids"] = ["failed-login"]
    window["features"].update(
        event_count=1, login_event_count=1, api_event_count=0, network_event_count=0, anomaly_event_count=0,
        failed_login_rate=1, failed_auth_count=1, requests_per_minute=0, avg_api_response_time=None,
        endpoint_error_rate=None, api_5xx_rate=None, avg_network_bytes=None, high_risk_network_event_count=0,
        anomaly_rate=0, risk_score_moving_average=81, high_risk_event_count=1,
        distinct_login_country_count=1, unique_destination_ip_count=0,
    )
    features = mapper.to_feature_vector(window)
    assert features["failed_login_rate"] == 1.0
    assert features["failed_auth_count"] == 1.0
    assert features["risk_score_moving_average"] == 81.0


def test_prediction_schema_serializes_uuid_and_scores():
    schema_module = load_ml_module("app.schemas.prediction")
    prediction = schema_module.PredictionEvent(
        model_name="isolation_forest",
        model_version="test",
        event_type="login",
        entity_id="user-1",
        anomaly_score=0.91,
        is_anomaly=True,
        ml_risk_score=88,
        confidence_score=0.93,
        severity="high",
        explanation="test",
        features_used={"failed_login_rate": 1.0},
    )
    assert isinstance(prediction.prediction_id, UUID)
    assert b"ml_risk_score" in prediction.to_json_bytes()


def test_model_artifact_contract_detects_missing_artifacts(tmp_path):
    artifact_module = load_ml_module("app.models.artifact_store")
    assert artifact_module.artifacts_exist(tmp_path) is False
