import sys
from pathlib import Path


def load_alert_module(module: str):
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    return __import__(module, fromlist=["*"])


def test_rule_engine_generates_high_risk_alert():
    rule_module = load_alert_module("app.rules.rule_engine")
    alerts = rule_module.AlertRulesEngine().evaluate(
        "ml.anomaly.predictions",
        {"event_type": "login", "entity_id": "user-1", "anomaly_score": 0.95, "ml_risk_score": 94},
    )
    assert alerts
    assert alerts[0].severity == "critical"


def test_rule_engine_honors_positive_model_decision_below_score_threshold():
    rule_module = load_alert_module("app.rules.rule_engine")
    alerts = rule_module.AlertRulesEngine().evaluate(
        "ml.anomaly.predictions",
        {
            "event_type": "feature_window",
            "entity_id": "2001:db8::1",
            "anomaly_score": 0.52,
            "ml_risk_score": 52,
            "is_anomaly": True,
        },
    )
    assert len(alerts) == 1
    assert alerts[0].severity == "medium"
    assert alerts[0].title == "ML anomaly detected"


def test_deduplicator_emits_when_redis_unavailable():
    settings_module = load_alert_module("app.config.settings")
    dedup_module = load_alert_module("app.dedup.redis_deduplicator")
    schema_module = load_alert_module("app.schemas.alerts")
    settings = settings_module.AlertEngineSettings(redis_host="127.0.0.1", redis_port=1)
    deduplicator = dedup_module.AlertDeduplicator(settings)
    alert = schema_module.AlertEvent(
        severity="high",
        title="Test",
        description="Test alert",
        entity_id="entity-1",
        event_type="login",
        source_topic="test",
        correlation_id="test:entity-1",
        explanation="test",
        recommended_action="test",
        tags=["test"],
        raw_payload={},
    )
    assert deduplicator.should_emit(alert) is True


def test_severity_escalation_rules():
    correlator_module = load_alert_module("app.incidents.correlator")
    assert correlator_module.IncidentCorrelator._escalate("medium", "medium", 3) == "high"
    assert correlator_module.IncidentCorrelator._escalate("high", "high", 3) == "critical"
