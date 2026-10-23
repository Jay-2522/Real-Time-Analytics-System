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
    assert alerts[0].title == "Positive ML anomaly decision"
    assert alerts[0].correlation_id.startswith("model_positive:")


def test_code_rules_have_positive_and_negative_cases():
    rule_module = load_alert_module("app.rules.rule_engine")
    engine = rule_module.AlertRulesEngine()

    high_positive = engine.evaluate("ml.anomaly.predictions", {"entity_id": "e1", "anomaly_score": 0.9})
    high_negative = engine.evaluate("analytics.enriched.events", {"entity_id": "e1", "anomaly_score": 0.9})
    assert "high_anomaly" in {alert.correlation_id.split(":", 1)[0] for alert in high_positive}
    assert "high_anomaly" not in {alert.correlation_id.split(":", 1)[0] for alert in high_negative}

    model_positive = engine.evaluate("ml.anomaly.predictions", {"entity_id": "e2", "is_anomaly": True, "anomaly_score": 0.2})
    model_negative = engine.evaluate("ml.anomaly.predictions", {"entity_id": "e2", "is_anomaly": False, "anomaly_score": 0.2})
    assert "model_positive" in {alert.correlation_id.split(":", 1)[0] for alert in model_positive}
    assert "model_positive" not in {alert.correlation_id.split(":", 1)[0] for alert in model_negative}

    api_positive = engine.evaluate("analytics.enriched.events", {"event_type": "api", "entity_id": "e3", "status_code": 503})
    api_negative = engine.evaluate("analytics.enriched.events", {"event_type": "api", "entity_id": "e3", "status_code": 404})
    assert "api_server_error" in {alert.correlation_id.split(":", 1)[0] for alert in api_positive}
    assert "api_server_error" not in {alert.correlation_id.split(":", 1)[0] for alert in api_negative}

    network_positive = engine.evaluate(
        "analytics.enriched.events",
        {"event_type": "network", "entity_id": "e4", "bytes_sent": 5_000_000, "bytes_received": 3_000_000},
    )
    network_negative = engine.evaluate(
        "analytics.enriched.events",
        {"event_type": "network", "entity_id": "e4", "bytes_sent": 500_000, "bytes_received": 300_000},
    )
    assert "large_network_transfer" in {alert.correlation_id.split(":", 1)[0] for alert in network_positive}
    assert "large_network_transfer" not in {alert.correlation_id.split(":", 1)[0] for alert in network_negative}

    risk_positive = engine.evaluate("analytics.risk.metrics", {"entity_id": "e5", "high_risk_event_count": 5})
    risk_negative = engine.evaluate("analytics.risk.metrics", {"entity_id": "e5", "high_risk_event_count": 4})
    assert "correlated_risk" in {alert.correlation_id.split(":", 1)[0] for alert in risk_positive}
    assert "correlated_risk" not in {alert.correlation_id.split(":", 1)[0] for alert in risk_negative}

    credential_positive_candidate = engine.evaluate(
        "analytics.enriched.events", {"event_type": "login", "entity_id": "e6", "login_success": False}
    )
    credential_negative = engine.evaluate(
        "analytics.enriched.events", {"event_type": "login", "entity_id": "e6", "login_success": True}
    )
    assert "credential_attack" not in {alert.correlation_id.split(":", 1)[0] for alert in credential_positive_candidate}
    assert "credential_attack" not in {alert.correlation_id.split(":", 1)[0] for alert in credential_negative}


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
