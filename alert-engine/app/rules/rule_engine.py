from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.rules.explanations import explain_alert
from app.schemas.alerts import AlertEvent


class AlertRulesEngine:
    def evaluate(self, source_topic: str, payload: dict[str, Any]) -> list[AlertEvent]:
        event_type = str(payload.get("event_type") or payload.get("metric_type") or payload.get("feature_name") or "analytics")
        entity_id = str(payload.get("entity_id") or payload.get("source_ip") or payload.get("user_id") or payload.get("endpoint") or "unknown")
        anomaly_score = _float(payload.get("anomaly_score"))
        ml_risk_score = _float(payload.get("ml_risk_score") or payload.get("risk_score") or payload.get("risk_score_moving_average"))
        source_event_id = _string(payload.get("source_event_id") or payload.get("event_id") or payload.get("prediction_id"))
        timestamp = _timestamp(payload.get("timestamp") or payload.get("event_timestamp") or payload.get("calculated_at"))

        candidates: list[tuple[str, str, str, list[str]]] = []
        if anomaly_score >= 0.88 or ml_risk_score >= 90:
            candidates.append(("high_anomaly", "critical", "Critical anomaly detected", ["ml", "critical", event_type]))
        elif anomaly_score >= 0.72 or ml_risk_score >= 75:
            candidates.append(("high_anomaly", "high", "High-risk anomaly detected", ["ml", "high", event_type]))
        elif anomaly_score >= 0.55 or ml_risk_score >= 60:
            candidates.append(("high_anomaly", "medium", "Elevated anomaly signal", ["ml", "medium", event_type]))
        elif payload.get("is_anomaly") is True:
            candidates.append(("model_anomaly", "medium", "ML anomaly detected", ["ml", "medium", event_type]))

        if event_type == "login" and (payload.get("login_success") is False or _float(payload.get("failed_login_rate")) >= 0.5):
            candidates.append(("credential_attack", "high", "Possible credential attack", ["identity", "login", "credential-stuffing"]))
        if event_type == "api" and (_float(payload.get("endpoint_error_rate")) >= 0.4 or _float(payload.get("requests_per_minute")) >= 80 or int(payload.get("status_code") or 200) >= 500):
            candidates.append(("api_abuse", "medium", "Possible API abuse pattern", ["api", "abuse"]))
        if event_type == "network" and (_float(payload.get("network_bytes_spike_score")) >= 6 or _float(payload.get("avg_network_bytes")) >= 150000):
            candidates.append(("network_spike", "medium", "Network spike anomaly", ["network", "traffic-spike"]))
        if _float(payload.get("suspicious_ip_frequency")) >= 10 or _float(payload.get("high_risk_event_count")) >= 5:
            candidates.append(("correlated_risk", "high", "Repeated high-risk activity", ["correlation", "repeat-activity"]))

        alerts = []
        for rule_name, severity, title, tags in candidates:
            explanation, action = explain_alert(rule_name, event_type, severity)
            alerts.append(
                AlertEvent(
                    timestamp=timestamp,
                    severity=severity,
                    title=title,
                    description=f"{title} for entity {entity_id} from {source_topic}.",
                    source_event_id=source_event_id,
                    entity_id=entity_id,
                    event_type=event_type,
                    source_topic=source_topic,
                    anomaly_score=anomaly_score or None,
                    ml_risk_score=ml_risk_score or None,
                    correlation_id=f"{rule_name}:{entity_id}",
                    explanation=explanation,
                    recommended_action=action,
                    tags=tags,
                    raw_payload=payload,
                )
            )
        return alerts


def _float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _string(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return datetime.now(UTC)
    return datetime.now(UTC)

