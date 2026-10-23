from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.db.models import Alert, AlertDedupCache
from app.schemas.alerts import AlertEvent


class AlertRepository:
    def persist_alert(self, session: Session, alert: AlertEvent) -> None:
        statement = insert(Alert).values(
            alert_id=alert.alert_id,
            timestamp=alert.timestamp,
            severity=alert.severity,
            title=alert.title,
            description=alert.description,
            source_event_id=alert.source_event_id,
            entity_id=alert.entity_id,
            event_type=alert.event_type,
            source_topic=alert.source_topic,
            anomaly_score=alert.anomaly_score,
            ml_risk_score=alert.ml_risk_score,
            correlation_id=alert.correlation_id,
            incident_id=alert.incident_id,
            explanation=alert.explanation,
            recommended_action=alert.recommended_action,
            status=alert.status,
            tags=alert.tags,
            raw_payload=alert.raw_payload,
        ).on_conflict_do_nothing(index_elements=["alert_id"])
        session.execute(statement)

    def should_emit(self, session: Session, alert: AlertEvent, cooldown_seconds: int) -> bool:
        now = datetime.now(UTC)
        record = session.scalar(
            select(AlertDedupCache)
            .where(AlertDedupCache.dedup_key == alert.correlation_id)
            .with_for_update()
        )
        if record is None:
            session.add(
                AlertDedupCache(
                    dedup_key=alert.correlation_id,
                    alert_id=alert.alert_id,
                    entity_id=alert.entity_id,
                    counter=1,
                    first_seen=now,
                    last_seen=now,
                )
            )
            session.flush()
            return True

        record.counter += 1
        if record.last_seen >= now - timedelta(seconds=cooldown_seconds):
            return False
        record.alert_id = alert.alert_id
        record.entity_id = alert.entity_id
        record.last_seen = now
        return True

