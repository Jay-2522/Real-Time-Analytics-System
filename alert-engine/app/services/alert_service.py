from pipeline_runtime.durable import claim, enqueue
from pipeline_runtime.identity import logical_key, stable_uuid, digest
from sqlalchemy import text, update
from app.db.models import Alert
import logging

from sqlalchemy.orm import Session

from app.dedup.redis_deduplicator import AlertDeduplicator
from app.elastic.indexer import AlertIndexer
from app.incidents.correlator import IncidentCorrelator
from app.repositories.alert_repository import AlertRepository
from app.rules.rule_engine import AlertRulesEngine
from app.utils.metrics import ALERTS_GENERATED_TOTAL, CRITICAL_ALERTS_TOTAL

logger = logging.getLogger(__name__)


class AlertProcessingService:
    def __init__(
        self,
        rules: AlertRulesEngine,
        deduplicator: AlertDeduplicator,
        correlator: IncidentCorrelator,
        repository: AlertRepository,
        indexer: AlertIndexer,
    ) -> None:
        self._rules = rules
        self._deduplicator = deduplicator
        self._correlator = correlator
        self._repository = repository
        self._indexer = indexer

    def process(self, session: Session, source_topic: str, payload: dict) -> int:
        input_key = logical_key(source_topic, payload)
        if not claim(session, "alert", input_key, payload):
            return 0
        emitted = 0
        for alert in sorted(self._rules.evaluate(source_topic, payload), key=lambda alert: alert.correlation_id):
            alert.alert_id = str(stable_uuid("alert-rules-v1", input_key, alert.correlation_id, alert.severity))
            # Serialize distinct inputs sharing a correlation key, including the no-incident-yet case.
            session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"), {"key": alert.correlation_id})
            self._repository.update_dedup_cache(session, alert)
            self._repository.persist_alert(session, alert)
            incident_id = self._correlator.correlate(session, alert)
            alert.incident_id = incident_id
            session.execute(update(Alert).where(Alert.alert_id == alert.alert_id).values(incident_id=incident_id))
            body = alert.model_dump(mode="json")
            for target in ("elasticsearch", "redis"):
                enqueue(session, "alert", digest(alert.alert_id), target, body)
            emitted += 1
            ALERTS_GENERATED_TOTAL.labels(alert.severity).inc()
            if alert.severity == "critical":
                CRITICAL_ALERTS_TOTAL.inc()
            logger.info(
                "alert_generated alert_id=%s severity=%s entity_id=%s incident_id=%s",
                alert.alert_id,
                alert.severity,
                alert.entity_id,
                incident_id,
            )
        return emitted
