from infrastructure_runtime.security import kafka_options
import json
import logging
import signal
import time
from datetime import UTC, datetime
from types import FrameType
from typing import Any
from uuid import uuid4

from confluent_kafka import Consumer
from sqlalchemy.orm import sessionmaker

from app.config.settings import ThreatIntelSettings
from app.config.topics import THREAT_INPUT_TOPICS
from app.detections.rule_engine import DetectionRuleEngine
from app.elastic.indexer import ThreatIndexer
from app.enrichment.ioc_enricher import IOCEnricher
from app.repositories.threat_repository import ThreatRepository
from app.schemas.threat import ThreatEnrichment
from app.scoring.threat_scorer import ThreatScorer
from app.utils.metrics import THREAT_ENRICHMENT_LATENCY_SECONDS, THREAT_ENGINE_ERRORS_TOTAL, THREAT_EVENTS_PROCESSED_TOTAL
from app.utils.retry import retry_with_backoff

logger = logging.getLogger(__name__)


class ThreatIntelConsumer:
    def __init__(
        self,
        settings: ThreatIntelSettings,
        session_factory: sessionmaker,
        enricher: IOCEnricher,
        rules: DetectionRuleEngine,
        scorer: ThreatScorer,
        repository: ThreatRepository,
        indexer: ThreatIndexer,
    ) -> None:
        self._settings = settings
        self._session_factory = session_factory
        self._enricher = enricher
        self._rules = rules
        self._scorer = scorer
        self._repository = repository
        self._indexer = indexer
        self._consumer: Consumer | None = None
        self._running = True

    def run(self) -> None:
        self._register_shutdown_handlers()
        self._consumer = retry_with_backoff(self._create_consumer, logger, "Kafka threat-intelligence consumer connection")
        logger.info("subscribing_threat_intelligence topics=%s group=%s", THREAT_INPUT_TOPICS, self._settings.consumer_group)
        self._consumer.subscribe(list(THREAT_INPUT_TOPICS))

        while self._running:
            message = self._consumer.poll(timeout=1.0)
            if message is None:
                continue
            if message.error():
                logger.error("threat_intel_kafka_error error=%s", message.error())
                continue
            self._handle_message(message.topic(), message.value())

        logger.info("closing_threat_intelligence_consumer")
        self._consumer.close()

    def _create_consumer(self) -> Consumer:
        consumer = Consumer(
            {
                **kafka_options(),
                "bootstrap.servers": self._settings.kafka_bootstrap_servers,
                "group.id": self._settings.consumer_group,
                "client.id": "streaming-analytics-threat-intelligence",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            }
        )
        metadata = consumer.list_topics(timeout=10)
        logger.info("threat_intel_kafka_connected brokers=%s", len(metadata.brokers))
        return consumer

    def _handle_message(self, topic: str, payload: bytes | None) -> None:
        start = time.perf_counter()
        try:
            if payload is None:
                return
            parsed = json.loads(payload.decode("utf-8"))
            enrichment = self._enrich(topic, parsed)
            with self._session_factory() as session:
                self._repository.persist(session, enrichment)
                session.commit()
            self._indexer.index(enrichment)
            THREAT_EVENTS_PROCESSED_TOTAL.labels(topic).inc()
            THREAT_ENRICHMENT_LATENCY_SECONDS.observe(time.perf_counter() - start)
            logger.info(
                "threat_event_enriched event_id=%s entity_id=%s score=%s severity=%s tactic=%s rule_hits=%s iocs=%s",
                enrichment.event_id,
                enrichment.entity_id,
                enrichment.threat_score,
                enrichment.severity,
                enrichment.tactic,
                len(enrichment.rule_hits),
                len(enrichment.iocs),
            )
        except Exception:
            THREAT_ENGINE_ERRORS_TOTAL.labels("message_processing").inc()
            logger.exception("threat_intel_message_failed topic=%s", topic)

    def _enrich(self, topic: str, payload: dict[str, Any]) -> ThreatEnrichment:
        base_risk = float(payload.get("ml_risk_score") or payload.get("risk_score") or payload.get("risk_score_moving_average") or 0)
        event_type = str(payload.get("event_type") or topic.split(".")[-2])
        entity_id = self._entity_id(payload)
        source_event_id = str(payload.get("prediction_id") or payload.get("event_id") or payload.get("source_event_id") or uuid4())
        iocs, country, asn = self._enricher.enrich(payload)
        rule_hits = self._rules.evaluate(payload, base_risk)
        mitre = self._mitre_from(rule_hits, payload, base_risk)
        threat_score, confidence, severity = self._scorer.score(base_risk, iocs, rule_hits, mitre["severity_weight"])
        return ThreatEnrichment(
            source_topic=topic,
            source_event_id=source_event_id,
            entity_id=entity_id,
            entity_type=self._entity_type(entity_id),
            event_type=event_type,
            tactic=mitre["tactic"],
            technique=mitre["technique"],
            mitre_id=mitre["mitre_id"],
            kill_chain_stage=mitre["kill_chain_stage"],
            mitre_confidence=mitre["confidence"],
            iocs=iocs,
            rule_hits=rule_hits,
            geo_country=country,
            asn=asn,
            threat_score=threat_score,
            confidence=confidence,
            severity=severity,
            raw_payload=payload,
        )

    @staticmethod
    def _mitre_from(rule_hits: list, payload: dict[str, Any], base_risk: float) -> dict[str, Any]:
        if rule_hits:
            top = max(rule_hits, key=lambda hit: hit.score)
            return {"tactic": top.tactic, "technique": top.technique, "mitre_id": top.mitre_id, "kill_chain_stage": top.tactic.lower().replace(" ", "-"), "confidence": 0.88, "severity_weight": top.score}
        text = str(payload).lower()
        if "login" in text or "credential" in text:
            return {"tactic": "Credential Access", "technique": "Brute Force", "mitre_id": "T1110", "kill_chain_stage": "credential-access", "confidence": 0.52, "severity_weight": 60}
        if "network" in text or "bytes" in text:
            return {"tactic": "Command and Control", "technique": "Application Layer Protocol", "mitre_id": "T1071", "kill_chain_stage": "command-and-control", "confidence": 0.48, "severity_weight": 55}
        return {"tactic": "Discovery", "technique": "Network Service Discovery", "mitre_id": "T1046", "kill_chain_stage": "discovery", "confidence": 0.40, "severity_weight": max(base_risk, 35)}

    @staticmethod
    def _entity_id(payload: dict[str, Any]) -> str:
        return str(payload.get("entity_id") or payload.get("user_id") or payload.get("username") or payload.get("source_ip") or payload.get("destination_ip") or "unknown")

    @staticmethod
    def _entity_type(entity_id: str) -> str:
        if entity_id.count(".") == 3:
            return "ip"
        if entity_id.startswith("dev-") or entity_id.startswith("host-"):
            return "device"
        if "@" in entity_id or entity_id.startswith("user") or entity_id.startswith("u-"):
            return "user"
        return "entity"

    def _register_shutdown_handlers(self) -> None:
        def shutdown_handler(signum: int, _frame: FrameType | None) -> None:
            logger.info("received_shutdown_signal signal=%s", signum)
            self._running = False

        signal.signal(signal.SIGTERM, shutdown_handler)
        signal.signal(signal.SIGINT, shutdown_handler)
