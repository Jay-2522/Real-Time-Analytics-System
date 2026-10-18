from infrastructure_runtime.security import kafka_options
from pipeline_runtime.kafka import CONSUMER_OPTIONS, DeliveryLoop, publish_confirmed
from pipeline_runtime.identity import coordinate, stable_uuid
import json
from datetime import datetime, UTC
import logging
import signal
from types import FrameType

from confluent_kafka import Consumer, KafkaException, Producer
from pydantic import ValidationError

from app.config.settings import KafkaRuntimeSettings
from app.config.topics import CONSUMER_TOPICS
from app.schemas.events import DeadLetterEvent, parse_telemetry_event
from app.utils.metrics import CONSUMER_ERRORS_TOTAL, EVENTS_CONSUMED_TOTAL
from app.utils.retry import retry_with_backoff

logger = logging.getLogger(__name__)


class TelemetryConsumer:
    def __init__(self, settings: KafkaRuntimeSettings) -> None:
        self._settings = settings
        self._consumer: Consumer | None = None
        self._deadletter_producer: Producer | None = None
        self._running = True

    def run(self) -> None:
        self._register_shutdown_handlers()
        self._consumer = retry_with_backoff(self._create_consumer, logger, "Kafka consumer connection")
        self._deadletter_producer = retry_with_backoff(
            self._create_deadletter_producer,
            logger,
            "Kafka dead-letter producer connection",
        )

        logger.info("Subscribing to telemetry topics topics=%s group=%s", CONSUMER_TOPICS, self._settings.kafka_consumer_group)
        try:
            DeliveryLoop(self._consumer, "telemetry").run(CONSUMER_TOPICS,
                lambda message: self._handle_message(message.topic(), message.value(), coordinate(message), message.timestamp()[1]),
                lambda: self._running)
        finally:
            self._deadletter_producer.flush(10)

    def _create_consumer(self) -> Consumer:
        consumer = Consumer(
            {
                **kafka_options(),
                "bootstrap.servers": self._settings.kafka_bootstrap_servers,
                "group.id": self._settings.kafka_consumer_group,
                "client.id": self._settings.consumer_client_id,
                "auto.offset.reset": "earliest",
                **CONSUMER_OPTIONS,
            }
        )
        metadata = consumer.list_topics(timeout=10)
        logger.info("Connected telemetry consumer to Kafka cluster brokers=%s", len(metadata.brokers))
        return consumer

    def _create_deadletter_producer(self) -> Producer:
        producer = Producer(
            {
                **kafka_options(),
                "bootstrap.servers": self._settings.kafka_bootstrap_servers,
                "client.id": f"{self._settings.consumer_client_id}-deadletter",
                "acks": "all",
                "enable.idempotence": True,
                "delivery.timeout.ms": 10000,
                "retries": 5,
            }
        )
        producer.list_topics(timeout=10)
        return producer

    def _handle_message(self, topic: str, payload: bytes, position=None, source_timestamp=None) -> None:
        try:
            event = parse_telemetry_event(payload)
            logger.info(
                "consumed event_id=%s event_type=%s topic=%s risk_score=%s",
                event.event_id,
                event.event_type,
                topic,
                event.risk_score,
            )
            EVENTS_CONSUMED_TOTAL.labels(topic, event.event_type).inc()
        except (ValidationError, UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as exc:
            logger.warning("Malformed telemetry message topic=%s reason=%s", topic, exc)
            CONSUMER_ERRORS_TOTAL.labels(topic).inc()
            self._publish_deadletter(topic, str(exc), payload, position, source_timestamp)

    def _publish_deadletter(self, source_topic: str, reason: str, payload: bytes, position=None, source_timestamp=None) -> None:
        assert self._deadletter_producer is not None
        try:
            decoded_payload: object = json.loads(payload.decode("utf-8"))
        except Exception:
            decoded_payload = {"raw_payload": (payload or b"").decode("utf-8", errors="replace")}

        # Kafka timestamps survive replay. An absent timestamp uses an explicit epoch
        # sentinel so retry cannot conflict with the same logical DLQ identity.
        timestamp = datetime.fromtimestamp(source_timestamp / 1000, UTC) if source_timestamp is not None and source_timestamp >= 0 else datetime(1970, 1, 1, tzinfo=UTC)
        deadletter = DeadLetterEvent(timestamp=timestamp, event_id=stable_uuid("telemetry-dlq-v1", source_topic, (payload or b"").hex(), position), source_topic=source_topic, reason=reason, payload=decoded_payload)
        publish_confirmed(self._deadletter_producer, topic=self._settings.deadletter_topic,
                          key=str(deadletter.event_id), value=deadletter.to_json_bytes())

    def _register_shutdown_handlers(self) -> None:
        def shutdown_handler(signum: int, _frame: FrameType | None) -> None:
            logger.info("Received shutdown signal=%s", signum)
            self._running = False

        signal.signal(signal.SIGTERM, shutdown_handler)
        signal.signal(signal.SIGINT, shutdown_handler)
