from infrastructure_runtime.security import kafka_options
import logging
import signal
import time
from types import FrameType

from confluent_kafka import KafkaException, Producer

from app.config.settings import KafkaRuntimeSettings
from app.config.topics import ANOMALY_EVENTS, API_EVENTS, LOGIN_EVENTS, NETWORK_EVENTS
from app.generators.telemetry_generator import SyntheticTelemetryGenerator
from app.schemas.events import AnomalyEvent, ApiEvent, DeadLetterEvent, LoginEvent, NetworkEvent, TelemetryEvent
from app.utils.metrics import EVENTS_PUBLISHED_TOTAL, PRODUCER_ERRORS_TOTAL
from app.utils.retry import retry_with_backoff

logger = logging.getLogger(__name__)


class TelemetryProducer:
    def __init__(self, settings: KafkaRuntimeSettings) -> None:
        self._settings = settings
        self._generator = SyntheticTelemetryGenerator(anomaly_rate=settings.anomaly_rate)
        self._producer: Producer | None = None
        self._running = True

    def run(self) -> None:
        self._register_shutdown_handlers()
        self._producer = retry_with_backoff(self._create_producer, logger, "Kafka producer connection")

        interval_seconds = 1.0 / self._settings.event_rate_per_second
        logger.info(
            "Starting telemetry producer bootstrap_servers=%s event_rate_per_second=%s anomaly_rate=%s",
            self._settings.kafka_bootstrap_servers,
            self._settings.event_rate_per_second,
            self._settings.anomaly_rate,
        )

        while self._running:
            event = self._generator.next_event()
            self._publish_event(event)
            self._producer.poll(0)
            time.sleep(interval_seconds)

        logger.info("Flushing telemetry producer before shutdown")
        self._producer.flush(10)

    def _create_producer(self) -> Producer:
        producer = Producer(
            {
                **kafka_options(),
                "bootstrap.servers": self._settings.kafka_bootstrap_servers,
                "client.id": self._settings.producer_client_id,
                "acks": "all",
                "retries": 5,
                "linger.ms": 25,
                "enable.idempotence": True,
            }
        )
        metadata = producer.list_topics(timeout=10)
        logger.info("Connected to Kafka cluster brokers=%s", len(metadata.brokers))
        return producer

    def _publish_event(self, event: TelemetryEvent) -> None:
        assert self._producer is not None
        topic = self._topic_for_event(event)

        try:
            payload = event.to_json_bytes()
            self._producer.produce(
                topic=topic,
                key=str(event.event_id),
                value=payload,
                callback=self._delivery_report,
            )
            logger.info(
                "published event_id=%s event_type=%s topic=%s risk_score=%s",
                event.event_id,
                event.event_type,
                topic,
                event.risk_score,
            )
            EVENTS_PUBLISHED_TOTAL.labels(topic, event.event_type).inc()
        except (KafkaException, BufferError, ValueError, TypeError) as exc:
            logger.exception("Failed to publish event_id=%s event_type=%s", event.event_id, event.event_type)
            PRODUCER_ERRORS_TOTAL.labels(topic).inc()
            self._publish_deadletter(source_topic=topic, reason=str(exc), payload=event.model_dump(mode="json"))

    def _publish_deadletter(self, source_topic: str, reason: str, payload: object) -> None:
        assert self._producer is not None
        deadletter = DeadLetterEvent(source_topic=source_topic, reason=reason, payload=payload)
        self._producer.produce(
            topic=self._settings.deadletter_topic,
            key=str(deadletter.event_id),
            value=deadletter.to_json_bytes(),
            callback=self._delivery_report,
        )

    @staticmethod
    def _topic_for_event(event: TelemetryEvent) -> str:
        if isinstance(event, LoginEvent):
            return LOGIN_EVENTS
        if isinstance(event, ApiEvent):
            return API_EVENTS
        if isinstance(event, NetworkEvent):
            return NETWORK_EVENTS
        if isinstance(event, AnomalyEvent):
            return ANOMALY_EVENTS
        raise TypeError(f"Unsupported telemetry event type: {type(event).__name__}")

    @staticmethod
    def _delivery_report(error: KafkaException | None, message: object) -> None:
        if error is not None:
            logger.error("Kafka delivery failed: %s", error)
            topic = message.topic() if hasattr(message, "topic") else "unknown"
            PRODUCER_ERRORS_TOTAL.labels(topic).inc()

    def _register_shutdown_handlers(self) -> None:
        def shutdown_handler(signum: int, _frame: FrameType | None) -> None:
            logger.info("Received shutdown signal=%s", signum)
            self._running = False

        signal.signal(signal.SIGTERM, shutdown_handler)
        signal.signal(signal.SIGINT, shutdown_handler)
