from infrastructure_runtime.security import kafka_options
from pipeline_runtime.kafka import publish_confirmed
import logging
import signal
import time
from types import FrameType

from confluent_kafka import KafkaException, Producer

from app.config.settings import KafkaRuntimeSettings
from app.config.topics import ANOMALY_EVENTS, API_EVENTS, LOGIN_EVENTS, NETWORK_EVENTS
from app.generators.telemetry_generator import SyntheticTelemetryGenerator
from app.schemas.events import AnomalyEvent, ApiEvent, LoginEvent, NetworkEvent, TelemetryEvent
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
            while self._running:
                try:
                    self._publish_event(event)
                    break
                except (KafkaException, BufferError, RuntimeError):
                    PRODUCER_ERRORS_TOTAL.labels(self._topic_for_event(event)).inc()
                    logger.exception("telemetry_delivery_unconfirmed event_id=%s; retrying same event", event.event_id)
                    time.sleep(1)
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
                "delivery.timeout.ms": 10000,
            }
        )
        metadata = producer.list_topics(timeout=10)
        logger.info("Connected to Kafka cluster brokers=%s", len(metadata.brokers))
        return producer

    def _publish_event(self, event: TelemetryEvent) -> None:
        assert self._producer is not None
        topic = self._topic_for_event(event)

        publish_confirmed(self._producer, topic=topic, key=str(event.event_id), value=event.to_json_bytes())
        EVENTS_PUBLISHED_TOTAL.labels(topic, event.event_type).inc()
        logger.info("published_confirmed event_id=%s topic=%s", event.event_id, topic)

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

    def _register_shutdown_handlers(self) -> None:
        def shutdown_handler(signum: int, _frame: FrameType | None) -> None:
            logger.info("Received shutdown signal=%s", signum)
            self._running = False

        signal.signal(signal.SIGTERM, shutdown_handler)
        signal.signal(signal.SIGINT, shutdown_handler)
