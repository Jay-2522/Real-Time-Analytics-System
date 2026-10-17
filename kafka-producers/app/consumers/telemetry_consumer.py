from infrastructure_runtime.security import kafka_options
import json
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
        self._consumer.subscribe(list(CONSUMER_TOPICS))

        while self._running:
            message = self._consumer.poll(timeout=1.0)
            if message is None:
                continue

            if message.error():
                logger.error("Kafka consumer error: %s", message.error())
                continue

            self._handle_message(message.topic(), message.value())

        logger.info("Closing telemetry consumer")
        self._consumer.close()
        self._deadletter_producer.flush(10)

    def _create_consumer(self) -> Consumer:
        consumer = Consumer(
            {
                **kafka_options(),
                "bootstrap.servers": self._settings.kafka_bootstrap_servers,
                "group.id": self._settings.kafka_consumer_group,
                "client.id": self._settings.consumer_client_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
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
                "retries": 5,
            }
        )
        producer.list_topics(timeout=10)
        return producer

    def _handle_message(self, topic: str, payload: bytes) -> None:
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
        except (ValidationError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            logger.warning("Malformed telemetry message topic=%s reason=%s", topic, exc)
            CONSUMER_ERRORS_TOTAL.labels(topic).inc()
            self._publish_deadletter(topic, str(exc), payload)

    def _publish_deadletter(self, source_topic: str, reason: str, payload: bytes) -> None:
        assert self._deadletter_producer is not None
        try:
            decoded_payload: object = json.loads(payload.decode("utf-8"))
        except Exception:
            decoded_payload = {"raw_payload": payload.decode("utf-8", errors="replace")}

        deadletter = DeadLetterEvent(source_topic=source_topic, reason=reason, payload=decoded_payload)
        self._deadletter_producer.produce(
            topic=self._settings.deadletter_topic,
            key=str(deadletter.event_id),
            value=deadletter.to_json_bytes(),
        )
        self._deadletter_producer.poll(0)

    def _register_shutdown_handlers(self) -> None:
        def shutdown_handler(signum: int, _frame: FrameType | None) -> None:
            logger.info("Received shutdown signal=%s", signum)
            self._running = False

        signal.signal(signal.SIGTERM, shutdown_handler)
        signal.signal(signal.SIGINT, shutdown_handler)
