from __future__ import annotations
from infrastructure_runtime.security import kafka_options

from pipeline_runtime.kafka import CONSUMER_OPTIONS, DeliveryLoop
from pipeline_runtime.identity import coordinate, logical_key, stable_uuid, quarantine
from pipeline_runtime.kafka import publish_confirmed

import json
import logging
import signal
import time
from types import FrameType
from typing import Any

from confluent_kafka import Consumer, KafkaException, Producer
from pydantic import ValidationError

from app.config.settings import MLSettings
from app.config.topics import ANALYTICS_INPUT_TOPICS
from app.features.feature_mapper import FeatureMapper
from app.inference.predictor import AnomalyPredictor
from app.schemas.prediction import DeadLetterEvent
from app.utils.retry import retry_with_backoff
from app.utils.metrics import ML_ANOMALIES_TOTAL, ML_INFERENCE_LATENCY_SECONDS, ML_PREDICTION_ERRORS_TOTAL, ML_PREDICTIONS_TOTAL

logger = logging.getLogger(__name__)


class MLInferenceConsumer:
    def __init__(self, settings: MLSettings, predictor: AnomalyPredictor) -> None:
        self._settings = settings
        self._predictor = predictor
        self._feature_mapper = FeatureMapper()
        self._consumer: Consumer | None = None
        self._producer: Producer | None = None
        self._running = True

    def run(self) -> None:
        self._register_shutdown_handlers()
        self._consumer = retry_with_backoff(self._create_consumer, logger, "Kafka ML inference consumer connection")
        self._producer = retry_with_backoff(self._create_producer, logger, "Kafka ML prediction producer connection")

        logger.info(
            "subscribing_ml_inference topics=%s group=%s prediction_topic=%s",
            ANALYTICS_INPUT_TOPICS,
            self._settings.kafka_consumer_group,
            self._settings.prediction_topic,
        )
        try:
            DeliveryLoop(self._consumer, "ml").run(ANALYTICS_INPUT_TOPICS,
                lambda message: self._handle_message(message.topic(), message.value(), coordinate(message)),
                lambda: self._running)
        finally:
            self._producer.flush(15)

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
        logger.info("connected_ml_consumer brokers=%s", len(metadata.brokers))
        return consumer

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
        producer.list_topics(timeout=10)
        logger.info("connected_ml_prediction_producer")
        return producer

    def _handle_message(self, topic: str, payload: bytes | None, position=None) -> None:
        try:
            if payload is None:
                raise ValueError("Kafka message had empty payload")
            parsed_payload = json.loads(payload.decode("utf-8"))
            features = self._feature_mapper.to_feature_vector(parsed_payload)
            input_key = logical_key(topic, parsed_payload)
        except (UnicodeDecodeError, json.JSONDecodeError, ValidationError, ValueError, TypeError, KeyError) as exc:
            ML_PREDICTION_ERRORS_TOTAL.inc()
            logger.warning("unprocessable_ml_message topic=%s reason=%s position=%s", topic, exc, position)
            self._publish_deadletter(topic, str(exc), quarantine(topic, payload, exc, position))
            return

        start = time.perf_counter()
        prediction = self._predictor.predict(
            features=features, source_event_id=None, event_type=parsed_payload["event_type"],
            entity_id=parsed_payload["entity_id"],
            window_metadata={key: parsed_payload[key] for key in (
                "feature_contract_version", "entity_type", "source_event_ids", "window_start",
                "window_end", "first_event_at", "last_event_at", "calculated_at")},
        )
        prediction.prediction_id = stable_uuid("prediction-v1", input_key, prediction.model_name,
                                              prediction.model_version, prediction.model_artifact_id)
        prediction.timestamp = prediction.window_end
        self._publish_prediction(prediction)  # Failure propagates; no input commit.
        ML_PREDICTIONS_TOTAL.inc()
        if prediction.is_anomaly:
            ML_ANOMALIES_TOTAL.inc()
        ML_INFERENCE_LATENCY_SECONDS.observe(time.perf_counter() - start)
        logger.info("prediction_confirmed prediction_id=%s position=%s", prediction.prediction_id, position)

    def _publish_prediction(self, prediction: Any) -> None:
        publish_confirmed(self._producer, topic=self._settings.prediction_topic,
                          key=str(prediction.prediction_id), value=prediction.to_json_bytes())

    def _publish_deadletter(self, source_topic: str, reason: str, payload: dict) -> None:
        publish_confirmed(self._producer, topic=self._settings.deadletter_topic,
                          key=payload["event_id"], value=json.dumps(payload).encode())

    def _register_shutdown_handlers(self) -> None:
        def shutdown_handler(signum: int, _frame: FrameType | None) -> None:
            logger.info("received_shutdown_signal signal=%s", signum)
            self._running = False

        signal.signal(signal.SIGTERM, shutdown_handler)
        signal.signal(signal.SIGINT, shutdown_handler)
