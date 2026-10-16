from __future__ import annotations

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
        self._consumer.subscribe(list(ANALYTICS_INPUT_TOPICS))

        while self._running:
            message = self._consumer.poll(timeout=1.0)
            if message is None:
                continue
            if message.error():
                logger.error("Kafka ML consumer error: %s", message.error())
                continue
            self._handle_message(message.topic(), message.value())

        logger.info("closing_ml_inference_consumer")
        self._consumer.close()
        self._producer.flush(10)

    def _create_consumer(self) -> Consumer:
        consumer = Consumer(
            {
                "bootstrap.servers": self._settings.kafka_bootstrap_servers,
                "group.id": self._settings.kafka_consumer_group,
                "client.id": self._settings.consumer_client_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            }
        )
        metadata = consumer.list_topics(timeout=10)
        logger.info("connected_ml_consumer brokers=%s", len(metadata.brokers))
        return consumer

    def _create_producer(self) -> Producer:
        producer = Producer(
            {
                "bootstrap.servers": self._settings.kafka_bootstrap_servers,
                "client.id": self._settings.producer_client_id,
                "acks": "all",
                "retries": 5,
                "linger.ms": 25,
                "enable.idempotence": True,
            }
        )
        producer.list_topics(timeout=10)
        logger.info("connected_ml_prediction_producer")
        return producer

    def _handle_message(self, topic: str, payload: bytes | None) -> None:
        if payload is None:
            self._publish_deadletter(topic, "Kafka message had empty payload", {"raw_payload": None})
            return

        try:
            start = time.perf_counter()
            parsed_payload = json.loads(payload.decode("utf-8"))
            features = self._feature_mapper.to_feature_vector(parsed_payload)
            prediction = self._predictor.predict(
                features=features,
                source_event_id=self._feature_mapper.source_event_id(parsed_payload),
                event_type=self._feature_mapper.event_type(parsed_payload, topic),
                entity_id=self._feature_mapper.entity_id(parsed_payload),
                window_metadata={key: parsed_payload[key] for key in (
                    "feature_contract_version", "entity_type", "source_event_ids", "window_start",
                    "window_end", "first_event_at", "last_event_at", "calculated_at")},
            )
            self._publish_prediction(prediction)
            ML_PREDICTIONS_TOTAL.inc()
            if prediction.is_anomaly:
                ML_ANOMALIES_TOTAL.inc()
            ML_INFERENCE_LATENCY_SECONDS.observe(time.perf_counter() - start)
            logger.info(
                "prediction_published prediction_id=%s entity_id=%s anomaly=%s risk=%s severity=%s source_topic=%s",
                prediction.prediction_id,
                prediction.entity_id,
                prediction.is_anomaly,
                prediction.ml_risk_score,
                prediction.severity,
                topic,
            )
        except (UnicodeDecodeError, json.JSONDecodeError, ValidationError, ValueError, TypeError, KeyError) as exc:
            ML_PREDICTION_ERRORS_TOTAL.inc()
            logger.warning("unprocessable_ml_message topic=%s reason=%s", topic, exc)
            self._publish_deadletter(topic, str(exc), self._decode_payload(payload))

    def _publish_prediction(self, prediction: Any) -> None:
        assert self._producer is not None
        self._producer.produce(
            topic=self._settings.prediction_topic,
            key=str(prediction.prediction_id),
            value=prediction.to_json_bytes(),
            callback=self._delivery_report,
        )
        self._producer.poll(0)

    def _publish_deadletter(self, source_topic: str, reason: str, payload: object) -> None:
        assert self._producer is not None
        deadletter = DeadLetterEvent(source_topic=source_topic, reason=reason, payload=payload)
        self._producer.produce(
            topic=self._settings.deadletter_topic,
            key=str(deadletter.event_id),
            value=deadletter.to_json_bytes(),
            callback=self._delivery_report,
        )
        self._producer.poll(0)

    @staticmethod
    def _decode_payload(payload: bytes) -> object:
        try:
            return json.loads(payload.decode("utf-8"))
        except Exception:
            return {"raw_payload": payload.decode("utf-8", errors="replace")}

    @staticmethod
    def _delivery_report(error: KafkaException | None, message: object) -> None:
        if error is not None:
            logger.error("Kafka ML delivery failed: %s", error)

    def _register_shutdown_handlers(self) -> None:
        def shutdown_handler(signum: int, _frame: FrameType | None) -> None:
            logger.info("received_shutdown_signal signal=%s", signum)
            self._running = False

        signal.signal(signal.SIGTERM, shutdown_handler)
        signal.signal(signal.SIGINT, shutdown_handler)
