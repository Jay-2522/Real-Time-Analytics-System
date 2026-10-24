from __future__ import annotations

import json
import logging
import signal
import time
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType
from typing import Any

from confluent_kafka import Producer
from infrastructure_runtime.security import kafka_options
from pipeline_runtime.identity import quarantine, stable_uuid
from pipeline_runtime.kafka import publish_confirmed

from app.config.settings import KafkaRuntimeSettings
from app.config.topics import DEADLETTER_EVENTS, LOGIN_EVENTS, NETWORK_EVENTS
from app.schemas.events import LoginEvent, NetworkEvent, TelemetryEvent
from app.utils.metrics import EVENTS_PUBLISHED_TOTAL, PRODUCER_ERRORS_TOTAL

logger = logging.getLogger(__name__)

COWRIE_SOURCE = "cowrie.json"
SUPPORTED_EVENT_IDS = {
    "cowrie.login.failed",
    "cowrie.login.success",
    "cowrie.session.connect",
}


def map_cowrie_event(
    payload: dict[str, Any],
    source_position: int,
    shifted_timestamp: datetime | None = None,
) -> TelemetryEvent | None:
    """Map supported Cowrie JSON events to the existing telemetry contract.

    Field mapping (Cowrie -> telemetry):
      eventid -> event_type/login_success selection; timestamp -> timestamp;
      src_ip -> source_ip; username -> username and user_id; dst_ip ->
      destination_ip; dst_port -> port; protocol (ssh/telnet) -> TCP.

    `event_id` is a deterministic adapter identifier derived from non-secret
    source fields and the file line number. Cowrie does not report country,
    device type, byte counts, packet counts, or risk scores, so the optional
    descriptive/count fields remain null and risk_score is neutral (0). Cowrie
    password, message, key, and fingerprint fields are intentionally ignored.
    """
    event_id = payload.get("eventid")
    if event_id not in SUPPORTED_EVENT_IDS:
        return None

    common = {
        "event_id": stable_uuid(
            "cowrie-v1",
            payload.get("session"),
            event_id,
            payload.get("timestamp"),
            payload.get("src_ip"),
            source_position,
        ),
        "timestamp": shifted_timestamp or payload.get("timestamp"),
        "original_timestamp": payload.get("timestamp") if shifted_timestamp else None,
        "risk_score": 0.0,
    }
    if event_id in {"cowrie.login.failed", "cowrie.login.success"}:
        username = payload.get("username")
        return LoginEvent(
            **common,
            user_id=username,
            username=username,
            source_ip=payload.get("src_ip"),
            login_success=event_id == "cowrie.login.success",
        )

    protocol = str(payload.get("protocol") or "").casefold()
    if protocol not in {"ssh", "telnet"}:
        raise ValueError("Cowrie session protocol is not SSH or Telnet")
    return NetworkEvent(
        **common,
        source_ip=payload.get("src_ip"),
        destination_ip=payload.get("dst_ip"),
        protocol="TCP",
        port=payload.get("dst_port"),
    )


class CowrieEventAdapter:
    """Validate Cowrie JSON lines and publish normalized events or safe DLQs."""

    def __init__(self, producer: Any) -> None:
        self._producer = producer

    def handle_line(
        self,
        line: str,
        source_position: int,
        shifted_timestamp: datetime | None = None,
    ) -> bool:
        try:
            parsed = json.loads(line)
            if not isinstance(parsed, dict):
                raise ValueError("Cowrie line must be a JSON object")
            event = map_cowrie_event(parsed, source_position, shifted_timestamp)
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            self._publish_deadletter(exc, source_position)
            return False

        if event is None:
            return False
        topic = LOGIN_EVENTS if isinstance(event, LoginEvent) else NETWORK_EVENTS
        try:
            publish_confirmed(
                self._producer,
                topic=topic,
                key=str(event.event_id),
                value=event.to_json_bytes(),
            )
        except Exception:
            PRODUCER_ERRORS_TOTAL.labels(topic).inc()
            raise
        EVENTS_PUBLISHED_TOTAL.labels(topic, event.event_type).inc()
        return True

    def _publish_deadletter(self, error: Exception, source_position: int) -> None:
        # Never place the raw line in the DLQ: malformed JSON can still contain
        # an attacker password, and Cowrie login JSON normally does contain one.
        body = quarantine(COWRIE_SOURCE, None, error, {"line": source_position})
        publish_confirmed(
            self._producer,
            topic=DEADLETTER_EVENTS,
            key=body["event_id"],
            value=json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        )
        EVENTS_PUBLISHED_TOTAL.labels(DEADLETTER_EVENTS, "deadletter").inc()
        logger.warning("cowrie_line_quarantined line=%s reason=%s", source_position, error)


class CowrieLogIngestor:
    """Publish a saved Cowrie log once or follow new lines in a live log."""

    def __init__(self, settings: KafkaRuntimeSettings) -> None:
        self._settings = settings
        self._running = True
        self._producer: Producer | None = None

    def replay(self, path: Path, time_shift: bool = False) -> int:
        self._producer = self._create_producer()
        adapter = CowrieEventAdapter(self._producer)
        origin = _earliest_supported_timestamp(path) if time_shift else None
        shifted_origin = datetime.now(UTC) if origin else None
        published = 0
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line_number, line in enumerate(handle, start=1):
                    shifted = None
                    if origin and shifted_origin:
                        parsed = _safe_json_object(line)
                        if parsed and parsed.get("eventid") in SUPPORTED_EVENT_IDS:
                            try:
                                shifted = shifted_origin + (_cowrie_timestamp(parsed) - origin)
                            except (TypeError, ValueError):
                                # The adapter publishes the invalid record to the DLQ.
                                pass
                    published += adapter.handle_line(line, line_number, shifted)
        finally:
            self._producer.flush(10)
        logger.info("cowrie_replay_complete file=%s published=%s time_shift=%s", path, published, time_shift)
        return published

    def tail(self, path: Path, poll_seconds: float = 0.25) -> None:
        self._register_shutdown_handlers()
        self._producer = self._create_producer()
        adapter = CowrieEventAdapter(self._producer)
        line_number = _line_count(path)
        try:
            with path.open("r", encoding="utf-8") as handle:
                handle.seek(0, 2)
                while self._running:
                    line = handle.readline()
                    if not line:
                        time.sleep(poll_seconds)
                        continue
                    line_number += 1
                    adapter.handle_line(line, line_number)
        finally:
            self._producer.flush(10)

    def _create_producer(self) -> Producer:
        producer = Producer(
            {
                **kafka_options(),
                "bootstrap.servers": self._settings.kafka_bootstrap_servers,
                "client.id": f"{self._settings.producer_client_id}-cowrie",
                "acks": "all",
                "retries": 5,
                "enable.idempotence": True,
                "delivery.timeout.ms": 10000,
            }
        )
        metadata = producer.list_topics(timeout=10)
        logger.info("cowrie_kafka_connected brokers=%s", len(metadata.brokers))
        return producer

    def _register_shutdown_handlers(self) -> None:
        def shutdown_handler(signum: int, _frame: FrameType | None) -> None:
            logger.info("cowrie_tail_shutdown signal=%s", signum)
            self._running = False

        signal.signal(signal.SIGTERM, shutdown_handler)
        signal.signal(signal.SIGINT, shutdown_handler)


def _safe_json_object(line: str) -> dict[str, Any] | None:
    try:
        value = json.loads(line)
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


def _cowrie_timestamp(payload: dict[str, Any]) -> datetime:
    value = payload.get("timestamp")
    if not isinstance(value, str):
        raise ValueError("Cowrie event has no timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _earliest_supported_timestamp(path: Path) -> datetime:
    earliest: datetime | None = None
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            payload = _safe_json_object(line)
            if not payload or payload.get("eventid") not in SUPPORTED_EVENT_IDS:
                continue
            try:
                timestamp = _cowrie_timestamp(payload)
            except (TypeError, ValueError):
                continue
            earliest = timestamp if earliest is None else min(earliest, timestamp)
    if earliest is None:
        raise ValueError("Cowrie replay file has no supported timestamped events")
    return earliest


def _line_count(path: Path) -> int:
    with path.open("r", encoding="utf-8") as handle:
        return sum(1 for _ in handle)
