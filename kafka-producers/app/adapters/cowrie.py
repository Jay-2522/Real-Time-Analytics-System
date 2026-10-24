from __future__ import annotations

import json
import logging
from typing import Any

from pipeline_runtime.identity import quarantine, stable_uuid
from pipeline_runtime.kafka import publish_confirmed

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


def map_cowrie_event(payload: dict[str, Any], source_position: int) -> TelemetryEvent | None:
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
        "timestamp": payload.get("timestamp"),
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

    def handle_line(self, line: str, source_position: int) -> bool:
        try:
            parsed = json.loads(line)
            if not isinstance(parsed, dict):
                raise ValueError("Cowrie line must be a JSON object")
            event = map_cowrie_event(parsed, source_position)
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
