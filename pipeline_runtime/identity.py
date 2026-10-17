import hashlib
import json
from datetime import datetime, timezone
from uuid import NAMESPACE_URL, uuid5

VOLATILE = {"processed_at", "calculated_at", "source_partition", "source_offset", "kafka_timestamp"}


def canonical(payload):
    if isinstance(payload, dict):
        return {k: canonical(v) for k, v in payload.items() if k not in VOLATILE}
    if isinstance(payload, list):
        return [canonical(v) for v in payload]
    if isinstance(payload, str) and "T" in payload:
        try:
            value = datetime.fromisoformat(payload.replace("Z", "+00:00"))
            if value.tzinfo:
                return value.astimezone(timezone.utc).isoformat()
        except ValueError:
            pass
    return payload


def digest(value):
    return hashlib.sha256(json.dumps(canonical(value), sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def logical_key(topic, payload):
    if not isinstance(payload, dict):
        raise ValueError("Expected a JSON object")
    identifier = payload.get("prediction_id") or payload.get("event_id")
    if identifier:
        return digest([topic, str(identifier)])
    if payload.get("window_start") and payload.get("window_end"):
        scope = payload.get("entity_id") or payload.get("event_type")
        if not scope:
            raise ValueError("Window has no entity or event-type scope")
        return digest([topic, payload.get("feature_contract_version"), payload.get("entity_type"),
                       scope, payload.get("feature_name"), payload.get("metric_type"),
                       payload["window_start"], payload["window_end"]])
    raise ValueError("Input has no stable event/prediction/window identity")


def stable_uuid(*parts):
    return uuid5(NAMESPACE_URL, digest(parts))


def coordinate(message):
    return {"topic": message.topic(), "partition": message.partition(), "offset": message.offset()}


def quarantine(topic, payload, reason, position=None):
    # Retain exact bytes as hex even when the input cannot be decoded as UTF-8/JSON.
    body = {"source_topic": topic, "reason": str(reason), "raw_hex": payload.hex() if payload is not None else None,
            "position": position}
    body["event_id"] = str(stable_uuid("quarantine-v1", topic, body["raw_hex"], position))
    body["event_type"] = "deadletter"
    return body
