import json
import sys
from pathlib import Path
from types import SimpleNamespace


for name in list(sys.modules):
    if name == "app" or name.startswith("app."):
        del sys.modules[name]
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.consumers.storage_consumer import StorageSinkConsumer


class ConfirmedProducer:
    def __init__(self):
        self.messages = []

    def produce(self, **message):
        self.messages.append(message)
        message["callback"](None, object())

    def flush(self, _timeout):
        return 0


def test_malformed_raw_event_is_published_to_deadletter_topic():
    consumer = StorageSinkConsumer.__new__(StorageSinkConsumer)
    consumer._settings = SimpleNamespace(deadletter_topic="telemetry.deadletter.events")
    consumer._deadletter_producer = ConfirmedProducer()
    persisted = []
    consumer._persist = lambda record, key: persisted.append((record, key))

    consumer._handle_message(
        "telemetry.login.events",
        b"not-json",
        {"topic": "telemetry.login.events", "partition": 0, "offset": 7},
    )

    assert len(consumer._deadletter_producer.messages) == 1
    message = consumer._deadletter_producer.messages[0]
    assert message["topic"] == "telemetry.deadletter.events"
    body = json.loads(message["value"])
    assert body["source_topic"] == "telemetry.login.events"
    assert body["raw_hex"] == b"not-json".hex()
    assert len(persisted) == 1


def test_malformed_deadletter_is_not_republished_recursively():
    consumer = StorageSinkConsumer.__new__(StorageSinkConsumer)
    consumer._settings = SimpleNamespace(deadletter_topic="telemetry.deadletter.events")
    consumer._deadletter_producer = ConfirmedProducer()
    consumer._persist = lambda record, key: None

    consumer._handle_message("telemetry.deadletter.events", b"not-json", {"partition": 0, "offset": 8})

    assert consumer._deadletter_producer.messages == []
