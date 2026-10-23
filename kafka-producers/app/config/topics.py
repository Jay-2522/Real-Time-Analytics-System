from dataclasses import dataclass


@dataclass(frozen=True)
class TopicDefinition:
    name: str
    partitions: int
    replication_factor: int
    description: str


LOGIN_EVENTS = "telemetry.login.events"
API_EVENTS = "telemetry.api.events"
NETWORK_EVENTS = "telemetry.network.events"
ANOMALY_EVENTS = "telemetry.anomaly.events"
DEADLETTER_EVENTS = "telemetry.deadletter.events"

TOPICS: tuple[TopicDefinition, ...] = (
    TopicDefinition(LOGIN_EVENTS, 3, 1, "User authentication telemetry and login risk signals."),
    TopicDefinition(API_EVENTS, 3, 1, "Application API request telemetry and service response metadata."),
    TopicDefinition(NETWORK_EVENTS, 3, 1, "Synthetic network flow telemetry for traffic analysis."),
    TopicDefinition(ANOMALY_EVENTS, 2, 1, "Injected anomaly signals for downstream detection workflows."),
    TopicDefinition(DEADLETTER_EVENTS, 1, 1, "Malformed or unprocessable telemetry messages."),
)
