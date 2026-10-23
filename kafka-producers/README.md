# Kafka Telemetry Producer

Python runtime for Phase 2 synthetic telemetry production.

## Responsibilities

- Generate realistic cybersecurity and platform telemetry events.
- Validate all outbound events with Pydantic schemas.
- Publish events to Kafka topics with retry-safe producer configuration.

## Runtime

```bash
python -m app.main producer
```

## Environment Variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker list. |
| `EVENT_RATE_PER_SECOND` | `5` | Producer event generation rate. |
| `ANOMALY_RATE` | `0.05` | Probability of generating anomaly events. |
| `PRODUCER_LOG_LEVEL` | `INFO` | Producer logging level. |

## Topics

- `telemetry.login.events`
- `telemetry.api.events`
- `telemetry.network.events`
- `telemetry.anomaly.events`
- `telemetry.deadletter.events`
