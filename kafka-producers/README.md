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

Follow only new lines appended to a live Cowrie JSON log:

```bash
python -m app.main cowrie-live --file /path/to/cowrie.json
```

Replay a saved log once:

```bash
python -m app.main cowrie-replay --file /path/to/cowrie.json --time-shift
```

Use `--time-shift` for historical logs when Spark is already running or has
seen newer events. Spark's event-time watermark treats records sufficiently
older than the newest observed timestamp as late and can drop them from windowed
results. Time-shifting moves the earliest supported Cowrie event to the current
time and keeps every original gap, so a ten-second burst remains a ten-second
burst. The actual Cowrie time remains in `original_timestamp`. Omit the option
for live ingestion or when historical event times must drive the windows.

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
