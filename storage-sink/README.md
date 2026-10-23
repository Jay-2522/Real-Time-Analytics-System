# Storage Sink

The storage sink is the Phase 5 persistence worker for the Real-Time Streaming Analytics System. It consumes telemetry, analytics, feature, and ML prediction events from Kafka and writes them to local PostgreSQL, Elasticsearch, and Redis.

This service is local-only and free. It does not use managed cloud services.

## Responsibilities

- Consume Kafka topics across telemetry, Spark analytics, feature engineering, and ML predictions.
- Normalize incoming JSON messages into storage records.
- Persist structured records to PostgreSQL.
- Index searchable documents in Elasticsearch.
- Cache latest predictions and counters in Redis.
- Continue PostgreSQL writes if Redis or Elasticsearch are temporarily unavailable.
- Gracefully shut down Kafka consumption.

## Layout

```text
storage-sink/
  app/
    config/        Settings and topic constants
    consumers/     Kafka consumer runtime
    db/            SQLAlchemy models and session initialization
    elastic/       Elasticsearch index management and indexing
    redis_cache/   Redis cache writer
    schemas/       Storage record contracts
    repositories/  PostgreSQL persistence repository
    services/      Message-to-record mapper
    utils/         Logging and retry helpers
    main.py        Runtime entrypoint
```

## Local Run

```bash
docker compose up --build -d storage-sink
docker logs -f streaming-analytics-storage-sink
```

The service expects Kafka and PostgreSQL to be available. Redis and Elasticsearch are optional-safe.
Malformed Kafka records are persisted and published to `telemetry.deadletter.events`.

## PostgreSQL Tables

- `telemetry_events`
- `analytics_metrics`
- `risk_metrics`
- `feature_snapshots`
- `anomaly_predictions`
- `deadletter_events`

## Redis Keys

- `latest:prediction:{entity_id}`
- `recent:high-risk:{entity_id}`
- `counters:events:{event_type}`
- `counters:severity:{severity}`

## Elasticsearch Indexes

- `telemetry-events`
- `analytics-events`
- `ml-predictions`
- `deadletter-events`

