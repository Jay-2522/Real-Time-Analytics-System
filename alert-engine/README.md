# Alert Engine

The alert engine is the Phase 7 real-time SOC intelligence service. It consumes ML predictions and analytics streams from Kafka, applies local alert rules, deduplicates repeated signals with PostgreSQL, correlates alerts into incidents, persists everything to PostgreSQL, and indexes searchable alert/incident documents in Elasticsearch.

All dependencies are local and free.

## Consumed Topics

- `ml.anomaly.predictions`
- `analytics.risk.metrics`
- `analytics.enriched.events`

## Runtime

```bash
docker compose up --build -d alert-engine
docker logs -f streaming-analytics-alert-engine
```

## Responsibilities

- Rule-based alert generation.
- Severity escalation.
- PostgreSQL cooldown deduplication keyed by rule and entity.
- Incident correlation by entity and severity.
- Incident timeline tracking.
- Rule-based AI explanations and recommended actions.

