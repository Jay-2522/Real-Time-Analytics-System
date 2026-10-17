# Kafka delivery and recovery contract

Scope: ML inference, storage sink, and alert engine, from the Phase 2A cutover.
This is at-least-once consumption with transactional deduplication, not end-to-end
exactly-once delivery. PostgreSQL is authoritative; search and caches converge
through a durable outbox. Feature contract 2.0 and its 18 inputs are unchanged.

## Defects and corrections

| Original path | Failure scenario / root cause | Correction |
| --- | --- | --- |
| Each service's `app/consumers/*_consumer.py` | Auto commits could advance while processing failed; caught exceptions allowed later records to continue | Disable automatic commit/store; one in-flight record; synchronous commit only after success; exit on failure |
| ML consumer | `produce`/`flush` did not prove delivery; random prediction IDs changed on retry | Require successful delivery callback and empty queue; deterministic prediction identity |
| Storage consumer/repository | Crash after SQL commit or replay inserted another row; ES/Redis errors were swallowed | Receipt, business row and outbox committed in one SQL transaction |
| Alert consumer/service | DB errors swallowed; Redis cooldown was neither durable nor concurrency-safe | SQL receipt and correlation advisory lock; alert, incident, link and outbox in one transaction |
| Both indexing/cache adapters | A failed external write was forgotten; unavailable-at-startup flags prevented recovery | Independent persistent retry worker; deterministic ES IDs and Redis projection markers |
| Consumer parsing | Malformed input could disappear without durable disposition | Confirmed ML Kafka DLQ, storage SQL deadletter, alert SQL quarantine |

## Identity and transaction boundaries

- Event identity: SHA-256 of topic plus `event_id` or `prediction_id`.
- Window identity: topic, contract, entity type/scope, feature/metric name and both
  boundaries. Two sliding windows containing the same source events remain distinct.
- Prediction UUID: input identity, model name/version, and SHA-256 of actual model
  and scaler files. The window end supplies a stable prediction timestamp.
- Alert UUID: rules version, input identity, correlation and severity. Separate
  legitimate inputs are retained; correlation groups them into an incident.
- PostgreSQL `(service, key)` receipt primary keys serialize concurrent duplicates.
  Receipt insertion and all related business/outbox writes share one transaction.
  Payload fingerprints detect conflicting reuse of an identity and quarantine it.
  UTC timestamps normalize; processing timestamps and Kafka coordinates do not
  change the semantic fingerprint.
- ML acknowledges only a confirmed prediction or confirmed DLQ publication.
  Kafka producer idempotence handles producer retries, not application replay.
  A crash after publication but before input commit can publish another message;
  downstream SQL receipts prevent duplicate business effects.
- Storage/alerts acknowledge after the SQL transaction, including pending external
  work, commits. No transaction spans Kafka, PostgreSQL, Elasticsearch and Redis.
  Kafka transactions alone would not make the latter systems atomic.

## Retry, shutdown and observability

The common delivery loop commits `message.offset + 1` synchronously and checks
partition errors. Assignment/revocation/loss changes an ownership epoch; changed
ownership refuses acknowledgement. No rebalance callback commits. A processing
or commit failure stops consumption, waits five seconds, and exits for Compose
restart. No later record is acknowledged over the failure. SIGTERM finishes the
current bounded operation; forced termination leaves it replayable.

The outbox worker locks due rows with `FOR UPDATE SKIP LOCKED`. It marks success
only after the effect returns. Failures retain payload/error/attempts with
exponential delay (2–256 seconds). Busy batches continue immediately; idle workers
wait two seconds. A crash after the effect repeats it safely: ES overwrites the
same ID; a Redis Lua marker prevents repeated increments/list entries. Latest-cache
versions prevent older retries overwriting newer values. Redis is not the durable
deduplication authority.

Monitor Kafka group lag, `pipeline_processing_failures_total`,
`pipeline_commits_total`, `pipeline_rebalances_total`, `pipeline_outbox_pending`,
`pipeline_outbox_failures_total`, and structured `input_unacknowledged`,
`outbox_retry`, and quarantine logs. Inspect pending work with:

```sql
SELECT service, target, count(*), max(attempts)
FROM pipeline_outbox WHERE NOT done GROUP BY service, target;
SELECT service, key, last_error FROM pipeline_outbox WHERE NOT done AND attempts > 0;
```

## Migration and recovery

Use the existing Docker context/environment. Stop all three affected consumers
before upgrading; do not reset groups or delete checkpoint/data volumes.

```sh
docker compose stop ml-inference storage-sink alert-engine
# Supply PGHOST, PGPORT, PGDATABASE, PGUSER and PGPASSWORD securely in the shell.
# Use the host Python environment with SQLAlchemy and psycopg installed.
python -m pipeline_runtime.migrate
docker compose build ml-inference storage-sink alert-engine
docker compose up -d --no-deps ml-inference storage-sink alert-engine
```

The repeatable additive migration creates receipts, outbox and quarantine tables
and seeds receipts from identifiable historical business rows. Review its counts
before starting consumers. It does not rewrite historical rows or create historical
outbox work. Run local Python entrypoints with the repository root on `PYTHONPATH`;
service Docker images include the shared package.

For infrastructure outages, restore the dependency, then restart an exited consumer
with `docker compose up -d --no-deps SERVICE`. Pending outbox work retries independently.
For permanent mapping/data errors, inspect and repair the cause; never mark failed
outbox rows done to hide them. Quarantine preserves exact input bytes (`raw_hex`)
and coordinates. Correct invalid input and publish it with an appropriate new
logical identity; retain the original quarantine for investigation.

## Validation and limits

Permanent tests: `tests/reliability/` exercises real Kafka/PostgreSQL database-session
termination, process exit after SQL commit but before acknowledgement, restart,
duplicate delivery, actual revocation, concurrent alert writes, invalid-input
quarantine, and failed Kafka delivery. Real ES/Redis connection failures leave
pending outbox rows; restoring endpoints completes them, and repeated effects
retain one document/counter increment. These tests use unique local topics/groups
and records, and intentionally do not delete existing data or reset groups.

```sh
PIPELINE_INTEGRATION=1 SPARK_CONTRACT_OUTPUT=/path/to/spark-contract.json python -m pytest
```

Integration tests require the existing local Kafka, PostgreSQL, ES, Redis and
contract-v2 model artifacts. Without the environment flag, integration skips are
explicit; they do not constitute reliability validation. Phase 1's Spark test
runs separately with Spark 3.5.3 and exports the contract fixture.

Known limits:

- Existing duplicates, missing historical search documents and legacy random-ID
  predictions are not repaired. Old and new ML identities cannot be equated
  retroactively. Backfill prevents re-inserting identifiable historical inputs.
- Receipts and Redis markers must outlive the replay horizon. No automatic pruning
  is introduced. Outbox/quarantine retention and capacity need operational policy.
- Redis loss/eviction or script resource failures can invalidate cache counters;
  PostgreSQL remains correct. Redis Cluster is not supported by this multi-key Lua
  projection. Search/cache updates are eventual, not atomic with SQL.
- Kafka retention bounds recovery. This development broker is single-node; these
  changes do not establish broker high availability or disaster recovery.
- A poison infrastructure/schema failure blocks the affected consumer until fixed;
  it is visible and replayable, never silently skipped. Unknown rule/code errors
  remain retryable rather than being mislabeled as invalid data.
- Other consumers (including threat intelligence) are outside this guarantee.
- During validation, the pre-existing Spark checkpoints contained incomplete offset
  logs (window/enriched batch 182, risk 183, feature-v2 55). Spark contract tests pass,
  but the existing streaming job cannot resume those checkpoints. They were preserved;
  full source-to-Spark live recovery remains blocked pending checkpoint recovery.
  No checkpoint deletion, historical-event deletion or group reset was performed.
