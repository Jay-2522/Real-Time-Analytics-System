# Spark 3.5.3 checkpoint recovery

Scope: the four contract-2.0 queries. Original checkpoint recovery remains blocked;
this change hardens future recovery and adds isolated failure regression tests.
Never substitute a new checkpoint merely to make the service healthy.

## Forensic findings (17 October 2026)

The driver uses a Docker named volume, `streaming-analytics-spark-checkpoints`, at
`/opt/spark/checkpoints`. The original standalone worker had **no volume mounts**.
Its HDFSBackedStateStoreProvider used the same `file:` path in the worker's writable
container layer. Driver offset/commit logs and executor aggregation state therefore
lived on different filesystems. Worker recreation could destroy state independently
of the driver logs. This is a confirmed configuration defect, not proof of what
caused the zero-byte files.

| Query / checkpoint suffix | Latest complete offset | Latest complete commit | Empty offsets | Empty commits |
| --- | ---: | ---: | --- | --- |
| enriched / analytics_enriched_events | 180 | 180 | 181,182 | 181,182 |
| window / analytics_window_metrics | 180 | 179 | 181,182 | 180,181,182 |
| risk / analytics_risk_metrics | 181 | 181 | 182,183 | 182,183 |
| features / analytics_feature_engineering_v2 | 53 | 52 | 54,55 | 53,54 |

Query UUIDs respectively: `3191b5aa-f63b-4434-9503-1da829b2e16c`,
`21d988b7-a1c1-45c8-acd6-3334268ee2af`, `fea232ea-f2ed-43e5-9105-b042ac103f6b`,
`712bda49-0a42-4140-ae26-85c4babea3aa`.

The driver archive contains 1,886 files (441,028 data bytes), including 34 empty
files counting checksum sidecars. Worker state contains 15,208 files (5,272,691
bytes), including 24 empty files in window/risk state and checksum sidecars. Empty
file mtimes cluster at 2026-10-16 23:20:24–23:20:53 UTC, before this recovery task.
Feature-v2 has state through version 55; risk through 184; window through 183.
There are six shuffle partitions per current stateful operator. File presence is
not a validation of compressed state contents or a complete state-version chain.
The older feature checkpoint/state is also preserved but incompatible with v2.
The three current state-schema files are byte-for-byte identical to schemas produced
by the isolated current-code run. A state-schema change is therefore not supported
as the cause of these startup failures; damaged progress/state contents remain the blocker.

The immediate startup failure is Spark reading an empty final offset log. Its
version-specific OffsetSeqLog deserializer rejects an empty file. Empty committed
names and multiple affected stores mean this cannot be treated as a single normal
uncommitted batch. Evidence does **not** establish whether VM/storage durability,
interrupted writes, or another historical operation emptied those files. No disk
fault or particular shutdown is claimed as the proven cause.

All four current source topics retain offset zero. Login/API/network each have three
partitions, anomaly two; retention is seven days, unlimited retention bytes, delete
policy. At the preserved inventory snapshot, latest offsets by partition were:
login `[2064,2054,2057]`, API `[2117,2021,2056]`, network `[2024,2014,2007]`,
anomaly `[520,494]`. Every recorded original initial/current source offset is within
these ranges. Availability must be rechecked before any future reconstruction.

Original Kafka output scanning found 14,294 enriched, 36 window, 26,710 risk and
19,522 feature records at the snapshot. Enriched output exists at 23:20:45 UTC;
risk/feature output at 23:20:30 UTC, overlapping the incomplete-log interval.
An empty commit file does not mean no output escaped. Window output cannot be
attributed to an incomplete batch from its filename alone. Raw records are retained,
but omitted historical micro-batch/watermark decisions and incomplete tail metadata
prevent a claim of exact historical state reconstruction.

## Preservation and strategy

The restart loop was stopped before backup. No executor process remained in the
worker and no running container mounted the driver checkpoint volume. The complete
driver volume was archived read-only; worker checkpoints were separately archived.
Both archives were read, counted and SHA-256 manifests generated per file. Archives,
source offset/config inventory, original logs and output evidence remain locally in
ignored `.codex-logs/phase2b/`. No progress files, volumes, databases or Kafka group
offsets were deleted, reset or rewritten. Keep both archives: neither alone contains
the original complete distributed checkpoint layout.

- **A — normal continuation:** blocked by empty offset/commit logs and state defects.
- **B — supported metadata recovery:** Spark 3.5.3 has no identified supported method
  to infer the missing contents while preserving this history. We do not fabricate
  logs, rename an older batch into place or delete the tail.
- **C — reconstruction:** source replay is possible in a separate namespace, beginning
  at each query's saved initial source offsets (feature-v2 starts later than the
  other three). This is the earliest available logical starting boundary, not a
  proven exact state restoration. Replaying in different batches changes watermark
  timing and potentially late-event inclusion. Existing output must be reconciled
  by event/window keys and full semantic payload before publishing replacements.
  Legacy random prediction IDs also predate the Phase 2A identity guarantee.

No reconstruction or original-state promotion was performed. A future recovery
proposal must preserve both archives, replay into isolated output topics, compare
all previously published keys/payloads, quantify unresolved windows and late records,
and obtain explicit approval for any production-topic promotion or alteration of
original state. The original driver remains stopped; it has not been declared healthy.

## Independently safe corrections

- Kafka `failOnDataLoss=true`: unavailable required offsets fail visibly, rather than
  silently advancing. `startingOffsets` applies only to new queries, not recovery.
- Compose gives worker and driver the same named checkpoint volume and matching UID.
  This fixes the single-Docker-host layout only; a multi-host cluster needs shared
  durable storage. The existing worker has not been recreated: doing so would hide
  its original container-local state. Do not apply the new mount as an implicit migration.
- Read-only preflight rejects incomplete logs, retained-sequence gaps, impossible
  offset/commit tails and absent/empty required local state versions. Spark remains
  responsible for decoding state and checking its schema and delta chain.
- A separate application contract records query/output/source identity, broker,
  watermark, output schema, semantic source hashes and shuffle configuration. A
  changed contract fails closed. A legacy populated checkpoint without this manifest
  requires reviewed adoption; there is no silent adoption or automatic fresh path.
- SIGTERM/SIGINT request orderly query shutdown; Compose allows 90 seconds. Partial
  query startup is covered by cleanup. Monitoring reports inactive queries/errors.

Kafka sink output is at-least-once. A batch may publish before Spark commits it;
restart can publish duplicates. Phase 2A event/window/prediction identities and SQL
receipts provide downstream idempotence. Separate sliding windows are not collapsed.
No model was retrained and no feature semantics were changed.

## Validation and operating procedure

Run ordinary tests with `python -m pytest`; set `PIPELINE_INTEGRATION=1` for real
Phase 2A infrastructure tests and `SPARK_RECOVERY_INTEGRATION=1` for the isolated Spark
recovery test. Supply the existing Docker environment and Phase 1 exported contract
via `SPARK_CONTRACT_OUTPUT`. Test infrastructure uses the built Spark image, dedicated
Kafka topics and an ignored per-run checkpoint directory. Do not delete them to make
an assertion pass.

The Spark recovery regression runs the real four transforms against real Kafka in
Spark 3.5.3 `local[2]`. A test-only foreachBatch Kafka sink exits the Python driver
after feature batch 1 publishes and before its commit. Restart uses the same query
UUID and checkpoint, followed by a clean restart with additional source records.
The test then invokes the actual ML/storage/alert handlers with the fitted existing
model, real Kafka and PostgreSQL, asserting durable counts. This is not a restart
of all deployed service processes or a distributed-worker failure test.

Preflight unit tests cover invalid logs, missing state, compatibility, namespace
isolation and insufficient retention. Insufficient retention is simulated by bounds;
no Kafka records were deleted to manufacture that condition.

For a valid established checkpoint: verify all four query statuses and source
retention, send controlled events, stop the driver gracefully, back up progress and
state consistently, restart with unchanged code/configuration, and compare source
IDs, window identities, query UUIDs, SQL counts and commit progression. A container
being up is insufficient evidence. On preflight failure, stop writers and preserve
state before investigating; never bypass the guard or reset offsets to suppress it.

References: [Spark 3.5.3 checkpoint format](https://github.com/apache/spark/blob/v3.5.3/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/OffsetSeqLog.scala),
[checkpoint atomic-write implementation](https://github.com/apache/spark/blob/v3.5.3/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/CheckpointFileManager.scala),
[version-specific streaming guide](https://github.com/apache/spark/blob/v3.5.3/docs/structured-streaming-programming-guide.md),
and [Kafka integration](https://github.com/apache/spark/blob/v3.5.3/docs/structured-streaming-kafka-integration.md).

## Verified results

Final combined Python run: **65 passed, 0 failed, 0 skipped**, with 59 dependency
warnings; this includes real Phase 2A integration and the isolated Spark recovery
case. Separate Spark feature-contract suite: **5 passed**. The isolated crash /
replay / clean continuation scenario passed twice in separate namespaces. Each run
verified 24 controlled source events, 8 five-minute event-type aggregates, 4 risk
windows, 6 feature messages representing 4 logical windows, 6 prediction messages,
and exactly 4 durable predictions plus 4 alerts. Query UUID and checkpoint identity
survived restart. The backend recent-alert API exposed the controlled alerts.

All 12 existing endpoint/service health checks passed; they do not test Spark query
health. Original driver preflight intentionally failed on empty offset 181, before
query startup, and original-volume SHA-256 verification remained unchanged. Original
query recovery and distributed-worker failure recovery remain **blocked/unverified**,
respectively. Insufficient retention was unit-tested, not induced by deleting data.
No full Phase 2B PASS or original-state restoration is claimed.

Committed scope: Spark main lifecycle, Kafka reader/sink, checkpoint preflight,
query monitor, Compose/pytest configuration, three recovery regression source files,
and this runbook. Archives, checksums, logs, generated records/checkpoints, local
credentials and test outputs are deliberately excluded from version control.
