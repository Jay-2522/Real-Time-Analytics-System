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

## Phase 4A hardened recovery and legacy disposition (18 October 2026)

The Phase 2B findings above remain historical. The original damaged checkpoints
have not been repaired or replaced. Phase 4A uses Spark 3.5.9 / Scala 2.12 / Java
17 / Python 3.11 with the Phase 3C base filesystem and code-only test derivatives.
The dedicated `phase4a-recovery` Colima VM is ARM64, two CPUs, 4 GiB RAM and 2 GiB
of temporary swap; this is functional recovery evidence, not capacity/soak or HA
qualification. Fresh Compose projects, credentials, Kafka state and volumes are
separate from the development VM and all retained Phase 3 deployments.

### State preservation boundary

The preserved development VM/context is `phase0-analytics` /
`colima-phase0-analytics`. Driver `322fa4d3af1c` remains stopped, and worker
`7d1305599c45` retains its original container-local state. Driver state is the
`streaming-analytics-spark-checkpoints` volume mounted at `/opt/spark/checkpoints`;
worker state was in that same path on a different filesystem. The two read-only
archive inputs are `.codex-logs/phase2b/original-checkpoints.tar` and
`.codex-logs/phase2b/worker-checkpoints.tar`; their before/after hashes are retained
in ignored Phase 4A evidence. No test mounts either archive or original volume.

Original stores use `streaming-analytics-postgres-data`,
`streaming-analytics-elasticsearch-data`, `streaming-analytics-redis-data`,
`streaming-analytics-kafka-data`, ZooKeeper data/log volumes, and retained
Prometheus/Grafana volumes. Original topics include the four `telemetry.*.events`
sources, four `analytics.*` products, `ml.anomaly.predictions` and the DLQ.
Existing application groups include `streaming-analytics-telemetry-consumers`,
`streaming-analytics-ml-inference`, `streaming-analytics-storage-sink`,
`streaming-analytics-alert-engine` and `streaming-analytics-threat-intelligence`,
plus retained `reliability-*` test groups. Group listing was read-only; none was
reset. The original producer continues its pre-existing operation independently.

Historical data ownership is **unknown**, not verified synthetic. Generator code,
reserved-address fixtures, known test groups and contract-v2 artifacts establish
that synthetic records exist; they do not establish that every retained event,
account, imported IOC or user annotation is disposable. Source retention and data
completeness at a future approval date must be re-inventoried. Historical offset
ranges in Phase 2B are not a present retention guarantee.

### Recovery tests and observability

`tests/hardening/test_full_stack_recovery.py` is an explicit opt-in destructive
**test-environment-only** suite. It requires a `test-only` credential marker and a
`soc-phase4a*` project. Never set the marker on retained or production state.
`HARDENED_PROJECT` and `HARDENED_COMPOSE_OVERRIDE` select the dedicated test stack;
the existing hardened tests retain their default project for backward compatibility.

The deterministic dataset fixes counts before publication: two batches, each with
four logins, four API events, three network events and one anomaly. Each batch has
one source IP, two failed logins, HTTP 200/404/500/503, two destination IPs and risk
80. Eight separately identified watermark-control records are not target events.
Expected target products are 24 enriched events, eight five-minute event-type
windows, four overlapping risk windows, four contract-2.0 feature vectors and four
predictions. Feature lineage and every one of the 18 numeric values are checked;
raw Kafka counts are not substituted for unique logical outputs.

The real four-query service is killed and restarted using the same state volume.
The test compares all query UUIDs, offset/commit progress and subsequent outputs.
The separate-worker regression runs real Spark tasks on the standalone worker,
uses a test-only observable task stall, kills/recreates that worker, and records
executor-host/task-attempt witnesses. Both worker and driver mount `spark_state`
at `/opt/spark/checkpoints`; state survives container replacement. This remains a
single-Docker-host guarantee. Multiple hosts require a supported shared durable
filesystem and a checkpoint preflight supporting its URI; container-local `file:`
paths do not become distributed storage by sharing the same spelling.

The full-stack scenario stops PostgreSQL, Elasticsearch and Redis, publishes the
next known input, restores SQL, verifies pending outbox work, kills outbox service
processes, restores search/cache and checks convergence. Separate secured tests
exercise failed SQL transactions, process exit after SQL commit before Kafka
acknowledgement, group reassignment and replay. Another real-broker test pauses
Kafka, requires publication confirmation to fail, resumes it and repeats the same
logical DLQ record without adding another SQL business record.

Spark now exposes query-active, batch-ID, input-row and monitor-timestamp metrics
on internal port 9107. A bearer token is required and comes from the provisioned
monitoring credential; no host port is added. Prometheus scrapes this eighth job.
Authenticated monitoring reports Spark healthy only when all four queries are
active and the observation is fresh. Missing, failed or stale observations degrade
pipeline status. `/health/live` is process liveness and `/health/ready` is database/
authorization-backed API readiness; neither certifies complete analytics health.
Use authenticated monitoring for pipeline readiness.

Consumer metrics expose confirmed commits, last-commit time, observed partition
lag, rebalances and processing failures. Lag is sampled during successful
processing, not a continuous broker-wide group exporter: stale/absent lag is
unknown, not zero. Correlate last-commit age with known incoming input, broker
group offsets and scrape status. Outbox database-observation failures now have a
counter in addition to pending-work/failure metrics and retained SQL error details.
Query status/metrics do not prove correctness without dataset reconciliation.

Run only against a newly provisioned isolated deployment with sufficient resources:

```sh
export HARDENED_PROJECT=soc-phase4a-new
export HARDENED_SECRETS_DIR=/absolute/private/new-test-credentials
export HARDENED_MODEL_DIR=/absolute/path/to/verified-contract-v2-artifacts
# Select the dedicated Docker endpoint explicitly; provision credentials with --test-only.
# Build/import the tested images, initialize stores/topics/ACLs, then start workers.
HARDENED_INTEGRATION=1 HARDENED_FULL_STACK_RECOVERY=1 \
  .venv/bin/python -m pytest tests/hardening/test_full_stack_recovery.py -ra
HARDENED_INTEGRATION=1 HARDENED_WORKERS_INTEGRATION=1 \
  .venv/bin/python -m pytest tests/hardening/test_stack.py -ra
# Reserve the small test worker for the separate one-shot driver tests.
HARDENED_STREAMING_INTEGRATION=1 HARDENED_RECOVERY_INTEGRATION=1 \
  .venv/bin/python -m pytest tests/hardening/test_streaming.py tests/hardening/test_recovery.py -ra
```

The full-stack baseline test requires fresh data. Reruns use a new project/volumes;
never truncate a retained dataset or revise expected counts after observing output.
Stop profiled test services with `--profile '*'`; an unprofiled Compose stop/down
may leave streaming services running. Failed-attempt evidence remains separate.

### Legacy options and proposed approval boundary

| Option | Preservation / possible gaps | Replay and retention risk | Verification, rollback and approval |
| --- | --- | --- | --- |
| A: supported historical continuation | Preserve both original filesystems, archives, offsets and published output. Missing log contents and damaged state remain unexplained. | Empty tail metadata can overlap already published output. No supported reconstruction of those missing contents has been demonstrated. | Work only from copies with matching Spark/Python versions. Require successful decoding and complete state/version chains plus output reconciliation. Currently blocked; do not fabricate/delete progress logs. Original archives remain rollback evidence. |
| B: bounded reconciliation/reconstruction | Preserve original history; create separate checkpoints, topics, SQL identities and search indexes. Produce an explicit gap/conflict ledger. | Requires all source offsets from each query's saved initial boundary to remain available. Different micro-batches/watermarks may change late-record inclusion; old random prediction IDs cannot be equated automatically. | Recheck every partition's retention bounds; replay copies into a shadow namespace; compare event IDs, window boundaries, all feature values and published payloads. No promotion while unexplained discrepancies remain. Roll back by stopping the shadow writers and retaining both datasets. Promotion requires owner approval. |
| C: approved clean cutover | Archive the complete historical environment and begin new state at a declared boundary. This does not recover historical state. | Old state may remain permanently unreadable; explicitly disclose missing history and avoid replay into the new identity namespace without reconciliation. | Requires owner attestation that excluded history is disposable synthetic data, or explicit acceptance of identified loss. Ownership is currently unknown, so this condition is unmet. Preserve old/new state separately and define read-only historical access. No deletion or replacement is authorized here. |

Recommendation: preserve the original environment and investigate **B in a shadow
namespace** if source retention still permits it. Keep A open only if a supported,
verifiable procedure becomes available. Do not select C merely because clean test
volumes pass. No legacy reconstruction, producer rerouting or cutover was executed.

Before a future cutover, obtain the data owner's disposition decision and approve
an exact source-offset/time boundary, gap ledger, identity mapping and rollback
point. Back up Kafka configuration/offset inventory, both Spark state filesystems,
SQL, search and provisioning configuration consistently; verify restore on copies.
Drain/stop old writers only under that approved plan. Initialize a distinct hardened
namespace, test mTLS identities/ACLs and replay policy, reconcile through the
boundary, and switch producers/readers in a controlled maintenance window. Retain
all old artifacts and record which history is visible in each system.

Rollback must preserve writes already accepted by the new deployment. Stop new
writers, reconcile that interval and restore matching tested image/state backups
on copies before rerouting. Do not point an older Spark binary at newer checkpoints,
downgrade a migrated monitoring database in place, or restart the damaged original
driver as an automatic rollback. Public deployment, production approval, destructive
migration and promotion to `main` remain outside this phase.


### Redis restart blocker discovered during reconciliation

The tested Redis image runs 7.4.11. With the default user disabled and scoped
service ACLs retained, AOF loading reports `NOPERM` during replayed `MULTI`/`EXEC`
blocks generated by Lua cache updates. Redis becomes available despite dropping
those effects. The SQL outbox has already completed that work, so normal outbox
retry cannot reconstruct the missing cache state. In the controlled run, SQL and
search retained all 44 target alerts while Redis retained only 22 counter increments
and 22 of 44 idempotency markers. This is persistent cache inconsistency, not a
successful outage recovery. SQL remains authoritative; dashboard cache counts must
not be treated as reconciled after this restart.

Upstream [Redis issue 14541](https://github.com/redis/redis/issues/14541) and
[fix 14545](https://github.com/redis/redis/pull/14545) describe this replay defect.
The [8.4 release notes](https://github.com/redis/redis/blob/8.4/00-RELEASENOTES)
include the fix in 8.4.4; the tested 7.4.11 release lacks it. No Redis major upgrade,
local server-source patch, ACL relaxation or AOF disabling was performed in this
phase. Validate a supported fixed release separately, including security scanning,
TLS/ACL compatibility, AOF restore on a copy, and repeated crash/replay tests.
Never downgrade a newer server's data in place: retain a pre-upgrade volume backup
and matching old image for rollback. AOF contents may permit reconstruction on a
fixed server only if they have not been rewritten; demonstrate this on a copy,
then reconcile against SQL before accepting any cache as authoritative.

The permanent cache reconciliation assertion deliberately remains failing on the
tested image. Do not weaken its expected count or reset the counter to manufacture
a pass. Release/cutover remains blocked until cache durability and reconciliation
are proven. Completed outbox rows alone are insufficient evidence of Redis restart
integrity, and current process/scrape health does not detect this silent cache loss.


### Observed deterministic results

The final fresh application baseline used project `soc-phase4a-r4`; previous
attempts retain separate private diagnostics and are not substituted for this run.

| Controlled target outcome | Expected | Observed |
| --- | ---: | ---: |
| Source IDs, excluding watermark controls | 24 | 24 |
| Additional watermark controls | 8 | 8 |
| Enriched source identities | 24 | 24 |
| Five-minute event-type windows | 8 | 8 |
| Overlapping risk windows | 4 | 4 |
| Contract-2.0 windows, with all 18 values and source lineage | 4 | 4 |
| Unique durable predictions | 4 | 4 |
| Unique durable alerts | 44 | 44 |
| Unique target incidents | 4 | 4 |
| Target threat inputs | 46 | 46 |
| Target prediction / alert / incident search documents | 4 / 44 / 4 | 4 / 44 / 4 |
| Target threat search documents | 46 | 46 |
| Pending outbox rows after restored dependencies | 0 | 0 |
| Redis alert counter / surviving alert markers after restart | 44 / 44 | **22 / 22** |

All four application query UUIDs survived driver SIGKILL/restart. Their source
next-offset maps reconciled to login 10, API 10, network 8 and anomaly 4 on partition
0 (32 total source/control records). The authenticated HTTPS alert API returned
all 44 controlled alerts and denied anonymous access. Three duplicate enriched
publications left SQL cardinalities at 24 enriched, 44 alerts and 46 threat inputs.

The separate-worker run interrupted a **nonempty** feature batch: stage 12,
partition 3, attempt 0 ran on worker `38f2b0cb3027`; after SIGKILL/recreation,
attempt 1 ran on worker `8b0fa561b095`. A further partition completed on the new
worker. Four checkpoint UUIDs survived a second driver application; sixteen
source IDs matched enriched identities, and six expected feature windows each
contained four events and 18 features after new input. Each stateful query retained
24 state-store delta files across the two applications. An initial probe overwrote
witness files and failed its evidence assertion; the corrected permanent probe
retains individual task attempts and the fresh rerun passed. No multiple-worker,
multi-host, broker-quorum or host-loss qualification is claimed.


### Final verification and disposition

**Phase 4A verdict: FAIL.** Distributed Spark recovery, SQL uniqueness and search
convergence passed, but Redis restart reconciliation failed. The original damaged
history also remains unresolved; a clean deployment is not historical recovery.

The 115 root-suite cases were executed in selected groups to keep fresh-baseline
counts deterministic and reserve the small Spark worker for one driver at a time:

| Final selected group | Passed | Failed | Skipped |
| --- | ---: | ---: | ---: |
| Python unit/authentication/browser (`-m 'not integration'`, browser enabled) | 85 | 0 | 0 |
| Hardened full-stack recovery (five cases across the fresh baseline and fault runs) | 4 | 1 | 0 |
| Existing secured streaming and crash/replay regressions | 4 | 0 | 0 |
| Hardened infrastructure/browser and monitoring | 11 | 0 | 0 |
| Legacy plaintext reliability, legacy Spark recovery and legacy auth migration | 0 | 0 | 10 |
| **Root-suite total** | **104** | **1** | **10** |

The ten legacy cases were explicitly invoked with their integration flags unset;
they target the preserved legacy deployment and were not silently repointed or
run against it. Equivalent secured scenarios do not erase these exclusions.
Collection also reports 30 integration and 85 non-integration cases. Deselected
cases in an individual selected command are accounted for by the other groups,
not reported as additional passes. Earlier setup/probe failures and repeated runs
are not added to this final distinct-case total.

The separately executed real Spark feature-contract suite passed **5/5**, using
an export from the modified Spark application image. Frontend `npm run lint`
passed with two existing hook warnings; `npm run build` passed with its existing
chunk-size warning. Both changed legacy Dockerfiles passed Docker build checks
without warnings, and hardened Compose validation and whitespace checks passed.
No dependency image upgrade or new vulnerability clearance is claimed.

Commands, with the isolated endpoint/project/secrets/model/override variables
selected as above, were:

```sh
AUTH_BROWSER_INTEGRATION=1 SPARK_CONTRACT_OUTPUT=/absolute/isolated/spark-contract.json \
  .venv/bin/python -m pytest -m 'not integration' -ra
HARDENED_INTEGRATION=1 HARDENED_WORKERS_INTEGRATION=1 \
HARDENED_MONITORING_INTEGRATION=1 .venv/bin/python -m pytest \
  tests/hardening/test_stack.py tests/hardening/test_monitoring.py -ra
# Other secured fault-suite commands are in the preceding section.
.venv/bin/python -m pytest tests/reliability tests/spark_recovery -m integration -ra
.venv/bin/python -m pytest backend/tests/test_auth_migration.py -ra
# The last two commands leave legacy integration flags unset: 9 + 1 skips.
```

The original 36 containers retained their IDs, images, lifecycle states/start times
and mount mappings (Docker's mount-array ordering is immaterial). Both original
checkpoint archive SHA-256 hashes remained identical. No original offsets, volumes,
checkpoint contents or historical records were reset, migrated or replaced.

Committed scope is delivery adapters/shared runtime, replay-safe threat persistence
and row/ACL provisioning, alert/incident search projection, protected Spark metrics
and truthful monitoring, both affected legacy Dockerfile runtime copies, essential
regressions and these two existing runbooks. Credentials, certificates, test data,
model copies, logs, temporary orchestration, exports, checkpoints, backups, Docker
volumes and generated frontend assets remain excluded. The verified improvements
are not a claim that this recovery phase or a production cutover is complete.
