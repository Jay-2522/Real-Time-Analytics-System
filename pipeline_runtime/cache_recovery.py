"""Bounded, SQL-authoritative repair of retained Redis outbox projections.

Cache writers take the same transaction lock. A repair failure leaves its health
lease invalid; SQL ingestion continues and new pending work drains after repair.
Temporary SQLite holds the projection on disk rather than in Python memory.
"""
import json
import logging
import sqlite3
import tempfile
import time
import math
from datetime import datetime
from redis.exceptions import ResponseError

from prometheus_client import Counter, Gauge
from sqlalchemy import select, text

log = logging.getLogger(__name__)
FAILURES = Counter("pipeline_cache_reconciliation_failures_total", "Failed SQL/cache reconciliations", ["service"])
PROGRESS = Gauge("pipeline_cache_reconciliation_records", "Records examined in current reconciliation", ["service"])
MISMATCHES = Gauge("pipeline_cache_inconsistent_keys", "Keys needing repair in last completed observation", ["service"])
UNRECONCILED = Gauge("pipeline_cache_unreconciled_records", "Records not yet verified; -1 means SQL unavailable", ["service"])
STATE = Gauge("pipeline_cache_reconciliation_state", "0 unverified, 1 rebuilding, 2 complete", ["service"])
VERIFIED = Gauge("pipeline_cache_verified_at_seconds", "Last successful SQL/cache verification; zero after failure", ["service"])


class CacheRecoveryError(ValueError):
    """A safe diagnostic containing no credentials or business payloads."""


def cache_lock(session, service):
    session.execute(text("SET LOCAL lock_timeout = '5s'"))
    session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                    {"key": "pipeline-cache-recovery:" + service})


def require_complete_ledger(session, service):
    """Refuse to overwrite counters that may contain pre-outbox contributions."""
    if service == "alert":
        missing = session.scalar(text("""
          SELECT EXISTS (SELECT 1 FROM alerts a
            WHERE a.source_topic = ANY(:topics) AND NOT EXISTS (
              SELECT 1 FROM pipeline_outbox o WHERE o.service='alert'
              AND o.target='redis' AND o.payload->>'alert_id'=a.alert_id))
        """), {"topics": ["analytics.enriched.events", "analytics.risk.metrics", "ml.anomaly.predictions"]})
    else:
        missing = session.scalar(text("""
          SELECT EXISTS (SELECT 1 FROM pipeline_receipts r WHERE r.service='storage'
            AND NOT EXISTS (SELECT 1 FROM pipeline_outbox o WHERE o.service=r.service
              AND o.key=r.key AND o.target='redis'))
          OR ((SELECT count(*) FROM telemetry_events) + (SELECT count(*) FROM analytics_metrics)
            + (SELECT count(*) FROM risk_metrics) + (SELECT count(*) FROM feature_snapshots)
            + (SELECT count(*) FROM anomaly_predictions) + (SELECT count(*) FROM deadletter_events)
            > (SELECT count(*) FROM pipeline_outbox WHERE service='storage' AND target='redis'))
        """))
    if missing:
        raise CacheRecoveryError("Incomplete durable projection ledger; explicit historical migration required")


def effects(service, key, body):
    """Return the original namespace, identity, dimensions and stable ordering."""
    if service == "alert":
        entity, severity = body["entity_id"], body["severity"]
        return [("alert", body["alert_id"], f"latest:alert:{entity}", datetime.fromisoformat(body["timestamp"].replace("Z", "+00:00")).isoformat(),
                 {k: v for k, v in body.items() if k != "_cache_expires_at"},
                 [f"counters:alerts:severity:{severity}", f"counters:alerts:entity:{entity}"],
                 "live:critical-alerts" if severity == "critical" else "")]
    if service != "storage":
        raise CacheRecoveryError("Unsupported cache projection service")
    stamp = body.get("timestamp")
    if not stamp:
        raise CacheRecoveryError("Storage projection lacks a durable timestamp; explicit migration required")
    stamp = datetime.fromisoformat(stamp.replace("Z", "+00:00")).isoformat()
    entity = body.get("entity_id") or key
    risk = body.get("ml_risk_score") or body.get("risk_score") or 0
    prediction = body["record_kind"] == "prediction"
    latest = (f"latest:prediction:{body.get('entity_id')}" if prediction else
              f"recent:high-risk:{entity}" if risk >= 70 else f"latest:event:{entity}")
    counters = ([f"counters:events:{body['event_type']}"] if body.get("event_type") else [])
    if body.get("severity"):
        counters.append(f"counters:severity:{body['severity']}")
    result = [("storage", key, latest, stamp, body["raw_payload"], counters, "")]
    if prediction and risk >= 70:
        result.append(("storage-high-risk", key, f"recent:high-risk:{entity}", stamp, body["raw_payload"], [], ""))
    return result


def encode(payload):
    return json.dumps(payload, default=str)


class Projection:
    def __init__(self, db):
        self.db = db
        db.executescript("""
          PRAGMA page_size=4096;
          PRAGMA cache_size=-2048;
          PRAGMA max_page_count=16384;
          PRAGMA temp_store=FILE;
          CREATE TABLE value (key TEXT PRIMARY KEY, value TEXT, expires REAL);
          CREATE TABLE counter (key TEXT PRIMARY KEY, n INTEGER);
          CREATE TABLE latest (key TEXT PRIMARY KEY, version TEXT, value TEXT, expires REAL);
          CREATE TABLE critical (version TEXT PRIMARY KEY, value TEXT);
        """)

    def add(self, service, key, body):
        for namespace, identity, latest, stamp, payload, counters, listing in effects(service, key, body):
            expires = body.get("_cache_expires_at")
            if expires is None:
                raise CacheRecoveryError("Legacy projection lacks durable expiry; explicit migration required")
            if not isinstance(expires, (int, float)) or not math.isfinite(expires):
                raise CacheRecoveryError("Invalid durable cache expiry")
            version = stamp + ":" + identity
            self.db.execute("INSERT INTO value VALUES (?, '1', NULL)",
                            (f"pipeline:cache:{namespace}:{identity}",))
            for counter in counters:
                self.db.execute("INSERT INTO counter VALUES (?,1) ON CONFLICT(key) DO UPDATE SET n=n+1", (counter,))
            self.db.execute("""INSERT INTO latest VALUES (?,?,?,?) ON CONFLICT(key) DO UPDATE
              SET version=excluded.version,value=excluded.value,expires=excluded.expires
              WHERE latest.version < excluded.version""", (latest, version, encode(payload), expires))
            if listing:
                self.db.execute("INSERT INTO critical VALUES (?,?)", (version, encode(payload)))

    def values(self):
        yield from self.db.execute("SELECT key,value,expires FROM value")
        yield from self.db.execute("SELECT key,CAST(n AS TEXT),NULL FROM counter")
        for key, version, value, expires in self.db.execute("SELECT * FROM latest"):
            yield key + ":version", version, None
            yield key, value, expires


# One owned key at a time, atomic replacement. No global deletion or increments.
REPLACE_LIST = """
redis.call('DEL', KEYS[1])
for i = #ARGV, 1, -1 do redis.call('LPUSH', KEYS[1], ARGV[i]) end
return 1
"""


class CacheReconciler:
    def __init__(self, factory, service, client, max_records=100000):
        if service not in {"alert", "storage"}:
            raise CacheRecoveryError("Unsupported cache projection service")
        self.factory, self.service, self.client = factory, service, client
        self.max_records = max_records

    def run(self, repair=True):
        from pipeline_runtime.durable import Outbox
        service, client = self.service, self.client
        lease = f"pipeline:cache:{service}:verified"
        VERIFIED.labels(service).set(0)
        PROGRESS.labels(service).set(0)
        UNRECONCILED.labels(service).set(-1)
        STATE.labels(service).set(1)
        count = mismatches = 0
        try:
            # Invalidate before touching live keys. Lease is telemetry only: readers
            # still use SQL, since Redis can lose a subset without losing a lease.
            client.delete(lease)
            with self.factory() as session, session.begin():
                cache_lock(session, service)
                require_complete_ledger(session, service)
                from sqlalchemy import func
                total = session.scalar(select(func.count()).select_from(Outbox).where(
                    Outbox.service == service, Outbox.target == "redis"))
                UNRECONCILED.labels(service).set(total)
                if total > self.max_records:
                    raise CacheRecoveryError("Automatic reconciliation record budget exceeded; operator review required")
                # Writers acquire this lock before selecting/locking outbox rows.
                # New SQL ingestion is allowed; its pending rows drain afterward.
                # SQLite needs a pathname; NamedTemporaryFile is mode 0600.
                with tempfile.NamedTemporaryFile(prefix="cache-projection-", suffix=".sqlite") as tmp:
                    db = sqlite3.connect(tmp.name)
                    try:
                        projection = Projection(db)
                        rows = session.execute(select(Outbox.key, Outbox.payload).where(
                            Outbox.service == service, Outbox.target == "redis").order_by(Outbox.key)
                            .execution_options(yield_per=200))
                        for key, body in rows:
                            projection.add(service, key, body)
                            count += 1
                            if count > self.max_records:
                                raise CacheRecoveryError("Reconciliation record budget exceeded")
                            if count % 200 == 0:
                                db.commit()
                                log.info("cache_reconciliation_progress service=%s records=%d", service, count)
                                PROGRESS.labels(service).set(count)
                        db.commit()
                        for key, expected, expires in projection.values():
                            try:
                                actual = client.get(key)
                            except ResponseError as exc:
                                if "WRONGTYPE" not in str(exc):
                                    raise
                                actual = object()
                            if isinstance(actual, bytes):
                                actual = actual.decode()
                            remaining = None if expires is None else int(expires - time.time())
                            wanted = expected if remaining is None or remaining > 0 else None
                            ttl = client.ttl(key)
                            expiry_bad = (wanted is not None and
                                ((remaining is None and ttl != -1) or
                                 (remaining is not None and abs(ttl - remaining) > 2)))
                            if actual != wanted or expiry_bad:
                                mismatches += 1
                            if repair:
                                if wanted is None:
                                    client.delete(key)
                                elif remaining is None:
                                    client.set(key, expected)
                                else:
                                    client.set(key, expected, exat=int(expires))
                        if service == "alert":
                            desired = [r[0] for r in db.execute("SELECT value FROM critical ORDER BY version DESC LIMIT 51")]
                            try:
                                actual = client.lrange("live:critical-alerts", 0, 51)
                            except ResponseError as exc:
                                if "WRONGTYPE" not in str(exc):
                                    raise
                                actual = ["invalid list type"]
                            actual = [x.decode() if isinstance(x, bytes) else x for x in actual]
                            if actual != desired:
                                mismatches += 1
                            if repair:
                                client.eval(REPLACE_LIST, 1, "live:critical-alerts", *desired)
                    finally:
                        db.close()
                MISMATCHES.labels(service).set(mismatches)
                if not repair and mismatches:
                    raise CacheRecoveryError("Cache projection differs from durable SQL ledger")
                PROGRESS.labels(service).set(count)
                MISMATCHES.labels(service).set(mismatches)
                UNRECONCILED.labels(service).set(0)
                STATE.labels(service).set(2)
                VERIFIED.labels(service).set(time.time())
                client.set(lease, json.dumps({"records": count, "repaired_keys": mismatches}), ex=120)
            log.info("cache_reconciliation_complete service=%s records=%d repaired_keys=%d", service, count, mismatches)
            return {"records": count, "inconsistent_keys": mismatches}
        except Exception as exc:
            STATE.labels(service).set(0)
            FAILURES.labels(service).inc()
            VERIFIED.labels(service).set(0)
            try:
                client.delete(lease)
            except Exception:
                pass
            reason = str(exc) if isinstance(exc, CacheRecoveryError) else type(exc).__name__
            log.error("cache_reconciliation_failed service=%s records=%d reason=%s", service, count, reason)
            raise


def main():
    """Run inside the matching scoped alert-engine/storage-sink container."""
    import argparse
    import redis
    from infrastructure_runtime.security import hardened, redis_options
    from app.config.settings import get_settings
    from app.db.session import create_session_factory

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('service', choices=['alert', 'storage'])
    parser.add_argument('--check', action='store_true', help='Compare without repairing projection keys')
    parser.add_argument('--max-records', type=int, default=100000)
    args = parser.parse_args()
    if not 1 <= args.max_records <= 1000000:
        parser.error('max-records must be between 1 and 1000000')
    try:
        settings = get_settings()
        client = redis.Redis(host=settings.redis_host, port=settings.redis_port,
                             decode_responses=True, socket_connect_timeout=3, socket_timeout=3,
                             ssl=hardened(), **redis_options())
        result = CacheReconciler(create_session_factory(settings), args.service, client,
                                 max_records=args.max_records).run(repair=not args.check)
        print(json.dumps({'status': 'verified', **result}))
    except Exception:
        raise SystemExit('Cache reconciliation unverified; inspect private service diagnostics.')


if __name__ == '__main__':
    main()
