"""Apply additive schema and seed receipts without modifying historical business rows.
Stop the affected consumers before running. Uses PG* environment variables (no secrets in argv).
"""
import json
import os
from pathlib import Path
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from pipeline_runtime.durable import Receipt
from pipeline_runtime.identity import logical_key, digest


def migrate(engine):
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cursor:
            cursor.execute((Path(__file__).parents[1] / "migrations/002_pipeline_reliability.sql").read_text())
        raw.commit()
    finally:
        raw.close()
    summary = {}
    for table in ("telemetry_events", "analytics_metrics", "risk_metrics", "feature_snapshots", "anomaly_predictions", "deadletter_events", "alerts"):
        if not inspect(engine).has_table(table):
            summary[table] = {"absent_new_install": True}
            continue
        count = skipped = 0
        last = 0
        service = "alert" if table == "alerts" else "storage"
        while True:
            with Session(engine) as session, session.begin():
                rows = session.execute(text(f"SELECT id, source_topic, raw_payload FROM {table} WHERE id > :last ORDER BY id LIMIT 500"), {"last": last}).all()
                receipts = {}
                for row in rows:
                    last = row.id
                    try:
                        key = logical_key(row.source_topic, row.raw_payload)
                        fingerprint = digest(row.raw_payload)
                    except (ValueError, TypeError):
                        skipped += 1
                        continue
                    receipts.setdefault(key, {"service": service, "key": key, "fingerprint": fingerprint})
                    count += 1
                if receipts:
                    session.execute(insert(Receipt).values(list(receipts.values())).on_conflict_do_nothing())
            if not rows:
                break
        summary[table] = {"examined_with_identity": count, "unidentifiable": skipped}
    return summary


if __name__ == "__main__":
    engine = create_engine(URL.create("postgresql+psycopg", username=os.environ["PGUSER"], password=os.environ["PGPASSWORD"],
        host=os.environ.get("PGHOST", "localhost"), port=int(os.environ.get("PGPORT", "5432")), database=os.environ["PGDATABASE"]))
    print(json.dumps(migrate(engine), indent=2))
