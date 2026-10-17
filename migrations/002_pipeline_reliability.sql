-- Additive, repeatable migration. Existing rows, IDs, constraints and volumes are retained.
BEGIN;
CREATE TABLE IF NOT EXISTS pipeline_receipts (
    service varchar(32) NOT NULL, key varchar(64) NOT NULL,
    fingerprint varchar(64) NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (service, key)
);
CREATE TABLE IF NOT EXISTS pipeline_outbox (
    service varchar(32) NOT NULL, key varchar(64) NOT NULL, target varchar(32) NOT NULL,
    payload jsonb NOT NULL, done boolean NOT NULL DEFAULT false,
    attempts integer NOT NULL DEFAULT 0, next_attempt timestamptz NOT NULL DEFAULT now(),
    last_error text, PRIMARY KEY (service, key, target)
);
CREATE INDEX IF NOT EXISTS ix_pipeline_outbox_pending ON pipeline_outbox(service, next_attempt) WHERE NOT done;
CREATE TABLE IF NOT EXISTS pipeline_quarantine (
    service varchar(32) NOT NULL, key varchar(64) NOT NULL, payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (service, key)
);
COMMIT;
