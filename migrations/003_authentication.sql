BEGIN;
CREATE TABLE IF NOT EXISTS auth_users (
 id varchar(36) PRIMARY KEY, username varchar(64) NOT NULL UNIQUE,
 password_hash text NOT NULL, role varchar(20) NOT NULL CHECK (role IN ('viewer','analyst','administrator')),
 active boolean NOT NULL DEFAULT true, created_at integer NOT NULL
);
CREATE TABLE IF NOT EXISTS auth_sessions (
 token_hash varchar(64) PRIMARY KEY, user_id varchar(36) NOT NULL REFERENCES auth_users(id),
 created_at integer NOT NULL, expires_at integer NOT NULL, last_seen integer NOT NULL,
 revoked boolean NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS ix_auth_sessions_user_id ON auth_sessions(user_id);
CREATE INDEX IF NOT EXISTS ix_auth_sessions_expires_at ON auth_sessions(expires_at);
CREATE TABLE IF NOT EXISTS auth_audit (
 id varchar(36) PRIMARY KEY, timestamp integer NOT NULL, actor_id varchar(36) REFERENCES auth_users(id),
 action varchar(64) NOT NULL, target varchar(200) NOT NULL, outcome varchar(32) NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_auth_audit_timestamp ON auth_audit(timestamp);
CREATE TABLE IF NOT EXISTS auth_login_throttle (
 key varchar(64) PRIMARY KEY, window_start integer NOT NULL, attempts integer NOT NULL
);
CREATE TABLE IF NOT EXISTS auth_control (
 id integer PRIMARY KEY CHECK (id=1), generation integer NOT NULL DEFAULT 0,
 bootstrapped boolean NOT NULL DEFAULT false
);
INSERT INTO auth_control (id,generation,bootstrapped) VALUES (1,0,false) ON CONFLICT DO NOTHING;
COMMIT;
