#!/usr/bin/env python3
"""Create private Compose development settings without replacing existing settings."""
import os
import secrets
import socket
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def available_port(preferred: int, alternate: int) -> int:
    for port in (preferred, alternate):
        with socket.socket() as probe:
            try:
                probe.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    raise RuntimeError(f"Ports {preferred} and {alternate} are occupied; choose ports in .env manually")


def main() -> None:
    secret_dir = ROOT / ".secrets"
    secret_dir.mkdir(mode=0o700, exist_ok=True)
    secret_dir.chmod(0o700)
    token_file = secret_dir / "backend_metrics_token"
    if not token_file.exists():
        fd = os.open(token_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as handle:
            handle.write(secrets.token_urlsafe(32) + "\n")
        # Parent directory is owner-only; the mounted file must be readable by Prometheus.
        token_file.chmod(0o444)
        print("Created private backend metrics credential.")
    target = ROOT / ".env"
    if target.exists():
        print("Existing .env preserved. Check its credentials and published ports before starting.")
        return
    password = secrets.token_hex(24)
    values = {
        "POSTGRES_PASSWORD": password,
        "DATABASE_URL": f"postgresql+psycopg://streaming_user:{password}@streaming-analytics-postgres:5432/streaming_analytics",
        "GRAFANA_ADMIN_PASSWORD": secrets.token_hex(24),
        "POSTGRES_PUBLISHED_PORT": str(available_port(5432, 15432)),
        "GRAFANA_PUBLISHED_PORT": str(available_port(3000, 13000)),
    }
    lines = (ROOT / ".env.example").read_text().splitlines()
    configured = [f"{line.split('=', 1)[0]}={values[line.split('=', 1)[0]]}"
                  if "=" in line and line.split("=", 1)[0] in values else line for line in lines]
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as handle:
        handle.write("\n".join(configured) + "\n")
    print("Created private .env with generated development credentials (mode 0600).")
    print(f"PostgreSQL host port: {values['POSTGRES_PUBLISHED_PORT']}; Grafana host port: {values['GRAFANA_PUBLISHED_PORT']}")
    print("Credentials apply to newly initialized volumes only; existing volumes are never reset.")


if __name__ == "__main__":
    main()
