#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/setup_local.py

if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose)
else
  echo "Docker Compose is required (docker compose or docker-compose)." >&2
  exit 1
fi

if grep -q '^DEV_BIND_ROOT=' .env; then
  python3 scripts/stage_local_mounts.py
fi

echo "Starting full Streaming Analytics AI SOC platform..."
echo "Using Compose command: ${COMPOSE[*]}"
"${COMPOSE[@]}" up -d --wait --wait-timeout 300
python3 scripts/health_check.py
