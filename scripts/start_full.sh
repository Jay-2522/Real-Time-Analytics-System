#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/setup_local.py

profile_args=()
health_args=()
if [[ "${1:-}" == "--dashboards" ]]; then
  profile_args=(--profile dashboards)
  health_args=(--include-grafana)
  shift
fi
if [[ $# -ne 0 ]]; then
  echo "Usage: $0 [--dashboards]" >&2
  exit 2
fi

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
"${COMPOSE[@]}" "${profile_args[@]}" up -d --wait --wait-timeout 300
python3 scripts/health_check.py "${health_args[@]}"
