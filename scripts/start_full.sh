#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/setup_local.py

include_dashboards=false
if [[ "${1:-}" == "--dashboards" ]]; then
  include_dashboards=true
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
if [[ "$include_dashboards" == true ]]; then
  "${COMPOSE[@]}" --profile dashboards up -d --wait --wait-timeout 300
  python3 scripts/health_check.py --include-grafana
else
  "${COMPOSE[@]}" up -d --wait --wait-timeout 300
  python3 scripts/health_check.py
fi
