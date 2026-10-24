#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/setup_local.py

include_dashboards=false
build_images=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dashboards) include_dashboards=true ;;
    --build) build_images=true ;;
    *)
      echo "Usage: $0 [--dashboards] [--build]" >&2
      exit 2
      ;;
  esac
  shift
done

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
build_args=()
if [[ "$build_images" == true ]]; then
  build_args+=(--build)
fi
if [[ "$include_dashboards" == true ]]; then
  "${COMPOSE[@]}" --profile dashboards up -d "${build_args[@]}" --wait --wait-timeout 300
  python3 scripts/health_check.py --include-grafana
else
  "${COMPOSE[@]}" up -d "${build_args[@]}" --wait --wait-timeout 300
  python3 scripts/health_check.py
fi
