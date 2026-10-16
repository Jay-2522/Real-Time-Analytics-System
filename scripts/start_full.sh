#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/setup_local.py

echo "Starting full Streaming Analytics AI SOC platform..."
docker compose up --build -d
python3 scripts/health_check.py
