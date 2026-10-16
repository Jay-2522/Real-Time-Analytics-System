#!/usr/bin/env bash
set -euo pipefail

python3 -m compileall backend/app ml-models/app alert-engine/app storage-sink/app kafka-producers/app threat-intelligence-engine/app
(cd frontend && npm run build)
docker compose config --quiet
python3 scripts/health_check.py --json
