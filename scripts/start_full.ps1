Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
python scripts/setup_local.py
if ($LASTEXITCODE -ne 0) { throw "Local environment setup failed" }

Write-Host "Starting full Streaming Analytics AI SOC platform..."
docker compose up --build -d
python scripts/health_check.py
