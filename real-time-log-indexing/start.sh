#!/usr/bin/env bash
## Bring the real-time log indexing stack up and wait for readiness.
##
## Starts Redis + the FastAPI app in detached mode, then polls
## /health for up to 30 seconds. Exits 0 only after /health replies
## successfully. Used by the Makefile ``up``, ``e2e``, ``load``, and
## ``demo`` targets so downstream steps never race the container
## startup.

set -euo pipefail

docker compose up -d redis app

echo "Waiting for app to become healthy..."

max_attempts=30
attempt=0
while [ "$attempt" -lt "$max_attempts" ]; do
  if curl -fsS http://localhost:8080/health >/dev/null 2>&1; then
    echo "App healthy after ${attempt}s."
    echo "Dashboard: http://localhost:8080/"
    exit 0
  fi
  attempt=$((attempt + 1))
  echo "  waiting... (${attempt}/${max_attempts})"
  sleep 1
done

echo "Timed out waiting for app to become healthy." >&2
docker compose logs app >&2 || true
exit 1
