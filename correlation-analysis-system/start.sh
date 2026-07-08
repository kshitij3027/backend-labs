#!/usr/bin/env bash
#
# Bring up the correlation-analysis stack (redis + backend + React dashboard) detached,
# then wait until the backend answers /health. Exits non-zero (dumping backend logs) on
# timeout. The `frontend` service (C9) is an nginx-served SPA that depends on a healthy
# backend and polls GET /api/v1/dashboard through its /api reverse proxy every 5s.

set -euo pipefail

BACKEND_PORT="${BACKEND_PORT:-8000}"
FRONTEND_PORT="${FRONTEND_PORT:-3000}"

docker compose up -d --build redis backend frontend

echo "Waiting for backend health on http://localhost:${BACKEND_PORT}/health ..."
for _ in $(seq 1 30); do
  if curl -fsS "http://localhost:${BACKEND_PORT}/health" >/dev/null 2>&1; then
    echo "Backend healthy — API:       http://localhost:${BACKEND_PORT}"
    echo "Dashboard (nginx SPA):       http://localhost:${FRONTEND_PORT}"
    exit 0
  fi
  sleep 1
done

echo "ERROR: backend not healthy after 30s — recent logs:" >&2
docker compose logs backend >&2
exit 1
