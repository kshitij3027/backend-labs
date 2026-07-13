#!/usr/bin/env bash
#
# Bring up the RCA Analysis Engine stack (backend + React dashboard) detached, then wait
# until the backend answers /api/health. Exits non-zero (dumping backend logs) on
# timeout. All analysis state is in-memory in the single backend process. The `frontend`
# service (C11) is an nginx-served SPA that depends on a healthy backend, loads the
# incident history over its /api reverse proxy, and live-updates over the /ws WebSocket.

set -euo pipefail

BACKEND_PORT="${BACKEND_PORT:-8000}"
FRONTEND_PORT="${FRONTEND_PORT:-3000}"

docker compose up -d --build backend frontend

echo "Waiting for backend health on http://localhost:${BACKEND_PORT}/api/health ..."
for _ in $(seq 1 30); do
  if curl -fsS "http://localhost:${BACKEND_PORT}/api/health" >/dev/null 2>&1; then
    echo "Backend healthy — API:       http://localhost:${BACKEND_PORT}  (GET /api/health)"
    echo "Dashboard (nginx SPA):       http://localhost:${FRONTEND_PORT}"
    exit 0
  fi
  sleep 1
done

echo "ERROR: backend not healthy after 30s — recent logs:" >&2
docker compose logs backend >&2
exit 1
