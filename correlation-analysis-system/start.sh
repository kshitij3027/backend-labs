#!/usr/bin/env bash
#
# Bring up the correlation-analysis stack (redis + backend) detached, then wait until
# the backend answers /health. Exits non-zero (dumping backend logs) on timeout.
# NOTE: the React dashboard `frontend` service lands in C9 and gets added here then.

set -euo pipefail

BACKEND_PORT="${BACKEND_PORT:-8000}"

docker compose up -d --build redis backend

echo "Waiting for backend health on http://localhost:${BACKEND_PORT}/health ..."
for _ in $(seq 1 30); do
  if curl -fsS "http://localhost:${BACKEND_PORT}/health" >/dev/null 2>&1; then
    echo "Backend healthy — API: http://localhost:${BACKEND_PORT}"
    exit 0
  fi
  sleep 1
done

echo "ERROR: backend not healthy after 30s — recent logs:" >&2
docker compose logs backend >&2
exit 1
