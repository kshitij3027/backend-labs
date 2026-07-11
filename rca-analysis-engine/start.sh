#!/usr/bin/env bash
#
# Bring up the RCA Analysis Engine backend detached, then wait until it answers
# /api/health. Exits non-zero (dumping backend logs) on timeout. All analysis state is
# in-memory in the single backend process — there are no other services to start in C1
# (the React dashboard is added in a later commit).

set -euo pipefail

BACKEND_PORT="${BACKEND_PORT:-8000}"

docker compose up -d --build backend

echo "Waiting for backend health on http://localhost:${BACKEND_PORT}/api/health ..."
for _ in $(seq 1 30); do
  if curl -fsS "http://localhost:${BACKEND_PORT}/api/health" >/dev/null 2>&1; then
    echo "Backend healthy — API: http://localhost:${BACKEND_PORT}  (GET /api/health)"
    exit 0
  fi
  sleep 1
done

echo "ERROR: backend not healthy after 30s — recent logs:" >&2
docker compose logs backend >&2
exit 1
