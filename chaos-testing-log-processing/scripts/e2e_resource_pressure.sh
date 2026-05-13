#!/usr/bin/env bash
# E2E smoke for the RESOURCE_EXHAUSTION fault type (CPU pressure).

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> docker compose up -d (framework + targets)"
docker compose up -d chaos-framework redis log-producer log-consumer

echo "==> waiting for healthy state"
for svc in chaos-framework redis log-producer log-consumer; do
  for i in $(seq 1 20); do
    status=$(docker inspect --format='{{.State.Health.Status}}' "$svc" 2>/dev/null || echo missing)
    if [ "$status" = "healthy" ]; then break; fi
    sleep 2
  done
  echo "  $svc -> $status"
  if [ "$status" != "healthy" ]; then
    echo "FAIL: $svc never reached healthy"
    docker compose logs --tail=80 "$svc" || true
    docker compose down --remove-orphans
    exit 1
  fi
done

echo "==> running resource-pressure driver inside chaos-framework"
set +e
docker exec chaos-framework python3 /app/scripts/e2e_resource_pressure.py
rc=$?
set -e

echo "==> teardown"
docker compose down --remove-orphans

echo "==> exit code = $rc"
exit "$rc"
