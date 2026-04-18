#!/usr/bin/env bash
set -e

docker compose up -d app redis

echo "Waiting for app to become healthy..."

max_attempts=30
attempt=0
while [ $attempt -lt $max_attempts ]; do
  if curl -sf http://localhost:8000/health >/dev/null 2>&1; then
    echo "App healthy after ${attempt}s."
    echo "Dashboard: http://localhost:8000"
    exit 0
  fi
  attempt=$((attempt + 1))
  echo "  waiting... (${attempt}/${max_attempts})"
  sleep 1
done

echo "Timed out waiting for app to become healthy." >&2
docker compose logs app >&2 || true
exit 1
