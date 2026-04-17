#!/usr/bin/env bash
set -e

docker compose up -d partition-1 partition-2 partition-3 coordinator

echo "Waiting for services to become healthy..."

deadline=$((SECONDS + 60))
check_urls=(
  "http://localhost:8101/health"
  "http://localhost:8102/health"
  "http://localhost:8103/health"
  "http://localhost:8000/api/health"
)

while [ $SECONDS -lt $deadline ]; do
  all_ok=1
  for url in "${check_urls[@]}"; do
    if ! curl -fsS "$url" >/dev/null 2>&1; then
      all_ok=0
      break
    fi
  done
  if [ $all_ok -eq 1 ]; then
    echo "All services healthy."
    echo "Coordinator: http://localhost:8000"
    echo "Partitions:  http://localhost:8101 | :8102 | :8103"
    exit 0
  fi
  sleep 2
done

echo "Timed out waiting for services to become healthy." >&2
exit 1
