#!/usr/bin/env bash
set -euo pipefail
docker compose down -v
docker image prune -f
echo "Cleaned up volumes and pruned dangling images."
