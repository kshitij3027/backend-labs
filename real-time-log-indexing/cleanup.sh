#!/usr/bin/env bash
## Tear down the compose stack and reclaim disk.
##
## Stops containers, deletes the ``app_data`` volume (so segments
## are wiped), and prunes dangling images. Use this between test
## runs when you want a known-clean slate; ``stop.sh`` is the
## non-destructive alternative.

set -euo pipefail

docker compose down -v
docker image prune -f
