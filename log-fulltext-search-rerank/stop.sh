#!/usr/bin/env bash
## Bring the compose stack down.
##
## Clean-exit counterpart to ``start.sh``. Does not remove volumes —
## once persistence is added in later commits, a stop/start cycle
## should be non-destructive.

set -euo pipefail

docker compose down
