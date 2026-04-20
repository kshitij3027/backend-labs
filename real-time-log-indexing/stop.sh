#!/usr/bin/env bash
## Bring the compose stack down without deleting volumes.
##
## Used by the Makefile and as the "clean exit" counterpart to
## ``start.sh``. Preserves the ``app_data`` volume so persisted
## segments survive a stop/start cycle.

set -euo pipefail

docker compose down
