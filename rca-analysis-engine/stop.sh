#!/usr/bin/env bash
#
# Tear down the RCA Analysis Engine stack (containers + network; volumes are kept —
# use `make clean` to drop everything).

set -euo pipefail

docker compose down
