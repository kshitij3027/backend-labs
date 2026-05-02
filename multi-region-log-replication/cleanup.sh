#!/bin/bash
# Conservative cleanup for multi-region-log-replication.
#
# What it DOES:
#   - Stops the docker compose stack and removes its volumes.
#   - Prunes dangling docker images.
#
# What it DOES NOT DO:
#   - Touch your virtualenv (.venv).
#   - Remove your build artifacts outside Docker.
#   - Force-remove named images.
#
# Per project_requirements.md §3D, the cleanup script must be conservative
# and never nuke the venv.

set -e

show_help() {
    cat <<'EOF'
Usage: ./cleanup.sh [--help]

Stops the multi-region-log-replication docker compose stack, removes
associated volumes, and prunes dangling docker images.

Does NOT remove your local Python virtualenv (.venv) or any source files.

Options:
  --help, -h    Show this help message and exit
EOF
}

case "${1:-}" in
    --help|-h)
        show_help
        exit 0
        ;;
    "")
        ;;
    *)
        echo "unknown argument: $1" >&2
        show_help
        exit 1
        ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo ">>> stopping docker compose stack and removing volumes"
docker compose down -v 2>/dev/null || true

echo ">>> pruning dangling docker images"
docker image prune -f

echo ">>> done"
