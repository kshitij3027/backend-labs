#!/usr/bin/env bash
# End-to-end probe — exercises every documented HTTP endpoint of the
# automated-log-retention service. Asserts each returns the expected
# status code (200, 200, 200, 200, 400 for unknown framework, 200 for
# static assets). Exits non-zero on the first failure.
#
# Usage:
#   BASE_URL=http://app:8000 bash scripts/e2e.sh
set -euo pipefail

BASE_URL=${BASE_URL:-http://localhost:8000}
FAIL_COUNT=0

probe() {
    local method=$1
    local path=$2
    local expected=$3
    local body=${4:-}
    local out
    if [[ -n "$body" ]]; then
        out=$(curl -s -o /dev/null -w "%{http_code}" -X "$method" -H 'Content-Type: application/json' -d "$body" "${BASE_URL}${path}")
    else
        out=$(curl -s -o /dev/null -w "%{http_code}" -X "$method" "${BASE_URL}${path}")
    fi
    if [[ "$out" != "$expected" ]]; then
        echo "FAIL: $method $path expected $expected got $out"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    else
        echo "ok:   $method $path -> $out"
    fi
}

# Public endpoints
probe GET  /api/health 200
probe POST /v1/logs/ingest 200 '{"records":[{"ts":"2026-05-23T00:00:00Z","level":"INFO","source":"e2e","category":"user_activity","message":"probe"}]}'
probe GET  /v1/files 200
probe POST /v1/evaluate 200

# Compliance reports
for fw in gdpr sox hipaa pci_dss soc2; do
    probe GET "/v1/reports/$fw" 200
done
probe GET /v1/reports/bogus 400

# Dashboard
probe GET / 200
probe GET /partials/stats 200
probe GET /partials/tiers 200
probe GET /partials/policies 200
probe GET /partials/audit 200

# Static assets
probe GET /static/dashboard.css 200
probe GET /static/htmx.min.js 200

if [[ $FAIL_COUNT -gt 0 ]]; then
    echo "e2e.sh: $FAIL_COUNT failures"
    exit 1
fi
echo "e2e.sh: all endpoints responded as expected"
