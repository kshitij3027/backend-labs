#!/usr/bin/env bash
#
# Demo walkthrough for the Log Search API.
#
# Brings up the docker compose stack, seeds ~5k synthetic logs, runs ten
# representative search queries via curl + JWT, and prints the /stats endpoint.
# Leaves the stack running on success so you can poke at the dashboard at
# http://localhost:8000/.

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_ROOT}"

API_URL_HOST="${API_URL_HOST:-http://localhost:8000}"
SEED_USERNAME="${SEED_USERNAME:-demo}"
SEED_PASSWORD="${SEED_PASSWORD:-demo}"

echo "============================================================"
echo "Log Search API demo"
echo "  project root : ${PROJECT_ROOT}"
echo "  api (host)   : ${API_URL_HOST}"
echo "  username     : ${SEED_USERNAME}"
echo "============================================================"

# 1. Bring up the stack and wait for healthchecks.
echo
echo ">> bringing up docker compose stack..."
docker compose up -d --wait

# 2. Seed sample data via the api container (so it can resolve api:8000
#    on the compose network and reuse the same Python image).
echo
echo ">> seeding ~5,000 synthetic log entries..."
docker compose run --rm --no-deps api python scripts/seed_data.py

# 3. Acquire a token from the host so we can run curl examples.
echo
echo ">> requesting JWT from ${API_URL_HOST}/api/v1/auth/token..."
TOKEN_JSON="$(curl -sS -X POST \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode "username=${SEED_USERNAME}" \
  --data-urlencode "password=${SEED_PASSWORD}" \
  "${API_URL_HOST}/api/v1/auth/token")"
TOKEN="$(printf '%s' "${TOKEN_JSON}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')"
AUTH_HEADER="Authorization: Bearer ${TOKEN}"
echo "got token (length=${#TOKEN})"

# Helper: pretty-print a search call.
run_search () {
  local description="$1"; shift
  local payload="$1"; shift
  echo
  echo "------------------------------------------------------------"
  echo ">> ${description}"
  echo "   payload: ${payload}"
  curl -sS -X POST \
    -H 'Content-Type: application/json' \
    -H "${AUTH_HEADER}" \
    -d "${payload}" \
    "${API_URL_HOST}/api/v1/logs/search" \
    | python3 -c 'import json,sys; d=json.load(sys.stdin); print(json.dumps({k: d[k] for k in ("total_hits","execution_time_ms","cache_hit","pagination") if k in d}, indent=2))'
}

# 4. Ten representative search queries.
run_search "1/10 broad text search for 'error'"        '{"q":"error","limit":10}'
run_search "2/10 narrow to ERROR/CRITICAL levels"      '{"q":"error","levels":["ERROR","CRITICAL"],"limit":10}'
run_search "3/10 payment service only"                 '{"q":"payment","services":["payment-service"],"limit":10}'
run_search "4/10 auth-service successful logins"       '{"q":"logged in","services":["auth-service"],"limit":10}'
run_search "5/10 sort by timestamp asc"                '{"q":"order","sort_by":"timestamp","sort_order":"asc","limit":10}'
run_search "6/10 paginate page 2 (offset=10)"          '{"q":"order","limit":10,"offset":10}'
run_search "7/10 multi-service WARN+"                  '{"q":"slow","levels":["WARN","ERROR","CRITICAL"],"limit":10}'
run_search "8/10 fuzzy hit ('paymnt' typo)"            '{"q":"paymnt","limit":10}'
run_search "9/10 large limit"                          '{"q":"notification","limit":50}'
run_search "10/10 empty query — recent logs"           '{"limit":10}'

# 5. Cache demo: same query twice, second should be a cache hit.
echo
echo "------------------------------------------------------------"
echo ">> cache demo: running the same query twice..."
run_search "cache demo (first call — populates cache)" '{"q":"refund","limit":5}'
run_search "cache demo (second call — should be a hit)" '{"q":"refund","limit":5}'

# 6. /stats snapshot.
echo
echo "------------------------------------------------------------"
echo ">> GET /api/v1/stats"
curl -sS -H "${AUTH_HEADER}" "${API_URL_HOST}/api/v1/stats" \
  | python3 -m json.tool

# 7. Wrap up.
echo
echo "============================================================"
echo "demo complete"
echo "  dashboard : ${API_URL_HOST}/"
echo "  swagger   : ${API_URL_HOST}/api/docs"
echo "  redoc     : ${API_URL_HOST}/api/redoc"
echo "  stack     : still running. 'docker compose down' to stop."
echo "============================================================"
