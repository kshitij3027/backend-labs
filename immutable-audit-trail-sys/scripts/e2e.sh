#!/usr/bin/env bash
# End-to-end probe — hits every public endpoint, asserts shape/status.
#
# Run from inside the tester container (the Makefile target sets it up):
#     make up && make e2e
#
# Or from the host with the app on :8000:
#     BASE_URL=http://localhost:8000 bash scripts/e2e.sh
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
echo "E2E probe — base=${BASE_URL}"
echo

# --- helpers ----------------------------------------------------------------

fail() { echo "FAIL: $*" >&2; exit 1; }

assert_200() {
    local url="$1"
    local code
    code=$(curl -s -o /dev/null -w "%{http_code}" "$url")
    [[ "$code" == "200" ]] || fail "$url -> $code (expected 200)"
    echo "  OK  $url"
}

assert_201_post() {
    local url="$1"
    local body="$2"
    shift 2
    local code
    code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$url" \
        -H "Content-Type: application/json" "$@" -d "$body")
    [[ "$code" == "201" ]] || fail "POST $url -> $code (expected 201)"
    echo "  OK  POST $url"
}

# --- 1. Health -------------------------------------------------------------

echo "== Health =="
assert_200 "${BASE_URL}/api/health"
echo

# --- 2. Audit appends -----------------------------------------------------

echo "== Audit appends =="
for actor in alice bob carol dave eve; do
    body=$(printf '{"action":"read","resource":"LOG_e2e","success":true,"args_digest":"%s","result_digest":"%s","processing_ms":1.0}' \
        "$(printf '%064d' "$RANDOM")" \
        "$(printf '%064d' "$RANDOM")")
    assert_201_post "${BASE_URL}/v1/audit/append" "$body" -H "X-User-ID: ${actor}"
done
echo

# --- 3. Records query ----------------------------------------------------

echo "== Records query =="
assert_200 "${BASE_URL}/v1/records"
assert_200 "${BASE_URL}/v1/records?actor=alice&limit=10"
assert_200 "${BASE_URL}/v1/records/0"
echo

# --- 4. Verify -----------------------------------------------------------

echo "== Chain verify =="
verify_body=$(curl -fsS "${BASE_URL}/v1/verify")
echo "  $verify_body" | head -c 200
echo
echo "$verify_body" | grep -q '"ok":true' || fail "/v1/verify reports BROKEN"
echo "  OK  chain integrity VALID"
echo

# --- 5. Compliance reports -----------------------------------------------

echo "== Compliance reports =="
for fw in gdpr hipaa soc2 pci_dss; do
    assert_200 "${BASE_URL}/v1/reports/${fw}"
done
# Unknown framework -> 400.
unknown_code=$(curl -s -o /dev/null -w "%{http_code}" "${BASE_URL}/v1/reports/unknown_framework")
[[ "$unknown_code" == "400" ]] || fail "unknown framework returned ${unknown_code} (expected 400)"
echo "  OK  unknown framework -> 400"
echo

# --- 6. Observability ----------------------------------------------------

echo "== Observability =="
stats=$(curl -fsS "${BASE_URL}/api/stats")
echo "  $stats"
echo "$stats" | grep -q '"records_appended"' || fail "stats missing records_appended"
echo "  OK  /api/stats"

metrics=$(curl -fsS "${BASE_URL}/metrics")
echo "$metrics" | grep -q 'audit_records_appended_total' || fail "metrics missing audit_records_appended_total"
echo "  OK  /metrics (custom metrics present)"
echo

# --- 7. Dashboard --------------------------------------------------------

echo "== Dashboard =="
assert_200 "${BASE_URL}/"
assert_200 "${BASE_URL}/static/dashboard.css"
assert_200 "${BASE_URL}/static/htmx.min.js"
assert_200 "${BASE_URL}/partials/stats"
assert_200 "${BASE_URL}/partials/records"
assert_200 "${BASE_URL}/partials/integrity"
assert_200 "${BASE_URL}/partials/alerts"
echo

echo "All e2e checks passed."
