#!/usr/bin/env bash
# End-to-end smoke runner for the Field-Level Log Encryption Service.
#
# Runs against an ALREADY-RUNNING stack (`make up` brought it up). Walks
# every public HTTP endpoint with realistic payloads, asserts the
# expected response shape, and prints a PASS/FAIL summary. The
# script does NOT start or stop docker — that is the Makefile's job
# (`make e2e` brings the stack up, invokes this script, and tears it
# down regardless of exit code).
#
# Designed to be loud on failure: every check is one helper call so a
# regression in any single endpoint surfaces in the summary line.
set -euo pipefail

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
#
# BASE_URL defaults to the host-side mapping that `docker compose up app`
# exposes (port 8000). Override via `BASE_URL=http://app:8000 bash
# scripts/e2e.sh` when running inside the compose network.
BASE_URL="${BASE_URL:-http://localhost:8000}"

# Resolve the project root so we can read fixtures by relative path
# regardless of where the script was invoked from.
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

ECOMMERCE_FIXTURE="tests/fixtures/ecommerce_log.json"
SUPPORT_FIXTURE="tests/fixtures/support_ticket_log.json"

# Counter state. `pass`/`fail` mutate these; the final summary uses them
# to compute the exit code (0 iff fail_count == 0).
pass_count=0
fail_count=0

# ANSI colours — disabled when stdout is not a TTY so CI logs stay clean.
if [[ -t 1 ]]; then
  GREEN=$'\033[0;32m'
  RED=$'\033[0;31m'
  YELLOW=$'\033[0;33m'
  CYAN=$'\033[0;36m'
  RESET=$'\033[0m'
else
  GREEN=""
  RED=""
  YELLOW=""
  CYAN=""
  RESET=""
fi

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

pass() {
  # $1: test name. Prints in green, bumps pass counter.
  pass_count=$((pass_count + 1))
  printf '  %s[PASS]%s %s\n' "$GREEN" "$RESET" "$1"
}

fail() {
  # $1: test name, $2: reason. Prints in red, bumps fail counter.
  fail_count=$((fail_count + 1))
  printf '  %s[FAIL]%s %s: %s\n' "$RED" "$RESET" "$1" "$2"
}

section() {
  # Print a small grouping header for readability.
  printf '\n%s== %s ==%s\n' "$CYAN" "$1" "$RESET"
}

# expect_contains <body> <needle> <test_name>
#   PASS iff $body contains the literal substring $needle.
#   Used for plain-text/Prometheus/HTML responses where we don't want
#   to invoke jq.
expect_contains() {
  local body="$1" needle="$2" name="$3"
  if printf '%s' "$body" | grep -qF -- "$needle"; then
    pass "$name"
  else
    fail "$name" "response did not contain '$needle'"
  fi
}

# expect_jq <body> <jq_filter> <expected> <test_name>
#   PASS iff jq returns the literal $expected string for $jq_filter.
#   Quoting note: $expected is compared as a string. JSON booleans /
#   numbers are stringified by jq before comparison.
expect_jq() {
  local body="$1" filter="$2" expected="$3" name="$4"
  local actual
  if ! actual=$(printf '%s' "$body" | jq -r "$filter" 2>&1); then
    fail "$name" "jq filter '$filter' failed: $actual"
    return
  fi
  if [[ "$actual" == "$expected" ]]; then
    pass "$name"
  else
    fail "$name" "expected '$expected', got '$actual'"
  fi
}

# Sanity: dependencies present.
for tool in curl jq python3; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    printf '%s[FATAL]%s required tool not on PATH: %s\n' "$RED" "$RESET" "$tool" >&2
    exit 2
  fi
done

printf '%s== Field-Level Log Encryption Service — E2E ==%s\n' "$CYAN" "$RESET"
printf '  base url: %s\n' "$BASE_URL"

# ---------------------------------------------------------------------------
# 1. /api/health  (liveness)
# ---------------------------------------------------------------------------
section "1. /api/health"
HEALTH_BODY=$(curl -fsS "$BASE_URL/api/health" 2>&1) \
  && pass "GET /api/health 200" \
  || fail "GET /api/health 200" "curl failed: $HEALTH_BODY"
expect_contains "$HEALTH_BODY" "healthy" "health response says 'healthy'"

# ---------------------------------------------------------------------------
# 2. /v1/keys  (at least one active DEK)
# ---------------------------------------------------------------------------
section "2. /v1/keys"
KEYS_BODY=$(curl -fsS "$BASE_URL/v1/keys" 2>&1) \
  && pass "GET /v1/keys 200" \
  || fail "GET /v1/keys 200" "curl failed: $KEYS_BODY"
# At least one key, at least one of them active.
expect_jq "$KEYS_BODY" '.keys | length >= 1' "true" "/v1/keys returns ≥1 key"
expect_jq \
  "$KEYS_BODY" \
  '[.keys[] | select(.status == "active")] | length >= 1' \
  "true" \
  "/v1/keys has an active key"

# ---------------------------------------------------------------------------
# 3. /v1/detect  (dry-run on the e-commerce fixture)
# ---------------------------------------------------------------------------
section "3. /v1/detect (e-commerce fixture)"
DETECT_PAYLOAD=$(jq -n --slurpfile log "$ECOMMERCE_FIXTURE" '{log: $log[0]}')
DETECT_BODY=$(curl -fsS -X POST "$BASE_URL/v1/detect" \
  -H "Content-Type: application/json" \
  -d "$DETECT_PAYLOAD" 2>&1) \
  && pass "POST /v1/detect 200" \
  || fail "POST /v1/detect 200" "curl failed: $DETECT_BODY"
# Email type detected and `customer_email` path is in the list.
expect_jq \
  "$DETECT_BODY" \
  '[.detections[] | select(.field_type == "email")] | length >= 1' \
  "true" \
  "detect: email field_type present"
expect_jq \
  "$DETECT_BODY" \
  '[.detections[] | select(.field_path == "customer_email")] | length >= 1' \
  "true" \
  "detect: customer_email field_path present"

# ---------------------------------------------------------------------------
# 4. /v1/logs/encrypt  (e-commerce fixture)
# ---------------------------------------------------------------------------
section "4. /v1/logs/encrypt (e-commerce fixture)"
ENCRYPT_PAYLOAD=$(jq -n --slurpfile log "$ECOMMERCE_FIXTURE" '{log: $log[0]}')
ENCRYPT_BODY=$(curl -fsS -X POST "$BASE_URL/v1/logs/encrypt" \
  -H "Content-Type: application/json" \
  -d "$ENCRYPT_PAYLOAD" 2>&1) \
  && pass "POST /v1/logs/encrypt 200" \
  || fail "POST /v1/logs/encrypt 200" "curl failed: $ENCRYPT_BODY"
# customer_email replaced by an EncryptedField dict (has encrypted_value).
expect_jq "$ENCRYPT_BODY" '.customer_email.encrypted_value | type' "string" \
  "encrypt: customer_email has encrypted_value"
expect_jq "$ENCRYPT_BODY" '.customer_email.algorithm' "AES-256-GCM" \
  "encrypt: customer_email algorithm == AES-256-GCM"
# order_id is unchanged (operational field, not PII).
EXPECTED_ORDER_ID=$(jq -r '.order_id' "$ECOMMERCE_FIXTURE")
expect_jq "$ENCRYPT_BODY" '.order_id' "$EXPECTED_ORDER_ID" \
  "encrypt: order_id unchanged"
# _processing envelope present.
expect_jq "$ENCRYPT_BODY" '._processing.key_id | type' "string" \
  "encrypt: _processing.key_id present"

# ---------------------------------------------------------------------------
# 5. /v1/logs/decrypt  (round-trip the encrypted e-commerce log)
# ---------------------------------------------------------------------------
section "5. /v1/logs/decrypt (round-trip)"
DECRYPT_PAYLOAD=$(printf '%s' "$ENCRYPT_BODY" | jq '{log: .}')
DECRYPT_BODY=$(curl -fsS -X POST "$BASE_URL/v1/logs/decrypt" \
  -H "Content-Type: application/json" \
  -d "$DECRYPT_PAYLOAD" 2>&1) \
  && pass "POST /v1/logs/decrypt 200" \
  || fail "POST /v1/logs/decrypt 200" "curl failed: $DECRYPT_BODY"
EXPECTED_EMAIL=$(jq -r '.customer_email' "$ECOMMERCE_FIXTURE")
expect_jq "$DECRYPT_BODY" '.customer_email' "$EXPECTED_EMAIL" \
  "decrypt: customer_email recovered"
EXPECTED_PHONE=$(jq -r '.phone' "$ECOMMERCE_FIXTURE")
expect_jq "$DECRYPT_BODY" '.phone' "$EXPECTED_PHONE" \
  "decrypt: phone recovered"
# _processing envelope stripped on decrypt.
expect_jq "$DECRYPT_BODY" '._processing' "null" \
  "decrypt: _processing envelope stripped"

# ---------------------------------------------------------------------------
# 6. /v1/logs/encrypt + /v1/logs/decrypt on the support-ticket fixture
# ---------------------------------------------------------------------------
section "6. Support-ticket fixture round-trip"
ST_ENCRYPT_PAYLOAD=$(jq -n --slurpfile log "$SUPPORT_FIXTURE" '{log: $log[0]}')
ST_ENCRYPT_BODY=$(curl -fsS -X POST "$BASE_URL/v1/logs/encrypt" \
  -H "Content-Type: application/json" \
  -d "$ST_ENCRYPT_PAYLOAD" 2>&1) \
  && pass "POST /v1/logs/encrypt (support) 200" \
  || fail "POST /v1/logs/encrypt (support) 200" "curl failed: $ST_ENCRYPT_BODY"
expect_jq "$ST_ENCRYPT_BODY" '.user_email.encrypted_value | type' "string" \
  "support: user_email encrypted"
expect_jq "$ST_ENCRYPT_BODY" '.customer_ssn.encrypted_value | type' "string" \
  "support: customer_ssn encrypted"
EXPECTED_TICKET_ID=$(jq -r '.ticket_id' "$SUPPORT_FIXTURE")
expect_jq "$ST_ENCRYPT_BODY" '.ticket_id' "$EXPECTED_TICKET_ID" \
  "support: ticket_id unchanged"

ST_DECRYPT_PAYLOAD=$(printf '%s' "$ST_ENCRYPT_BODY" | jq '{log: .}')
ST_DECRYPT_BODY=$(curl -fsS -X POST "$BASE_URL/v1/logs/decrypt" \
  -H "Content-Type: application/json" \
  -d "$ST_DECRYPT_PAYLOAD" 2>&1) \
  && pass "POST /v1/logs/decrypt (support) 200" \
  || fail "POST /v1/logs/decrypt (support) 200" "curl failed: $ST_DECRYPT_BODY"
EXPECTED_USER_EMAIL=$(jq -r '.user_email' "$SUPPORT_FIXTURE")
expect_jq "$ST_DECRYPT_BODY" '.user_email' "$EXPECTED_USER_EMAIL" \
  "support: user_email recovered"
EXPECTED_SSN=$(jq -r '.customer_ssn' "$SUPPORT_FIXTURE")
expect_jq "$ST_DECRYPT_BODY" '.customer_ssn' "$EXPECTED_SSN" \
  "support: customer_ssn recovered"

# ---------------------------------------------------------------------------
# 7. /v1/logs/encrypt/batch  (5 e-commerce logs)
# ---------------------------------------------------------------------------
section "7. /v1/logs/encrypt/batch (5 e-commerce logs)"
# Build {"logs": [log, log, log, log, log]} from the fixture.
BATCH_PAYLOAD=$(jq -n --slurpfile log "$ECOMMERCE_FIXTURE" \
  '{logs: [$log[0], $log[0], $log[0], $log[0], $log[0]]}')
BATCH_BODY=$(curl -fsS -X POST "$BASE_URL/v1/logs/encrypt/batch" \
  -H "Content-Type: application/json" \
  -d "$BATCH_PAYLOAD" 2>&1) \
  && pass "POST /v1/logs/encrypt/batch 200" \
  || fail "POST /v1/logs/encrypt/batch 200" "curl failed: $BATCH_BODY"
expect_jq "$BATCH_BODY" '.encrypted_logs | length' "5" \
  "batch: response has 5 encrypted entries"
# Spot-check the first entry has the envelope and an encrypted email.
expect_jq "$BATCH_BODY" '.encrypted_logs[0]._processing.key_id | type' "string" \
  "batch: first entry has _processing.key_id"
expect_jq "$BATCH_BODY" '.encrypted_logs[0].customer_email.encrypted_value | type' "string" \
  "batch: first entry customer_email encrypted"

# ---------------------------------------------------------------------------
# 8. /metrics  (Prometheus exposition)
# ---------------------------------------------------------------------------
section "8. /metrics"
METRICS_BODY=$(curl -fsS "$BASE_URL/metrics" 2>&1) \
  && pass "GET /metrics 200" \
  || fail "GET /metrics 200" "curl failed: $METRICS_BODY"
# We previously fired several encrypts; encryptions_total must exist.
expect_contains "$METRICS_BODY" "encryptions_total" \
  "/metrics contains encryptions_total"
# Default HTTP instrumentation populated by the instrumentator.
expect_contains "$METRICS_BODY" "http_requests_total" \
  "/metrics contains http_requests_total"

# ---------------------------------------------------------------------------
# 9. /api/stats  (counters incremented by the prior calls)
# ---------------------------------------------------------------------------
section "9. /api/stats"
STATS_BODY=$(curl -fsS "$BASE_URL/api/stats" 2>&1) \
  && pass "GET /api/stats 200" \
  || fail "GET /api/stats 200" "curl failed: $STATS_BODY"
expect_jq "$STATS_BODY" '.counters.logs_processed > 0' "true" \
  "stats: logs_processed > 0"
expect_jq "$STATS_BODY" '.counters.fields_encrypted > 0' "true" \
  "stats: fields_encrypted > 0"

# ---------------------------------------------------------------------------
# 10. /  (dashboard HTML reachable)
# ---------------------------------------------------------------------------
section "10. / (dashboard HTML)"
DASH_BODY=$(curl -fsS "$BASE_URL/" 2>&1) \
  && pass "GET / 200" \
  || fail "GET / 200" "curl failed: $DASH_BODY"
expect_contains "$DASH_BODY" "Field-Level Log Encryption" \
  "dashboard HTML contains service title"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
total=$((pass_count + fail_count))
printf '\n%s=== E2E summary: %d passed, %d failed (of %d) ===%s\n' \
  "$YELLOW" "$pass_count" "$fail_count" "$total" "$RESET"

if [[ "$fail_count" -gt 0 ]]; then
  exit 1
fi
exit 0
