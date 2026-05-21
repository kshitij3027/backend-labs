#!/usr/bin/env bash
# End-to-end smoke runner for the Intelligent Log Redaction Engine.
#
# Runs against an ALREADY-RUNNING stack (`make up` brought it up). Walks
# every public HTTP endpoint with the real fixtures under
# ``tests/fixtures/``, asserts the expected response shape, and prints a
# PASS/FAIL summary. The script does NOT start or stop docker — that is
# the Makefile's job (`make e2e` invokes this from inside the ``tester``
# compose service so the request travels via the compose network at
# ``http://app:8000``).
#
# Designed to be loud on failure: every check is one helper call so a
# regression in any single endpoint surfaces in the summary line.
set -euo pipefail

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
#
# BASE_URL defaults to the compose-network DNS name (``app``) on port 8000
# so the script works without modification inside the ``tester`` service.
# Override via ``BASE_URL=http://localhost:8000 bash scripts/e2e.sh`` to
# run it from the host against a published port.
BASE_URL="${BASE_URL:-http://app:8000}"

# Resolve the project root so fixture paths work regardless of where the
# script was invoked from.
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

PII_FIXTURE="tests/fixtures/log_pii.json"
PHI_FIXTURE="tests/fixtures/log_phi.json"
PCI_FIXTURE="tests/fixtures/log_pci.json"
MIXED_FIXTURE="tests/fixtures/log_mixed_batch.json"
HEALTHCARE_PRESET="config/presets/healthcare.json"

# ANSI colours — disabled when stdout is not a TTY so CI logs stay clean.
if [[ -t 1 ]]; then
  GREEN=$'\033[32m'
  RED=$'\033[31m'
  BOLD=$'\033[1m'
  RESET=$'\033[0m'
else
  GREEN=""
  RED=""
  BOLD=""
  RESET=""
fi

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# pass <name>: emit a green PASS line. No counter — `set -e` ensures the
# whole script fails on the first ``fail`` call so a separate tally is
# unnecessary.
pass() {
  printf '%s  PASS:%s %s\n' "$GREEN" "$RESET" "$1"
}

# fail <name>: emit a red FAIL line and exit with code 1.
fail() {
  printf '%s  FAIL:%s %s\n' "$RED" "$RESET" "$1" >&2
  exit 1
}

# step <name>: emit a bold section header so the operator can match
# console output back to the numbered list in the script comments.
step() {
  printf '\n%s--- %s ---%s\n' "$BOLD" "$1" "$RESET"
}

# Sanity: required tools on PATH. ``jq`` is installed into the tester
# image via Dockerfile.test; ``curl`` is already there for the
# healthcheck shape.
for tool in curl jq; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    printf '%sFATAL:%s required tool not on PATH: %s\n' "$RED" "$RESET" "$tool" >&2
    exit 2
  fi
done

printf '%s=== Log Redaction Engine — E2E ===%s\n' "$BOLD" "$RESET"
printf '  base url: %s\n' "$BASE_URL"

# ---------------------------------------------------------------------------
# 1. GET /api/health  (liveness probe)
# ---------------------------------------------------------------------------
step "1. GET /api/health"
HEALTH_BODY=$(curl -fsS "$BASE_URL/api/health") || fail "GET /api/health 200 (curl failed)"
echo "  $HEALTH_BODY"
if printf '%s' "$HEALTH_BODY" | grep -qF '"healthy"'; then
  pass "GET /api/health returns healthy"
else
  fail "GET /api/health body did not contain \"healthy\""
fi

# ---------------------------------------------------------------------------
# 2. GET /api/config  (current active config)
# ---------------------------------------------------------------------------
step "2. GET /api/config"
CONFIG_BODY=$(curl -fsS "$BASE_URL/api/config") || fail "GET /api/config 200 (curl failed)"
# Truncate the echo so a fat config doesn't drown the log.
printf '  %s...\n' "$(printf '%s' "$CONFIG_BODY" | head -c 120)"
if printf '%s' "$CONFIG_BODY" | grep -qF '"version"'; then
  pass "GET /api/config contains \"version\""
else
  fail "GET /api/config body did not contain \"version\""
fi

# ---------------------------------------------------------------------------
# 3. POST /api/redact (PII fixture)
# ---------------------------------------------------------------------------
step "3. POST /api/redact (PII fixture)"
# Wrap the single-entry fixture in the {"log_entries": [...]} envelope
# the API expects. ``jq -c '{log_entries:[.]}'`` reads the fixture from
# stdin and emits a compact JSON object on stdout.
PII_PAYLOAD=$(cat "$PII_FIXTURE" | jq -c '{log_entries:[.]}')
PII_RESP=$(curl -fsS -X POST "$BASE_URL/api/redact" \
  -H "Content-Type: application/json" \
  -d "$PII_PAYLOAD") || fail "POST /api/redact (PII) 200 (curl failed)"
printf '  %s...\n' "$(printf '%s' "$PII_RESP" | head -c 200)"
if printf '%s' "$PII_RESP" | grep -qF '"processed_entries"'; then
  pass "PII response contains \"processed_entries\""
else
  fail "PII response missing \"processed_entries\""
fi

# ---------------------------------------------------------------------------
# 4. POST /api/redact (PHI fixture)
# ---------------------------------------------------------------------------
#
# The default ``general`` preset binds ``mrn`` and ``ssn`` to the ``mask``
# strategy, which produces an all-asterisk replacement of the matched
# value (length-preserving). So the fixture's "MRN-123456" should appear
# masked as "**********" in the redacted message.
step "4. POST /api/redact (PHI fixture)"
PHI_PAYLOAD=$(cat "$PHI_FIXTURE" | jq -c '{log_entries:[.]}')
PHI_RESP=$(curl -fsS -X POST "$BASE_URL/api/redact" \
  -H "Content-Type: application/json" \
  -d "$PHI_PAYLOAD") || fail "POST /api/redact (PHI) 200 (curl failed)"
printf '  %s...\n' "$(printf '%s' "$PHI_RESP" | head -c 200)"
# The masked MRN under default preset is a run of asterisks the length
# of "MRN-123456" (10 chars). Just assert a run of >= 6 asterisks
# appears in the response message — that's enough to confirm a mask
# strategy fired without over-specifying length.
if printf '%s' "$PHI_RESP" | grep -qE '\*{6,}'; then
  pass "PHI response contains masked value (run of asterisks)"
else
  fail "PHI response did not contain a masked value"
fi
# And the raw PHI must NOT survive.
if printf '%s' "$PHI_RESP" | grep -qF 'MRN-123456'; then
  fail "PHI response leaked raw MRN-123456"
else
  pass "PHI response does not echo raw MRN"
fi

# ---------------------------------------------------------------------------
# 5. POST /api/redact (PCI fixture)
# ---------------------------------------------------------------------------
#
# The default preset binds ``credit_card`` to ``mask``, so the
# "4111-1111-1111-1111" PAN becomes a run of 19 asterisks.
step "5. POST /api/redact (PCI fixture)"
PCI_PAYLOAD=$(cat "$PCI_FIXTURE" | jq -c '{log_entries:[.]}')
PCI_RESP=$(curl -fsS -X POST "$BASE_URL/api/redact" \
  -H "Content-Type: application/json" \
  -d "$PCI_PAYLOAD") || fail "POST /api/redact (PCI) 200 (curl failed)"
printf '  %s...\n' "$(printf '%s' "$PCI_RESP" | head -c 200)"
if printf '%s' "$PCI_RESP" | grep -qE '\*{13,}'; then
  pass "PCI response contains masked credit-card value"
else
  fail "PCI response did not contain a masked credit card"
fi
if printf '%s' "$PCI_RESP" | grep -qF '4111-1111-1111-1111'; then
  fail "PCI response leaked raw card number"
else
  pass "PCI response does not echo raw card"
fi

# ---------------------------------------------------------------------------
# 6. POST /api/redact (mixed batch fixture, 10 entries)
# ---------------------------------------------------------------------------
#
# The mixed fixture is already a JSON array, so wrap directly without
# re-array-ing it. We assert ``processed_entries`` length == 10 via jq.
step "6. POST /api/redact (mixed batch fixture)"
MIXED_PAYLOAD=$(jq -c '{log_entries: .}' "$MIXED_FIXTURE")
MIXED_RESP=$(curl -fsS -X POST "$BASE_URL/api/redact" \
  -H "Content-Type: application/json" \
  -d "$MIXED_PAYLOAD") || fail "POST /api/redact (mixed) 200 (curl failed)"
MIXED_LEN=$(printf '%s' "$MIXED_RESP" | jq '.processed_entries | length')
echo "  processed_entries length: $MIXED_LEN"
if [[ "$MIXED_LEN" == "10" ]]; then
  pass "mixed batch returns 10 processed_entries"
else
  fail "mixed batch returned $MIXED_LEN processed_entries (expected 10)"
fi

# ---------------------------------------------------------------------------
# 7. POST /v1/detect (PHI fixture) — dry-run, must NEVER echo plaintext
# ---------------------------------------------------------------------------
#
# Two invariants:
#   a) the response shape has ``detections`` and not ``processed_entries``
#      (dry-run never applies redaction).
#   b) no ``value_preview`` field contains the raw SSN ``123-45-6789`` —
#      the masking rule (``_value_preview`` in src/api/routes.py) is
#      load-bearing for the project's "no plaintext" invariant.
step "7. POST /v1/detect (PHI fixture)"
DETECT_RESP=$(curl -fsS -X POST "$BASE_URL/v1/detect" \
  -H "Content-Type: application/json" \
  -d "$PHI_PAYLOAD") || fail "POST /v1/detect 200 (curl failed)"
printf '  %s...\n' "$(printf '%s' "$DETECT_RESP" | head -c 200)"
if printf '%s' "$DETECT_RESP" | grep -qF '"detections"'; then
  pass "detect response has \"detections\" key"
else
  fail "detect response missing \"detections\""
fi
if printf '%s' "$DETECT_RESP" | grep -qF '"processed_entries"'; then
  fail "detect response unexpectedly contains \"processed_entries\""
else
  pass "detect response does NOT contain \"processed_entries\""
fi
# Plaintext SSN must be absent from any value_preview field. ``jq -r``
# pulls every value_preview onto its own line so a substring search is
# scoped to just that column of the response.
PREVIEWS=$(printf '%s' "$DETECT_RESP" | jq -r '.detections[].value_preview')
if printf '%s' "$PREVIEWS" | grep -qF '123-45-6789'; then
  fail "detect response leaked raw SSN in a value_preview"
else
  pass "value_preview fields do not echo raw SSN"
fi

# ---------------------------------------------------------------------------
# 8. POST /api/config (healthcare preset) → hot-reload + verify partial MRN
# ---------------------------------------------------------------------------
#
# Switch to the healthcare preset (mrn → partial, ssn → partial). Then
# re-redact the PHI fixture and assert the message now contains
# ``MRN-***`` — the partial-strategy output for ``MRN-123456`` is
# ``MRN-***456``, so ``MRN-***`` is a safe substring assertion.
step "8. POST /api/config (healthcare preset) + re-redact"
HEALTHCARE_BODY=$(cat "$HEALTHCARE_PRESET")
RELOAD_RESP=$(curl -fsS -X POST "$BASE_URL/api/config" \
  -H "Content-Type: application/json" \
  -d "$HEALTHCARE_BODY") || fail "POST /api/config (healthcare) 200 (curl failed)"
printf '  %s...\n' "$(printf '%s' "$RELOAD_RESP" | head -c 120)"
pass "POST /api/config (healthcare) returned 200"

# Re-issue the PHI redact under the new preset.
PHI_RESP2=$(curl -fsS -X POST "$BASE_URL/api/redact" \
  -H "Content-Type: application/json" \
  -d "$PHI_PAYLOAD") || fail "POST /api/redact (PHI re-run) 200 (curl failed)"
printf '  %s...\n' "$(printf '%s' "$PHI_RESP2" | head -c 200)"
if printf '%s' "$PHI_RESP2" | grep -qF 'MRN-***'; then
  pass "healthcare preset applied: message contains \"MRN-***\""
else
  fail "healthcare preset did not produce \"MRN-***\" partial MRN"
fi

# ---------------------------------------------------------------------------
# 9. GET /api/stats  (counters incremented by the prior calls)
# ---------------------------------------------------------------------------
#
# Five single-entry redacts (PII / PHI / PCI / PHI re-run = 4 entries) +
# one 10-entry batch + one detect (detect MAY contribute to
# logs_processed depending on counter wiring). We assert the SAFE lower
# bound: at least 14 logs processed. Note: we count 1+1+1+10+1 = 14 from
# guaranteed counted calls (4 redacts + 10 mixed batch). Detect may not
# contribute, hence ``>= 14`` rather than ``>= 15``.
step "9. GET /api/stats"
STATS_RESP=$(curl -fsS "$BASE_URL/api/stats") || fail "GET /api/stats 200 (curl failed)"
echo "  $STATS_RESP"
LOGS_PROCESSED=$(printf '%s' "$STATS_RESP" | jq '.logs_processed')
if [[ "$LOGS_PROCESSED" -ge 14 ]]; then
  pass "stats.logs_processed = $LOGS_PROCESSED (>= 14)"
else
  fail "stats.logs_processed = $LOGS_PROCESSED (expected >= 14)"
fi

# ---------------------------------------------------------------------------
# 10. GET /api/compliance/HIPAA  (per-regime report)
# ---------------------------------------------------------------------------
step "10. GET /api/compliance/HIPAA"
COMP_RESP=$(curl -fsS "$BASE_URL/api/compliance/HIPAA") || fail "GET /api/compliance/HIPAA 200 (curl failed)"
printf '  %s...\n' "$(printf '%s' "$COMP_RESP" | head -c 200)"
if printf '%s' "$COMP_RESP" | jq -e 'has("breakdown")' >/dev/null; then
  pass "compliance response has \"breakdown\" key"
else
  fail "compliance response missing \"breakdown\" key"
fi

# ---------------------------------------------------------------------------
# 11. GET /metrics  (Prometheus exposition)
# ---------------------------------------------------------------------------
step "11. GET /metrics"
METRICS_BODY=$(curl -fsS "$BASE_URL/metrics") || fail "GET /metrics 200 (curl failed)"
# Print just the first few lines so the log doesn't drown in metrics.
printf '  (first lines)\n%s\n  ...\n' "$(printf '%s' "$METRICS_BODY" | head -n 5)"
if printf '%s' "$METRICS_BODY" | grep -qF 'redactions_total'; then
  pass "/metrics contains \"redactions_total\""
else
  fail "/metrics body did not contain \"redactions_total\""
fi

# ---------------------------------------------------------------------------
# 12. GET /  (dashboard HTML reachable)
# ---------------------------------------------------------------------------
step "12. GET / (dashboard HTML)"
DASH_BODY=$(curl -fsS "$BASE_URL/") || fail "GET / 200 (curl failed)"
# Don't echo the whole HTML; just confirm the title is in there.
if printf '%s' "$DASH_BODY" | grep -qF 'Log Redaction Engine'; then
  pass "dashboard HTML contains service title"
else
  fail "dashboard HTML missing \"Log Redaction Engine\""
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
printf '\n%s=== All E2E steps passed. ===%s\n' "$GREEN" "$RESET"
exit 0
