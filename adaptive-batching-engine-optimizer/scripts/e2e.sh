#!/usr/bin/env bash
# End-to-end probe of the LIVE adaptive batching engine over the Docker network.
#
# Driven by `make e2e` (docker compose --profile test run --rm tester bash
# scripts/e2e.sh) against the app service at http://app:8000. Walks the system
# through its four traffic patterns and the success criteria:
#   1. health           2. steady adaptation   3. no constraint violation
#   4. burst handling   5. constraint->EMERGENCY   6. recovery->optimizing
# Every assertion has a bounded poll loop so the script can never hang forever.
set -euo pipefail

BASE_URL="${BASE_URL:-http://app:8000}"

log() { printf "[e2e] %s\n" "$*" >&2; }
fail() { log "FAIL: $*"; exit 1; }

# get FIELD -> echo a jq-extracted field from a GET on $1 (path), jq filter $2.
get_field() { curl -fsS "${BASE_URL}$1" | jq -r "$2"; }

# ---------------------------------------------------------------------------
# 1) Health: wait for the app's liveness probe.
# ---------------------------------------------------------------------------
log "phase 1: wait for /health…"
HEALTHY=""
for i in $(seq 1 40); do
  if [ "$(curl -fsS "${BASE_URL}/health" 2>/dev/null | jq -r '.status' 2>/dev/null)" = "healthy" ]; then
    HEALTHY="yes"
    log "  healthy after ${i}s"
    break
  fi
  sleep 1
done
[ -n "${HEALTHY}" ] || fail "health never became OK within 40s"

# ---------------------------------------------------------------------------
# 2) Steady adaptation: push a steady rate and watch the batch move off 100.
# ---------------------------------------------------------------------------
log "phase 2: steady load 300 msg/s, expect adaptation off batch=100…"
curl -fsS -X POST "${BASE_URL}/api/load" \
  -H "Content-Type: application/json" \
  -d '{"messages_per_second":300,"burst_probability":0.0}' >/dev/null

ADAPTED=""
for i in $(seq 1 20); do
  S=$(curl -fsS "${BASE_URL}/api/optimizer")
  STATE=$(echo "${S}" | jq -r '.state')
  BATCH=$(echo "${S}" | jq -r '.batch_size')
  log "  t=${i}s state=${STATE} batch=${BATCH}"
  if { [ "${STATE}" = "optimizing" ] || [ "${STATE}" = "stable" ]; } && [ "${BATCH}" -ne 100 ]; then
    ADAPTED="yes"
    log "  adapted: state=${STATE} batch=${BATCH}"
    break
  fi
  sleep 1
done
[ -n "${ADAPTED}" ] || fail "optimizer did not adapt (state/optimizing-or-stable + batch!=100) within 20s"

# ---------------------------------------------------------------------------
# 3) No constraint violation in steady state.
# ---------------------------------------------------------------------------
log "phase 3: assert no constraint violation in steady state…"
M=$(curl -fsS "${BASE_URL}/api/metrics")
CONSTRAINT=$(echo "${M}" | jq -r '.status.constraint_active')
CPU=$(echo "${M}" | jq -r '.series.cpu_percent[-1] // 0')
MEM=$(echo "${M}" | jq -r '.series.memory_percent[-1] // 0')
log "  constraint_active=${CONSTRAINT} cpu=${CPU} mem=${MEM}"
[ "${CONSTRAINT}" = "false" ] || fail "constraint active during steady state"
# Latest cpu/memory must be at or below the 90% safety threshold.
awk -v c="${CPU}" 'BEGIN { exit !(c <= 90) }' || fail "steady-state cpu ${CPU} > 90"
awk -v m="${MEM}" 'BEGIN { exit !(m <= 90) }' || fail "steady-state memory ${MEM} > 90"

# ---------------------------------------------------------------------------
# 4) Burst handling: high burst probability, app must stay healthy + valid.
# ---------------------------------------------------------------------------
log "phase 4: burst traffic 300 msg/s p=0.5, expect no crash…"
curl -fsS -X POST "${BASE_URL}/api/load" \
  -H "Content-Type: application/json" \
  -d '{"messages_per_second":300,"burst_probability":0.5}' >/dev/null
sleep 4
[ "$(get_field /health '.status')" = "healthy" ] || fail "app unhealthy after burst"
M=$(curl -fsS "${BASE_URL}/api/metrics")
BURST_BATCH=$(echo "${M}" | jq -r '.status.batch_size')
BURST_STATE=$(echo "${M}" | jq -r '.status.state')
# A valid metrics payload has a current snapshot and a positive batch size.
[ "$(echo "${M}" | jq -r '.current != null')" = "true" ] || fail "no current snapshot after burst"
[ "${BURST_BATCH}" -ge 50 ] || fail "invalid batch_size ${BURST_BATCH} after burst"
log "  burst handled: state=${BURST_STATE} batch=${BURST_BATCH}"

# ---------------------------------------------------------------------------
# 5) Constraint -> EMERGENCY: force the CPU threshold below the live pressure.
# ---------------------------------------------------------------------------
log "phase 5: tighten cpu_constraint_threshold=5, expect EMERGENCY…"
PRE_BATCH=$(get_field /api/optimizer '.batch_size')
curl -fsS -X POST "${BASE_URL}/api/optimizer/config" \
  -H "Content-Type: application/json" \
  -d '{"cpu_constraint_threshold":5}' >/dev/null
log "  pre_batch=${PRE_BATCH}"

EMERGENCY=""
EMERGENCY_BATCH="${PRE_BATCH}"
for i in $(seq 1 10); do
  S=$(curl -fsS "${BASE_URL}/api/optimizer")
  STATE=$(echo "${S}" | jq -r '.state')
  EMERGENCY_BATCH=$(echo "${S}" | jq -r '.batch_size')
  log "  t=${i}s state=${STATE} batch=${EMERGENCY_BATCH}"
  if [ "${STATE}" = "emergency" ]; then
    EMERGENCY="yes"
    break
  fi
  sleep 1
done
[ -n "${EMERGENCY}" ] || fail "did not reach EMERGENCY within 10s of constraint tightening"
[ "${EMERGENCY_BATCH}" -lt "${PRE_BATCH}" ] || fail "batch did not drop in EMERGENCY (${EMERGENCY_BATCH} >= ${PRE_BATCH})"
log "  emergency reached: batch dropped ${PRE_BATCH} -> ${EMERGENCY_BATCH}"

# ---------------------------------------------------------------------------
# 6) Recovery: relax the threshold; hysteresis clears, loop resumes optimizing.
# ---------------------------------------------------------------------------
log "phase 6: relax cpu_constraint_threshold=90, expect recovery…"
curl -fsS -X POST "${BASE_URL}/api/optimizer/config" \
  -H "Content-Type: application/json" \
  -d '{"cpu_constraint_threshold":90}' >/dev/null

RECOVERED=""
for i in $(seq 1 15); do
  S=$(curl -fsS "${BASE_URL}/api/optimizer")
  STATE=$(echo "${S}" | jq -r '.state')
  BATCH=$(echo "${S}" | jq -r '.batch_size')
  log "  t=${i}s state=${STATE} batch=${BATCH}"
  # Recovery hysteresis clears back into the active loop (optimizing, possibly
  # then settling to stable). Either means EMERGENCY was left behind.
  if [ "${STATE}" = "optimizing" ] || [ "${STATE}" = "stable" ]; then
    RECOVERED="yes"
    log "  recovered to ${STATE}"
    break
  fi
  sleep 1
done
[ -n "${RECOVERED}" ] || fail "did not recover out of EMERGENCY within 15s"

log "E2E OK"
exit 0
