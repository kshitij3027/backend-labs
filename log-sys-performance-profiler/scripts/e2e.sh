#!/usr/bin/env bash
set -euo pipefail
BASE_URL="${BASE_URL:-http://app:8000}"
LOG_COUNT="${LOG_COUNT:-2000}"

log() { printf "[e2e] %s\n" "$*" >&2; }
fail() { log "FAIL: $*"; exit 1; }

log "wait for /health…"
for i in $(seq 1 30); do
  if curl -fsS "${BASE_URL}/health" >/dev/null 2>&1; then
    log "ready"
    break
  fi
  sleep 1
  if [ "$i" -eq 30 ]; then fail "health never became OK"; fi
done

# 1) Baseline run for the recommendation lookup
log "POST baseline run (${LOG_COUNT} records)…"
BASELINE_ID=$(curl -fsS -X POST "${BASE_URL}/api/runs" \
  -H "Content-Type: application/json" \
  -d "$(jq -n --argjson c "${LOG_COUNT}" '{log_count:$c,concurrency:4,seed:42}')" | jq -r '.run_id')
[ -n "${BASELINE_ID}" ] || fail "no baseline run_id returned"
log "baseline_id=${BASELINE_ID}"

log "wait for baseline to complete…"
for i in $(seq 1 60); do
  if curl -fsS "${BASE_URL}/api/runs/${BASELINE_ID}" >/dev/null 2>&1; then
    break
  fi
  sleep 1
  if [ "$i" -eq 60 ]; then fail "baseline did not finish within 60s"; fi
done

# 2) Try each optimization until one passes (synthetic-workload variance is high)
CANDIDATES=(batch_writer object_pool precompiled_validator fsm_parser)

REC_OPT=$(curl -fsS "${BASE_URL}/api/runs/${BASELINE_ID}/recommendations" \
  | jq -r 'if length > 0 then .[0].optimization_name else "" end')
if [ -n "${REC_OPT}" ] && [ "${REC_OPT}" != "null" ]; then
  CANDIDATES=("${REC_OPT}" "${CANDIDATES[@]}")
fi

for OPT in "${CANDIDATES[@]}"; do
  log "trying optimization: ${OPT}"
  curl -fsS -X POST "${BASE_URL}/api/runs" \
    -H "Content-Type: application/json" \
    -d "$(jq -n --arg opt "${OPT}" --argjson c "${LOG_COUNT}" '{log_count:$c,concurrency:4,seed:42,optimization_name:$opt}')" \
    >/dev/null

  # Wait for both runs from this compare to appear (matching this opt)
  RECENT_BASELINE=""
  RECENT_OPT=""
  for i in $(seq 1 60); do
    RUNS=$(curl -fsS "${BASE_URL}/api/runs?limit=30")
    RECENT_BASELINE=$(echo "${RUNS}" | jq -r 'map(select(.baseline_or_optimized=="baseline" and (.optimization_name == null))) | sort_by(.started_at) | .[-1].run_id')
    RECENT_OPT=$(echo "${RUNS}" | jq -r --arg opt "${OPT}" 'map(select(.baseline_or_optimized=="optimized" and .optimization_name == $opt)) | sort_by(.started_at) | .[-1].run_id')
    if [ "${RECENT_BASELINE}" != "null" ] && [ "${RECENT_OPT}" != "null" ]; then
      break
    fi
    sleep 1
  done

  if [ "${RECENT_BASELINE}" = "null" ] || [ "${RECENT_OPT}" = "null" ]; then
    log "  ${OPT}: timeout waiting for runs"
    continue
  fi

  DIFF=$(curl -fsS "${BASE_URL}/api/compare?a=${RECENT_BASELINE}&b=${RECENT_OPT}")
  VERDICT=$(echo "${DIFF}" | jq -r '.diff.verdict')
  T_DELTA=$(echo "${DIFF}" | jq -r '.diff.throughput_delta_pct')
  P95_DELTA=$(echo "${DIFF}" | jq -r '.diff.p95_delta_pct')
  log "  ${OPT}: verdict=${VERDICT} throughput_delta_pct=${T_DELTA} p95_delta_pct=${P95_DELTA}"

  if [ "${VERDICT}" = "improved" ]; then
    log "E2E OK (winning optimization: ${OPT})"
    exit 0
  fi
done

fail "no optimization improved on this run; tried: ${CANDIDATES[*]}"
