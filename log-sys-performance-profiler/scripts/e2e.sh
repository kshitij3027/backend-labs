#!/usr/bin/env bash
set -euo pipefail
BASE_URL="${BASE_URL:-http://app:8000}"

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

# 1) Baseline run
log "POST baseline run (2000 records)…"
BASELINE_ID=$(curl -fsS -X POST "${BASE_URL}/api/runs" \
  -H "Content-Type: application/json" \
  -d '{"log_count":2000,"concurrency":4,"seed":42}' | jq -r '.run_id')
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

# 2) Recommendations: pick the top one (or default to batch_writer)
log "fetch recommendations…"
REC_OPT=$(curl -fsS "${BASE_URL}/api/runs/${BASELINE_ID}/recommendations" \
  | jq -r 'if length > 0 then .[0].optimization_name else "batch_writer" end')
[ "$REC_OPT" = "null" ] && REC_OPT="batch_writer"
log "chosen_optimization=${REC_OPT}"

# 3) Compare run (baseline_id + optimized_id created inside)
log "POST compare run (same seed) with ${REC_OPT}…"
curl -fsS -X POST "${BASE_URL}/api/runs" \
  -H "Content-Type: application/json" \
  -d "$(jq -n --arg opt "${REC_OPT}" '{log_count:2000,concurrency:4,seed:42,optimization_name:$opt}')" \
  >/dev/null

log "wait for the compare-mode runs to land in the store…"
for i in $(seq 1 90); do
  COUNT=$(curl -fsS "${BASE_URL}/api/runs?limit=20" | jq 'length')
  if [ "${COUNT}" -ge 3 ]; then break; fi
  sleep 1
  if [ "$i" -eq 90 ]; then fail "compare runs did not complete within 90s"; fi
done

# 4) Find the latest baseline + optimized produced by the harness
RECENT_BASELINE=$(curl -fsS "${BASE_URL}/api/runs?limit=20" \
  | jq -r --arg opt "${REC_OPT}" 'map(select(.baseline_or_optimized=="baseline" and (.optimization_name == null))) | sort_by(.started_at) | .[-1].run_id')
RECENT_OPT=$(curl -fsS "${BASE_URL}/api/runs?limit=20" \
  | jq -r --arg opt "${REC_OPT}" 'map(select(.baseline_or_optimized=="optimized" and .optimization_name == $opt)) | sort_by(.started_at) | .[-1].run_id')
[ -n "${RECENT_BASELINE}" ] && [ "${RECENT_BASELINE}" != "null" ] || fail "no recent baseline found"
[ -n "${RECENT_OPT}" ] && [ "${RECENT_OPT}" != "null" ] || fail "no recent optimized found"
log "compare a=${RECENT_BASELINE} b=${RECENT_OPT}"

# 5) /api/compare and assert verdict not regressed
DIFF=$(curl -fsS "${BASE_URL}/api/compare?a=${RECENT_BASELINE}&b=${RECENT_OPT}")
VERDICT=$(echo "${DIFF}" | jq -r '.diff.verdict')
T_DELTA=$(echo "${DIFF}" | jq -r '.diff.throughput_delta_pct')
P95_DELTA=$(echo "${DIFF}" | jq -r '.diff.p95_delta_pct')

log "verdict=${VERDICT} throughput_delta_pct=${T_DELTA} p95_delta_pct=${P95_DELTA}"

if [ "${VERDICT}" = "regressed" ]; then
  fail "optimization regressed; verdict=${VERDICT}"
fi

log "E2E OK"
