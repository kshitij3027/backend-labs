#!/usr/bin/env bash
# End-to-end probe: track → request → poll → verify chain.
# Expects to run inside the `tester` container with BASE_URL set.
set -euo pipefail

BASE_URL="${BASE_URL:-http://app:8000}"

say() { echo "─── $*"; }

say "wait for app health"
for i in $(seq 1 30); do
  if curl -fsS "$BASE_URL/health" > /dev/null 2>&1; then
    echo "[$i] OK"
    break
  fi
  echo "[$i] not ready yet"
  sleep 1
done

USER="e2e-user-$$"

say "register 4 mappings for $USER"
for body in \
  "{\"user_id\":\"$USER\",\"data_type\":\"system_logs\",\"storage_location\":\"l1\",\"metadata\":{\"user_id\":\"$USER\",\"ip\":\"10.0.0.1\"}}" \
  "{\"user_id\":\"$USER\",\"data_type\":\"analytics_events\",\"storage_location\":\"l2\",\"metadata\":{\"user_id\":\"$USER\",\"email\":\"a@b.c\"}}" \
  "{\"user_id\":\"$USER\",\"data_type\":\"performance_metrics\",\"storage_location\":\"l3\"}" \
  "{\"user_id\":\"$USER\",\"data_type\":\"personal_profile\",\"storage_location\":\"l4\",\"metadata\":{\"user_id\":\"$USER\"}}"; do
  curl -fsS -X POST "$BASE_URL/api/user-data-tracking" \
    -H 'Content-Type: application/json' -d "$body" > /dev/null
done

say "submit ANONYMIZE erasure"
RID=$(curl -fsS -X POST "$BASE_URL/api/erasure-requests" \
  -H 'Content-Type: application/json' \
  -d "{\"user_id\":\"$USER\",\"request_type\":\"ANONYMIZE\"}" | python -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo "request_id=$RID"

say "poll until terminal (max 15s)"
for i in $(seq 1 30); do
  STATE=$(curl -fsS "$BASE_URL/api/erasure-requests/$RID" | python -c "import sys,json; print(json.load(sys.stdin)['state'])")
  echo "[$i] state=$STATE"
  if [ "$STATE" = "COMPLETED" ]; then break; fi
  if [ "$STATE" = "FAILED" ]; then
    curl -fsS "$BASE_URL/api/erasure-requests/$RID" | python -m json.tool
    echo "E2E FAIL: erasure ended in FAILED"
    exit 1
  fi
  sleep 0.5
done
if [ "$STATE" != "COMPLETED" ]; then
  echo "E2E FAIL: never reached COMPLETED (last state=$STATE)"
  exit 1
fi

say "verify final state + audit timeline"
EVENTS=$(curl -fsS "$BASE_URL/api/erasure-requests/$RID" | python -c "
import sys, json
data = json.load(sys.stdin)
types = sorted({e['event_type'] for e in data['audit_entries']})
print(','.join(types))
")
echo "audit event types: $EVENTS"
for required in REQUEST_CREATED STATE_TRANSITION DISCOVERY_COMPLETE LOCATION_ERASED VERIFICATION_OK; do
  if ! echo "$EVENTS" | grep -q "$required"; then
    echo "E2E FAIL: missing audit event $required"
    exit 1
  fi
done

say "verify hash chain integrity via verifier"
python -c "
import asyncio, os
from src.persistence.db import make_engine, make_session_factory
from src.audit.verifier import verify_chain

async def main():
    eng = make_engine(os.environ['DATABASE_URL'])
    sf = make_session_factory(eng)
    async with sf() as s:
        ok, bad = await verify_chain(s)
        if not ok:
            print(f'CHAIN BROKEN at sequence={bad}')
            raise SystemExit(2)
        print('chain ok')
    await eng.dispose()

asyncio.run(main())
"

say "verify per-location outcomes"
SURVIVING=$(curl -fsS "$BASE_URL/api/data-locations/$USER" | python -c "
import sys, json
rows = json.load(sys.stdin)
print(','.join(sorted(r['data_type'] for r in rows)))
")
echo "surviving data_types for $USER: $SURVIVING"
# system_logs, analytics_events, performance_metrics are anonymisable → survive
# personal_profile is NOT in allowlist → DELETE fallback → gone
EXPECTED="analytics_events,performance_metrics,system_logs"
if [ "$SURVIVING" != "$EXPECTED" ]; then
  echo "E2E FAIL: surviving types mismatch — expected '$EXPECTED', got '$SURVIVING'"
  exit 1
fi

echo
echo "════════════════════════════════════════"
echo "  E2E PASS — request $RID, chain ok"
echo "════════════════════════════════════════"
