#!/usr/bin/env bash
# End-to-end probe: seed -> generate per framework -> verify -> download.
# Expects to run inside the ``tester`` container with BASE_URL set.
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
FRAMEWORKS=(SOX HIPAA PCI_DSS GDPR FINHEALTH)
POLL_TIMEOUT="${POLL_TIMEOUT:-60}"

echo "==> e2e: waiting for $BASE_URL/health"
for i in $(seq 1 30); do
  if curl -fsS "$BASE_URL/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fsS "$BASE_URL/health" >/dev/null || { echo "/health never came up"; exit 1; }

echo "==> e2e: seeding 2000 log events"
python scripts/seed_logs.py --count 2000

period_end="$(python -c 'from datetime import datetime, timezone; print(datetime.now(timezone.utc).isoformat())')"
period_start="$(python -c 'from datetime import datetime, timezone, timedelta; print((datetime.now(timezone.utc) - timedelta(days=30)).isoformat())')"

for framework in "${FRAMEWORKS[@]}"; do
  echo "==> e2e: generating $framework / JSON"
  body=$(jq -c -n --arg fw "$framework" --arg fmt JSON --arg ps "$period_start" --arg pe "$period_end" \
    '{framework: $fw, export_format: $fmt, period_start: $ps, period_end: $pe}')
  resp=$(curl -fsS -X POST "$BASE_URL/reports/generate" -H "Content-Type: application/json" -d "$body")
  report_id=$(echo "$resp" | jq -r .report_id)
  echo "    report_id=$report_id"

  echo "    polling for COMPLETED..."
  for i in $(seq 1 "$POLL_TIMEOUT"); do
    state=$(curl -fsS "$BASE_URL/reports/$report_id" | jq -r .state)
    if [ "$state" = "COMPLETED" ]; then
      break
    fi
    if [ "$state" = "FAILED" ]; then
      echo "FAIL: $framework report $report_id transitioned to FAILED"
      curl -fsS "$BASE_URL/reports/$report_id"
      exit 1
    fi
    sleep 1
  done
  if [ "$state" != "COMPLETED" ]; then
    echo "FAIL: $framework report $report_id never reached COMPLETED in ${POLL_TIMEOUT}s (last state=$state)"
    exit 1
  fi

  echo "    verifying signature..."
  verify=$(curl -fsS "$BASE_URL/reports/$report_id/verify")
  verified=$(echo "$verify" | jq -r .verified)
  if [ "$verified" != "true" ]; then
    echo "FAIL: $framework verify returned verified=$verified"
    echo "$verify"
    exit 1
  fi
  if [ "$framework" = "FINHEALTH" ]; then
    secondary=$(echo "$verify" | jq -r '.secondary_verified // false')
    if [ "$secondary" != "true" ]; then
      echo "FAIL: FinHealth secondary signature not verified ($secondary)"
      echo "$verify"
      exit 1
    fi
  fi

  echo "    downloading..."
  tmp=$(mktemp /tmp/e2e-$framework.XXXX.json)
  curl -fsS "$BASE_URL/reports/$report_id/download" -o "$tmp"
  size=$(wc -c < "$tmp")
  if [ "$size" -lt 100 ]; then
    echo "FAIL: $framework download empty ($size bytes)"
    exit 1
  fi
  echo "    ok ($size bytes)"
  rm -f "$tmp"
done

echo "==> E2E OK"
