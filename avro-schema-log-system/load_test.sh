#!/usr/bin/env bash
set -e

BASE_URL="${1:-http://localhost:5050}"
REQUESTS=10
PASS=0
FAIL=0
TOTAL_TIME=0

echo "=== Load Test: $REQUESTS sequential requests to $BASE_URL ==="
echo ""

for i in $(seq 1 $REQUESTS); do
    START=$(python3 -c "import time; print(time.time())")
    STATUS=$(curl -sf -o /dev/null -w "%{http_code}" "$BASE_URL/api/schema-info")
    END=$(python3 -c "import time; print(time.time())")

    ELAPSED=$(python3 -c "print(f'{($END - $START) * 1000:.1f}')")

    if [ "$STATUS" = "200" ]; then
        echo "  Request $i: ${STATUS} (${ELAPSED}ms)"
        PASS=$((PASS + 1))
    else
        echo "  Request $i: ${STATUS} FAIL (${ELAPSED}ms)"
        FAIL=$((FAIL + 1))
    fi
done

echo ""
echo "=== Results: $PASS/$REQUESTS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ] && echo "LOAD TEST PASSED" && exit 0 || echo "LOAD TEST FAILED" && exit 1
