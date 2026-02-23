#!/usr/bin/env bash
set -e

PASS=0
FAIL=0

check() {
    local name="$1"
    shift
    echo -n "  [$name] ... "
    if "$@" > /dev/null 2>&1; then
        echo "PASS"
        PASS=$((PASS + 1))
    else
        echo "FAIL"
        FAIL=$((FAIL + 1))
    fi
}

echo "=== Avro Schema Log System Verification ==="
echo ""

# 1. Project structure check
check "Project Structure" test -f Dockerfile -a -f docker-compose.yml -a -f Makefile -a -d schemas -a -d src -a -d tests -a -d benchmarks

# 2. Schema files exist
check "Schema Files" test -f schemas/log_event_v1.avsc -a -f schemas/log_event_v2.avsc -a -f schemas/log_event_v3.avsc

# 3. Docker build
check "Docker Build" docker compose build

# 4. Unit tests in Docker
check "Unit Tests" docker compose run --rm tests

# 5. Start app, health check, stop
echo -n "  [Health Check] ... "
docker compose up -d app || true
sleep 5
if curl -sf http://localhost:5050/health | grep -q healthy; then
    echo "PASS"
    PASS=$((PASS + 1))
else
    echo "FAIL"
    FAIL=$((FAIL + 1))
fi

# 6. Schema info endpoint
echo -n "  [Schema Info API] ... "
if curl -sf http://localhost:5050/api/schema-info | python3 -c "import sys,json; d=json.load(sys.stdin); assert d['data']['available_schemas']==['v1','v2','v3']"; then
    echo "PASS"
    PASS=$((PASS + 1))
else
    echo "FAIL"
    FAIL=$((FAIL + 1))
fi

# 7. Compatibility matrix (9/9)
echo -n "  [Compatibility 9/9] ... "
if curl -sf http://localhost:5050/api/schema-info | python3 -c "
import sys,json
d=json.load(sys.stdin)
m=d['data']['compatibility_matrix']
total=sum(1 for w in m for r in m[w] if m[w][r])
assert total==9, f'{total}/9'
"; then
    echo "PASS"
    PASS=$((PASS + 1))
else
    echo "FAIL"
    FAIL=$((FAIL + 1))
fi

# 8. Round-trip test
echo -n "  [Round-trip Test] ... "
if curl -sf -X POST http://localhost:5050/api/test-compatibility -H 'Content-Type: application/json' -d '{"schema_version":"v2"}' | python3 -c "import sys,json; d=json.load(sys.stdin); assert d['status']=='success'"; then
    echo "PASS"
    PASS=$((PASS + 1))
else
    echo "FAIL"
    FAIL=$((FAIL + 1))
fi

docker compose down 2>/dev/null || true

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
if [ "$FAIL" -eq 0 ]; then
    exit 0
else
    exit 1
fi
