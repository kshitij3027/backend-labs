#!/bin/bash
# Broker outage resilience test
# Run from the project root directory on the host

set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

echo "============================================================"
echo "Broker Outage Resilience Test"
echo "============================================================"

# Clean start
docker compose down -v 2>/dev/null || true
docker compose build

echo "Starting services..."
docker compose up -d rabbitmq app
sleep 20  # Wait for RabbitMQ health + app startup

# Verify service is healthy
echo "Checking service health..."
HEALTH=$(curl -s http://localhost:8080/health)
echo "Health: $HEALTH"

# Send initial logs
echo "Sending initial batch of logs..."
for i in $(seq 1 5); do
    curl -s -X POST http://localhost:8080/logs \
        -H 'Content-Type: application/json' \
        -d "[{\"level\":\"info\",\"message\":\"pre-outage $i\",\"source\":\"outage-test\"}]" > /dev/null
done
sleep 3

# Get pre-outage metrics
PRE_METRICS=$(curl -s http://localhost:8080/metrics)
echo "Pre-outage metrics: $PRE_METRICS"
PRE_PUBLISHED=$(echo "$PRE_METRICS" | python3 -c "import sys,json; print(json.load(sys.stdin).get('messages_published',0))")
echo "Pre-outage published: $PRE_PUBLISHED"

# Pause RabbitMQ to simulate outage
echo ""
echo ">>> PAUSING RabbitMQ (simulating broker outage)..."
docker compose pause rabbitmq
sleep 5

# Send logs during outage (should be accepted -> fallback)
echo "Sending logs during outage..."
OUTAGE_LOGS=0
for i in $(seq 1 10); do
    RESP=$(curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:8080/logs \
        -H 'Content-Type: application/json' \
        -d "[{\"level\":\"warn\",\"message\":\"during-outage $i\",\"source\":\"outage-test\"}]")
    if [ "$RESP" = "202" ]; then
        OUTAGE_LOGS=$((OUTAGE_LOGS + 1))
    else
        echo "WARN: Got status $RESP during outage"
    fi
done
echo "Logs accepted during outage: $OUTAGE_LOGS"

# Check health during outage (may show degraded)
sleep 5
OUTAGE_HEALTH=$(curl -s http://localhost:8080/health)
echo "Health during outage: $OUTAGE_HEALTH"

# Check for fallback writes
OUTAGE_METRICS=$(curl -s http://localhost:8080/metrics)
FALLBACK_WRITES=$(echo "$OUTAGE_METRICS" | python3 -c "import sys,json; print(json.load(sys.stdin).get('fallback_writes',0))")
echo "Fallback writes during outage: $FALLBACK_WRITES"

# Unpause RabbitMQ
echo ""
echo ">>> UNPAUSING RabbitMQ (recovering from outage)..."
docker compose unpause rabbitmq
echo "Waiting for recovery and fallback drain..."
sleep 15

# Check post-recovery metrics
POST_METRICS=$(curl -s http://localhost:8080/metrics)
echo "Post-recovery metrics: $POST_METRICS"
POST_PUBLISHED=$(echo "$POST_METRICS" | python3 -c "import sys,json; print(json.load(sys.stdin).get('messages_published',0))")
FALLBACK_DRAINED=$(echo "$POST_METRICS" | python3 -c "import sys,json; print(json.load(sys.stdin).get('fallback_drained',0))")
TOTAL_RECEIVED=$(echo "$POST_METRICS" | python3 -c "import sys,json; print(json.load(sys.stdin).get('messages_received',0))")

echo ""
echo "============================================================"
echo "Results:"
echo "  Total received: $TOTAL_RECEIVED"
echo "  Total published: $POST_PUBLISHED"
echo "  Fallback writes: $FALLBACK_WRITES"
echo "  Fallback drained: $FALLBACK_DRAINED"
echo "  Logs during outage: $OUTAGE_LOGS"
echo "============================================================"

# Verify
PASS=true

if [ "$OUTAGE_LOGS" -ge 10 ]; then
    echo "PASS: All logs accepted during outage"
else
    echo "FAIL: Not all logs accepted during outage"
    PASS=false
fi

if [ "$TOTAL_RECEIVED" -ge 15 ]; then
    echo "PASS: All logs received (>= 15)"
else
    echo "FAIL: Expected >= 15 received, got $TOTAL_RECEIVED"
    PASS=false
fi

echo "============================================================"
if [ "$PASS" = true ]; then
    echo "BROKER OUTAGE TEST PASSED"
else
    echo "BROKER OUTAGE TEST FAILED"
fi
echo "============================================================"

# Cleanup
docker compose down -v

if [ "$PASS" = true ]; then
    exit 0
else
    exit 1
fi
