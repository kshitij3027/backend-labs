#!/bin/bash
set -euo pipefail

BOOTSTRAP_SERVER="kafka1:29092"
TOPICS=("transactions.pending" "transactions.completed" "transactions.dlq")
PARTITIONS=3
REPLICATION_FACTOR=3
MIN_ISR=2
MAX_RETRIES=30
RETRY_INTERVAL=5

echo "=== Kafka Topic Initialization ==="

# ── Wait for all brokers to be reachable ──
echo "Waiting for Kafka brokers to be ready..."
attempt=0
while [ $attempt -lt $MAX_RETRIES ]; do
    # Check that we can list brokers and see all 3 nodes
    broker_count=$(kafka-broker-api-versions --bootstrap-server "$BOOTSTRAP_SERVER" 2>/dev/null | grep -c "id:" || true)
    if [ "$broker_count" -ge 3 ]; then
        echo "All 3 brokers are ready."
        break
    fi
    attempt=$((attempt + 1))
    echo "  Attempt $attempt/$MAX_RETRIES: found $broker_count broker(s), waiting ${RETRY_INTERVAL}s..."
    sleep $RETRY_INTERVAL
done

if [ $attempt -ge $MAX_RETRIES ]; then
    echo "ERROR: Brokers did not become ready within $((MAX_RETRIES * RETRY_INTERVAL))s"
    exit 1
fi

# ── Create topics ──
for topic in "${TOPICS[@]}"; do
    echo "Creating topic: $topic"

    # Check if topic already exists
    if kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --list 2>/dev/null | grep -q "^${topic}$"; then
        echo "  Topic '$topic' already exists, skipping."
        continue
    fi

    kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" \
        --create \
        --topic "$topic" \
        --partitions "$PARTITIONS" \
        --replication-factor "$REPLICATION_FACTOR" \
        --config min.insync.replicas="$MIN_ISR"

    echo "  Topic '$topic' created successfully."
done

# ── Verify ──
echo ""
echo "=== Topic Verification ==="
kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --list

echo ""
for topic in "${TOPICS[@]}"; do
    echo "--- $topic ---"
    kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --describe --topic "$topic"
    echo ""
done

echo "=== Topic initialization complete ==="
