#!/bin/bash
set -e

BROKER="kafka:29092"
TOPIC="user-profiles"

echo "Waiting for Kafka to be ready..."
MAX_RETRIES=30
RETRY=0
until kafka-topics --bootstrap-server "$BROKER" --list > /dev/null 2>&1; do
    RETRY=$((RETRY + 1))
    if [ "$RETRY" -ge "$MAX_RETRIES" ]; then
        echo "ERROR: Kafka not ready after $MAX_RETRIES attempts. Exiting."
        exit 1
    fi
    echo "  Kafka not ready yet (attempt $RETRY/$MAX_RETRIES)... waiting 2s"
    sleep 2
done
echo "Kafka is ready!"

echo "Creating topic: $TOPIC"
kafka-topics --bootstrap-server "$BROKER" \
    --create \
    --topic "$TOPIC" \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists \
    --config cleanup.policy=compact \
    --config segment.bytes=1048576 \
    --config min.cleanable.dirty.ratio=0.1 \
    --config delete.retention.ms=60000 \
    --config max.compaction.lag.ms=60000

echo "Topic '$TOPIC' created successfully with compaction settings."
echo ""
echo "Topic details:"
kafka-topics --bootstrap-server "$BROKER" --describe --topic "$TOPIC"
