#!/bin/bash
set -e

BROKER="kafka:29092"

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

TOPICS=("log-events:3" "error-events:3" "user-events:3" "derived-metrics:1")

for TOPIC_DEF in "${TOPICS[@]}"; do
    TOPIC="${TOPIC_DEF%%:*}"
    PARTITIONS="${TOPIC_DEF##*:}"

    echo "Creating topic: $TOPIC (partitions=$PARTITIONS)"
    kafka-topics --bootstrap-server "$BROKER" \
        --create \
        --topic "$TOPIC" \
        --partitions "$PARTITIONS" \
        --replication-factor 1 \
        --if-not-exists
done

echo ""
echo "All topics created successfully."
echo ""
echo "Topic listing:"
kafka-topics --bootstrap-server "$BROKER" --list
