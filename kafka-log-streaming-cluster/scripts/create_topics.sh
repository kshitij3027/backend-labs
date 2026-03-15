#!/bin/bash
set -e

KAFKA_BROKER="${KAFKA_BROKER:-kafka-1:29092}"

echo "Waiting for Kafka broker at $KAFKA_BROKER to be ready..."

RETRIES=30
until kafka-topics --bootstrap-server "$KAFKA_BROKER" --list > /dev/null 2>&1; do
  RETRIES=$((RETRIES - 1))
  if [ "$RETRIES" -le 0 ]; then
    echo "ERROR: Kafka broker not ready after 30 attempts. Exiting."
    exit 1
  fi
  echo "Kafka not ready yet. Retrying in 2s... ($RETRIES attempts left)"
  sleep 2
done

echo "Kafka is ready. Creating topics..."

# Create topics (idempotent with --if-not-exists)
TOPICS=("web-api-logs" "user-service-logs" "payment-service-logs")

for TOPIC in "${TOPICS[@]}"; do
  echo "Creating topic: $TOPIC (3 partitions, RF 3)"
  kafka-topics --bootstrap-server "$KAFKA_BROKER" \
    --create \
    --if-not-exists \
    --topic "$TOPIC" \
    --partitions 3 \
    --replication-factor 3
done

echo "Creating topic: critical-logs (1 partition, RF 3)"
kafka-topics --bootstrap-server "$KAFKA_BROKER" \
  --create \
  --if-not-exists \
  --topic critical-logs \
  --partitions 1 \
  --replication-factor 3

# Set retention policy on all topics
ALL_TOPICS=("web-api-logs" "user-service-logs" "payment-service-logs" "critical-logs")

for TOPIC in "${ALL_TOPICS[@]}"; do
  echo "Setting retention.ms=604800000 on topic: $TOPIC"
  kafka-configs --bootstrap-server "$KAFKA_BROKER" \
    --alter \
    --entity-type topics \
    --entity-name "$TOPIC" \
    --add-config retention.ms=604800000
done

echo ""
echo "All topics created successfully. Listing topics:"
kafka-topics --bootstrap-server "$KAFKA_BROKER" --list

echo ""
echo "Topic details:"
for TOPIC in "${ALL_TOPICS[@]}"; do
  kafka-topics --bootstrap-server "$KAFKA_BROKER" --describe --topic "$TOPIC"
  echo ""
done

echo "Topic initialization complete."
