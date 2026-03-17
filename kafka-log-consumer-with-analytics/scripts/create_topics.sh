#!/bin/bash
set -e

echo "Waiting for Kafka to be ready..."
cub kafka-ready -b kafka:29092 1 60

echo "Creating topics..."
for topic in web-logs app-logs error-logs dead-letter-logs; do
  kafka-topics --create \
    --bootstrap-server kafka:29092 \
    --topic "$topic" \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists
  echo "Created topic: $topic"
done

echo "All topics created successfully."
kafka-topics --list --bootstrap-server kafka:29092
