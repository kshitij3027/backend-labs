#!/bin/bash
set -e

echo "Waiting for Kafka to be ready..."
cub kafka-ready -b kafka:29092 1 60

echo "Creating topic: log-processing-topic with 6 partitions..."
kafka-topics --create \
  --bootstrap-server kafka:29092 \
  --topic log-processing-topic \
  --partitions 6 \
  --replication-factor 1 \
  --if-not-exists

echo "Topic created successfully."
kafka-topics --list --bootstrap-server kafka:29092
