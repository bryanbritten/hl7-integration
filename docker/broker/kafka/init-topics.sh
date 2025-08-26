#!/bin/bash
set -euxo pipefail

echo "Waiting for Kafka..."
until kafka-topics.sh --bootstrap-server kafka:9092 --list >/dev/null 2>&1; do
  sleep 2
done

echo "Creating topics..."
for topic in hl7.ingest hl7.accepted hl7.validated DLQ ACKS; do
  kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists \
    --topic "$topic" --partitions 1 --replication-factor 1
done

echo "Finished creating topics!"
