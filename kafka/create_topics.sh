#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVERS:-kafka:19092}"
TOPIC_NAME="${KAFKA_TOPIC:-transactions}"

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create \
  --if-not-exists \
  --topic "${TOPIC_NAME}" \
  --partitions 1 \
  --replication-factor 1

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --describe \
  --topic "${TOPIC_NAME}"
