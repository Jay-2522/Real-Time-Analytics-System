#!/bin/bash
set -euo pipefail
config=/run/security/client.properties
broker=kafka:9092
for topic in telemetry.login.events telemetry.api.events telemetry.network.events telemetry.anomaly.events telemetry.deadletter.events analytics.enriched.events analytics.window.metrics analytics.risk.metrics analytics.feature.engineering ml.anomaly.predictions; do
  kafka-topics --bootstrap-server "$broker" --command-config "$config" --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1
done
allow() { kafka-acls --bootstrap-server "$broker" --command-config "$config" --add --allow-principal "User:CN=$1" --operation "$2" --resource-pattern-type prefixed --topic "$3"; }
for identity in producer consumer spark ml; do
  kafka-acls --bootstrap-server "$broker" --command-config "$config" --add --allow-principal "User:CN=$identity" --operation IdempotentWrite --cluster
done
allow producer Write telemetry.
allow consumer Read telemetry.
allow consumer Write telemetry.deadletter.events
allow spark Read telemetry.
allow spark Write analytics.
allow ml Read analytics.feature.engineering
allow ml Write ml.anomaly.predictions
allow ml Write telemetry.deadletter.events
for prefix in telemetry. analytics. ml.; do allow storage Read "$prefix"; done
allow alerts Read ml.anomaly.predictions
allow alerts Read analytics.
allow threat Read telemetry.
allow threat Read ml.anomaly.predictions
for identity in consumer spark ml storage alerts threat; do
  kafka-acls --bootstrap-server "$broker" --command-config "$config" --add --allow-principal "User:CN=$identity" --operation Read --resource-pattern-type prefixed --group "$identity-"
done
