#!/bin/bash
# EC2 user-data for one Ledgerline Kafka node (Lab 02). Runs once, as root, at first boot.
# The node number N comes from the private IP: 10.0.1.11 -> 1, .12 -> 2, .13 -> 3.
# Each node runs two containers from apache/kafka:4.3.2, both on the host network:
#   controller  KRaft controller, node id 10N, port 9093 (the metadata quorum)
#   kafka       broker, node id N, port 9092 (replication) and 9094 (clients)
set -euxo pipefail
apt-get update -q
apt-get install -y -q docker.io
usermod -aG docker ubuntu

IMDS=http://169.254.169.254/latest
TOKEN=$(curl -sX PUT $IMDS/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 300')
md() { curl -s -H "X-aws-ec2-metadata-token: $TOKEN" "$IMDS/meta-data/$1"; }
PRIVATE=$(md local-ipv4)
PUBLIC=$(md public-ipv4)
N=${PRIVATE##*.1}

IMAGE=apache/kafka:4.3.2
COMMON=(
  -e CLUSTER_ID=bGVkZ2VybGluZWxhYjAyIQ
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=101@10.0.1.11:9093,102@10.0.1.12:9093,103@10.0.1.13:9093
  -e KAFKA_LOG_DIRS=/var/lib/kafka/data
)
docker pull -q $IMAGE

docker run -d --name controller --network host "${COMMON[@]}" \
  -e KAFKA_NODE_ID=10$N \
  -e KAFKA_PROCESS_ROLES=controller \
  -e KAFKA_LISTENERS=CONTROLLER://$PRIVATE:9093 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT \
  -e KAFKA_HEAP_OPTS="-Xms256m -Xmx256m" \
  $IMAGE

# Cluster defaults are Kafka's own (min.insync.replicas=1, unclean election off);
# the lab sets durability per topic. A stalled follower leaves the ISR after 10 s.
docker run -d --name kafka --network host "${COMMON[@]}" \
  -e KAFKA_NODE_ID=$N \
  -e KAFKA_PROCESS_ROLES=broker \
  -e KAFKA_LISTENERS=INTERNAL://$PRIVATE:9092,EXTERNAL://0.0.0.0:9094 \
  -e KAFKA_ADVERTISED_LISTENERS=INTERNAL://$PRIVATE:9092,EXTERNAL://$PUBLIC:9094 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
  -e KAFKA_DEFAULT_REPLICATION_FACTOR=3 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2 \
  -e KAFKA_REPLICA_LAG_TIME_MAX_MS=10000 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e KAFKA_HEAP_OPTS="-Xms512m -Xmx512m" \
  $IMAGE
