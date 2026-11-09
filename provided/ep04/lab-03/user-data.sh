#!/bin/bash
# Bootstrap one Ubuntu 24.04 host. Kafka starts only after the learner copies
# the lab-scoped Compose file and node.env over SSH.
set -euxo pipefail

apt-get update -q
DEBIAN_FRONTEND=noninteractive apt-get install -y -q docker.io docker-compose-v2
systemctl enable --now docker
usermod -aG docker ubuntu
