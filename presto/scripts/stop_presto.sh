#!/bin/bash

set -e

if [[ "${PRESTO_DATA_DIR}" =~ ^s3: ]]; then
  unset PRESTO_DATA_DIR
fi

docker compose -f ../docker/docker-compose.java.yml -f ../docker/docker-compose.native-cpu.yml -f ../docker/docker-compose.native-gpu.yml down
