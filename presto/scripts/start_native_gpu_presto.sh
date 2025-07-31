#!/bin/bash

set -e

./stop_presto.sh
docker compose -f ../docker/docker-compose.native-gpu.yml build --progress plain
docker compose -f ../docker/docker-compose.native-gpu.yml up -d
