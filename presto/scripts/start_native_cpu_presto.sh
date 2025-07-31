#!/bin/bash

set -e

./stop_presto.sh
docker compose -f ../docker/docker-compose.native-cpu.yml build --progress plain
docker compose -f ../docker/docker-compose.native-cpu.yml up -d
