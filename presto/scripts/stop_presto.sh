#!/bin/bash

set -e

OVERRIDE=""
[ -f ../docker/docker-compose.workers.override.yml ] && OVERRIDE="-f ../docker/docker-compose.workers.override.yml"

docker compose -f ../docker/docker-compose.java.yml -f ../docker/docker-compose.native-cpu.yml -f ../docker/docker-compose.native-gpu.yml ${OVERRIDE} down
