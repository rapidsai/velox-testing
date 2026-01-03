#!/bin/bash

set -e

docker compose -f ../docker/docker-compose.java.yml \
  -f ../docker/docker-compose.native-cpu.yml \
  -f ../docker/docker-compose.native-gpu.yml \
  -f ../docker/docker-compose.native-gpu-dev.yml down
