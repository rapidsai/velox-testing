#!/bin/bash

set -e

./stop_presto.sh
./build_centos_deps_image.sh
docker compose -f ../docker/docker-compose.native-gpu.yml build --build-arg NUM_THREADS=$(($(nproc) * 3 / 4)) --progress plain
docker compose -f ../docker/docker-compose.native-gpu.yml up -d

docker compose -f ../docker/docker-compose.native-gpu.yml exec presto-cli presto-cli --server presto-coordinator:8080 --catalog hive-parquet #--schema tpch_sf1
