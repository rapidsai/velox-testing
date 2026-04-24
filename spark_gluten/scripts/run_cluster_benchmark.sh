#!/bin/bash
# Wrapper script for fix-loop: start a 4-executor Spark cluster and run TPC-H SF100.
# On startup failure, dumps container logs for diagnosis.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

echo "=== GPU Info ==="
nvidia-smi -L

echo ""
echo "=== Stopping existing cluster ==="
./stop_spark_connect.sh 2>&1 || true

echo ""
echo "=== Starting cluster with 4 executors ==="
if ! SPARK_DATA_DIR=/raid/ocs_benchmark_data/tpch \
     MASTER_WEB_PORT=8081 \
     SPARK_WORKER_MEMORY=100g \
     ./start_spark_connect.sh --image-tag "dynamic_gpu_ws2_${USER:-latest}" -e 4 2>&1; then
  echo ""
  echo "=== Cluster startup FAILED. Collecting diagnostics ==="
  for c in spark-master spark-executor-0 spark-executor-1 spark-executor-2 spark-executor-3; do
    echo "--- $c logs (last 80 lines) ---"
    docker logs "$c" 2>&1 | tail -80
    echo ""
  done
  echo "--- spark-connect logs (last 120 lines) ---"
  CONNECT_CONTAINER=$(docker ps -a --filter "name=spark-connect" --format "{{.Names}}" | head -1)
  if [ -n "$CONNECT_CONTAINER" ]; then
    docker logs "$CONNECT_CONTAINER" 2>&1 | tail -120
  else
    echo "(no spark-connect container found)"
  fi
  echo ""
  echo "--- docker ps -a (spark) ---"
  docker ps -a --filter "name=spark" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>&1
  exit 1
fi

echo ""
echo "=== Master state (applications & executors) ==="
sleep 5
MASTER_JSON=$(curl -sf http://localhost:8081/json/ 2>/dev/null)
echo "$MASTER_JSON" | jq '{workers: .aliveworkers, cores: .cores, coresused: .coresused, apps: [.activeapps[]? | {id: .id, name: .name, cores: .cores, executors: [.executors[]? | {id: .id, host: .host, cores: .cores, memory: .memoryperslave}]}]}' 2>/dev/null || echo "$MASTER_JSON"
echo ""
echo "=== Cluster is up. Running TPC-H SF100 benchmark (skipping Q13) ==="
SPARK_DATA_DIR=/raid/ocs_benchmark_data/tpch \
./run_benchmark.sh -b tpch -d sf100_64mb --skip-drop-cache -i 1 \
  -q "1,2,3,4,5,6,7,8,9,10,11,12,14,15,16,17,18,19,20,21,22" 2>&1
