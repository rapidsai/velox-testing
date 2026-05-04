#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
#
# Run master plan TPC-H benchmarks directly in the masterplan Docker image.
#
# Usage:
#   ./run_masterplan_benchmark.sh -d /path/to/data/tpch_sf10 -q "1,3"
#   ./run_masterplan_benchmark.sh -d /path/to/data/tpch_sf10 -q "1,3" --gpu
#   ./run_masterplan_benchmark.sh -d /path/to/data/tpch_sf10 --baseline
#
# Modes:
#   (default)    CPU master plan (collapsed single-stage)
#   --gpu        GPU master plan (collapsed + cuDF)
#   --baseline   CPU velox-spark (normal Spark stages, for comparison)

set -e

DATA_DIR=""
QUERIES="1,3"
GPU=false
NSYS=false
BASELINE=false
ITERATIONS=5
MASTER="local[1]"
PARTITIONS=2

while [[ $# -gt 0 ]]; do
  case $1 in
    -d|--data-dir)   DATA_DIR="$2"; shift 2 ;;
    -q|--queries)    QUERIES="$2"; shift 2 ;;
    -i|--iterations) ITERATIONS="$2"; shift 2 ;;
    -m|--master)     MASTER="$2"; shift 2 ;;
    -p|--partitions) PARTITIONS="$2"; shift 2 ;;
    --gpu)           GPU=true; shift ;;
    --nsys)          NSYS=true; shift ;;
    --baseline)      BASELINE=true; shift ;;
    -h|--help)       sed -n '2,/^$/s/^# \?//p' "$0"; exit 0 ;;
    *)               echo "Unknown: $1"; exit 1 ;;
  esac
done

[[ -z "$DATA_DIR" ]] && { echo "Error: -d/--data-dir required"; exit 1; }
[[ -d "$DATA_DIR" ]] || { echo "Error: $DATA_DIR not found"; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
IMAGE="apache/gluten:masterplan"
DATA_DIR="$(readlink -f "$DATA_DIR")"

OUTPUT_DIR="${REPO_ROOT}/benchmark_output/masterplan"
mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR="$(readlink -f "$OUTPUT_DIR")"

MODE="cpu-masterplan"
if $BASELINE; then MODE="baseline"; elif $GPU; then MODE="gpu-masterplan"; fi

echo "Image:      $IMAGE"
echo "Data:       $DATA_DIR"
echo "Queries:    $QUERIES"
echo "Iterations: $ITERATIONS"
echo "Mode:       $MODE"
echo ""

docker run --rm --gpus all \
  -v "${DATA_DIR}:${DATA_DIR}:ro" \
  -v "${OUTPUT_DIR}:/output" \
  -v "${REPO_ROOT}:/workspace/velox-testing:ro" \
  -e "DATA_DIR=${DATA_DIR}" \
  -e "QUERIES=${QUERIES}" \
  -e "ITERATIONS=${ITERATIONS}" \
  -e "GPU=${GPU}" \
  -e "NSYS=${NSYS}" \
  -e "BASELINE=${BASELINE}" \
  -e "MASTER=${MASTER}" \
  -e "PARTITIONS=${PARTITIONS}" \
  -e "MODE=${MODE}" \
  -e "CUDF_DISABLE_BUFFERED_INPUT=${CUDF_DISABLE_BUFFERED_INPUT:-false}" \
  "$IMAGE" \
  bash -c '
    set -e
    export JAVA_HOME=/usr/lib/jvm/java-17
    export PATH="$JAVA_HOME/bin:$PATH"
    export LD_LIBRARY_PATH="/opt/velox-runtime-libs:/usr/local/lib64:/usr/local/cuda/lib64:${LD_LIBRARY_PATH:-}"

    source /workspace/velox-testing/scripts/py_env_functions.sh
    VENV=/tmp/bench_venv
    init_python_virtual_env "$VENV"
    pip install -q pyspark==3.4.4 nvtx matplotlib

    if [ "$NSYS" = "true" ]; then
      ts=$(date +%Y%m%d_%H%M%S)
      nsys profile --trace=cuda,nvtx,osrt --force-overwrite=true \
        -o /output/masterplan-${MODE}-${ts}.nsys-rep \
        python3 /workspace/velox-testing/spark_gluten/scripts/masterplan_bench_runner.py
    else
      python3 /workspace/velox-testing/spark_gluten/scripts/masterplan_bench_runner.py
    fi
  '
