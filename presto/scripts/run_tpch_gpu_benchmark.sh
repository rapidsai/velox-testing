#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
#
# Run TPC-H against a running native-GPU Presto cluster, optionally with nsys
# profiling. Mirrors the calling convention of
# spark_gluten/scripts/run_masterplan_benchmark.sh so the same data dir works
# for both. Profile output is dropped under benchmark_output/presto/ next to
# the masterplan profiles.
#
# Usage:
#   ./run_tpch_gpu_benchmark.sh -d /path/to/data/tpch_sf10
#   ./run_tpch_gpu_benchmark.sh -d data/tpch_sf10 -q "1,3" --nsys
#
# Prereqs:
#   - Presto cluster already running:
#       DOCKER_DEFAULT_PLATFORM=linux/amd64 \
#       velox-testing/presto/scripts/start_native_gpu_presto.sh -j 12 --profile
#   - Data directory contains TPC-H parquet (same layout the masterplan flow
#     uses); the basename is used as the Hive schema name.

set -e

DATA_DIR=""
QUERIES=""
ITERATIONS=""
NSYS=false

while [[ $# -gt 0 ]]; do
  case $1 in
    -d|--data-dir)   DATA_DIR="$2"; shift 2 ;;
    -q|--queries)    QUERIES="$2"; shift 2 ;;
    -i|--iterations) ITERATIONS="$2"; shift 2 ;;
    --nsys)          NSYS=true; shift ;;
    -h|--help)       sed -n '2,/^$/s/^# \?//p' "$0"; exit 0 ;;
    *)               echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$DATA_DIR" ]]; then
  echo "Error: -d/--data-dir required" >&2
  exit 1
fi
if [[ ! -d "$DATA_DIR" ]]; then
  echo "Error: $DATA_DIR not found" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$(readlink -f "$DATA_DIR")"
DATA_NAME="$(basename "$DATA_DIR")"
SCHEMA_NAME="${DATA_NAME}"  # e.g. tpch_sf10

export PRESTO_DATA_DIR="$(dirname "$DATA_DIR")"

REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUTPUT_DIR="${REPO_ROOT}/benchmark_output/presto"
mkdir -p "$OUTPUT_DIR"

# profiler_functions.sh + run_benchmark.sh both look up the worker container
# by image tag. start_native_gpu_presto.sh defaults this to $USER.
export PRESTO_IMAGE_TAG="${PRESTO_IMAGE_TAG:-${USER:-latest}}"

echo "PRESTO_DATA_DIR: $PRESTO_DATA_DIR"
echo "Schema:          $SCHEMA_NAME"
echo "Data dir name:   $DATA_NAME"
echo "Output:          $OUTPUT_DIR"
echo

# Require the cluster to be up before doing any work.
if ! curl -s -o /dev/null http://localhost:8080/ ; then
  echo "Error: Presto coordinator not reachable on localhost:8080." >&2
  echo "Start it first with: PRESTO_DATA_DIR=$(pwd)/data \\" >&2
  echo "  DOCKER_DEFAULT_PLATFORM=linux/amd64 \\" >&2
  echo "  velox-testing/presto/scripts/start_native_gpu_presto.sh -j 12 --profile" >&2
  exit 1
fi

# Wait for at least one worker to register with the coordinator. The
# coordinator-up signal isn't enough — query plans need an active worker.
echo "===== waiting for worker registration ====="
source "$SCRIPT_DIR/common_functions.sh"
wait_for_worker_node_registration localhost 8080

# Set up tables (idempotent — safe if schema already exists).
# --no-docker keeps our GPU cluster up; the helper would otherwise tear it
# down and swap in a CPU presto cluster (which we haven't built).
# --skip-analyze-tables — ANALYZE on GPU presto fails with
#   "Unsupported type_id conversion to cudf"; queries still run, just with
#   default cardinality estimates instead of collected stats.
echo "===== setup_benchmark_tables ====="
"$SCRIPT_DIR/setup_benchmark_tables.sh" -b tpch -s "$SCHEMA_NAME" -d "$DATA_NAME" \
  --no-docker --skip-analyze-tables

# Optionally bracket the benchmark with nsys profile start/stop.
# Quirks of profiler_functions.sh:
#   start_profiler "<path>" → writes /presto_profiles/$(basename <path>).nsys-rep
#     i.e. it appends `.nsys-rep` regardless. Pass a path WITHOUT the extension.
#   stop_profiler "<path>" → does the docker-cp itself to <path>.nsys-rep on host.
#     So we hand it the desired output path and don't run a second cp.
PROFILE_HOST_PATH=""
if [[ "$NSYS" == "true" ]]; then
  ts="$(date +%Y%m%d_%H%M%S)"
  # No extension here — the helpers append `.nsys-rep`.
  PROFILE_HOST_PATH="${OUTPUT_DIR}/presto-gpu-${SCHEMA_NAME}-${ts}"
  source "$SCRIPT_DIR/profiler_functions.sh"
  echo "===== start_profiler $PROFILE_HOST_PATH ====="
  start_profiler "$PROFILE_HOST_PATH"
fi

# Run the benchmark.
RC=0
RUN_ARGS=(-b tpch -s "$SCHEMA_NAME")
[[ -n "$QUERIES" ]] && RUN_ARGS+=(-q "$QUERIES")
[[ -n "$ITERATIONS" ]] && RUN_ARGS+=(-i "$ITERATIONS")
echo "===== run_benchmark ${RUN_ARGS[*]} ====="
"$SCRIPT_DIR/run_benchmark.sh" "${RUN_ARGS[@]}" || RC=$?

if [[ "$NSYS" == "true" ]]; then
  echo "===== stop_profiler $PROFILE_HOST_PATH ====="
  stop_profiler "$PROFILE_HOST_PATH"
  echo "Profile written: ${PROFILE_HOST_PATH}.nsys-rep"
fi

exit $RC
