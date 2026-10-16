#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# TPC-H on S3 with 1 GPU worker on one g7e.12xlarge; see print_help below. This is the
# single-GPU counterpart of run_tpch_g7e_48xlarge.sh: the same per-worker driver and Velox
# settings, but a single worker that owns the whole host's memory and KvikIO reactors, runs in
# single-node mode (no exchange) and sends S3 traffic through the instance's only NIC.
#
# Common runs (images presto-coordinator:<tag> and presto-native-worker-gpu:<tag> must exist):
#   Async data cache ON, cleared before each query (iteration 1 cold, the rest hot):
#     ./run_tpch_g7e_12xlarge_1gpu.sh --image-tag <tag> --async-data-cache on --cache-mode cold-once
#   Async data cache ON, cleared once before the first query:
#     ./run_tpch_g7e_12xlarge_1gpu.sh --image-tag <tag> --async-data-cache on --cache-mode lukewarm
#   Async data cache OFF:
#     ./run_tpch_g7e_12xlarge_1gpu.sh --image-tag <tag> --async-data-cache off
#   Start the cluster for ad-hoc queries, then stop it:
#     ./run_tpch_g7e_12xlarge_1gpu.sh --image-tag <tag> --async-data-cache on --start-only
#     ../stop_presto.sh
# On a shared host, also pass --compose-project <name> so other people's clusters are not
# stopped, and use the same COMPOSE_PROJECT_NAME and PRESTO_IMAGE_TAG for stop_presto.sh.
# Results go to presto/scripts/benchmark_output/<run tag> (see -t).
#
# Free host memory first if MemFree is < 470 GiB:
#   grep MemFree /proc/meminfo
#   sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && sudo sh -c 'echo 1 > /proc/sys/vm/compact_memory'

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

This script runs TPC-H on S3 with 1 GPU worker on one g7e.12xlarge (2x RTX PRO 6000), set up like
run_tpch_g7e_48xlarge.sh: KvikIO S3 reads on the host network and the Velox async data cache
enabled or disabled. With one worker the query runs in single-node mode, so there is no exchange.
It never builds images.

OPTIONS:
    -h, --help              Show this help message.
    -s, --schema-name       Name of the schema to query. Its metastore must be in presto/docker/.hive_metastore.
                            By default, "tpch_sf1k_v2_float_s3".
    -q, --queries           Set of benchmark queries to run. This should be a comma separate list of query numbers.
                            By default, all benchmark queries are run.
    -i, --iterations        Number of query run iterations. By default, 3 iterations are run.
    -t, --tag               Tag associated with the benchmark run. By default, a tag is built from the options.
    --async-data-cache      Enable ("on") or disable ("off") the Velox async data cache on the workers.
                            By default, "off".
    --cache-mode            When to clear caches between queries, passed on to run_benchmark.sh: "off", "lukewarm",
                            "cold-once", "cold" or "hot". By default, "off".
    --image-tag             Tag of the presto-coordinator and presto-native-worker-gpu images to run.
                            By default, \$USER (as in start_native_gpu_presto.sh).
    --compose-project       Docker Compose project name. By default, Docker Compose chooses it.
    --gpu-id                GPU the worker runs on. By default, 0.
    --dispatch              KvikIO reactor dispatch: "SHARED_QUEUE" or "PER_CHUNK". By default, "SHARED_QUEUE".
    --no-nontemporal-copy   Disable KvikIO's non-temporal copy.
    --start-only            Start the cluster and exit. Stop it with stop_presto.sh.

EXAMPLES:
    $0 --async-data-cache on --cache-mode cold-once
    $0 --async-data-cache on --cache-mode lukewarm --image-tag wxd-upstream
    $0 -q "1,6" --async-data-cache off

EOF
}

SCHEMA_NAME=tpch_sf1k_v2_float_s3
ITERATIONS=3
ASYNC_DATA_CACHE=off
CACHE_MODE=off
IMAGE_TAG=${USER:-latest}
GPU_ID=0
DISPATCH=SHARED_QUEUE
NONTEMPORAL_COPY=true
START_ONLY=false

parse_args() {
  while [[ $# -gt 0 ]]; do
    case $1 in
      -h|--help)
        print_help
        exit 0
        ;;
      -s|--schema-name|-q|--queries|-i|--iterations|-t|--tag|--async-data-cache|--cache-mode|--image-tag|--compose-project|\
      --gpu-id|--dispatch)
        if [[ -z $2 ]]; then
          echo "Error: $1 requires a value"
          exit 1
        fi
        case $1 in
          -s|--schema-name) SCHEMA_NAME=$2 ;;
          -q|--queries) QUERIES=$2 ;;
          -i|--iterations) ITERATIONS=$2 ;;
          -t|--tag) TAG=$2 ;;
          --async-data-cache) ASYNC_DATA_CACHE=$2 ;;
          --cache-mode) CACHE_MODE=$2 ;;
          --image-tag) IMAGE_TAG=$2 ;;
          --compose-project) COMPOSE_PROJECT=$2 ;;
          --gpu-id) GPU_ID=$2 ;;
          --dispatch) DISPATCH=$2 ;;
        esac
        shift 2
        ;;
      --no-nontemporal-copy)
        NONTEMPORAL_COPY=false
        shift
        ;;
      --start-only)
        START_ONLY=true
        shift
        ;;
      *)
        echo "Error: Unknown argument $1"
        print_help
        exit 1
        ;;
    esac
  done
}

parse_args "$@"

case $ASYNC_DATA_CACHE in
  on) ASYNC_DATA_CACHE_ENABLED=true ;;
  off) ASYNC_DATA_CACHE_ENABLED=false ;;
  *)
    echo "Error: --async-data-cache must be on or off"
    exit 1
    ;;
esac
if [[ ! $CACHE_MODE =~ ^(off|lukewarm|cold-once|cold|hot)$ ]]; then
  echo "Error: --cache-mode must be off, lukewarm, cold-once, cold or hot"
  exit 1
fi
if [[ $ASYNC_DATA_CACHE == off && $CACHE_MODE =~ ^(lukewarm|cold-once|cold)$ ]]; then
  echo "Warning: --cache-mode ${CACHE_MODE} with --async-data-cache off: there is no async data cache to clear," \
    "so only the Hive file-handle cache is cleared."
fi
if [[ ! $DISPATCH =~ ^(SHARED_QUEUE|PER_CHUNK)$ ]]; then
  echo "Error: --dispatch must be SHARED_QUEUE or PER_CHUNK"
  exit 1
fi
if [[ ! $GPU_ID =~ ^[0-9]+$ ]] || ! nvidia-smi -i "$GPU_ID" > /dev/null 2>&1; then
  echo "Error: GPU $GPU_ID not found"
  exit 1
fi

# run_benchmark.sh writes benchmark_output under the current directory.
cd "${SCRIPT_DIR}/.."

if [[ ! -d ../docker/.hive_metastore/${SCHEMA_NAME} ]]; then
  dataset=${SCHEMA_NAME#tpch_}
  echo "Error: no metastore for ${SCHEMA_NAME} in presto/docker/.hive_metastore. For the rapids-tpch data sets:"
  echo "  aws s3 cp --recursive s3://rapids-tpch/presto-gpu/${dataset%_s3}/hive_metastore/ ../docker/.hive_metastore/"
  exit 1
fi
# start_native_gpu_presto.sh builds any missing image; fail instead.
for image in presto-coordinator:${IMAGE_TAG} presto-native-worker-gpu:${IMAGE_TAG}; do
  if ! docker image inspect "$image" > /dev/null 2>&1; then
    echo "Error: image $image not found"
    exit 1
  fi
done

export PRESTO_IMAGE_TAG=${IMAGE_TAG}
if [[ -n $COMPOSE_PROJECT ]]; then
  export COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT}
fi

# Worker on the host network (rendered by the compose template). A g7e.12xlarge has one ENA
# NIC and no EFA, so there is nothing to bind per worker and no verbs devices to pass in.
export PRESTO_GPU_HOST_NETWORK=true PRESTO_GPU_EFA_DEVICES=false
# The NIC of the default route and its IPv4 address (SIOCGIFADDR).
NIC=$(awk '$2 == "00000000" { print $1; exit }' /proc/net/route)
HOST_IP=$(python3 -c 'import fcntl, socket, struct, sys
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
print(socket.inet_ntoa(fcntl.ioctl(s.fileno(), 0x8915, struct.pack("256s", sys.argv[1].encode()))[20:24]))' "$NIC" 2> /dev/null)
if [[ -z $HOST_IP ]]; then
  echo "Error: no IPv4 address on the default-route NIC ${NIC}"
  exit 1
fi
echo "Worker internal address: $HOST_IP ($NIC)"
# The TPC-H data sets live in us-east-2. KvikIO signs S3 requests with AWS_* from the
# environment, which the compose files pass into the workers. Without exported credentials,
# use the instance role (IMDS) and skip any configured profile.
export AWS_REGION=us-east-2 AWS_DEFAULT_REGION=us-east-2
if [[ -z ${AWS_ACCESS_KEY_ID} ]]; then
  eval "$(AWS_CONFIG_FILE=/dev/null AWS_SHARED_CREDENTIALS_FILE=/dev/null aws configure export-credentials --format env)"
fi

# KvikIO as in run_tpch_g7e_48xlarge.sh (KVIKIO_NTHREADS=16 comes from --kvikio-threads below),
# except for the reactors and the request cap. Those are per worker there, and one worker with
# them reads S3 at a fraction of the NIC, so this one worker gets N reactors and an 8N cap
# (N = 48 logical cores).
# MULTI_POLL needs the task size to be at most the bounce buffer size.
export KVIKIO_REMOTE_IO_BACKEND=MULTI_POLL KVIKIO_REMOTE_IO_NUM_REACTORS=48
export KVIKIO_REMOTE_IO_REACTOR_DISPATCH=${DISPATCH} KVIKIO_REMOTE_IO_MAX_CONCURRENT_REQUESTS=384
export KVIKIO_TASK_SIZE=33554432 KVIKIO_BOUNCE_BUFFER_SIZE=33554432
export KVIKIO_REMOTE_IO_NONTEMPORAL_COPY=${NONTEMPORAL_COPY}

# 1. Generate configs: 1 worker, 2 drivers. With one worker, generate_presto_config.sh keeps
#    single-node execution on and the cuDF exchange off.
VARIANT_TYPE=gpu NUM_WORKERS=1 GPU_IDS=${GPU_ID} VCPU_PER_WORKER=2 OVERWRITE_CONFIG=true \
  ./generate_presto_config.sh

# 2. Align the generated configs with run_tpch_g7e_48xlarge.sh. The memory settings are twice
#    its per-worker values, since this worker has the host (~500 GiB) to itself.
G=../docker/config/generated/gpu
setprop() { if grep -q "^$2=" "$1"; then sed -i "s|^$2=.*|$2=$3|" "$1"; else echo "$2=$3" >> "$1"; fi; }
W=$G/etc_worker_${GPU_ID}
setprop $W/node.properties node.internal-address "$HOST_IP"
setprop $W/config_native.properties discovery.uri http://127.0.0.1:8080
setprop $W/config_native.properties async-data-cache-enabled $ASYNC_DATA_CACHE_ENABLED
setprop $W/config_native.properties system-memory-gb 438
setprop $W/config_native.properties query-memory-gb 306
setprop $W/config_native.properties system-mem-limit-gb 458
setprop $W/config_native.properties query.max-memory-per-node 306GB
setprop $W/config_native.properties cudf.partitioned_output_batch_rows 10000000
setprop $W/config_native.properties cudf.batch_size_min_threshold 40000000
setprop $W/config_native.properties driver.max-split-preload 2
setprop $W/config_native.properties exchange.max-buffer-size 512MB
setprop $W/config_native.properties exchange.max-response-size 64MB
setprop $W/config_native.properties sink.max-buffer-size 512MB
setprop $W/config_native.properties local-exchange.max-buffer-size 536870912
setprop $W/config_native.properties announcement-max-frequency-ms 1000
setprop $W/catalog/hive.properties hive.s3.max-connections 96
# Unlike run_tpch_g7e_48xlarge.sh, prefetch column chunks when a split is prepared: the one
# worker's few drivers do not keep enough S3 reads in flight otherwise.
setprop $W/catalog/hive.properties cudf.hive.preload-column-chunks true
C=$G/etc_coordinator/config_native.properties
setprop $C join-max-broadcast-table-size 20788MB
setprop $C node-scheduler.schedule-splits-based-on-task-load true
setprop $C exchange.max-buffer-size 512MB
setprop $C exchange.max-response-size 64MB
setprop $C sink.max-buffer-size 512MB
setprop $C query.max-execution-time 100m

# 3. Start the cluster with the configs above. One worker runs in the combined
#    presto-native-worker-gpu container.
./start_native_gpu_presto.sh --skip-generate-config -w 1 -g "${GPU_ID}" --kvikio-threads 16 --num-drivers 2
WORKER=presto-native-worker-gpu

# 4. The start script does not wait for the worker; wait until it is registered.
for _ in $(seq 150); do
  [[ $(curl -s localhost:8080/v1/node | jq length 2>/dev/null || echo 0) == 1 ]] && break
  sleep 2
done
echo "Registered workers: $(curl -s localhost:8080/v1/node | jq length)"

# 5. Record what the worker actually runs with.
docker inspect $WORKER --format 'network={{.HostConfig.NetworkMode}}'
docker exec $WORKER env | grep -E '^(KVIKIO_|LIBCUDF_)' | sort
docker exec $WORKER grep -E '^(async-data-cache-enabled|system-memory-gb|query-memory-gb|single-node-execution-enabled|cudf.exchange)=' \
  /opt/presto-server/etc${GPU_ID}/config.properties
docker exec $WORKER grep -E '^cudf.hive.preload-column-chunks=' /opt/presto-server/etc${GPU_ID}/catalog/hive.properties

if [[ $START_ONLY == true ]]; then
  exit 0
fi

# 6. Run the benchmark.
if [[ -z $TAG ]]; then
  TAG="${SCHEMA_NAME}_g7e_12xlarge_1gpu_${IMAGE_TAG}_adc_${ASYNC_DATA_CACHE}_${CACHE_MODE}"
  [[ $DISPATCH != SHARED_QUEUE ]] && TAG+="_${DISPATCH,,}"
  [[ $NONTEMPORAL_COPY == false ]] && TAG+=_no_nontemporal_copy
  TAG+="_q${QUERIES:-all}_$(date -u +%Y%m%dT%H%M%SZ)"
  TAG=${TAG//[,.-]/_}
fi
# Session properties from run_tpch_g7e_48xlarge.sh, less the two exchange and placement ones.
# With one worker the cuDF exchange is off, so native_cudf_exchange_enabled=true would fail any
# query with a worker-to-worker edge. Single-node execution assigns splits through the bucketed
# path, which rejects experimental_deterministic_bounded_splits=true, and with one worker there
# is no placement to choose anyway.
SESSION=(hive.file_splittable=false schedule_splits_based_on_task_load=true
         hive.node_selection_strategy=SOFT_AFFINITY native_max_split_preload_per_driver=2)
SESSION_ARGS=()
for p in "${SESSION[@]}"; do
  SESSION_ARGS+=(--session-property "$p")
done
./run_benchmark.sh -b tpch -s "${SCHEMA_NAME}" -i "${ITERATIONS}" -t "${TAG}" -m ${QUERIES:+-q "$QUERIES"} \
  "${SESSION_ARGS[@]}" --cache-mode "${CACHE_MODE}" --skip-drop-cache --skip-analyze-check \
  || echo "run_benchmark.sh reported failures; stopping the cluster anyway"

./stop_presto.sh
