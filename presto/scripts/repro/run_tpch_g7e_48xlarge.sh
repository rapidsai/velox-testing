#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# TPC-H on S3 with 8 GPU workers on one g7e.48xlarge; see print_help below. The NIC, EFA and
# memory settings are specific to that instance type.
#
# Common runs (images presto-coordinator:<tag> and presto-native-worker-gpu:<tag> must exist):
#   Async data cache ON, cleared before each query (iteration 1 cold, the rest hot):
#     ./run_tpch_g7e_48xlarge.sh --image-tag <tag> --async-data-cache on --cache-mode cold-once
#   Async data cache ON, cleared once before the first query:
#     ./run_tpch_g7e_48xlarge.sh --image-tag <tag> --async-data-cache on --cache-mode lukewarm
#   Async data cache OFF:
#     ./run_tpch_g7e_48xlarge.sh --image-tag <tag> --async-data-cache off
#   SF3K instead of SF1K:
#     ./run_tpch_g7e_48xlarge.sh --image-tag <tag> --async-data-cache off -s tpch_sf3k_v2_float_s3
#   A few queries with the HTTP exchange:
#     ./run_tpch_g7e_48xlarge.sh --image-tag <tag> -q 1,6 --exchange http
#   Start the cluster for ad-hoc queries, then stop it:
#     ./run_tpch_g7e_48xlarge.sh --image-tag <tag> --async-data-cache on --start-only
#     ../stop_presto.sh
# On a shared host, also pass --compose-project <name> so other people's clusters are not
# stopped, and use the same COMPOSE_PROJECT_NAME and PRESTO_IMAGE_TAG for stop_presto.sh.
# Results go to presto/scripts/benchmark_output/<run tag> (see -t).
#
# Free host memory first if either NUMA node has < 500 GiB MemFree:
#   grep MemFree /sys/devices/system/node/node*/meminfo
#   sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && sudo sh -c 'echo 1 > /proc/sys/vm/compact_memory'

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

This script runs TPC-H on S3 with 8 GPU workers on one g7e.48xlarge (8x RTX PRO 6000), set up to
match the WXD-launch runs: UCX GPU exchange over EFA, KvikIO S3 reads with each GPU pair on its
NUMA-local NIC, and the Velox async data cache enabled or disabled. It never builds images.

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
    --exchange              Shuffle transport: "ucx" or "http". "ucx" needs images built with the Velox and Presto
                            UCX exchange changes. By default, "ucx".
    --dispatch              KvikIO reactor dispatch: "SHARED_QUEUE" or "PER_CHUNK". By default, "SHARED_QUEUE".
    --no-nontemporal-copy   Disable KvikIO's non-temporal copy.
    --no-nic-bind           Send all S3 traffic through the primary NIC instead of each GPU pair's NUMA-local NIC.
    --start-only            Start the cluster and exit. Stop it with stop_presto.sh.

EXAMPLES:
    $0 --async-data-cache on --cache-mode cold-once
    $0 --async-data-cache on --cache-mode lukewarm --image-tag wxd-upstream
    $0 -s tpch_sf3k_v2_float_s3
    $0 -q "1,6" --exchange http

EOF
}

SCHEMA_NAME=tpch_sf1k_v2_float_s3
ITERATIONS=3
ASYNC_DATA_CACHE=off
CACHE_MODE=off
IMAGE_TAG=${USER:-latest}
EXCHANGE=ucx
DISPATCH=SHARED_QUEUE
NONTEMPORAL_COPY=true
NIC_BIND=true
START_ONLY=false

parse_args() {
  while [[ $# -gt 0 ]]; do
    case $1 in
      -h|--help)
        print_help
        exit 0
        ;;
      -s|--schema-name|-q|--queries|-i|--iterations|-t|--tag|--async-data-cache|--cache-mode|--image-tag|\
      --compose-project|--exchange|--dispatch)
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
          --exchange) EXCHANGE=$2 ;;
          --dispatch) DISPATCH=$2 ;;
        esac
        shift 2
        ;;
      --no-nontemporal-copy)
        NONTEMPORAL_COPY=false
        shift
        ;;
      --no-nic-bind)
        NIC_BIND=false
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
if [[ ! $EXCHANGE =~ ^(ucx|http)$ ]]; then
  echo "Error: --exchange must be ucx or http"
  exit 1
fi
if [[ ! $DISPATCH =~ ^(SHARED_QUEUE|PER_CHUNK)$ ]]; then
  echo "Error: --dispatch must be SHARED_QUEUE or PER_CHUNK"
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
for dev in /sys/class/net/enp135s0 /sys/class/infiniband/rdmap145s0; do
  if [[ ! -e $dev ]]; then
    echo "Error: $dev not found; this script expects a g7e.48xlarge"
    exit 1
  fi
done

export PRESTO_IMAGE_TAG=${IMAGE_TAG}
if [[ -n $COMPOSE_PROJECT ]]; then
  export COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT}
fi

# Workers on the host network with EFA devices (rendered by the compose template).
export PRESTO_GPU_HOST_NETWORK=true
HOST_IP=$(ip -4 -o addr show dev enp135s0 | awk '{ split($4, a, "/"); print a[1]; exit }')
if [[ -z $HOST_IP ]]; then
  echo "Error: no IPv4 address on enp135s0"
  exit 1
fi
echo "Worker internal address: $HOST_IP"
# The TPC-H data sets live in us-east-2. KvikIO signs S3 requests with AWS_* from the
# environment, which the compose files pass into the workers. Without exported credentials,
# use the instance role (IMDS) and skip any configured profile.
export AWS_REGION=us-east-2 AWS_DEFAULT_REGION=us-east-2
if [[ -z ${AWS_ACCESS_KEY_ID} ]]; then
  eval "$(AWS_CONFIG_FILE=/dev/null AWS_SHARED_CREDENTIALS_FILE=/dev/null aws configure export-credentials --format env)"
fi

# Exchange over EFA: one NUMA-local EFA device per GPU pair, with the WXD-launch rendezvous tuning.
export PRESTO_GPU_UCX_TLS=tcp,srd,cuda_copy,self
export PRESTO_GPU_UCX_RNDV_PIPELINE_ERROR_HANDLING=n
export UCX_MAX_RNDV_RAILS=1 UCX_RNDV_FRAG_SIZE=cuda:32M UCX_RNDV_FRAG_MEM_TYPES=cuda UCX_SOCKADDR_TLS_PRIORITY=tcp
export UCX_RNDV_FRAG_ALLOC_COUNT=host:128,cuda:32 UCX_CUDA_IPC_CACHE_MAX_REGIONS=inf UCX_CUDA_IPC_CACHE_MAX_SIZE=16G
export UCX_NET_DEVICES_GPU_0=rdmap145s0:1,enp135s0 UCX_NET_DEVICES_GPU_1=rdmap145s0:1,enp135s0
export UCX_NET_DEVICES_GPU_2=rdmap162s0:1,enp135s0 UCX_NET_DEVICES_GPU_3=rdmap162s0:1,enp135s0
export UCX_NET_DEVICES_GPU_4=rdmap179s0:1,enp135s0 UCX_NET_DEVICES_GPU_5=rdmap179s0:1,enp135s0
export UCX_NET_DEVICES_GPU_6=rdmap196s0:1,enp135s0 UCX_NET_DEVICES_GPU_7=rdmap196s0:1,enp135s0

# S3 through KvikIO: each GPU pair on its NUMA-local ENA NIC.
if [[ $NIC_BIND == true ]]; then
  export KVIKIO_REMOTE_IO_INTERFACE_GPU_0='if!enp135s0' KVIKIO_REMOTE_IO_INTERFACE_GPU_1='if!enp135s0'
  export KVIKIO_REMOTE_IO_INTERFACE_GPU_2='if!enp153s0' KVIKIO_REMOTE_IO_INTERFACE_GPU_3='if!enp153s0'
  export KVIKIO_REMOTE_IO_INTERFACE_GPU_4='if!enp170s0' KVIKIO_REMOTE_IO_INTERFACE_GPU_5='if!enp170s0'
  export KVIKIO_REMOTE_IO_INTERFACE_GPU_6='if!enp187s0' KVIKIO_REMOTE_IO_INTERFACE_GPU_7='if!enp187s0'
fi

# KvikIO as in the WXD-launch runs (KVIKIO_NTHREADS=16 comes from --kvikio-threads below).
# MULTI_POLL needs the task size to be at most the bounce buffer size.
export KVIKIO_REMOTE_IO_BACKEND=MULTI_POLL KVIKIO_REMOTE_IO_NUM_REACTORS=4
export KVIKIO_REMOTE_IO_REACTOR_DISPATCH=${DISPATCH} KVIKIO_REMOTE_IO_MAX_CONCURRENT_REQUESTS=128
export KVIKIO_TASK_SIZE=33554432 KVIKIO_BOUNCE_BUFFER_SIZE=33554432
export KVIKIO_REMOTE_IO_NONTEMPORAL_COPY=${NONTEMPORAL_COPY}

# 1. Generate configs: 8 workers, 2 drivers each.
VARIANT_TYPE=gpu NUM_WORKERS=8 GPU_IDS=0,1,2,3,4,5,6,7 VCPU_PER_WORKER=2 OVERWRITE_CONFIG=true \
  ./generate_presto_config.sh

# 2. Align the generated configs with the WXD-launch runs.
G=../docker/config/generated/gpu
setprop() { if grep -q "^$2=" "$1"; then sed -i "s|^$2=.*|$2=$3|" "$1"; else echo "$2=$3" >> "$1"; fi; }
for i in 0 1 2 3 4 5 6 7; do
  W=$G/etc_worker_$i
  setprop $W/node.properties node.internal-address "$HOST_IP"
  setprop $W/config_native.properties discovery.uri http://127.0.0.1:8080
  setprop $W/config_native.properties async-data-cache-enabled $ASYNC_DATA_CACHE_ENABLED
  setprop $W/config_native.properties system-memory-gb 219
  setprop $W/config_native.properties query-memory-gb 153
  setprop $W/config_native.properties system-mem-limit-gb 229
  setprop $W/config_native.properties query.max-memory-per-node 153GB
  setprop $W/config_native.properties cudf.partitioned_output_batch_rows 10000000
  setprop $W/config_native.properties cudf.batch_size_min_threshold 40000000
  setprop $W/config_native.properties cudf.kvikio_cache_threads 16
  setprop $W/config_native.properties driver.max-split-preload 2
  setprop $W/config_native.properties exchange.max-buffer-size 512MB
  setprop $W/config_native.properties exchange.max-response-size 64MB
  setprop $W/config_native.properties sink.max-buffer-size 512MB
  setprop $W/config_native.properties local-exchange.max-buffer-size 536870912
  setprop $W/config_native.properties announcement-max-frequency-ms 1000
  setprop $W/catalog/hive.properties hive.s3.max-connections 96
  setprop $W/catalog/hive.properties cudf.hive.preload-column-chunks false
  # The worker configs already carry cudf.exchange=true, a per-worker
  # cudf.exchange.server.port and cudf.intra_node_exchange=true; the coordinator only
  # names the UCX transport when the native_cudf_exchange_enabled session property is set.
  if [[ $EXCHANGE == ucx ]]; then
    setprop $W/config_native.properties ucxx.error_handling false
    setprop $W/config_native.properties ucxx.blocking_progress false
  fi
done
C=$G/etc_coordinator/config_native.properties
setprop $C join-max-broadcast-table-size 20788MB
setprop $C node-scheduler.schedule-splits-based-on-task-load true
setprop $C exchange.max-buffer-size 512MB
setprop $C exchange.max-response-size 64MB
setprop $C sink.max-buffer-size 512MB
setprop $C query.max-execution-time 100m

# 3. Start the cluster with the configs above.
./start_native_gpu_presto.sh --skip-generate-config -w 8 -g 0,1,2,3,4,5,6,7 --kvikio-threads 16 --num-drivers 2

# 4. The start script does not wait for the workers; wait until all 8 are registered.
for _ in $(seq 150); do
  [[ $(curl -s localhost:8080/v1/node | jq length 2>/dev/null || echo 0) == 8 ]] && break
  sleep 2
done
echo "Registered workers: $(curl -s localhost:8080/v1/node | jq length)"

# 5. Record what worker 3 actually runs with.
docker inspect presto-native-worker-gpu-3 --format 'network={{.HostConfig.NetworkMode}}'
docker exec presto-native-worker-gpu-3 env | grep -E '^(KVIKIO_|UCX_TLS|UCX_NET_DEVICES)' | sort

if [[ $START_ONLY == true ]]; then
  exit 0
fi

# 6. Run the benchmark.
if [[ -z $TAG ]]; then
  TAG="${SCHEMA_NAME}_g7e_48xlarge_${IMAGE_TAG}_${EXCHANGE}_adc_${ASYNC_DATA_CACHE}_${CACHE_MODE}"
  [[ $DISPATCH != SHARED_QUEUE ]] && TAG+="_${DISPATCH,,}"
  [[ $NONTEMPORAL_COPY == false ]] && TAG+=_no_nontemporal_copy
  [[ $NIC_BIND == false ]] && TAG+=_no_nic_bind
  TAG+="_q${QUERIES:-all}_$(date -u +%Y%m%dT%H%M%SZ)"
  TAG=${TAG//[,.-]/_}
fi
# Session properties from the WXD-launch runs. The last two exist only in a coordinator
# built with the Presto UCX exchange and split placement changes.
SESSION=(hive.file_splittable=false schedule_splits_based_on_task_load=true
         hive.node_selection_strategy=SOFT_AFFINITY native_max_split_preload_per_driver=2)
if [[ $EXCHANGE == ucx ]]; then
  SESSION+=(native_cudf_exchange_enabled=true experimental_deterministic_bounded_splits=true)
fi
SESSION_ARGS=()
for p in "${SESSION[@]}"; do
  SESSION_ARGS+=(--session-property "$p")
done
./run_benchmark.sh -b tpch -s "${SCHEMA_NAME}" -i "${ITERATIONS}" -t "${TAG}" -m ${QUERIES:+-q "$QUERIES"} \
  "${SESSION_ARGS[@]}" --cache-mode "${CACHE_MODE}" --skip-drop-cache --skip-analyze-check \
  || echo "run_benchmark.sh reported failures; stopping the cluster anyway"

./stop_presto.sh
