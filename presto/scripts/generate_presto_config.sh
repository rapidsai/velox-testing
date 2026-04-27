#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

function echo_error {
  echo -e "${RED}$1${NC}"
  exit 1
}

function echo_warning {
  echo -e "${YELLOW}$1${NC}"
}

function echo_success {
  echo -e "${GREEN}$1${NC}"
}

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ ! -x "${SCRIPT_DIR}/../pbench/pbench" ]; then
  echo_error "ERROR: generate_presto_config.sh script cannot find pbench at ${SCRIPT_DIR}/../pbench/pbench"
fi

# This function duplicates the worker configs when we are running multiple workers.
# It also adds certain config options to the workers if those options apply only to multi-worker environments.
function duplicate_worker_configs() {
  local worker_id=$1
  echo "Duplicating worker configs for worker ID $worker_id"
  local worker_config="${CONFIG_DIR}/etc_worker_${worker_id}"
  local coord_config="${CONFIG_DIR}/etc_coordinator"
  local worker_native_config="${worker_config}/config_native.properties"
  local coord_native_config="${coord_config}/config_native.properties"
  # Need to stagger the port numbers because ucx exchange currently expects to be exactly
  # 3 higher than the http port.
  local http_port="10$(printf "%02d\n" "$worker_id")0"
  local exch_port="10$(printf "%02d\n" "$worker_id")3"
  rm -rf ${worker_config}
  cp -r ${CONFIG_DIR}/etc_worker ${worker_config}

  # Some configs should only be applied if we are in a multi-worker environment.
  if [[ ${NUM_WORKERS} -gt 1 ]]; then
    sed -i "s+single-node-execution-enabled.*+single-node-execution-enabled=false+g" ${coord_native_config}
    sed -i "s+single-node-execution-enabled.*+single-node-execution-enabled=false+g" ${worker_native_config}
    # make cudf.exchange=true if we are running multiple workers
    sed -i "s+cudf.exchange=false+cudf.exchange=true+g" ${worker_native_config}
    # make join-distribution-type=PARTITIONED if we are running multiple workers
  fi

  # Each worker node needs to have it's own http-server port.  This isn't used, but
  # the cudf.exchange server port is currently hard-coded to be the server port +3
  # and that needs to be unique for each worker.
  sed -i "s+http-server\.http\.port.*+http-server\.http\.port=${http_port}+g" ${worker_native_config}
  sed -i "s+cudf.exchange.server.port=.*+cudf.exchange.server.port=${exch_port}+g" ${worker_native_config}
  # Give each worker a unique id.
  sed -i "s+node\.id.*+node\.id=worker_${worker_id}+g" ${worker_config}/node.properties
}

# get host values
NPROC=$(nproc)
# lsmem will report in SI.  Make sure we get values in GB.
RAM_GB=$(lsmem -b | grep "Total online memory" | awk '{print int($4 / (1024*1024*1024)); }')

# Detect host NUMA / SMT topology. Used below to auto-tune CPU workers so the
# same scripts produce sensible configs on any host (not just 0374).
# `lscpu -p` emits one machine-parseable row per logical CPU; each row has
# fields `CPU,Core,Socket,Node,...`. Counting unique (Socket,Core) pairs gives
# the physical-core count regardless of SMT state or lscpu locale strings.
NUMA_NODES=$(ls -d /sys/devices/system/node/node* 2>/dev/null | wc -l)
[[ ${NUMA_NODES} -lt 1 ]] && NUMA_NODES=1
PHYSICAL_CORES=$(lscpu -p 2>/dev/null | grep -v '^#' | awk -F, '{print $3","$2}' | sort -u | wc -l)
[[ -z "${PHYSICAL_CORES}" || "${PHYSICAL_CORES}" -lt 1 ]] && PHYSICAL_CORES=${NPROC}
SMT_RATIO=$(( NPROC / PHYSICAL_CORES ))
[[ ${SMT_RATIO} -lt 1 ]] && SMT_RATIO=1

# variant-specific behavior
# for GPU you must set vcpu_per_worker to a small number, not the CPU count
if [[ -z ${VARIANT_TYPE} || ! ${VARIANT_TYPE} =~ ^(cpu|gpu|java)$ ]]; then
  echo_error "ERROR: VARIANT_TYPE must be set to a valid variant type (cpu, gpu, java)."
fi
if [[ -z ${VCPU_PER_WORKER:-} ]]; then
  if [[ "${VARIANT_TYPE}" == "gpu" ]]; then
    VCPU_PER_WORKER=2
  else
    VCPU_PER_WORKER=${NPROC}
  fi
fi

# move to config directory
pushd "${SCRIPT_DIR}/../docker/config" >/dev/null

# always move back even on failure
trap "popd > /dev/null" EXIT

CONFIG_DIR=generated/${VARIANT_TYPE}

# generate only if no existing config or overwrite flag is set
if [[ ! -d ${CONFIG_DIR} || "${OVERWRITE_CONFIG}" == "true" ]]; then
  echo "Generating Presto Config files for '${VARIANT_TYPE}' for host with ${NPROC} CPU cores and ${RAM_GB}GB RAM"

  # (re-)generate the config.json file
  rm -rf ${CONFIG_DIR}
  mkdir -p ${CONFIG_DIR}
  cat >${CONFIG_DIR}/config.json <<EOF
{
    "cluster_size": "small",
    "coordinator_instance_type": "${NPROC}-core CPU and ${RAM_GB}GB RAM",
    "coordinator_instance_ebs_size": 50,
    "worker_instance_type": "${NPROC}-core CPU and ${RAM_GB}GB RAM",
    "worker_instance_ebs_size": 50,
    "number_of_workers": ${NUM_WORKERS},
    "memory_per_node_gb": ${RAM_GB},
    "vcpu_per_worker": ${VCPU_PER_WORKER},
    "fragment_result_cache_enabled": true,
    "data_cache_enabled": true
}
EOF

  # run pbench to generate the config files
  # hide default pbench logging which goes to stderr so we only see any errors
  if "${SCRIPT_DIR}/../pbench/pbench" genconfig -p params.json -t template ${CONFIG_DIR} 2>&1 | grep '\{\"level":"error"'; then
    echo_error "ERROR: Errors reported by pbench genconfig. Configs were not generated successfully."
  fi

  if [ -n "${HIVE_METASTORE_URI:-}" ]; then
    sed -i 's/hive.metastore=file/#hive.metastore=file/' "${CONFIG_DIR}/etc_coordinator/catalog/hive.properties" "${CONFIG_DIR}/etc_worker/catalog/hive.properties"
    sed -i "s|hive.metastore.catalog.dir=.*|hive.metastore.uri=${HIVE_METASTORE_URI}|" "${CONFIG_DIR}/etc_coordinator/catalog/hive.properties" "${CONFIG_DIR}/etc_worker/catalog/hive.properties"
  fi

  COORD_CONFIG="${CONFIG_DIR}/etc_coordinator/config_native.properties"
  WORKER_CONFIG="${CONFIG_DIR}/etc_worker/config_native.properties"
  # now perform other variant-specific modifications to the generated configs
  if [[ "${VARIANT_TYPE}" == "gpu" ]]; then
    # for GPU variant, uncomment these optimizer settings
    # optimizer.joins-not-null-inference-strategy=USE_FUNCTION_METADATA
    # optimizer.default-filter-factor-enabled=true
    sed -i 's/\#optimizer/optimizer/g' ${COORD_CONFIG}
    echo "cluster-tag=native-gpu" >>${COORD_CONFIG}
  fi

  if [[ "${VARIANT_TYPE}" == "cpu" ]]; then
    echo "cluster-tag=native-cpu" >>${COORD_CONFIG}
  fi

  # for Java variant, disable some Parquet properties which are now rejected
  if [[ "${VARIANT_TYPE}" == "java" ]]; then
    HIVE_CONFIG="${CONFIG_DIR}/etc_worker/catalog/hive.properties"
    sed -i 's/parquet\.reader\.chunk-read-limit/#parquet\.reader\.chunk-read-limit/' ${HIVE_CONFIG}
    sed -i 's/parquet\.reader\.pass-read-limit/#parquet\.reader\.pass-read-limit/' ${HIVE_CONFIG}
    sed -i 's/^cudf/#cudf/' ${HIVE_CONFIG}
  fi

  if [[ "${VARIANT_TYPE}" != "gpu" ]]; then
    HIVE_CONFIG="${CONFIG_DIR}/etc_worker/catalog/hive.properties"
    sed -i 's/hive.file-splittable=false/hive.file-splittable=true/' ${HIVE_CONFIG}
    HIVE_CONFIG="${CONFIG_DIR}/etc_coordinator/catalog/hive.properties"
    sed -i 's/hive.file-splittable=false/hive.file-splittable=true/' ${HIVE_CONFIG}
  fi

  # success message
  echo_success "Configs were generated successfully"
else
  # otherwise, reuse existing config
  echo_success "Reusing existing Presto Config files for '${VARIANT_TYPE}'"
fi

# We want to propagate any changes from the original worker config to the new worker configs even if
# we did not re-generate the configs.
#
# GPU always gets per-worker config dirs (even -w 1) because its compose
# template mounts etc_worker_<id> in both multi-worker and combined-container
# layouts. GPU IDs are explicit (via GPU_IDS) since each worker is bound to
# a specific device.
#
# CPU only needs per-worker dirs when multi-worker (-w N > 1); the -w 1 CPU
# path mounts etc_worker directly, matching historical single-container
# behaviour. CPU workers use 0-indexed sequential IDs.
if [[ -n "$NUM_WORKERS" && ( "$VARIANT_TYPE" == "gpu" || ( "$VARIANT_TYPE" == "cpu" && "$NUM_WORKERS" -gt 1 ) ) ]]; then
  if [[ "$VARIANT_TYPE" == "gpu" && -n ${GPU_IDS:-} ]]; then
    WORKER_IDS=($(echo "$GPU_IDS" | tr ',' ' '))
  else
    WORKER_IDS=($(seq 0 $((NUM_WORKERS - 1))))
  fi
  for i in "${WORKER_IDS[@]}"; do
    duplicate_worker_configs $i
  done
fi

# Reconcile single-node-execution-enabled and cudf.exchange on every start.
#
# duplicate_worker_configs flips both to their multi-worker values when
# NUM_WORKERS > 1 (single-node-execution-enabled=false on coord + workers,
# cudf.exchange=true on workers), and never reverts them for NUM_WORKERS == 1.
# Switching from -w N>1 to -w 1 without `--overwrite-config` therefore leaves
# stale multi-worker values in place. Concretely, stale
# single-node-execution-enabled=false on the coord makes the planner keep
# generating multi-stage distributed plans, which a lone worker then executes
# with HTTP-exchange roundtrips between stages — measured 5x regression on
# TPC-H Q17 for both -w 1 CPU and -w 1 GPU after a multi-worker run.
#
# Always set both to match the current NUM_WORKERS so toggling worker count
# doesn't require --overwrite-config.
if [[ -n "$NUM_WORKERS" && ( "$VARIANT_TYPE" == "gpu" || "$VARIANT_TYPE" == "cpu" ) ]]; then
  if [[ "$NUM_WORKERS" -gt 1 ]]; then
    SINGLE_NODE_EXECUTION="false"
    CUDF_EXCHANGE="true"
  else
    SINGLE_NODE_EXECUTION="true"
    CUDF_EXCHANGE="false"
  fi
  for cfg in \
    "${CONFIG_DIR}/etc_coordinator/config_native.properties" \
    "${CONFIG_DIR}"/etc_worker*/config_native.properties; do
    [[ -f "$cfg" ]] || continue
    sed -i "s/^single-node-execution-enabled=.*/single-node-execution-enabled=${SINGLE_NODE_EXECUTION}/" "$cfg"
  done
  for cfg in "${CONFIG_DIR}"/etc_worker*/config_native.properties; do
    [[ -f "$cfg" ]] || continue
    sed -i "s/^cudf.exchange=.*/cudf.exchange=${CUDF_EXCHANGE}/" "$cfg"
  done
fi

# CPU auto-tuning keyed on NUM_WORKERS + detected host topology.
# Runs idempotently on every start so manual sed-per-run isn't required, and
# switching between `-w 1` and `-w 2` just needs `stop_presto.sh && start_native_cpu_presto.sh -w N`.
# Any value can be overridden via env var.
#
# Logic:
#   -w 1 -> container spans all NUMA nodes (launcher uses --interleave=all).
#           Give it the full host memory; drivers = min(NPROC, 255) to max SMT
#           up to Velox's uint8_t cap on parallel hash-build tables.
#   -w N -> each container pins to one NUMA node via NUMA_NODE env var.
#           Memory budget = per-node RAM; drivers = min(threads-per-node, 255)
#           so each worker has 1 driver per hw thread on its socket, no cross-
#           container oversubscription of the NUMA's cores.
# Cache headroom (system-memory-gb - query-memory-gb) defaults to ~30% of the
# worker's memory envelope, enough for the async data cache to hold a
# meaningful chunk of SF1000 TPC-H.
if [[ "${VARIANT_TYPE}" == "cpu" ]]; then
  : "${CPU_ASYNC_DATA_CACHE:=true}"
  if [[ "${NUM_WORKERS:-1}" -gt 1 ]]; then
    THREADS_PER_WORKER=$(( NPROC / NUM_WORKERS ))
    RAM_PER_WORKER_GB=$(( RAM_GB / NUM_WORKERS ))
    : "${CPU_SYSTEM_MEM_GB:=$(( RAM_PER_WORKER_GB - 35 ))}"
    : "${CPU_DRIVERS:=$(( THREADS_PER_WORKER < 255 ? THREADS_PER_WORKER : 255 ))}"
    # Multi-worker bottlenecks on HTTP exchange backpressure for shuffle-heavy
    # queries (Q9/Q18 time out at 30m with the 32MB defaults). 16x larger
    # buffers unlock those queries on SF1000.
    : "${CPU_EXCHANGE_BUFFER:=512MB}"
    : "${CPU_SINK_BUFFER:=512MB}"
  else
    : "${CPU_SYSTEM_MEM_GB:=${RAM_GB}}"
    : "${CPU_DRIVERS:=$(( NPROC < 255 ? NPROC : 255 ))}"
    # Single-worker has no inter-worker shuffle, so the exchange buffers
    # only matter for in-process exchange. Use Velox's defaults (32MB) —
    # bigger buffers add latency on small shuffles for no benefit here.
    : "${CPU_EXCHANGE_BUFFER:=32MB}"
    : "${CPU_SINK_BUFFER:=32MB}"
  fi
  # ~30% of the worker's memory envelope reserved for the Velox async data cache
  : "${CPU_QUERY_MEM_GB:=$(( CPU_SYSTEM_MEM_GB * 70 / 100 ))}"
  CPU_SYSTEM_MEM_LIMIT_GB=$(( CPU_SYSTEM_MEM_GB + 30 ))

  echo "CPU auto-tune: NUM_WORKERS=${NUM_WORKERS:-1} NPROC=${NPROC} NUMA_NODES=${NUMA_NODES} PHYSICAL_CORES=${PHYSICAL_CORES} SMT_RATIO=${SMT_RATIO}"
  echo "               system-memory-gb=${CPU_SYSTEM_MEM_GB} query-memory-gb=${CPU_QUERY_MEM_GB} task.max-drivers-per-task=${CPU_DRIVERS} async-data-cache-enabled=${CPU_ASYNC_DATA_CACHE}"
  echo "               exchange.max-buffer-size=${CPU_EXCHANGE_BUFFER} sink.max-buffer-size=${CPU_SINK_BUFFER}"

  # set_or_append <key> <value> <file>: idempotent — replaces existing
  # `key=...` line if present, otherwise appends. Used here because the
  # buffer-size keys aren't in the pbench template (Velox falls back to
  # built-in defaults), so on first apply we have to append; on subsequent
  # applies we replace.
  set_or_append() {
    local key=$1 value=$2 cfg=$3
    if grep -q "^${key}=" "$cfg"; then
      sed -i "s|^${key}=.*|${key}=${value}|" "$cfg"
    else
      echo "${key}=${value}" >> "$cfg"
    fi
  }

  # Apply to every CPU worker config dir: etc_worker (single-worker + template
  # for duplicate_worker_configs) plus etc_worker_<N> (multi-worker instances).
  for worker_dir in "${CONFIG_DIR}"/etc_worker*/; do
    cfg="${worker_dir}config_native.properties"
    [[ -f "$cfg" ]] || continue
    sed -i "s/^system-memory-gb=.*/system-memory-gb=${CPU_SYSTEM_MEM_GB}/" "$cfg"
    sed -i "s/^system-mem-limit-gb=.*/system-mem-limit-gb=${CPU_SYSTEM_MEM_LIMIT_GB}/" "$cfg"
    sed -i "s/^query-memory-gb=.*/query-memory-gb=${CPU_QUERY_MEM_GB}/" "$cfg"
    sed -i "s|^query.max-memory-per-node=.*|query.max-memory-per-node=${CPU_QUERY_MEM_GB}GB|" "$cfg"
    sed -i "s/^task.max-drivers-per-task=.*/task.max-drivers-per-task=${CPU_DRIVERS}/" "$cfg"
    sed -i "s/^async-data-cache-enabled=.*/async-data-cache-enabled=${CPU_ASYNC_DATA_CACHE}/" "$cfg"
    set_or_append "exchange.max-buffer-size" "${CPU_EXCHANGE_BUFFER}" "$cfg"
    set_or_append "sink.max-buffer-size" "${CPU_SINK_BUFFER}" "$cfg"
  done

  # hive.file-splittable flip for CPU. Put outside the regen gate so restarts
  # without a full regen still land the right value; idempotent because the
  # pattern only matches the untouched GPU-oriented template value.
  for hive_cfg in \
    "${CONFIG_DIR}/etc_coordinator/catalog/hive.properties" \
    "${CONFIG_DIR}"/etc_worker*/catalog/hive.properties; do
    [[ -f "$hive_cfg" ]] || continue
    sed -i 's/^hive.file-splittable=false/hive.file-splittable=true/' "$hive_cfg"
  done
fi
