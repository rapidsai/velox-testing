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
source "${SCRIPT_DIR}/common_functions.sh"

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
    sed -i "s+single-node-execution-enabled.*+single-node-execution-enabled=false+g" ${coord_native_config} ${worker_native_config}
    # cuDF exchange is GPU-only. CPU distributed exchange is selected by the
    # plan transport plus VELOX_UCX_CPU_EXCHANGE, not this worker property.
    if [[ "${VARIANT_TYPE}" == "gpu" ]]; then
      sed -i "s+cudf.exchange=false+cudf.exchange=true+g" ${worker_native_config}
    fi
  fi

  # Each worker node needs to have it's own http-server port.  This isn't used, but
  # the cudf.exchange server port is currently hard-coded to be the server port +3
  # and that needs to be unique for each worker.
  sed -i "s+http-server\.http\.port.*+http-server\.http\.port=${http_port}+g" ${worker_native_config}
  sed -i "s+cudf.exchange.server.port=.*+cudf.exchange.server.port=${exch_port}+g" ${worker_native_config}
  # Give each worker a unique id.
  sed -i "s+node\.id.*+node\.id=worker_${worker_id}+g" ${worker_config}/node.properties
}

# set_or_append_property <key> <value> <file>
# Variant override files append properties after the common templates. Keep
# subsequent topology tuning idempotent whether a key came from either source
# or is being introduced here for the first time.
# Values must not contain sed metacharacters (& or \). All callers in this
# script pass integers, booleans, or simple strings (e.g. 512MB, 10m) so this
# is safe without escaping.
function set_or_append_property() {
  local key=$1 value=$2 cfg=$3
  if grep -q "^${key}=" "${cfg}"; then
    sed -i "s|^${key}=.*|${key}=${value}|" "${cfg}"
  else
    echo "${key}=${value}" >> "${cfg}"
  fi
}

# Get host values. lsmem can be installed but unusable when memory sysfs is
# hidden from a container, so fall back to /proc/meminfo. NPROC/RAM_GB may be
# supplied explicitly to make topology tests reproducible.
NPROC="${NPROC:-$(nproc)}"
RAM_GB="${RAM_GB:-}"
if [[ -z "${RAM_GB}" ]] && command -v lsmem >/dev/null 2>&1; then
  RAM_GB="$(lsmem -b 2>/dev/null \
    | awk '/Total online memory/ { print int($4 / (1024*1024*1024)); exit }' \
    || true)"
fi
if [[ -z "${RAM_GB}" && -r /proc/meminfo ]]; then
  RAM_GB="$(awk '/^MemTotal:/ { print int($2 / (1024*1024)); exit }' /proc/meminfo)"
fi
if [[ ! "${NPROC}" =~ ^[1-9][0-9]*$ || ! "${RAM_GB}" =~ ^[1-9][0-9]*$ ]]; then
  echo_error "ERROR: could not determine positive NPROC/RAM_GB host values. Set them explicitly."
fi

# Linux exposes Grace CPU/DRAM nodes and accelerator/HBM proximity domains in
# the same NUMA namespace. Count them separately: only nodes with both CPUs and
# memory are valid targets for CPU workers.
NUMA_TOPOLOGY_AVAILABLE=0
if discover_cpu_numa_topology; then
  NUMA_TOPOLOGY_AVAILABLE=1
fi
NUMA_NODES="${VISIBLE_NUMA_NODE_COUNT:-0}"
(( NUMA_NODES > 0 )) || NUMA_NODES=1

# Diagnostic only — logged in the auto-tune summary below for SMT context.
# Neither variable is applied to any Presto property; CPU_DRIVERS defaults to
# logical threads per worker and can be overridden explicitly via CPU_DRIVERS=N.
PHYSICAL_CORES=0
if command -v lscpu >/dev/null 2>&1; then
  PHYSICAL_CORES=$(lscpu -p 2>/dev/null \
    | awk -F, '!/^#/ { cores[$3 "," $2] = 1 } END { print length(cores) }')
fi
[[ "${PHYSICAL_CORES}" =~ ^[0-9]+$ ]] || PHYSICAL_CORES=0
(( PHYSICAL_CORES > 0 )) || PHYSICAL_CORES=${NPROC}
SMT_RATIO=$((NPROC / PHYSICAL_CORES))
(( SMT_RATIO > 0 )) || SMT_RATIO=1

# variant-specific behavior
# for GPU you must set vcpu_per_worker to a small number, not the CPU count
if [[ -z ${VARIANT_TYPE:-} || ! ${VARIANT_TYPE} =~ ^(cpu|gpu|java)$ ]]; then
  echo_error "ERROR: VARIANT_TYPE must be set to a valid variant type (cpu, gpu, java)."
fi
if [[ ! "${NUM_WORKERS:-}" =~ ^[1-9][0-9]*$ ]]; then
  echo_error "ERROR: NUM_WORKERS must be a positive integer."
fi

# NUM_WORKERS describes the whole Presto cluster. CPU resources, however, are
# divided only among workers sharing this host. Slurm supplies
# CPU_WORKERS_PER_HOST=NUM_GPUS_PER_NODE; local Docker runs fall back to the
# total because every worker is local there.
if [[ "${VARIANT_TYPE}" == "cpu" ]]; then
  CPU_WORKERS_PER_HOST="${CPU_WORKERS_PER_HOST:-${NUM_WORKERS}}"
  if [[ ! "${CPU_WORKERS_PER_HOST}" =~ ^[1-9][0-9]*$ ]]; then
    echo_error "ERROR: CPU_WORKERS_PER_HOST must be a positive integer."
  fi
  if (( CPU_WORKERS_PER_HOST > NUM_WORKERS )); then
    echo_error "ERROR: CPU_WORKERS_PER_HOST (${CPU_WORKERS_PER_HOST}) cannot exceed NUM_WORKERS (${NUM_WORKERS})."
  fi
  if (( CPU_WORKERS_PER_HOST > NPROC )); then
    echo_error "ERROR: CPU_WORKERS_PER_HOST (${CPU_WORKERS_PER_HOST}) exceeds the ${NPROC} available logical CPUs."
  fi

  if [[ ! "${USE_NUMA:-0}" =~ ^[01]$ ]]; then
    echo_error "ERROR: USE_NUMA must be 0 or 1; got '${USE_NUMA:-}'."
  fi
  CPU_RESOURCE_POLICY="host-wide"
  CPU_THREADS_PER_WORKER=$((NPROC / CPU_WORKERS_PER_HOST))
  CPU_RAM_PER_WORKER_GB=$((RAM_GB / CPU_WORKERS_PER_HOST))
  CPU_NUMA_WORKER_LAYOUT="unbound"

  if [[ "${USE_NUMA:-0}" == "1" ]]; then
    if (( NUMA_TOPOLOGY_AVAILABLE == 0 )); then
      echo_error "ERROR: CPU NUMA binding was requested, but no NUMA node with both CPUs and memory was discovered."
    fi

    # Match the launcher's balanced contiguous placement. Workers are sized
    # to the smallest share assigned anywhere on the host. Placement yields
    # 0; 0,1; and 0,0,1,1 for one, two, and four workers on a two-socket
    # Grace host without treating HBM nodes as CPU sockets.
    declare -a cpu_numa_worker_counts=()
    local_worker_id=0
    while (( local_worker_id < CPU_WORKERS_PER_HOST )); do
      node_index=$((local_worker_id * CPU_NUMA_NODE_COUNT / CPU_WORKERS_PER_HOST))
      cpu_numa_worker_counts[${node_index}]=$(( ${cpu_numa_worker_counts[${node_index}]:-0} + 1 ))
      local_worker_id=$((local_worker_id + 1))
    done

    CPU_NUMA_WORKER_LAYOUT=""
    CPU_THREADS_PER_WORKER=0
    CPU_RAM_PER_WORKER_GB=0
    node_index=0
    while (( node_index < CPU_NUMA_NODE_COUNT )); do
      workers_on_node="${cpu_numa_worker_counts[${node_index}]:-0}"
      if (( workers_on_node > 0 )); then
        candidate_threads=$((CPU_NUMA_NODE_CPU_COUNTS[${node_index}] / workers_on_node))
        candidate_memory_gb=$((CPU_NUMA_NODE_MEMORY_GB[${node_index}] / workers_on_node))
        if (( CPU_THREADS_PER_WORKER == 0 || candidate_threads < CPU_THREADS_PER_WORKER )); then
          CPU_THREADS_PER_WORKER="${candidate_threads}"
        fi
        if (( CPU_RAM_PER_WORKER_GB == 0 || candidate_memory_gb < CPU_RAM_PER_WORKER_GB )); then
          CPU_RAM_PER_WORKER_GB="${candidate_memory_gb}"
        fi
        CPU_NUMA_WORKER_LAYOUT+="${CPU_NUMA_WORKER_LAYOUT:+,}${CPU_NUMA_NODE_IDS[${node_index}]}:${workers_on_node}"
      fi
      node_index=$((node_index + 1))
    done

    if (( CPU_THREADS_PER_WORKER < 1 || CPU_RAM_PER_WORKER_GB < 1 )); then
      echo_error "ERROR: CPU NUMA placement left fewer than one CPU or 1GB of DRAM per worker."
    fi
    CPU_RESOURCE_POLICY="cpu-numa-bound"
  fi
fi

if [[ -z ${VCPU_PER_WORKER:-} ]]; then
  if [[ "${VARIANT_TYPE}" == "gpu" ]]; then
    VCPU_PER_WORKER=2
  elif [[ "${VARIANT_TYPE}" == "cpu" ]]; then
    VCPU_PER_WORKER="${CPU_THREADS_PER_WORKER}"
    (( VCPU_PER_WORKER > 0 )) || VCPU_PER_WORKER=1
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
if [[ ! -d ${CONFIG_DIR} || "${OVERWRITE_CONFIG:-false}" == "true" ]]; then
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

  # Apply variant-specific override files by appending them to the generated configs
  OVERRIDES_DIR="${SCRIPT_DIR}/../docker/config/template/overrides/${VARIANT_TYPE}"
  if [[ -d "${OVERRIDES_DIR}" ]]; then
    mapfile -d '' override_files < <(find "${OVERRIDES_DIR}" -type f -print0)
    for src_file in "${override_files[@]}"; do
      rel_path="${src_file#${OVERRIDES_DIR}/}"
      dest_file="${CONFIG_DIR}/${rel_path}"
      if [[ -f "${dest_file}" ]]; then
        cat "${src_file}" >> "${dest_file}"
      fi
    done
  fi

  # success message
  echo_success "Configs were generated successfully"
else
  # otherwise, reuse existing config
  echo_success "Reusing existing Presto Config files for '${VARIANT_TYPE}'"
fi

# We want to propagate any changes from the original worker config to the new worker configs even if
# we did not re-generate the configs.
if [[ -n "$NUM_WORKERS" ]]; then
  if [[ -n ${GPU_IDS:-} ]]; then
    WORKER_IDS=($(echo "$GPU_IDS" | tr ',' ' '))
  else
    WORKER_IDS=($(seq 0 $((NUM_WORKERS - 1))))
  fi
  for i in "${WORKER_IDS[@]}"; do
    duplicate_worker_configs $i
  done
fi

# Reconcile settings that depend on the total cluster size on every run. This
# prevents a reused generated directory from retaining distributed settings
# after switching back to one worker (or vice versa).
if [[ "${VARIANT_TYPE}" == "gpu" || "${VARIANT_TYPE}" == "cpu" ]]; then
  if (( NUM_WORKERS > 1 )); then
    SINGLE_NODE_EXECUTION=false
  else
    SINGLE_NODE_EXECUTION=true
  fi
  CUDF_EXCHANGE=false
  if [[ "${VARIANT_TYPE}" == "gpu" && "${SINGLE_NODE_EXECUTION}" == "false" ]]; then
    CUDF_EXCHANGE=true
  fi
  for cfg in \
    "${CONFIG_DIR}/etc_coordinator/config_native.properties" \
    "${CONFIG_DIR}"/etc_worker*/config_native.properties; do
    [[ -f "${cfg}" ]] || continue
    set_or_append_property "single-node-execution-enabled" "${SINGLE_NODE_EXECUTION}" "${cfg}"
  done
  for cfg in "${CONFIG_DIR}"/etc_worker*/config_native.properties; do
    [[ -f "${cfg}" ]] || continue
    set_or_append_property "cudf.exchange" "${CUDF_EXCHANGE}" "${cfg}"
  done
fi

# CPU defaults measured for the row-based UCX exchange path. Resource
# partitioning uses workers on this host, while exchange-buffer sizing uses the
# total cluster size because one worker on each of two hosts still shuffles.
# Every CPU_* value remains explicitly overridable for controlled sweeps.
if [[ "${VARIANT_TYPE}" == "cpu" ]]; then
  : "${CPU_ASYNC_DATA_CACHE:=true}"
  : "${CPU_SYSTEM_MEM_HEADROOM_GB:=35}"
  : "${CPU_SYSTEM_MEM_LIMIT_HEADROOM_GB:=5}"
  for headroom_var in CPU_SYSTEM_MEM_HEADROOM_GB CPU_SYSTEM_MEM_LIMIT_HEADROOM_GB; do
    if [[ ! "${!headroom_var}" =~ ^[1-9][0-9]*$ ]]; then
      echo_error "ERROR: ${headroom_var} must be a positive integer; got '${!headroom_var}'."
    fi
  done

  # MmapAllocator reserves virtual address space for every size class during
  # startup. Giving even a single worker all of MemTotal can therefore fail
  # before it registers, and system-mem-limit-gb must never exceed the memory
  # available to that worker. Apply the same per-worker headroom for one or
  # many local workers; the previous single-worker special case used all RAM.
  THREADS_PER_WORKER="${CPU_THREADS_PER_WORKER}"
  RAM_PER_WORKER_GB="${CPU_RAM_PER_WORKER_GB}"
  if [[ -z "${CPU_SYSTEM_MEM_GB:-}" ]]; then
    if (( RAM_PER_WORKER_GB <= CPU_SYSTEM_MEM_HEADROOM_GB )); then
      echo_error "ERROR: only ${RAM_PER_WORKER_GB}GB is available per CPU worker, which does not leave the requested ${CPU_SYSTEM_MEM_HEADROOM_GB}GB headroom; use fewer workers per host or override the CPU memory settings."
    fi
    CPU_SYSTEM_MEM_GB=$((RAM_PER_WORKER_GB - CPU_SYSTEM_MEM_HEADROOM_GB))
  fi
  if [[ ! "${CPU_SYSTEM_MEM_GB}" =~ ^[1-9][0-9]*$ ]]; then
    echo_error "ERROR: CPU_SYSTEM_MEM_GB must be a positive integer; got '${CPU_SYSTEM_MEM_GB}'."
  fi
  : "${CPU_DRIVERS:=$((THREADS_PER_WORKER < 255 ? THREADS_PER_WORKER : 255))}"

  if (( NUM_WORKERS > 1 )); then
    : "${CPU_EXCHANGE_BUFFER:=512MB}"
    : "${CPU_SINK_BUFFER:=512MB}"
  else
    : "${CPU_EXCHANGE_BUFFER:=32MB}"
    : "${CPU_SINK_BUFFER:=32MB}"
  fi

  : "${CPU_QUERY_MEM_GB:=$((CPU_SYSTEM_MEM_GB * 70 / 100))}"
  : "${CPU_SYSTEM_MEM_LIMIT_GB:=$((RAM_PER_WORKER_GB - CPU_SYSTEM_MEM_LIMIT_HEADROOM_GB))}"
  : "${CPU_LOCAL_EXCHANGE_BUFFER:=512MB}"
  : "${CPU_EXCHANGE_MAX_RESPONSE_SIZE:=64MB}"
  : "${CPU_LARGEST_SIZE_CLASS_PAGES:=256}"
  : "${CPU_TABLE_SCAN_SHUFFLE_STRATEGY:=DISABLED}"
  : "${CPU_SCHEDULE_SPLITS_BASED_ON_TASK_LOAD:=true}"
  : "${CPU_MAX_SPLITS_PER_TASK:=512}"
  : "${CPU_QUERY_MAX_EXECUTION_TIME:=10m}"

  for numeric_var in CPU_QUERY_MEM_GB CPU_SYSTEM_MEM_LIMIT_GB CPU_DRIVERS CPU_LARGEST_SIZE_CLASS_PAGES CPU_MAX_SPLITS_PER_TASK; do
    if [[ ! "${!numeric_var}" =~ ^[1-9][0-9]*$ ]]; then
      echo_error "ERROR: ${numeric_var} must be a positive integer; got '${!numeric_var}'."
    fi
  done
  if (( CPU_SYSTEM_MEM_GB >= RAM_PER_WORKER_GB )); then
    echo_error "ERROR: CPU_SYSTEM_MEM_GB (${CPU_SYSTEM_MEM_GB}) must be smaller than the ${RAM_PER_WORKER_GB}GB available per CPU worker."
  fi
  if (( CPU_SYSTEM_MEM_LIMIT_GB < CPU_SYSTEM_MEM_GB || CPU_SYSTEM_MEM_LIMIT_GB > RAM_PER_WORKER_GB )); then
    echo_error "ERROR: CPU_SYSTEM_MEM_LIMIT_GB (${CPU_SYSTEM_MEM_LIMIT_GB}) must be at least CPU_SYSTEM_MEM_GB (${CPU_SYSTEM_MEM_GB}) and no greater than the ${RAM_PER_WORKER_GB}GB available per CPU worker."
  fi
  if (( CPU_QUERY_MEM_GB >= CPU_SYSTEM_MEM_GB )); then
    echo_error "ERROR: CPU_QUERY_MEM_GB (${CPU_QUERY_MEM_GB}) must be smaller than CPU_SYSTEM_MEM_GB (${CPU_SYSTEM_MEM_GB})."
  fi
  if (( CPU_LARGEST_SIZE_CLASS_PAGES < 256 || (CPU_LARGEST_SIZE_CLASS_PAGES & (CPU_LARGEST_SIZE_CLASS_PAGES - 1)) != 0 )); then
    echo_error "ERROR: CPU_LARGEST_SIZE_CLASS_PAGES must be a power of two of at least 256."
  fi

  echo "CPU topology: visible-nodes=${NUMA_NODES} cpu-nodes=${CPU_NUMA_NODE_COUNT:-0} cpu-node-ids=${CPU_NUMA_NODE_IDS[*]:-none} memory-node-ids=${MEMORY_NUMA_NODE_IDS[*]:-none}"
  echo "CPU auto-tune: policy=${CPU_RESOURCE_POLICY} layout=${CPU_NUMA_WORKER_LAYOUT} total-workers=${NUM_WORKERS} workers-per-host=${CPU_WORKERS_PER_HOST} NPROC=${NPROC} PHYSICAL_CORES=${PHYSICAL_CORES} SMT_RATIO=${SMT_RATIO}"
  echo "               memory-per-worker-gb=${RAM_PER_WORKER_GB} system-memory-gb=${CPU_SYSTEM_MEM_GB} query-memory-gb=${CPU_QUERY_MEM_GB} system-mem-limit-gb=${CPU_SYSTEM_MEM_LIMIT_GB}"
  echo "               task.max-drivers-per-task=${CPU_DRIVERS} async-data-cache-enabled=${CPU_ASYNC_DATA_CACHE} largest-size-class-pages=${CPU_LARGEST_SIZE_CLASS_PAGES}"
  echo "               local-exchange.max-buffer-size=${CPU_LOCAL_EXCHANGE_BUFFER} exchange.max-buffer-size=${CPU_EXCHANGE_BUFFER} sink.max-buffer-size=${CPU_SINK_BUFFER} exchange.max-response-size=${CPU_EXCHANGE_MAX_RESPONSE_SIZE}"
  echo "               optimizer.table-scan-shuffle-strategy=${CPU_TABLE_SCAN_SHUFFLE_STRATEGY} node-scheduler.schedule-splits-based-on-task-load=${CPU_SCHEDULE_SPLITS_BASED_ON_TASK_LOAD} node-scheduler.max-splits-per-task=${CPU_MAX_SPLITS_PER_TASK}"
  echo "               query.max-execution-time=${CPU_QUERY_MAX_EXECUTION_TIME}"

  coordinator_cfg="${CONFIG_DIR}/etc_coordinator/config_native.properties"
  set_or_append_property "optimizer.table-scan-shuffle-strategy" "${CPU_TABLE_SCAN_SHUFFLE_STRATEGY}" "${coordinator_cfg}"
  set_or_append_property "node-scheduler.schedule-splits-based-on-task-load" "${CPU_SCHEDULE_SPLITS_BASED_ON_TASK_LOAD}" "${coordinator_cfg}"
  set_or_append_property "node-scheduler.max-splits-per-task" "${CPU_MAX_SPLITS_PER_TASK}" "${coordinator_cfg}"
  set_or_append_property "exchange.max-buffer-size" "${CPU_EXCHANGE_BUFFER}" "${coordinator_cfg}"
  set_or_append_property "sink.max-buffer-size" "${CPU_SINK_BUFFER}" "${coordinator_cfg}"
  set_or_append_property "exchange.max-response-size" "${CPU_EXCHANGE_MAX_RESPONSE_SIZE}" "${coordinator_cfg}"
  set_or_append_property "query.max-execution-time" "${CPU_QUERY_MAX_EXECUTION_TIME}" "${coordinator_cfg}"

  for worker_dir in "${CONFIG_DIR}"/etc_worker*/; do
    cfg="${worker_dir}config_native.properties"
    [[ -f "${cfg}" ]] || continue
    set_or_append_property "system-memory-gb" "${CPU_SYSTEM_MEM_GB}" "${cfg}"
    set_or_append_property "system-mem-limit-gb" "${CPU_SYSTEM_MEM_LIMIT_GB}" "${cfg}"
    set_or_append_property "query-memory-gb" "${CPU_QUERY_MEM_GB}" "${cfg}"
    set_or_append_property "query.max-memory-per-node" "${CPU_QUERY_MEM_GB}GB" "${cfg}"
    set_or_append_property "task.max-drivers-per-task" "${CPU_DRIVERS}" "${cfg}"
    set_or_append_property "async-data-cache-enabled" "${CPU_ASYNC_DATA_CACHE}" "${cfg}"
    set_or_append_property "local-exchange.max-buffer-size" "${CPU_LOCAL_EXCHANGE_BUFFER}" "${cfg}"
    set_or_append_property "exchange.max-buffer-size" "${CPU_EXCHANGE_BUFFER}" "${cfg}"
    set_or_append_property "sink.max-buffer-size" "${CPU_SINK_BUFFER}" "${cfg}"
    set_or_append_property "exchange.max-response-size" "${CPU_EXCHANGE_MAX_RESPONSE_SIZE}" "${cfg}"
    set_or_append_property "largest-size-class-pages" "${CPU_LARGEST_SIZE_CLASS_PAGES}" "${cfg}"
    # query.execution-policy is coordinator-only and causes native worker
    # bootstrap failures when retained in reused configs from older branches.
    sed -i '/^query\.execution-policy=/d' "${cfg}"
  done
fi
