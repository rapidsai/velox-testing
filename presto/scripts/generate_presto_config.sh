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
  if [[ ! "$worker_id" =~ ^[0-9]+$ ]]; then
    echo_error "ERROR: worker IDs must be non-negative integers."
  fi
  worker_id=$((10#$worker_id))
  echo "Duplicating worker configs for worker ID $worker_id"
  local worker_config="${CONFIG_DIR}/etc_worker_${worker_id}"
  local coord_config="${CONFIG_DIR}/etc_coordinator"
  local worker_native_config="${worker_config}/config_native.properties"
  local coord_native_config="${coord_config}/config_native.properties"
  # Need to stagger the port numbers because ucx exchange currently expects to be exactly
  # 3 higher than the http port.
  local http_port=$((10000 + 10 * worker_id))
  local exch_port=$((http_port + 3))
  if [[ "$exch_port" -gt 65535 ]]; then
    echo_error "ERROR: worker ID ${worker_id} exceeds the available HTTP/exchange port range."
  fi
  rm -rf ${worker_config}
  cp -r ${CONFIG_DIR}/etc_worker ${worker_config}

  # Some configs should only be applied if we are in a multi-worker environment.
  if [[ ${NUM_WORKERS} -gt 1 ]]; then
    sed -i "s+single-node-execution-enabled.*+single-node-execution-enabled=false+g" ${coord_native_config} ${worker_native_config}
    # make cudf.exchange=true if we are running multiple workers
    sed -i "s+cudf.exchange=false+cudf.exchange=true+g" ${worker_native_config}
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
NUMA_NODES=$({ ls -d /sys/devices/system/node/node* 2>/dev/null || true; } | wc -l)
[[ ${NUMA_NODES} -lt 1 ]] && NUMA_NODES=1
PHYSICAL_CORES=$({ lscpu -p 2>/dev/null || true; } | awk -F, '!/^#/ && NF >= 3 {print $3","$2}' | sort -u | wc -l)
[[ -z "${PHYSICAL_CORES}" || "${PHYSICAL_CORES}" -lt 1 ]] && PHYSICAL_CORES=${NPROC}
SMT_RATIO=$(( NPROC / PHYSICAL_CORES ))
[[ ${SMT_RATIO} -lt 1 ]] && SMT_RATIO=1

# variant-specific behavior
# for GPU you must set vcpu_per_worker to a small number, not the CPU count
if [[ -z ${VARIANT_TYPE} || ! ${VARIANT_TYPE} =~ ^(cpu|gpu|java)$ ]]; then
  echo_error "ERROR: VARIANT_TYPE must be set to a valid variant type (cpu, gpu, java)."
fi
REQUESTED_VCPU_PER_WORKER="${VCPU_PER_WORKER:-}"
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

  # Keep the pbench exit status as well as its JSON error diagnostics. A
  # pipeline inside `if` can otherwise hide a failed generator behind grep.
  if ! PBENCH_OUTPUT=$("${SCRIPT_DIR}/../pbench/pbench" genconfig -p params.json -t template "${CONFIG_DIR}" 2>&1); then
    printf '%s\n' "$PBENCH_OUTPUT" >&2
    echo_error "ERROR: pbench genconfig failed. Configs were not generated successfully."
  fi
  if printf '%s\n' "$PBENCH_OUTPUT" | grep -F -- '{"level":"error"'; then
    echo_error "ERROR: Errors reported by pbench genconfig. Configs were not generated successfully."
  fi

  if [ -n "${HIVE_METASTORE_URI:-}" ]; then
    sed -i 's/hive.metastore=file/#hive.metastore=file/' "${CONFIG_DIR}/etc_coordinator/catalog/hive.properties" "${CONFIG_DIR}/etc_worker/catalog/hive.properties"
    sed -i "s|hive.metastore.catalog.dir=.*|hive.metastore.uri=${HIVE_METASTORE_URI}|" "${CONFIG_DIR}/etc_coordinator/catalog/hive.properties" "${CONFIG_DIR}/etc_worker/catalog/hive.properties"
  fi

  # Override the Velox native allocator (see use-mmap-allocator in
  # etc_worker/config_native.properties). Set by the slurm launchers from
  # CLUSTER_{GPU,CPU}_USE_MMAP_ALLOCATOR; defaults to the template's "true"
  # (Presto C++ default) when unset, e.g. for non-slurm/Docker workflows.
  if [ -n "${USE_MMAP_ALLOCATOR:-}" ]; then
    sed -i "s+^use-mmap-allocator=.*+use-mmap-allocator=${USE_MMAP_ALLOCATOR}+g" "${CONFIG_DIR}/etc_worker/config_native.properties"
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
# Keep numbered configs even for a single CPU worker: Slurm mounts
# etc_worker_<id>, while single-worker Docker mounts etc_worker directly.
if [[ -n "$NUM_WORKERS" ]]; then
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

# CPU tuning uses workers on this host, while NUM_WORKERS remains the
# cluster-wide count for worker identities and single-node execution. Slurm
# exports CPU_WORKERS_PER_NODE and CPU_BIND_NUMA from its resolved layout.
# Explicit CPU_* settings apply on every generation/reuse of the configs.
if [[ "${VARIANT_TYPE}" == "cpu" ]]; then
  : "${CPU_WORKERS_PER_NODE:=${NUM_WORKERS:-1}}"
  if [[ ! "$CPU_WORKERS_PER_NODE" =~ ^[1-9][0-9]*$ ]]; then
    echo_error "ERROR: CPU_WORKERS_PER_NODE must be a positive integer."
  fi
  if [[ -z "${CPU_BIND_NUMA:-}" ]]; then
    CPU_BIND_NUMA=false
    [[ "$CPU_WORKERS_PER_NODE" -gt 1 ]] && CPU_BIND_NUMA=true
  fi

  # nproc respects process affinity. Cap host RAM by the Slurm allocation
  # when one is specified (Slurm reports these memory values in MB).
  CPU_RAM_GB=$RAM_GB
  if [[ "${SLURM_MEM_PER_NODE:-0}" =~ ^[0-9]+$ && "${SLURM_MEM_PER_NODE:-0}" -gt 0 ]]; then
    CPU_RAM_GB=$(( SLURM_MEM_PER_NODE / 1024 ))
  elif [[ "${SLURM_MEM_PER_CPU:-0}" =~ ^[0-9]+$ && "${SLURM_MEM_PER_CPU:-0}" -gt 0 ]]; then
    CPU_RAM_GB=$(( SLURM_MEM_PER_CPU * NPROC / 1024 ))
  fi
  [[ "$CPU_RAM_GB" -gt "$RAM_GB" ]] && CPU_RAM_GB=$RAM_GB
  CPU_RESOURCES=$(python3 "${SCRIPT_DIR}/../../template_rendering/cpu_topology.py" \
    --workers "$CPU_WORKERS_PER_NODE" --threads "$NPROC" \
    --memory-gb "$CPU_RAM_GB" --bind-numa "$CPU_BIND_NUMA")
  read -r THREADS_PER_WORKER RAM_PER_WORKER_GB <<< "$CPU_RESOURCES"

  : "${CPU_ASYNC_DATA_CACHE:=true}"
  # Preserve the original 35GB reserve on large multi-worker hosts, but scale
  # it down on smaller nodes instead of producing zero/negative budgets.
  CPU_RESERVED_MEM_GB=0
  if [[ "$CPU_WORKERS_PER_NODE" -gt 1 ]]; then
    CPU_RESERVED_MEM_GB=$(( RAM_PER_WORKER_GB / 4 ))
    [[ "$CPU_RESERVED_MEM_GB" -gt 35 ]] && CPU_RESERVED_MEM_GB=35
  fi
  : "${CPU_SYSTEM_MEM_GB:=$(( RAM_PER_WORKER_GB - CPU_RESERVED_MEM_GB ))}"
  : "${CPU_DRIVERS:=${REQUESTED_VCPU_PER_WORKER:-$(( THREADS_PER_WORKER < 255 ? THREADS_PER_WORKER : 255 ))}}"
  for setting in CPU_SYSTEM_MEM_GB CPU_DRIVERS; do
    if [[ ! "${!setting}" =~ ^[1-9][0-9]*$ ]]; then
      echo_error "ERROR: ${setting} must be a positive integer."
    fi
  done
  if [[ "$CPU_SYSTEM_MEM_GB" -gt "$RAM_PER_WORKER_GB" || "$CPU_DRIVERS" -gt 255 ]]; then
    echo_error "ERROR: CPU_SYSTEM_MEM_GB must fit the ${RAM_PER_WORKER_GB}GB worker budget; CPU_DRIVERS must be at most 255."
  fi
  : "${CPU_QUERY_MEM_GB:=$(( CPU_SYSTEM_MEM_GB * 70 / 100 > 0 ? CPU_SYSTEM_MEM_GB * 70 / 100 : 1 ))}"
  if [[ ! "$CPU_QUERY_MEM_GB" =~ ^[1-9][0-9]*$ || "$CPU_QUERY_MEM_GB" -gt "$CPU_SYSTEM_MEM_GB" ]]; then
    echo_error "ERROR: CPU_QUERY_MEM_GB must be positive and no larger than CPU_SYSTEM_MEM_GB."
  fi
  if [[ "$CPU_ASYNC_DATA_CACHE" != true && "$CPU_ASYNC_DATA_CACHE" != false ]]; then
    echo_error "ERROR: CPU_ASYNC_DATA_CACHE must be true or false."
  fi
  CPU_SYSTEM_MEM_LIMIT_GB=$(( CPU_SYSTEM_MEM_GB + 30 ))
  [[ "$CPU_SYSTEM_MEM_LIMIT_GB" -gt "$RAM_PER_WORKER_GB" ]] && CPU_SYSTEM_MEM_LIMIT_GB=$RAM_PER_WORKER_GB
  CPU_SYSTEM_MEM_SHRINK_GB=$(( CPU_SYSTEM_MEM_LIMIT_GB / 10 > 0 ? CPU_SYSTEM_MEM_LIMIT_GB / 10 : 1 ))
  [[ "$CPU_SYSTEM_MEM_SHRINK_GB" -gt 20 ]] && CPU_SYSTEM_MEM_SHRINK_GB=20

  # Main's override files own the default (currently 1GB). Change the buffers
  # only when explicitly requested, preserving both defaults and local edits.
  for setting in CPU_EXCHANGE_BUFFER CPU_SINK_BUFFER; do
    if [[ -n "${!setting:-}" && ! "${!setting}" =~ ^[1-9][0-9]*(B|kB|KB|MB|GB|TB)$ ]]; then
      echo_error "ERROR: ${setting} must be a positive data size such as 512MB or 1GB."
    fi
  done

  echo "CPU auto-tune: NUM_WORKERS=${NUM_WORKERS} CPU_WORKERS_PER_NODE=${CPU_WORKERS_PER_NODE} NPROC=${NPROC} NUMA_NODES=${NUMA_NODES} SMT_RATIO=${SMT_RATIO}"
  echo "               worker-budget=${RAM_PER_WORKER_GB}GB system-memory-gb=${CPU_SYSTEM_MEM_GB} query-memory-gb=${CPU_QUERY_MEM_GB} task.max-drivers-per-task=${CPU_DRIVERS} async-data-cache-enabled=${CPU_ASYNC_DATA_CACHE}"
  echo "               exchange.max-buffer-size=${CPU_EXCHANGE_BUFFER:-configured default} sink.max-buffer-size=${CPU_SINK_BUFFER:-configured default}"

  # set_or_append <key> <value> <file>: idempotent — replaces existing
  # `key=...` line if present, otherwise appends. Used here because the
  # buffer-size keys aren't in the pbench template (Velox falls back to
  # built-in defaults), so on first apply we have to append; on subsequent
  # applies we replace.
  set_or_append() {
    local key=$1 value=$2 cfg=$3
    local pattern=${key//./\\.}
    if grep -q "^${pattern}=" "$cfg"; then
      sed -i "s|^${pattern}=.*|${key}=${value}|" "$cfg"
    else
      echo "${key}=${value}" >> "$cfg"
    fi
  }

  # The coordinator also receives result pages. Apply explicit buffer tuning
  # there as well, as in the later CPU benchmark fixes.
  cfg="${CONFIG_DIR}/etc_coordinator/config_native.properties"
  [[ -z "${CPU_EXCHANGE_BUFFER:-}" ]] || set_or_append "exchange.max-buffer-size" "$CPU_EXCHANGE_BUFFER" "$cfg"
  [[ -z "${CPU_SINK_BUFFER:-}" ]] || set_or_append "sink.max-buffer-size" "$CPU_SINK_BUFFER" "$cfg"

  # Apply to every CPU worker config dir: etc_worker (single-worker + template
  # for duplicate_worker_configs) plus etc_worker_<N> (multi-worker instances).
  for worker_dir in "${CONFIG_DIR}"/etc_worker*/; do
    cfg="${worker_dir}config_native.properties"
    [[ -f "$cfg" ]] || continue
    sed -i "s/^system-memory-gb=.*/system-memory-gb=${CPU_SYSTEM_MEM_GB}/" "$cfg"
    sed -i "s/^system-mem-limit-gb=.*/system-mem-limit-gb=${CPU_SYSTEM_MEM_LIMIT_GB}/" "$cfg"
    sed -i "s/^system-mem-shrink-gb=.*/system-mem-shrink-gb=${CPU_SYSTEM_MEM_SHRINK_GB}/" "$cfg"
    sed -i "s/^query-memory-gb=.*/query-memory-gb=${CPU_QUERY_MEM_GB}/" "$cfg"
    sed -i "s|^query.max-memory-per-node=.*|query.max-memory-per-node=${CPU_QUERY_MEM_GB}GB|" "$cfg"
    sed -i "s/^task.max-drivers-per-task=.*/task.max-drivers-per-task=${CPU_DRIVERS}/" "$cfg"
    sed -i "s/^async-data-cache-enabled=.*/async-data-cache-enabled=${CPU_ASYNC_DATA_CACHE}/" "$cfg"
    [[ -z "${CPU_EXCHANGE_BUFFER:-}" ]] || set_or_append "exchange.max-buffer-size" "$CPU_EXCHANGE_BUFFER" "$cfg"
    [[ -z "${CPU_SINK_BUFFER:-}" ]] || set_or_append "sink.max-buffer-size" "$CPU_SINK_BUFFER" "$cfg"
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
