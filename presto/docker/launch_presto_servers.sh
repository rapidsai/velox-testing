#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e
# Activate image-owned UCX only when the GPU build wrote its version marker.
# CPU images do not contain this marker and retain their original environment.
if [[ -f /opt/presto-ucx/VERSION ]]; then
  read -r BUNDLED_UCX_VERSION < /opt/presto-ucx/VERSION
  BUNDLED_UCX_ROOT="/opt/presto-ucx/${BUNDLED_UCX_VERSION}"
  export BUNDLED_UCX_VERSION BUNDLED_UCX_ROOT
  export PATH="${BUNDLED_UCX_ROOT}/bin:${PATH}"
  if [[ -n ${LD_LIBRARY_PATH:-} ]]; then
    export LD_LIBRARY_PATH="${BUNDLED_UCX_ROOT}/lib:${BUNDLED_UCX_ROOT}/lib/ucx:${LD_LIBRARY_PATH}"
  else
    export LD_LIBRARY_PATH="${BUNDLED_UCX_ROOT}/lib:${BUNDLED_UCX_ROOT}/lib/ucx"
  fi
  export UCX_MODULE_DIR="${BUNDLED_UCX_ROOT}/lib/ucx"
  # UCX treats every UCX_* variable as runtime configuration. The image keeps
  # BUNDLED_UCX_VERSION as provenance, so do not forward the build-time alias.
  unset UCX_VERSION
fi

# Run ldconfig once after selecting the image's runtime libraries.
ldconfig

if [[ "${PRESTO_UCX_EFA_ENABLED:-false}" == "true" ]]; then
  /opt/verify_ucx_runtime.sh
fi

LOGS_DIR="/opt/presto-server/logs"
mkdir -p "${LOGS_DIR}"
: "${SERVER_START_TIMESTAMP:?SERVER_START_TIMESTAMP must be set before starting the container}"

# Surface baked-in image provenance in the shared logs dir so the host-side
# pytest (Docker) or in-container pytest (SLURM) can read it via LOGS_DIR.
if [ -f /opt/velox-testing/provenance.json ]; then
  cp /opt/velox-testing/provenance.json "${LOGS_DIR}/worker_provenance.json"
fi

ETC_BASE="/opt/presto-server/etc"

# Resolve the NUMA node for a worker and launch presto_server pinned to it.
# For GPU workers: pins to the NUMA node closest to the GPU via nvidia-smi topology.
# For CPU workers: interleaves memory across all NUMA nodes via numactl (requires SYS_NICE).
#   $1 — GPU ID (or 0 for CPU single-worker)
#   $2 — etc-dir path for this instance
launch_worker() {
  local worker_id=$1 etc_dir=$2
  echo "Launching worker $worker_id (config: $etc_dir)"

  local launcher=()
  local cuda_env=()

  if command -v nvidia-smi &> /dev/null; then
    local topo
    topo=$(nvidia-smi topo -C -M -i "$worker_id")
    echo "$topo"

    local cpu_numa mem_numa
    cpu_numa=$(echo "$topo" | awk -F: '/NUMA IDs of closest CPU/{ gsub(/ /,"",$2); print $2 }')
    mem_numa=$(echo "$topo" | awk -F: '/NUMA IDs of closest memory/{ gsub(/ /,"",$2); print $2 }')

    if [[ $cpu_numa =~ ^[0-9]+$ ]]; then
      launcher=(numactl --cpunodebind="$cpu_numa")
      if [[ $mem_numa =~ ^[0-9]+$ ]]; then
        launcher+=(--membind="$mem_numa")
      else
        launcher+=(--membind="$cpu_numa")
      fi
    fi

    cuda_env=("CUDA_VISIBLE_DEVICES=$worker_id")
    local ucx_device_env_name="UCX_NET_DEVICES_GPU_${worker_id}"
    local ucx_device="${UCX_NET_DEVICES:-}"
    if [[ -n "${!ucx_device_env_name:-}" ]]; then
      ucx_device="${!ucx_device_env_name}"
    fi
    if [[ -n "$ucx_device" ]]; then
      cuda_env+=("UCX_NET_DEVICES=$ucx_device")
      echo "UCX network device: $ucx_device"
    fi
    gpu_name="$(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null -i "$worker_id")"
  # No GPU: fall back to NUMA interleaving across all nodes for CPU workers.
  # Requires SYS_NICE capability in the container (set via cap_add in docker-compose).
  elif command -v numactl &> /dev/null; then
    local num_nodes
    num_nodes=$(numactl --hardware 2>/dev/null | grep -c "node [0-9]* cpus:" || echo 0)
    if [[ $num_nodes -gt 1 ]]; then
      echo "No GPU detected; found $num_nodes NUMA nodes -- launching with --interleave=all"
      launcher=(numactl --interleave=all)
    fi
  fi

  log_file="${LOGS_DIR}/worker_${worker_id}_${SERVER_START_TIMESTAMP}.log"
  echo "GPU Name: ${gpu_name:-unknown}" > "${log_file}"
  env "${cuda_env[@]}" "${launcher[@]}" presto_server --etc-dir="$etc_dir" >> "${log_file}" 2>&1 &
}

# No args → single worker using CUDA_VISIBLE_DEVICES (default 0), shared config dir.
# With args → one worker per GPU ID, each with its own config dir (etc<gpu_id>).
if [ $# -eq 0 ]; then
  # Single worker mode.
  launch_worker "${CUDA_VISIBLE_DEVICES:-0}" "${ETC_BASE}/"
else
  # Multi-worker single-container mode.  Each GPU ID is an argument.
  for gpu_id in "$@"; do
    launch_worker "$gpu_id" "${ETC_BASE}${gpu_id}"
  done
fi

wait
