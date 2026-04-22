#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e
# Run ldconfig once
ldconfig

LOGS_DIR="/opt/presto-server/logs"
mkdir -p "${LOGS_DIR}"
: "${SERVER_START_TIMESTAMP:?SERVER_START_TIMESTAMP must be set before starting the container}"

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
