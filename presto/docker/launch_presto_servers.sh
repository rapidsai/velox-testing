#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e
# Run ldconfig once
ldconfig

LOGS_DIR="${LOGS_DIR:-/opt/presto-server/logs}"
mkdir -p "${LOGS_DIR}"
: "${SERVER_START_TIMESTAMP:?SERVER_START_TIMESTAMP must be set before starting the container}"

# Surface baked-in image provenance in the shared logs dir so the host-side
# pytest (Docker) or in-container pytest (SLURM) can read it via LOGS_DIR.
if [ -f /opt/velox-testing/provenance.json ]; then
  cp /opt/velox-testing/provenance.json "${LOGS_DIR}/worker_provenance.json"
fi

ETC_BASE="/opt/presto-server/etc"
WORKER_PIDS=()

# Keep the container's lifetime tied to all of its native servers. A bare
# `wait` hides failed children and can leave a partial worker set running.
stop_workers() {
  ((${#WORKER_PIDS[@]})) || return 0
  kill -TERM "${WORKER_PIDS[@]}" 2>/dev/null || true
  local deadline=$((SECONDS + 5)) pid running
  while ((SECONDS < deadline)); do
    running=false
    for pid in "${WORKER_PIDS[@]}"; do
      if kill -0 "$pid" 2>/dev/null; then running=true; fi
    done
    [[ "$running" == true ]] || break
    sleep 0.1
  done
  kill -KILL "${WORKER_PIDS[@]}" 2>/dev/null || true
  wait "${WORKER_PIDS[@]}" 2>/dev/null || true
}
trap stop_workers EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
trap 'exit 129' HUP

# Resolve the NUMA node for a worker and launch presto_server pinned to it.
# Precedence:
#   1. NUMA_NODE env var: explicit bind (used by multi-worker CPU where each
#      container represents one NUMA socket).
#   2. nvidia-smi topology: pins to the NUMA node closest to the worker's GPU.
#   3. numactl fallback: --interleave=all across all NUMA nodes (single-
#      container CPU where one worker spans both sockets).
#   $1 — GPU ID (or 0 for CPU single-worker)
#   $2 — etc-dir path for this instance
launch_worker() {
  local worker_id=$1 etc_dir=$2
  echo "Launching worker $worker_id (config: $etc_dir)"

  local launcher=()
  local cuda_env=()
  local gpu_name="unknown"

  if [[ -n "${NUMA_NODE:-}" ]]; then
    echo "NUMA_NODE=${NUMA_NODE} -- launching with numactl --cpunodebind=${NUMA_NODE} --membind=${NUMA_NODE}"
    launcher=(numactl --cpunodebind="${NUMA_NODE}" --membind="${NUMA_NODE}")
  elif [[ "${PRESTO_WORKER_VARIANT:-}" != cpu ]] && command -v nvidia-smi &> /dev/null; then
    local topo
    topo=$(timeout -k 1 5 nvidia-smi topo -C -M -i "$worker_id" || true)
    echo "$topo"

    local cpu_numa mem_numa
    cpu_numa=$(echo "$topo" | awk -F: '/NUMA IDs of closest CPU/{ gsub(/ /,"",$2); print $2 }')
    mem_numa=$(echo "$topo" | awk -F: '/NUMA IDs of closest memory/{ gsub(/ /,"",$2); print $2 }')

    if [[ $cpu_numa =~ ^[0-9]+$ ]] && command -v numactl >/dev/null 2>&1; then
      launcher=(numactl --cpunodebind="$cpu_numa")
      if [[ $mem_numa =~ ^[0-9]+$ ]]; then
        launcher+=(--membind="$mem_numa")
      else
        launcher+=(--membind="$cpu_numa")
      fi
    fi

    cuda_env=("CUDA_VISIBLE_DEVICES=$worker_id")
    gpu_name="$(timeout -k 1 5 nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null -i "$worker_id" || true)"
  # No GPU: fall back to NUMA interleaving across all nodes for CPU workers.
  # Requires SYS_NICE capability in the container (set via cap_add in docker-compose).
  elif command -v numactl &> /dev/null; then
    local num_nodes
    num_nodes=$(numactl --hardware 2>/dev/null | grep -c "node [0-9]* cpus:" || true)
    if [[ $num_nodes -gt 1 ]]; then
      echo "No GPU detected; found $num_nodes NUMA nodes -- launching with --interleave=all"
      launcher=(numactl --interleave=all)
    fi
  fi

  log_file="${LOGS_DIR}/worker_${worker_id}_${SERVER_START_TIMESTAMP}.log"
  echo "GPU Name: ${gpu_name:-unknown}" > "${log_file}"
  env "${cuda_env[@]}" "${launcher[@]}" presto_server --etc-dir="$etc_dir" >> "${log_file}" 2>&1 &
  WORKER_PIDS+=("$!")
}

# No args → single worker. WORKER_ID env var (set by multi-worker CPU compose
# where each container is one logical worker) takes precedence; otherwise fall
# back to CUDA_VISIBLE_DEVICES for GPU runs, defaulting to 0.
# With args → one worker per GPU ID, each with its own config dir (etc<gpu_id>).
if [ $# -eq 0 ]; then
  # Single worker mode.
  launch_worker "${WORKER_ID:-${CUDA_VISIBLE_DEVICES:-0}}" "${ETC_BASE}/"
else
  # Multi-worker single-container mode.  Each GPU ID is an argument.
  for gpu_id in "$@"; do
    launch_worker "$gpu_id" "${ETC_BASE}${gpu_id}"
  done
fi

status=0
wait -n "${WORKER_PIDS[@]}" || status=$?
echo "ERROR: a native worker exited unexpectedly (status ${status}); stopping the worker set." >&2
[[ "$status" -ne 0 ]] || status=1
exit "$status"
