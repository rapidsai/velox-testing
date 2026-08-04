#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Sets PRESTO_SHA, PRESTO_BRANCH, PRESTO_REPO, VELOX_SHA, VELOX_BRANCH, VELOX_REPO
# by reading the sibling presto and velox repos relative to the given velox-testing root.
function capture_build_provenance() {
  local repo_root="$1"
  PRESTO_SHA=$(git -C "${repo_root}/../presto" rev-parse HEAD 2>/dev/null || echo "")
  PRESTO_BRANCH=$(git -C "${repo_root}/../presto" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
  PRESTO_REPO=$(git -C "${repo_root}/../presto" remote get-url origin 2>/dev/null || echo "")
  VELOX_SHA=$(git -C "${repo_root}/../velox" rev-parse HEAD 2>/dev/null || echo "")
  VELOX_BRANCH=$(git -C "${repo_root}/../velox" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
  VELOX_REPO=$(git -C "${repo_root}/../velox" remote get-url origin 2>/dev/null || echo "")
}

# Count CPUs represented by a Linux cpulist such as "0-3,8,10-11".
function count_linux_cpu_list() {
  local cpulist="${1:-}"
  [[ -n "${cpulist}" ]] || {
    printf '0\n'
    return 0
  }
  awk -F, '
    {
      count = 0
      for (i = 1; i <= NF; ++i) {
        if ($i ~ /^[0-9]+$/) {
          count += 1
        } else if ($i ~ /^[0-9]+-[0-9]+$/) {
          split($i, bounds, "-")
          count += bounds[2] - bounds[1] + 1
        } else {
          exit 2
        }
      }
      print count
    }
  ' <<< "${cpulist}"
}

# Discover CPU-bearing and memory-bearing NUMA nodes from sysfs.
#
# Results are returned in globals so callers can use the same topology for
# resource sizing and worker placement:
#   CPU_NUMA_NODE_IDS              indexed CPU-bearing node IDs
#   CPU_NUMA_NODE_CPU_COUNTS       matching logical CPU counts
#   CPU_NUMA_NODE_MEMORY_GB        matching memory sizes, rounded down
#   CPU_NUMA_NODE_COUNT
#   CPU_NUMA_MIN_CPUS
#   CPU_NUMA_MIN_MEMORY_GB
#   CPU_NUMA_TOTAL_MEMORY_GB
#   MEMORY_NUMA_NODE_IDS           all non-empty memory nodes (CPU DRAM + HBM)
#   VISIBLE_NUMA_NODE_COUNT        every node* directory, including zero-memory
#                                  accelerator proximity domains
#
# CPU_NUMA_SYSFS_ROOT can point at a synthetic tree for tests.
function discover_cpu_numa_topology() {
  local sysfs_root="${1:-${CPU_NUMA_SYSFS_ROOT:-/sys/devices/system/node}}"
  local nullglob_was_set=0
  shopt -q nullglob && nullglob_was_set=1
  shopt -s nullglob

  CPU_NUMA_NODE_IDS=()
  CPU_NUMA_NODE_CPU_COUNTS=()
  CPU_NUMA_NODE_MEMORY_GB=()
  MEMORY_NUMA_NODE_IDS=()
  VISIBLE_NUMA_NODE_COUNT=0
  CPU_NUMA_NODE_COUNT=0
  CPU_NUMA_MIN_CPUS=0
  CPU_NUMA_MIN_MEMORY_GB=0
  CPU_NUMA_TOTAL_MEMORY_GB=0

  local -a node_dirs=("${sysfs_root}"/node[0-9]*)
  if ((${#node_dirs[@]} > 1)); then
    mapfile -t node_dirs < <(printf '%s\n' "${node_dirs[@]}" | sort -V)
  fi

  local node_dir node_id cpulist cpu_count memory_kb memory_gb
  for node_dir in "${node_dirs[@]}"; do
    [[ -d "${node_dir}" ]] || continue
    VISIBLE_NUMA_NODE_COUNT=$((VISIBLE_NUMA_NODE_COUNT + 1))
    node_id="${node_dir##*/node}"
    cpulist=""
    [[ -r "${node_dir}/cpulist" ]] && cpulist="$(tr -d '[:space:]' < "${node_dir}/cpulist")"
    memory_kb=0
    if [[ -r "${node_dir}/meminfo" ]]; then
      memory_kb="$(awk '/MemTotal:/ { print $(NF - 1); exit }' "${node_dir}/meminfo")"
    fi
    [[ "${memory_kb}" =~ ^[0-9]+$ ]] || memory_kb=0
    if (( memory_kb > 0 )); then
      MEMORY_NUMA_NODE_IDS+=("${node_id}")
    fi

    [[ -n "${cpulist}" ]] || continue
    cpu_count="$(count_linux_cpu_list "${cpulist}")" || {
      (( nullglob_was_set )) || shopt -u nullglob
      return 1
    }
    [[ "${cpu_count}" =~ ^[1-9][0-9]*$ ]] || continue
    memory_gb=$((memory_kb / 1024 / 1024))
    (( memory_gb > 0 )) || continue

    CPU_NUMA_NODE_IDS+=("${node_id}")
    CPU_NUMA_NODE_CPU_COUNTS+=("${cpu_count}")
    CPU_NUMA_NODE_MEMORY_GB+=("${memory_gb}")
    CPU_NUMA_TOTAL_MEMORY_GB=$((CPU_NUMA_TOTAL_MEMORY_GB + memory_gb))
    if (( CPU_NUMA_MIN_CPUS == 0 || cpu_count < CPU_NUMA_MIN_CPUS )); then
      CPU_NUMA_MIN_CPUS="${cpu_count}"
    fi
    if (( CPU_NUMA_MIN_MEMORY_GB == 0 || memory_gb < CPU_NUMA_MIN_MEMORY_GB )); then
      CPU_NUMA_MIN_MEMORY_GB="${memory_gb}"
    fi
  done

  (( nullglob_was_set )) || shopt -u nullglob
  CPU_NUMA_NODE_COUNT="${#CPU_NUMA_NODE_IDS[@]}"
  (( CPU_NUMA_NODE_COUNT > 0 ))
}

# Map a local worker slot to CPU-bearing NUMA nodes in contiguous balanced
# groups. For two CPU nodes this produces:
#   1 worker:  0
#   2 workers: 0,1
#   4 workers: 0,0,1,1
function cpu_numa_node_for_worker() {
  local local_worker_id="${1:-}" workers_per_host="${2:-}"
  [[ "${local_worker_id}" =~ ^[0-9]+$ ]] || return 1
  [[ "${workers_per_host}" =~ ^[1-9][0-9]*$ ]] || return 1
  (( local_worker_id < workers_per_host )) || return 1
  (( ${CPU_NUMA_NODE_COUNT:-0} > 0 )) || return 1

  local node_index=$((local_worker_id * CPU_NUMA_NODE_COUNT / workers_per_host))
  printf '%s\n' "${CPU_NUMA_NODE_IDS[${node_index}]}"
}

function wait_for_worker_node_registration() {
  local host="$1"
  local port="$2"

  if [[ -z "${host}" || -z "${port}" ]]; then
    echo "Error: wait_for_worker_node_registration requires hostname and port arguments."
    exit 1
  fi

  trap "rm -rf node_response.json" RETURN

  echo "Waiting for a worker node to be registered..."
  COORDINATOR_URL=http://${host}:${port}
  echo "Coordinator URL: $COORDINATOR_URL"
  local -r MAX_RETRIES=12
  local retry_count=0
  local node_count=0
  while true; do
    if curl -s -f -o node_response.json "${COORDINATOR_URL}/v1/node"; then
      node_count=$(python3 -c \
        'import json, sys; print(len(json.load(open(sys.argv[1]))))' \
        node_response.json 2>/dev/null || echo 0)
      if [[ "${node_count}" =~ ^[0-9]+$ ]] && (( node_count > 0 )); then
        break
      fi
    fi
    if (( $retry_count >= $MAX_RETRIES )); then
      echo "Error: Worker node not registered after 60s. Exiting."
      exit 1
    fi
    sleep 5
    retry_count=$(( retry_count + 1 ))
  done
  echo "Worker node registered"
}
