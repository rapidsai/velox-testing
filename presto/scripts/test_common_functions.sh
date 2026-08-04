#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Unit tests for the NUMA topology functions in common_functions.sh.
# No external dependencies; synthetic sysfs trees are built in $TMPDIR.
# Usage: presto/scripts/test_common_functions.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common_functions.sh"

# ---------- test harness ----------
PASS=0
FAIL=0

pass() { echo "PASS: $*"; PASS=$((PASS + 1)); }
fail() { echo "FAIL: $*"; FAIL=$((FAIL + 1)); }

assert_eq() {
  local label="$1" expected="$2" actual="$3"
  if [[ "${actual}" == "${expected}" ]]; then
    pass "${label}"
  else
    fail "${label}: expected '${expected}', got '${actual}'"
  fi
}

assert_success() {
  local label="$1"; shift
  if "$@"; then
    pass "${label}"
  else
    fail "${label}: expected exit 0"
  fi
}

assert_failure() {
  local label="$1"; shift
  if "$@"; then
    fail "${label}: expected non-zero exit"
  else
    pass "${label}"
  fi
}

# make_node <root> <id> <cpulist-or-empty> <memtotal_kb>
make_node() {
  local root="$1" id="$2" cpulist="$3" memtotal_kb="$4"
  mkdir -p "${root}/node${id}"
  [[ -z "${cpulist}" ]] || printf '%s\n' "${cpulist}" > "${root}/node${id}/cpulist"
  (( memtotal_kb > 0 )) && printf 'MemTotal: %s kB\n' "${memtotal_kb}" \
    > "${root}/node${id}/meminfo" || true
}

TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

# ---------- count_linux_cpu_list ----------
assert_eq "count: empty string"       "0"  "$(count_linux_cpu_list '')"
assert_eq "count: single cpu"         "1"  "$(count_linux_cpu_list '5')"
assert_eq "count: range 0-3"          "4"  "$(count_linux_cpu_list '0-3')"
assert_eq "count: range 0-71"         "72" "$(count_linux_cpu_list '0-71')"
assert_eq "count: mixed 0-3,8,10-11" "7"  "$(count_linux_cpu_list '0-3,8,10-11')"
assert_failure "count: invalid input exits non-zero" count_linux_cpu_list "abc"

# ---------- discover_cpu_numa_topology: no nodes ----------
assert_failure "no-nodes: returns non-zero" discover_cpu_numa_topology "${TMP}/empty"
assert_eq "no-nodes: CPU_NUMA_NODE_COUNT"     "0" "${CPU_NUMA_NODE_COUNT}"
assert_eq "no-nodes: VISIBLE_NUMA_NODE_COUNT" "0" "${VISIBLE_NUMA_NODE_COUNT}"

# ---------- discover_cpu_numa_topology: single node ----------
sysfs="${TMP}/single"
make_node "${sysfs}" 0 "0-71" $((480 * 1024 * 1024))
assert_success "single-node: returns zero" discover_cpu_numa_topology "${sysfs}"
assert_eq "single-node: CPU_NUMA_NODE_COUNT"      "1"   "${CPU_NUMA_NODE_COUNT}"
assert_eq "single-node: VISIBLE_NUMA_NODE_COUNT"  "1"   "${VISIBLE_NUMA_NODE_COUNT}"
assert_eq "single-node: node id"                  "0"   "${CPU_NUMA_NODE_IDS[0]}"
assert_eq "single-node: cpu count"                "72"  "${CPU_NUMA_NODE_CPU_COUNTS[0]}"
assert_eq "single-node: memory gb"                "480" "${CPU_NUMA_NODE_MEMORY_GB[0]}"
assert_eq "single-node: CPU_NUMA_MIN_CPUS"        "72"  "${CPU_NUMA_MIN_CPUS}"
assert_eq "single-node: CPU_NUMA_TOTAL_MEMORY_GB" "480" "${CPU_NUMA_TOTAL_MEMORY_GB}"

# ---------- discover_cpu_numa_topology: HBM node excluded from CPU nodes ----------
# Simulates a Grace-Hopper-style topology: one CPU+DRAM node, one GPU HBM proximity
# domain (memory only, no cpulist). The HBM node must appear in MEMORY_NUMA_NODE_IDS
# and VISIBLE_NUMA_NODE_COUNT but not in CPU_NUMA_NODE_IDS.
sysfs="${TMP}/with-hbm"
make_node "${sysfs}" 0 "0-71" $((480 * 1024 * 1024))  # CPU + DRAM
make_node "${sysfs}" 1 ""     $((80  * 1024 * 1024))  # HBM only
assert_success "hbm-node: returns zero" discover_cpu_numa_topology "${sysfs}"
assert_eq "hbm-node: CPU_NUMA_NODE_COUNT"     "1" "${CPU_NUMA_NODE_COUNT}"
assert_eq "hbm-node: VISIBLE_NUMA_NODE_COUNT" "2" "${VISIBLE_NUMA_NODE_COUNT}"
assert_eq "hbm-node: MEMORY_NUMA_NODE_IDS count" "2" "${#MEMORY_NUMA_NODE_IDS[@]}"

# ---------- discover_cpu_numa_topology: symmetric 2-node ----------
sysfs="${TMP}/two-node"
make_node "${sysfs}" 0 "0-71"   $((480 * 1024 * 1024))
make_node "${sysfs}" 1 "72-143" $((480 * 1024 * 1024))
assert_success "two-node: returns zero" discover_cpu_numa_topology "${sysfs}"
assert_eq "two-node: CPU_NUMA_NODE_COUNT"      "2"   "${CPU_NUMA_NODE_COUNT}"
assert_eq "two-node: node 0 id"                "0"   "${CPU_NUMA_NODE_IDS[0]}"
assert_eq "two-node: node 1 id"                "1"   "${CPU_NUMA_NODE_IDS[1]}"
assert_eq "two-node: cpu count node 0"         "72"  "${CPU_NUMA_NODE_CPU_COUNTS[0]}"
assert_eq "two-node: cpu count node 1"         "72"  "${CPU_NUMA_NODE_CPU_COUNTS[1]}"
assert_eq "two-node: CPU_NUMA_MIN_CPUS"        "72"  "${CPU_NUMA_MIN_CPUS}"
assert_eq "two-node: CPU_NUMA_MIN_MEMORY_GB"   "480" "${CPU_NUMA_MIN_MEMORY_GB}"
assert_eq "two-node: CPU_NUMA_TOTAL_MEMORY_GB" "960" "${CPU_NUMA_TOTAL_MEMORY_GB}"

# ---------- cpu_numa_node_for_worker (uses two-node topology from above) ----------
# 1 worker → all on node 0
assert_eq "worker-map: 1 worker,  slot 0 → node 0" "0" "$(cpu_numa_node_for_worker 0 1)"
# 2 workers → one per node
assert_eq "worker-map: 2 workers, slot 0 → node 0" "0" "$(cpu_numa_node_for_worker 0 2)"
assert_eq "worker-map: 2 workers, slot 1 → node 1" "1" "$(cpu_numa_node_for_worker 1 2)"
# 4 workers → balanced 2 per node (0,0,1,1)
assert_eq "worker-map: 4 workers, slot 0 → node 0" "0" "$(cpu_numa_node_for_worker 0 4)"
assert_eq "worker-map: 4 workers, slot 1 → node 0" "0" "$(cpu_numa_node_for_worker 1 4)"
assert_eq "worker-map: 4 workers, slot 2 → node 1" "1" "$(cpu_numa_node_for_worker 2 4)"
assert_eq "worker-map: 4 workers, slot 3 → node 1" "1" "$(cpu_numa_node_for_worker 3 4)"
assert_failure "worker-map: out-of-range slot exits non-zero" cpu_numa_node_for_worker 4 4
assert_failure "worker-map: zero workers_per_host exits non-zero" cpu_numa_node_for_worker 0 0

# ---------- summary ----------
printf '\n%d passed, %d failed\n' "${PASS}" "${FAIL}"
(( FAIL == 0 ))
