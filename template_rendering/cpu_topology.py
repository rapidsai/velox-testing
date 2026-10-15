#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""CPU placement and conservative per-worker budgets for a single host."""

import argparse
import os
import re
from collections import Counter
from pathlib import Path


def parse_cpu_list(value):
    cpus = set()
    for entry in value.strip().split(","):
        if not entry:
            continue
        first, _, last = entry.partition("-")
        cpus.update(range(int(first), int(last or first) + 1))
    return cpus


def read_numa_nodes(node_dir="/sys/devices/system/node", allowed_cpus=None):
    if allowed_cpus is None:
        allowed_cpus = os.sched_getaffinity(0) if hasattr(os, "sched_getaffinity") else None
    nodes = {}
    for path in Path(node_dir).glob("node[0-9]*"):
        if not re.fullmatch(r"node\d+", path.name):
            continue
        try:
            cpus = parse_cpu_list((path / "cpulist").read_text())
            if allowed_cpus is not None:
                cpus &= allowed_cpus
            memory = re.search(r"MemTotal:\s*(\d+)\s+kB", (path / "meminfo").read_text())
            if cpus and memory and int(memory[1]) > 0:
                nodes[int(path.name[4:])] = (len(cpus), int(memory[1]) // (1024 * 1024))
        except (OSError, ValueError):
            continue
    return dict(sorted(nodes.items()))


def detect_numa_nodes():
    # Match the nodes used for sizing; omit memory-only nodes and nodes outside
    # the process's CPU affinity mask. Minimal Linux environments use node 0.
    return list(read_numa_nodes()) or [0]


def worker_resources(workers, threads, memory_gb, bind_numa, nodes):
    """Use the smallest budget across the round-robin worker assignments.

    All workers share a config template. This bounds that template by both
    the host/allocation budget and the NUMA nodes each worker can access,
    including uneven worker counts and multiple workers sharing a node.
    """
    if min(workers, threads, memory_gb) < 1:
        raise ValueError("worker count, CPU count, and memory must be positive")
    cpu_budget = max(1, threads // workers)
    memory_budget = memory_gb // workers
    if bind_numa and nodes:
        node_ids = list(nodes)
        occupancy = Counter(node_ids[i % len(node_ids)] for i in range(workers))
        cpu_budget = min(cpu_budget, *(max(1, nodes[node][0] // count) for node, count in occupancy.items()))
        memory_budget = min(memory_budget, *(nodes[node][1] // count for node, count in occupancy.items()))
    if memory_budget < 2:
        raise ValueError("CPU workers need at least 2GB each within the selected host/NUMA memory budget")
    return cpu_budget, memory_budget


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node-ids", action="store_true")
    parser.add_argument("--workers", type=int)
    parser.add_argument("--threads", type=int)
    parser.add_argument("--memory-gb", type=int)
    parser.add_argument("--bind-numa", choices=("true", "false"))
    args = parser.parse_args()
    if args.node_ids:
        print(*detect_numa_nodes())
        return
    if None in (args.workers, args.threads, args.memory_gb, args.bind_numa):
        parser.error("--workers, --threads, --memory-gb and --bind-numa are required for sizing")
    try:
        resources = worker_resources(
            args.workers, args.threads, args.memory_gb, args.bind_numa == "true", read_numa_nodes()
        )
    except ValueError as error:
        parser.error(str(error))
    print(*resources)


if __name__ == "__main__":
    main()
