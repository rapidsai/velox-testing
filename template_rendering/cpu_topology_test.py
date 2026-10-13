# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import pytest
from cpu_topology import parse_cpu_list, read_numa_nodes, worker_resources


def test_available_numa_nodes(tmp_path):
    for node, cpus in [(10, "8-11"), (2, "0-3"), (3, ""), (4, "4-7")]:
        directory = tmp_path / f"node{node}"
        directory.mkdir()
        (directory / "cpulist").write_text(cpus)
        (directory / "meminfo").write_text(f"Node {node} MemTotal: 67108864 kB\n")
    nodes = read_numa_nodes(tmp_path, {0, 1, 8, 9, 10, 11})
    assert list(nodes) == [2, 10]
    assert nodes[2] == (2, 64)
    assert nodes[10] == (4, 64)


def test_cpu_list():
    assert parse_cpu_list("0-2,5,8-9\n") == {0, 1, 2, 5, 8, 9}


@pytest.mark.parametrize(
    "workers,bound,expected",
    [
        (1, False, (128, 512)),
        (1, True, (32, 128)),
        (2, True, (32, 128)),
        (4, True, (32, 128)),
        (5, True, (16, 64)),
        (8, True, (16, 64)),
        (2, False, (64, 256)),
    ],
)
def test_worker_budget_matches_numa_placement(workers, bound, expected):
    nodes = {0: (32, 128), 1: (32, 128), 2: (32, 128), 3: (32, 128)}
    assert worker_resources(workers, 128, 512, bound, nodes) == expected


def test_asymmetric_nodes_and_allocation_caps():
    nodes = {0: (32, 128), 2: (16, 64)}
    assert worker_resources(2, 48, 192, True, nodes) == (16, 64)
    assert worker_resources(2, 16, 32, True, nodes) == (8, 16)


def test_no_topology_and_oversubscribed_threads():
    assert worker_resources(4, 2, 32, True, {}) == (1, 8)


def test_insufficient_memory():
    with pytest.raises(ValueError, match="at least 2GB"):
        worker_resources(8, 16, 8, False, {})
