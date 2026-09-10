# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0


import pytest

from .cache_utils import drop_cache


@pytest.fixture(scope="session")
def benchmark_result_collector(request):
    benchmark_results = {}
    yield benchmark_results

    request.session.benchmark_results = benchmark_results


@pytest.fixture(scope="session", autouse=True)
def drop_cache_once(request):
    """Drops the OS page cache once at the start of the benchmark run.

    This is the legacy path, kept for --cache-mode=off (the presto default, and the only
    mode Java workers support) and for engines that don't register --cache-mode at all
    (e.g. spark_gluten). Every other mode schedules its own reset — including the OS
    page-cache drop — at its own cadence, so this fixture stands down for them.
    """
    if request.config.getoption("--cache-mode", default="off") != "off":
        return
    drop_cache_enabled = not request.config.getoption("--skip-drop-cache")
    if drop_cache_enabled:
        drop_cache()
        print("[Cache] System cache dropped successfully.")
    else:
        print("[Cache] Skipping cache drop (--skip-drop-cache flag set).")


@pytest.fixture(scope="module")
def benchmark_queries(request, tpch_queries, tpcds_queries):
    if request.node.obj.BENCHMARK_TYPE == "tpch":
        return tpch_queries
    else:
        assert request.node.obj.BENCHMARK_TYPE == "tpcds"
        return tpcds_queries
