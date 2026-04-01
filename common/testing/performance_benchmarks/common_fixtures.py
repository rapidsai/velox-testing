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
    """Session-scoped fixture that drops the cache once at the start of the benchmark run."""
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
