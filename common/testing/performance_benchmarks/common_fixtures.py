# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0


import pytest

from .cache_utils import drop_cache


@pytest.fixture(scope="session")
def benchmark_result_collector(request):
    benchmark_results = {}
    yield benchmark_results

    request.session.benchmark_results = benchmark_results


@pytest.fixture(scope="session")
def benchmark_data_dir(request):
    """Override this fixture in engine-specific conftest to provide the data directory."""
    raise NotImplementedError("Engine-specific conftest must define a 'benchmark_data_dir' fixture")


@pytest.fixture(scope="session", autouse=True)
def cache_setup_per_session(request, benchmark_data_dir):
    """Session-scoped fixture that drops the cache once at the start of the benchmark
    run for the default cache mode."""
    cache_mode = request.config.getoption("--cache-mode")
    if cache_mode == "default":
        drop_cache(benchmark_data_dir)
        print(f"[Cache] Cache mode: {cache_mode}. Dropped cache for: {benchmark_data_dir}")
    elif cache_mode == "cold":
        print(f"[Cache] Cache mode: {cache_mode}. Cache will be dropped before each iteration.")
    elif cache_mode == "hot":
        print(f"[Cache] Cache mode: {cache_mode}. Warmup query will run before timed iterations.")
    elif cache_mode == "none":
        print(f"[Cache] Cache mode: {cache_mode}. No cache management.")


@pytest.fixture(scope="module")
def benchmark_queries(request, tpch_queries, tpcds_queries):
    if request.node.obj.BENCHMARK_TYPE == "tpch":
        return tpch_queries
    else:
        assert request.node.obj.BENCHMARK_TYPE == "tpcds"
        return tpcds_queries
