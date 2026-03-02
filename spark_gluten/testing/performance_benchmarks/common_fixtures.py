# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import time

import pytest

from common.testing.performance_benchmarks.benchmark_keys import BenchmarkKeys


@pytest.fixture(scope="module")
def benchmark_query(request, base_setup_and_teardown, spark_session, benchmark_queries, benchmark_result_collector):
    iterations = request.config.getoption("--iterations")
    benchmark_type = request.node.obj.BENCHMARK_TYPE

    benchmark_result_collector[benchmark_type] = {
        BenchmarkKeys.RAW_TIMES_KEY: {},
        BenchmarkKeys.FAILED_QUERIES_KEY: {},
    }

    benchmark_dict = benchmark_result_collector[benchmark_type]
    raw_times_dict = benchmark_dict[BenchmarkKeys.RAW_TIMES_KEY]
    assert raw_times_dict == {}

    failed_queries_dict = benchmark_dict[BenchmarkKeys.FAILED_QUERIES_KEY]
    assert failed_queries_dict == {}

    def benchmark_query_function(query_id):
        try:
            result = []
            query = f"--{benchmark_type}_{query_id}--\n{benchmark_queries[query_id]}"
            for _ in range(iterations):
                start_time_ns = time.perf_counter_ns()
                df = spark_session.sql(query)
                df.write.format("noop").mode("overwrite").save()
                end_time_ns = time.perf_counter_ns()
                ns_to_ms_divisor = 1000000.0
                result.append((end_time_ns - start_time_ns) / ns_to_ms_divisor)
            raw_times_dict[query_id] = result
        except Exception as e:
            failed_queries_dict[query_id] = f"{type(e).__name__}: {e!s}"
            raw_times_dict[query_id] = None
            raise

    return benchmark_query_function
