# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import time
from pathlib import Path

import pytest

from common.testing.performance_benchmarks.benchmark_keys import BenchmarkKeys
from common.testing.performance_benchmarks.conftest import get_output_dir
from common.testing.performance_benchmarks.profiler_utils import start_profiler, stop_profiler


@pytest.fixture(scope="module")
def benchmark_query(request, base_setup_and_teardown, spark_session, benchmark_queries, benchmark_result_collector):
    iterations = request.config.getoption("--iterations")
    profile = request.config.getoption("--profile")
    profile_script_path = request.config.getoption("--profile-script-path")
    benchmark_type = request.node.obj.BENCHMARK_TYPE
    bench_output_dir = get_output_dir(request.config)

    if profile:
        assert profile_script_path is not None
        profile_output_dir_path = Path(f"{bench_output_dir}/profiles/{benchmark_type}")
        profile_output_dir_path.mkdir(parents=True, exist_ok=True)

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
        profile_output_file_path = None
        try:
            if profile:
                profile_output_file_path = f"{profile_output_dir_path.absolute()}/{query_id}"
                start_profiler(profile_script_path, profile_output_file_path)
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
        finally:
            if profile and profile_output_file_path is not None:
                stop_profiler(profile_script_path, profile_output_file_path)

    return benchmark_query_function
