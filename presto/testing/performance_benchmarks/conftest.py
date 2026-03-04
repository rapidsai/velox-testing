# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0


import json
from datetime import datetime, timezone

from ..common.conftest import *  # noqa: F403
from .run_context import gather_run_context

# ruff: noqa: I001
import pytest

from common.testing.performance_benchmarks.benchmark_keys import BenchmarkKeys
from common.testing.performance_benchmarks.common_fixtures import (
    benchmark_queries,  # noqa: F401
    benchmark_result_collector,  # noqa: F401
    drop_cache_once,  # noqa: F401
)
from common.testing.performance_benchmarks.conftest import (
    DataLocation,
    compute_aggregate_timings,
    get_output_dir,
    pytest_terminal_summary,  # noqa: F401
)

from ..common.fixtures import (
    tpcds_queries,  # noqa: F401
    tpch_queries,  # noqa: F401
)
from .common_fixtures import (
    benchmark_query,  # noqa: F401
    presto_cursor,  # noqa: F401
)


def pytest_addoption(parser):
    parser.addoption("--queries")
    parser.addoption("--schema-name", required=True)
    parser.addoption("--scale-factor")
    parser.addoption("--hostname", default="localhost")
    parser.addoption("--port", default=8080, type=int)
    parser.addoption("--user", default="test_user")
    parser.addoption("--iterations", default=5, type=int)
    parser.addoption("--output-dir", default="benchmark_output")
    parser.addoption("--tag")
    parser.addoption("--profile", action="store_true", default=False)
    parser.addoption("--profile-script-path")
    parser.addoption("--metrics", action="store_true", default=False)
    parser.addoption("--skip-drop-cache", action="store_true", default=False)


def _build_run_config(session):
    """
    Build run-config dict from execution context (Presto nodes, nvidia-smi, schema
    data source, env). Used for the context section in benchmark_result.json.
    """
    hostname = session.config.getoption("--hostname")
    port = session.config.getoption("--port")
    user = session.config.getoption("--user")
    schema_name = session.config.getoption("--schema-name")

    ctx = gather_run_context(
        hostname=hostname,
        port=port,
        user=user,
        schema_name=schema_name,
    )
    ctx["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return ctx


def pytest_sessionfinish(session, exitstatus):
    iterations = session.config.getoption("--iterations")
    schema_name = session.config.getoption("--schema-name")
    json_result = {
        BenchmarkKeys.CONTEXT_KEY: {
            BenchmarkKeys.ITERATIONS_COUNT_KEY: iterations,
            BenchmarkKeys.SCHEMA_NAME_KEY: schema_name,
        },
    }

    tag = session.config.getoption("--tag")
    if tag:
        json_result[BenchmarkKeys.CONTEXT_KEY][BenchmarkKeys.TAG_KEY] = tag

    run_config = _build_run_config(session)
    for key, value in run_config.items():
        json_result[BenchmarkKeys.CONTEXT_KEY][key] = value

    bench_output_dir = get_output_dir(session.config)
    bench_output_dir.mkdir(parents=True, exist_ok=True)

    if iterations > 1:
        AGG_KEYS = [
            BenchmarkKeys.AVG_KEY,
            BenchmarkKeys.MIN_KEY,
            BenchmarkKeys.MAX_KEY,
            BenchmarkKeys.MEDIAN_KEY,
            BenchmarkKeys.GMEAN_KEY,
            BenchmarkKeys.LUKEWARM_KEY,
        ]
    else:
        AGG_KEYS = [BenchmarkKeys.LUKEWARM_KEY]
    if not hasattr(session, "benchmark_results"):
        return
    benchmark_types = list(session.benchmark_results.keys())
    json_result[BenchmarkKeys.CONTEXT_KEY]["benchmark"] = (
        benchmark_types[0] if len(benchmark_types) == 1 else benchmark_types
    )
    for benchmark_type, result in session.benchmark_results.items():
        compute_aggregate_timings(result)
        json_result[benchmark_type] = {
            BenchmarkKeys.AGGREGATE_TIMES_KEY: {},
            BenchmarkKeys.RAW_TIMES_KEY: result[BenchmarkKeys.RAW_TIMES_KEY],
            BenchmarkKeys.FAILED_QUERIES_KEY: result[BenchmarkKeys.FAILED_QUERIES_KEY],
        }
        json_agg_timings = json_result[benchmark_type][BenchmarkKeys.AGGREGATE_TIMES_KEY]
        for agg_key in AGG_KEYS:
            json_agg_timings[agg_key] = {}

        for query_id, agg_timings in result[BenchmarkKeys.AGGREGATE_TIMES_KEY].items():
            if agg_timings:
                assert len(AGG_KEYS) == len(agg_timings)
                for i, agg_key in enumerate(AGG_KEYS):
                    json_agg_timings[agg_key][query_id] = agg_timings[i]

    with open(f"{bench_output_dir}/benchmark_result.json", "w") as file:
        json.dump(json_result, file, indent=2)
        file.write("\n")


def pytest_configure(config):
    pytest.data_location = DataLocation("--schema-name", "Schema", BenchmarkKeys.SCHEMA_NAME_KEY)
