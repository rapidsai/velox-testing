# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0


from ..common.conftest import *  # noqa: F403

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
    pytest_sessionfinish,  # noqa: F401
    pytest_terminal_summary,  # noqa: F401
)

from ..common.fixtures import (
    tpcds_queries,  # noqa: F401
    tpch_queries,  # noqa: F401
)
from .cache_reset import DEFAULT_CONNECTOR_ID
from .common_fixtures import (
    benchmark_query,  # noqa: F401
    ctas_results,  # noqa: F401
    presto_cursor,  # noqa: F401
    run_context_collector,  # noqa: F401
    verify_tables_analyzed,  # noqa: F401
)


def pytest_addoption(parser):
    parser.addoption("--queries")
    parser.addoption("--queries-file")  # path to a custom JSON file containing query definitions
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
    parser.addoption(
        "--cache-mode",
        choices=["off", "lukewarm", "cold-once", "cold", "hot"],
        default="off",
        help=(
            "Cache reset schedule (default: off); never restarts Presto. off: legacy "
            "behavior — OS page-cache drop once, no worker cache clearing, and the only "
            "mode that works against Java workers. lukewarm: full reset once, before the "
            "first measured query. cold-once: full reset before each query's iterations, "
            "giving one cold sample then hot ones. cold: full reset before every "
            "iteration. hot: no resets; --warmup-iterations primes the cache. See "
            "presto/testing/performance_benchmarks/cache_reset.py."
        ),
    )
    parser.addoption(
        "--warmup-iterations",
        default=1,
        type=int,
        help="Leading iterations per query that prime the cache and are excluded from the aggregate stats.",
    )
    parser.addoption(
        "--connector-id",
        default=DEFAULT_CONNECTOR_ID,
        help="Connector ID to target for worker cache-clear operations (must match the catalog name).",
    )
    parser.addoption("--skip-analyze-check", action="store_true", default=False)
    parser.addoption("--run-as-ctas-queries", action="store_true", default=False)


def pytest_configure(config):
    # Validate rather than silently falling back at report time: a warm-up iteration
    # reported as a steady-state number is worse than a failed run.
    warmup = config.getoption("--warmup-iterations")
    iterations = config.getoption("--iterations")
    if warmup < 0:
        raise pytest.UsageError(f"--warmup-iterations must be >= 0, got {warmup}")
    if config.getoption("--cache-mode") != "cold" and warmup >= iterations:
        raise pytest.UsageError(
            f"--warmup-iterations ({warmup}) must be less than --iterations ({iterations}); "
            "otherwise every iteration is a warm-up and there is nothing left to report."
        )
    pytest.data_location = DataLocation("--schema-name", "Schema", BenchmarkKeys.SCHEMA_NAME_KEY)
