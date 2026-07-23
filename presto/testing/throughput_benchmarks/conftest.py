# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timezone

import prestodb
import pytest

from common.testing.performance_benchmarks.benchmark_keys import BenchmarkKeys
from common.testing.performance_benchmarks.conftest import DataLocation

from ..common.conftest import *  # noqa: F403
from ..common.fixtures import (
    tpcds_queries,  # noqa: F401
    tpch_queries,  # noqa: F401
)
from ..integration_tests.analyze_tables import check_tables_analyzed
from ..performance_benchmarks.run_context import gather_run_context
from common.testing.performance_benchmarks.common_fixtures import benchmark_queries  # noqa: F401


def pytest_addoption(parser):
    parser.addoption("--queries")
    parser.addoption("--exclude-queries", default="")
    parser.addoption("--queries-file")
    parser.addoption("--schema-name", required=True)
    parser.addoption("--scale-factor")
    parser.addoption("--hostname", default="localhost")
    parser.addoption("--port", default=8080, type=int)
    parser.addoption("--user", default="test_user")
    # Power-run compatibility: unused for throughput timing, kept so shared
    # reporting helpers that read --iterations do not explode.
    parser.addoption("--iterations", default=1, type=int)
    parser.addoption("--output-dir", default="throughput_benchmark_output")
    parser.addoption("--tag")
    parser.addoption("--skip-analyze-check", action="store_true", default=False)
    parser.addoption("--num-streams", default=1, type=int)
    parser.addoption("--suite-repeats", default=1, type=int)
    parser.addoption("--repetitions-per-query", default=1, type=int)
    parser.addoption("--run-seed", default=1, type=int)
    parser.addoption("--reference-results-dir", default=None)
    parser.addoption(
        "--no-save-results",
        action="store_true",
        default=False,
        help="Do not write query_results parquet files.",
    )


def pytest_configure(config):
    pytest.data_location = DataLocation("--schema-name", "Schema", BenchmarkKeys.SCHEMA_NAME_KEY)


@pytest.fixture(scope="session", autouse=True)
def run_context_collector(request):
    hostname = request.config.getoption("--hostname")
    port = request.config.getoption("--port")
    user = request.config.getoption("--user")
    schema_name = request.config.getoption("--schema-name")

    ctx = gather_run_context(
        hostname=hostname,
        port=port,
        user=user,
        schema_name=schema_name,
    )
    ctx["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ctx["mode"] = "throughput"
    ctx["concurrency_streams"] = request.config.getoption("--num-streams")
    ctx["suite_repeats"] = request.config.getoption("--suite-repeats")
    ctx["repetitions_per_query"] = request.config.getoption("--repetitions-per-query")
    ctx["run_seed"] = request.config.getoption("--run-seed")
    yield ctx
    request.session.run_context = ctx


@pytest.fixture(scope="session", autouse=True)
def verify_tables_analyzed(request):
    """Verify ANALYZE TABLE has been run before collecting times."""
    if request.config.getoption("--skip-analyze-check"):
        print("[Analyze] Skipping analyze check (--skip-analyze-check flag set).")
        return
    hostname = request.config.getoption("--hostname")
    port = request.config.getoption("--port")
    user = request.config.getoption("--user")
    schema = request.config.getoption("--schema-name")
    conn = prestodb.dbapi.connect(host=hostname, port=port, user=user, catalog="hive", schema=schema)
    cursor = conn.cursor()
    try:
        check_tables_analyzed(cursor, schema)
    except RuntimeError as e:
        pytest.exit(str(e), returncode=1)
    finally:
        cursor.close()
        conn.close()


@pytest.fixture(scope="module")
def presto_cursor(request):
    """Cursor used only for query setup (Q11 scale factor); streams open their own."""
    hostname = request.config.getoption("--hostname")
    port = request.config.getoption("--port")
    user = request.config.getoption("--user")
    schema = request.config.getoption("--schema-name")
    conn = prestodb.dbapi.connect(host=hostname, port=port, user=user, catalog="hive", schema=schema)
    return conn.cursor()
