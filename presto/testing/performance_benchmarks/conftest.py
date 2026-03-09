# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0


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
    pytest_sessionfinish,  # noqa: F401
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


def pytest_sessionstart(session):
    """Gather Presto-specific run context and attach it to the session.

    The common pytest_sessionfinish merges session.run_context into the
    benchmark_result.json context section.
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
    session.run_context = ctx


def pytest_configure(config):
    pytest.data_location = DataLocation("--schema-name", "Schema", BenchmarkKeys.SCHEMA_NAME_KEY)
