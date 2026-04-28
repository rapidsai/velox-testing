# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import os

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
from .common_fixtures import (
    benchmark_query,  # noqa: F401
    presto_cursor,  # noqa: F401
    run_context_collector,  # noqa: F401
    verify_tables_analyzed,  # noqa: F401
)


def _default_port():
    env_port = os.getenv("PRESTO_COORDINATOR_PORT")
    if env_port:
        try:
            return int(env_port)
        except ValueError:
            pass
    return 8080


DEFAULT_HOST = os.getenv("PRESTO_COORDINATOR_HOST", "localhost")
DEFAULT_PORT = _default_port()


def pytest_addoption(parser):
    parser.addoption("--queries")
    parser.addoption("--queries-file")  # path to a custom JSON file containing query definitions
    parser.addoption("--schema-name", required=True)
    parser.addoption("--scale-factor")
    parser.addoption("--hostname", default=DEFAULT_HOST)
    parser.addoption("--port", default=DEFAULT_PORT, type=int)
    parser.addoption("--user", default="test_user")
    parser.addoption("--iterations", default=5, type=int)
    parser.addoption("--output-dir", default="benchmark_output")
    parser.addoption("--tag")
    parser.addoption("--profile", action="store_true", default=False)
    parser.addoption("--profile-script-path")
    parser.addoption("--metrics", action="store_true", default=False)
    parser.addoption("--skip-drop-cache", action="store_true", default=False)
    parser.addoption("--skip-analyze-check", action="store_true", default=False)


def pytest_configure(config):
    pytest.data_location = DataLocation("--schema-name", "Schema", BenchmarkKeys.SCHEMA_NAME_KEY)
