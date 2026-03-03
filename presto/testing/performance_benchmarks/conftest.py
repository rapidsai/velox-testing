# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# ruff: noqa: I001
import pytest

from ..common.conftest import pytest_generate_tests  # noqa: F401

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


def pytest_configure(config):
    pytest.data_location = DataLocation("--schema-name", "Schema", BenchmarkKeys.SCHEMA_NAME_KEY)
