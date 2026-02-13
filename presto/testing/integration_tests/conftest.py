# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

from ..common.conftest import pytest_generate_tests  # noqa: F401
from ..common.fixtures import tpcds_queries, tpch_queries  # noqa: F401
from .common_fixtures import presto_cursor, setup_and_teardown  # noqa: F401


def pytest_addoption(parser):
    parser.addoption("--queries")  # default is all queries for the benchmark type
    parser.addoption("--keep-tables", action="store_true", default=False)
    parser.addoption("--hostname", default="localhost")
    parser.addoption("--port", default=8080, type=int)
    parser.addoption("--user", default="test_user")
    parser.addoption("--schema-name")
    parser.addoption("--scale-factor")
    parser.addoption("--output-dir", default="integ_test_output")
    parser.addoption("--reference-results-dir")
    parser.addoption("--store-presto-results", action="store_true", default=False)
    parser.addoption("--store-reference-results", action="store_true", default=False)
    parser.addoption("--show-presto-result-preview", action="store_true", default=False)
    parser.addoption("--show-reference-result-preview", action="store_true", default=False)
    parser.addoption("--preview-rows-count", default=3, type=int)
    parser.addoption("--skip-reference-comparison", action="store_true", default=False)
