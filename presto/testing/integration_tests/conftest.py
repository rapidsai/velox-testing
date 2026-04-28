# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import os

from ..common.conftest import pytest_generate_tests  # noqa: F401
from ..common.fixtures import tpcds_queries, tpch_queries  # noqa: F401
from .common_fixtures import presto_cursor, setup_and_teardown  # noqa: F401


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
    parser.addoption("--queries")  # default is all queries for the benchmark type
    parser.addoption("--queries-file")  # path to a custom JSON file containing query definitions
    parser.addoption("--keep-tables", action="store_true", default=False)
    parser.addoption("--hostname", default=DEFAULT_HOST)
    parser.addoption("--port", default=DEFAULT_PORT, type=int)
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
    parser.addoption("--explain", action="store_true", default=False)
    parser.addoption("--explain-analyze", action="store_true", default=False)
