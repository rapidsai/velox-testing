# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

from ..common.conftest import pytest_generate_tests  # noqa: F401
from ..common.fixtures import tpch_queries  # noqa: F401
from .common_fixtures import setup_and_teardown, spark_session  # noqa: F401


def pytest_addoption(parser):
    parser.addoption("--queries")  # default is all queries for the benchmark type
    parser.addoption("--data-dir", default=None, help="Path to TPC-H parquet data directory")
    parser.addoption("--scale-factor", default=None, type=float)
    parser.addoption("--output-dir", default="integ_test_output")
    parser.addoption("--reference-results-dir")
    parser.addoption("--store-spark-results", action="store_true", default=False)
    parser.addoption("--store-reference-results", action="store_true", default=False)
    parser.addoption("--show-spark-result-preview", action="store_true", default=False)
    parser.addoption("--show-reference-result-preview", action="store_true", default=False)
    parser.addoption("--preview-rows-count", default=3, type=int)
    parser.addoption("--skip-reference-comparison", action="store_true", default=False)
