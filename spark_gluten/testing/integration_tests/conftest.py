# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

from ..common.conftest import pytest_generate_tests  # noqa: F401
from ..common.fixtures import (
    base_setup_and_teardown,  # noqa: F401
    spark_session,  # noqa: F401
    tpcds_queries,  # noqa: F401
    tpch_queries,  # noqa: F401
)
from .common_fixtures import setup_and_teardown  # noqa: F401


def pytest_addoption(parser):
    parser.addoption("--queries")
    parser.addoption("--dataset-name")
    parser.addoption("--output-dir", default="integ_test_output")
    parser.addoption("--reference-results-dir")
    parser.addoption("--store-spark-results", action="store_true", default=False)
    parser.addoption("--store-reference-results", action="store_true", default=False)
    parser.addoption("--show-spark-result-preview", action="store_true", default=False)
    parser.addoption("--show-reference-result-preview", action="store_true", default=False)
    parser.addoption("--preview-rows-count", default=3, type=int)
    parser.addoption("--skip-reference-comparison", action="store_true", default=False)
    parser.addoption("--static-gluten-jar-path")
