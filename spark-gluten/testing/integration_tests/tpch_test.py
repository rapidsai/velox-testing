# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import pytest

from ..common.fixtures import tpch_queries as tpch_queries
from . import test_utils
from .common_fixtures import setup_and_teardown as setup_and_teardown
from .common_fixtures import spark_session as spark_session

BENCHMARK_TYPE = "tpch"


@pytest.mark.usefixtures("setup_and_teardown")
def test_query(request, spark_session, tpch_queries, tpch_query_id):  # noqa: F811
    test_utils.execute_query_and_compare_results(request.config, spark_session, tpch_queries, tpch_query_id)
