# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import pytest

from ..common.fixtures import tpcds_queries as tpcds_queries
from . import test_utils
from .common_fixtures import presto_cursor as presto_cursor
from .common_fixtures import setup_and_teardown as setup_and_teardown

BENCHMARK_TYPE = "tpcds"


@pytest.mark.usefixtures("setup_and_teardown")
def test_query(request, presto_cursor, tpcds_queries, tpcds_query_id):  # noqa: F811
    test_utils.execute_query_and_compare_results(request.config, presto_cursor, tpcds_queries, tpcds_query_id)
