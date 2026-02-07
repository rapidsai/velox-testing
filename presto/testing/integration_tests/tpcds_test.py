# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import pytest

from . import test_utils

BENCHMARK_TYPE = "tpcds"


@pytest.mark.usefixtures("setup_and_teardown")
def test_query(presto_cursor, tpcds_queries, tpcds_query_id):
    test_utils.execute_query_and_compare_results(presto_cursor, tpcds_queries, tpcds_query_id)
