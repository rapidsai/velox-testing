# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import pytest

from . import test_utils

BENCHMARK_TYPE = "tpch"


@pytest.mark.usefixtures("setup_and_teardown")
def test_query(presto_cursor, tpch_queries, tpch_query_id):
    test_utils.execute_query_and_compare_results(presto_cursor, tpch_queries, tpch_query_id)
