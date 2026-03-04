# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

from common.testing.integration_tests.test_utils import (
    create_duckdb_table,  # noqa: F401
    initialize_output_dir,  # noqa: F401
)
from common.testing.integration_tests.test_utils import (
    execute_query_and_compare_results as base_execute_query_and_compare_results,
)


def execute_query_and_compare_results(request_config, presto_cursor, queries, query_id):
    query = queries[query_id]

    presto_cursor.execute(query)
    presto_rows = presto_cursor.fetchall()
    presto_columns = [desc[0] for desc in presto_cursor.description]

    base_execute_query_and_compare_results(request_config, queries, query_id, "presto", presto_rows, presto_columns)
