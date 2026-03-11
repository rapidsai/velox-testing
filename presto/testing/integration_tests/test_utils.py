# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pandas as pd

from common.testing.integration_tests.test_utils import (
    create_duckdb_table,  # noqa: F401
    initialize_output_dir,  # noqa: F401
)
from common.testing.integration_tests.test_utils import (
    execute_query_and_compare_results as base_execute_query_and_compare_results,
)


def execute_query_and_compare_results(request_config, presto_cursor, queries, query_id):
    query = queries[query_id]

    explain = request_config.getoption("--explain")
    explain_analyze = request_config.getoption("--explain-analyze")
    explain_statement = "EXPLAIN " if explain else "EXPLAIN ANALYZE " if explain_analyze else ""

    presto_cursor.execute(explain_statement + query)
    presto_rows = presto_cursor.fetchall()
    presto_columns = [desc[0] for desc in presto_cursor.description]

    if explain or explain_analyze:
        if request_config.getoption("--store-presto-results"):
            output_dir = request_config.getoption("--output-dir")
            plan_path = Path(output_dir) / "presto_results" / f"{query_id.lower()}.plan"
            df = pd.DataFrame(presto_rows, columns=presto_columns)
            df.to_csv(plan_path, index=False)
        return

    base_execute_query_and_compare_results(request_config, queries, query_id, "presto", presto_rows, presto_columns)
