# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0


from common.testing.integration_tests import test_utils


def execute_query_and_compare_results(request_config, spark_session, queries, query_id):
    query = queries[query_id]

    df = spark_session.sql(query)
    spark_rows = [tuple(row) for row in df.collect()]
    spark_columns = df.columns

    test_utils.execute_query_and_compare_results(request_config, queries, query_id, "spark", spark_rows, spark_columns)
