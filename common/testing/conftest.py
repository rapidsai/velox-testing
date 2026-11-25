# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import json
from collections import Counter


def pytest_generate_tests(metafunc):
    TPCH_FIXTURE_NAME = "tpch_query_id"
    if TPCH_FIXTURE_NAME in metafunc.fixturenames:
        TPCH_NUM_QUERIES = 22
        _set_query_id_param(metafunc, TPCH_FIXTURE_NAME, TPCH_NUM_QUERIES, [])

    TPCDS_FIXTURE_NAME = "tpcds_query_id"
    if TPCDS_FIXTURE_NAME in metafunc.fixturenames:
        TPCDS_NUM_QUERIES = 99
        TPCDS_DISABLED_QUERIES = [
            # All queries now pass with SQL fixes
        ]
        _set_query_id_param(metafunc, TPCDS_FIXTURE_NAME, TPCDS_NUM_QUERIES, TPCDS_DISABLED_QUERIES)

    CUSTOM_FIXTURE_NAME = "custom_query_id"
    if CUSTOM_FIXTURE_NAME in metafunc.fixturenames:
        # Read with a default because not every engine registers --queries-file.
        queries_file = metafunc.config.getoption("--queries-file", default=None)
        queries = metafunc.config.getoption("--queries")
        metafunc.parametrize(CUSTOM_FIXTURE_NAME, _get_custom_query_ids(queries_file, queries))


def _set_query_id_param(metafunc, param_name, num_queries, disabled_queries):
    queries = metafunc.config.getoption("--queries")
    metafunc.parametrize(param_name, _get_query_ids(num_queries, queries, disabled_queries))


def _get_query_ids(num_queries, selected_query_ids, disabled_queries):
    query_ids = _parse_selected_query_ids(selected_query_ids, num_queries)
    if len(query_ids) == 0:
        query_ids = [id for id in range(1, num_queries + 1) if id not in disabled_queries]
    return _format_query_ids(query_ids)


def _parse_selected_query_ids(selected_query_ids, num_queries):
    query_ids = []
    if selected_query_ids and selected_query_ids.strip():
        for id_str in selected_query_ids.split(","):
            id_int = int(id_str)
            if id_int < 1 or id_int > num_queries:
                raise ValueError(f"Invalid Query ID: {id_str}. Query ID must be between 1 and {num_queries}.")
            query_ids.append(id_int)
    return query_ids


def _format_query_ids(query_ids):
    return [f"Q{query_id}" for query_id in query_ids]


def _get_custom_query_ids(queries_file, selected_query_ids):
    """Query IDs of the custom benchmark type: the keys of the queries file, in file order."""
    if not queries_file:
        raise ValueError("--queries-file is required for the custom benchmark type.")
    with open(queries_file) as file:
        queries = json.load(file)
    if not isinstance(queries, dict) or not queries:
        raise ValueError(f"{queries_file} must contain a non-empty JSON object that maps query IDs to SQL text.")
    # Each query result is written to query_results/<lower-case query ID>.parquet.
    lower_case_id_counts = Counter(query_id.lower() for query_id in queries)
    unusable = [query_id for query_id in queries if "/" in query_id or lower_case_id_counts[query_id.lower()] > 1]
    if unusable:
        raise ValueError(
            f"Query IDs in {queries_file} must be unique ignoring case and must not contain '/': {', '.join(unusable)}"
        )
    if selected_query_ids and selected_query_ids.strip():
        selected = [query_id.strip() for query_id in selected_query_ids.split(",")]
        unknown = [query_id for query_id in selected if query_id not in queries]
        if unknown:
            raise ValueError(f"Query IDs not found in {queries_file}: {', '.join(unknown)}")
        return selected
    return list(queries)
