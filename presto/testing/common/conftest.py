# Copyright (c) 2025, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

def pytest_generate_tests(metafunc):
    TPCH_FIXTURE_NAME = "tpch_query_id"
    if TPCH_FIXTURE_NAME in metafunc.fixturenames:
        TPCH_NUM_QUERIES = 23
        set_query_id_param(metafunc, TPCH_FIXTURE_NAME, TPCH_NUM_QUERIES, [])

    TPCDS_FIXTURE_NAME = "tpcds_query_id"
    if TPCDS_FIXTURE_NAME in metafunc.fixturenames:
        TPCDS_NUM_QUERIES = 99
        TPCDS_DISABLED_QUERIES = [
            # All queries now pass with SQL fixes
        ]
        set_query_id_param(metafunc, TPCDS_FIXTURE_NAME, TPCDS_NUM_QUERIES, TPCDS_DISABLED_QUERIES)


def set_query_id_param(metafunc, param_name, num_queries, disabled_queries):
    queries = metafunc.config.getoption("--queries")
    metafunc.parametrize(param_name, get_query_ids(num_queries, queries, disabled_queries))


def get_query_ids(num_queries, selected_query_ids, disabled_queries):
    query_ids = parse_selected_query_ids(selected_query_ids, num_queries)
    if len(query_ids) == 0:
        query_ids = [id for id in range(1, num_queries + 1) if id not in disabled_queries]
    return format_query_ids(query_ids)


def parse_selected_query_ids(selected_query_ids, num_queries):
    query_ids = []
    if selected_query_ids and selected_query_ids.strip():
        for id_str in selected_query_ids.split(","):
            id_int = int(id_str)
            if id_int < 1 or id_int > num_queries:
                raise ValueError(f"Invalid Query ID: {id_str}. Query ID must be between 1 and {num_queries}.")
            query_ids.append(id_int)
    return query_ids


def format_query_ids(query_ids):
    return [f"Q{query_id}" for query_id in query_ids]
