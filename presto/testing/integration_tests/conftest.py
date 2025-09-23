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

def pytest_addoption(parser):
    parser.addoption("--queries") # default is all queries for the benchmark type
    parser.addoption("--keep-tables", action="store_true", default=False)
    parser.addoption("--hostname", default="localhost")
    parser.addoption("--port", default=8080)
    parser.addoption("--user", default="test_user")
    parser.addoption("--schema-name") # default is determined dynamically based on benchmark type
    parser.addoption("--scale-factor") # if not provided, SF is detected from data files.


def pytest_generate_tests(metafunc):
    TPCH_FIXTURE_NAME = "tpch_query_id"
    if TPCH_FIXTURE_NAME in metafunc.fixturenames:
        TPCH_NUM_QUERIES = 22
        set_query_id_param(metafunc, TPCH_FIXTURE_NAME, TPCH_NUM_QUERIES, [])

    TPCDS_FIXTURE_NAME = "tpcds_query_id"
    if TPCDS_FIXTURE_NAME in metafunc.fixturenames:
        TPCDS_NUM_QUERIES = 99
        TPCDS_DISABLED_QUERIES = [
            16, # PrestoUserError(type=USER_ERROR, name=SYNTAX_ERROR, message="line 1:224: Cannot check if date is BETWEEN varchar(10) and date", query_id=20250815_182910_01441_uy5t2)
            32, # PrestoUserError(type=USER_ERROR, name=SYNTAX_ERROR, message="line 1:162: Cannot check if date is BETWEEN varchar(10) and date", query_id=20250815_182915_01457_uy5t2)
            58, # PrestoUserError(type=USER_ERROR, name=SYNTAX_ERROR, message="line 1:251: '=' cannot be applied to date, varchar(10)", query_id=20250815_182921_01483_uy5t2)
            70, # PrestoUserError(type=USER_ERROR, name=SYNTAX_ERROR, message="Invalid reference to output of SELECT clause from grouping() expression in ORDER BY", query_id=20250815_182928_01495_uy5t2)
            72, # PrestoUserError(type=USER_ERROR, name=SYNTAX_ERROR, message="line 1:886: '+' cannot be applied to date, integer", query_id=20250815_182928_01497_uy5t2)
            83, # PrestoUserError(type=USER_ERROR, name=SYNTAX_ERROR, message="line 1:258: IN value and list items must be the same type: date", query_id=20250815_182930_01508_uy5t2)
            86, # PrestoUserError(type=USER_ERROR, name=SYNTAX_ERROR, message="Invalid reference to output of SELECT clause from grouping() expression in ORDER BY", query_id=20250815_182935_01511_uy5t2)
            92, # PrestoUserError(type=USER_ERROR, name=SYNTAX_ERROR, message="line 1:156: Cannot check if date is BETWEEN varchar(10) and date", query_id=20250815_182936_01517_uy5t2)
            94, # PrestoUserError(type=USER_ERROR, name=SYNTAX_ERROR, message="line 1:222: Cannot check if date is BETWEEN varchar(10) and date", query_id=20250815_182936_01519_uy5t2)
            95, # PrestoUserError(type=USER_ERROR, name=SYNTAX_ERROR, message="line 1:444: Cannot check if date is BETWEEN varchar(10) and date", query_id=20250815_182936_01520_uy5t2)

            # The following queries fail on presto native CPU with PrestoQueryError(type=INTERNAL_ERROR, name=GENERIC_INTERNAL_ERROR, message="Internal error", query_id=...)
            14,
            31,
            64,
            74,
            88,
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
