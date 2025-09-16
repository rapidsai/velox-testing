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

import prestodb
import pytest

from . import create_hive_tables
from . import test_utils


@pytest.fixture(scope="module")
def presto_cursor(request):
    benchmark_type = request.node.obj.BENCHMARK_TYPE
    conn = prestodb.dbapi.connect(host="localhost", port=8080, user="test_user", catalog="hive",
                                  schema=f"{benchmark_type}_test")
    return conn.cursor()


@pytest.fixture(scope="module")
def setup_and_teardown(request, presto_cursor):
    benchmark_type = request.node.obj.BENCHMARK_TYPE
    test_utils.init_duckdb_tables(benchmark_type)

    schema_name = f"{benchmark_type}_test"
    schemas_dir = test_utils.get_abs_file_path(f"schemas/{benchmark_type}")
    data_sub_directory = f"integration_test/{benchmark_type}"
    create_hive_tables.create_tables(presto_cursor, schema_name, schemas_dir, data_sub_directory)

    yield

    keep_tables = request.config.getoption("--keep-tables")
    if not keep_tables:
        create_hive_tables.drop_schema(presto_cursor, schema_name)
