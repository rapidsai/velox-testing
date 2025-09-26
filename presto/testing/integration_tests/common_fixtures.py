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
    hostname = request.config.getoption("--hostname")
    port = request.config.getoption("--port")
    user = request.config.getoption("--user")
    schema = request.config.getoption("--schema-name")
    schema = schema if schema else f"{benchmark_type}_test"
    conn = prestodb.dbapi.connect(host=hostname, port=port, user=user, catalog="hive", schema=schema)
    return conn.cursor()


@pytest.fixture(scope="module")
def setup_and_teardown(request, presto_cursor):
    benchmark_type = request.node.obj.BENCHMARK_TYPE

    has_schema_name = bool(request.config.getoption("--schema-name"))
    scale_factor = request.config.getoption("--scale-factor")

    if has_schema_name and not scale_factor:
        raise pytest.UsageError("--scale-factor must be set when --schema-name is specified")

    if scale_factor and not has_schema_name:
        raise pytest.UsageError("--scale-factor should be set only when --schema-name is specified")

    scale_factor = scale_factor if scale_factor else test_utils.get_scale_factor(benchmark_type)
    test_utils.init_duckdb_tables(benchmark_type, scale_factor)

    should_create_tables = not has_schema_name
    if should_create_tables:
        schema_name = f"{benchmark_type}_test"
        schemas_dir = test_utils.get_abs_file_path(f"../common/schemas/{benchmark_type}")
        data_sub_directory = f"integration_test/{benchmark_type}"
        create_hive_tables.create_tables(presto_cursor, schema_name, schemas_dir, data_sub_directory)

    yield

    if should_create_tables:
        keep_tables = request.config.getoption("--keep-tables")
        if not keep_tables:
            create_hive_tables.drop_schema(presto_cursor, schema_name)
