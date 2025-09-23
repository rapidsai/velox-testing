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
import re
import os

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
def get_scale_factor(request, presto_cursor):
    scale_factor = request.config.getoption("--scale-factor")
    if bool(scale_factor):
        return scale_factor

    # If no SF was provided, then we need to detect one from the data.
    schema_name = request.config.getoption("--schema-name")
    benchmark_type = request.node.obj.BENCHMARK_TYPE
    meta_file = ""
    if bool(schema_name):
        # If a schema name is specified, get the scale factor from the metadata file located
        # where the table are fetching data from.
        table = presto_cursor.execute(f"SHOW TABLES in {schema_name}").fetchone()[0]
        location = get_table_external_location(schema_name, table, presto_cursor)
        meta_file = test_utils.get_abs_file_path(f"{location}/../metadata.json")
    else:
        # default assumed location for metadata file.
        meta_file = test_utils.get_abs_file_path(f"data/{benchmark_type}/metadata.json")
    if meta_file == "":
        raise pytest.UsageError("Could not find metadata file in data repository")
    return test_utils.get_scale_factor_from_file(meta_file)

def get_table_external_location(schema_name, table, presto_cursor):
    create_table_text = presto_cursor.execute(f"SHOW CREATE TABLE hive.{schema_name}.{table}").fetchone()
    test_pattern = r"external_location = 'file:/var/lib/presto/data/hive/data/integration_test/(.*)'"
    user_pattern = r"external_location = 'file:/var/lib/presto/data/hive/data/user_data/(.*)'"
    for line in create_table_text:
        test_match = re.search(test_pattern, line)
        if test_match:
            return f"data/{test_match.group(1)}"
        else:
            user_match = re.search(user_pattern, line)
            if user_match:
                return f"{os.environ['PRESTO_DATA_DIR']}/{user_match.group(1)}"

@pytest.fixture(scope="module")
def setup_and_teardown(request, presto_cursor):
    benchmark_type = request.node.obj.BENCHMARK_TYPE
    has_schema_name = bool(request.config.getoption("--schema-name"))
    schema_name = request.config.getoption("--schema-name") if has_schema_name else f"{benchmark_type}_test"

    should_create_tables = not has_schema_name
    if should_create_tables:
        schemas_dir = test_utils.get_abs_file_path(f"schemas/{benchmark_type}")
        data_sub_directory = f"integration_test/{benchmark_type}"
        create_hive_tables.create_tables(presto_cursor, schema_name, schemas_dir, data_sub_directory)

    # duckdb will need to know the name of each table in a hive schema, as well as the path to the parquet directory they are based on.
    tables = presto_cursor.execute(f"SHOW TABLES in {schema_name}").fetchall()
    for table, in tables:
        location = get_table_external_location(schema_name, table, presto_cursor)
        test_utils.create_duckdb_table(table, location)

    yield

    if should_create_tables:
        keep_tables = request.config.getoption("--keep-tables")
        if not keep_tables:
            create_hive_tables.drop_schema(presto_cursor, schema_name)
