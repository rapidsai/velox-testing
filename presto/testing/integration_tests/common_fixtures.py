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

from . import create_hive_tables
from . import test_utils


@pytest.fixture(scope="module")
def presto_cursor(request):
    benchmark_type = request.node.obj.BENCHMARK_TYPE
    hostname = request.config.getoption("--hostname")
    port = request.config.getoption("--port")
    user = request.config.getoption("--user")
    schema = f"{benchmark_type}_test"
    schema = request.config.getoption("--schema-name") if request.config.getoption("--schema-name") else schema
    conn = prestodb.dbapi.connect(host=hostname, port=port, user=user, catalog="hive", schema=schema)
    return conn.cursor()

@pytest.fixture(scope="module")
def get_scale_factor(request, presto_cursor):
    data_dir = request.config.getoption("--data-dir")
    schema_name = request.config.getoption("--schema-name")
    benchmark_type = request.node.obj.BENCHMARK_TYPE
    meta_file = ""
    if bool(data_dir):
        # If a data directory is specicied, get the scale factor from the metadata file there.
        meta_file = test_utils.get_abs_file_path(f"data/{data_dir}/metadata.json")
    elif bool(schema_name):
        # If a schema name is specified, get the scale factor from the metadata file located
        # where the table are fetching data from.
        table = presto_cursor.execute(f"SHOW TABLES in {schema_name}").fetchone()[0]
        create_table_text = presto_cursor.execute(f"SHOW CREATE TABLE {table}").fetchone()
        pattern = r"external_location = 'file:/var/lib/presto/data/hive/data/integration_test/(.*)/[^/]*'"
        for line in create_table_text:
            matches = re.search(pattern, line)
            if matches:
                meta_file = test_utils.get_abs_file_path(f"data/{matches.group(1)}/metadata.json")
                break
    else:
        # default assumed location for metadata file.
        meta_file = test_utils.get_abs_file_path(f"data/{benchmark_type}/metadata.json")
    if meta_file == "":
        raise pytest.UsageError("Could not find metadata file in data repository")
    return test_utils.get_scale_factor_from_file(meta_file)

def validate_options(request):
    benchmark_type = request.node.obj.BENCHMARK_TYPE
    data_dir = request.config.getoption("--data-dir")

    if bool(data_dir) and not test_utils.dir_exists(test_utils.get_abs_file_path(f"data/{data_dir}")):
        raise pytest.UsageError("--data-dir must point to a valid directory in {test_utils.get_abs_file_path('data')}")

    if not bool(data_dir): # default data directory
        data_dir = benchmark_type
        abs_data_dir = test_utils.get_abs_file_path(f"data/{data_dir}")
        if not test_utils.dir_exists(abs_data_dir):
            raise pytest.UsageError("default data directory {abs_data_dir} does not exist and --data-dir was not specified")

@pytest.fixture(scope="module")
def setup_and_teardown(request, presto_cursor):
    validate_options(request)

    benchmark_type = request.node.obj.BENCHMARK_TYPE
    has_schema_name = bool(request.config.getoption("--schema-name"))
    schema_name = request.config.getoption("--schema-name") if has_schema_name else f"{benchmark_type}_test"
    create_schema = request.config.getoption("--create-schema")
    data_dir = request.config.getoption("--data-dir")
    data_dir = data_dir if bool(data_dir) else benchmark_type

    should_create_tables = create_schema or not has_schema_name
    if should_create_tables:
        schemas_dir = test_utils.get_abs_file_path(f"schemas/{benchmark_type}")
        print(f"data_dir: {data_dir}")
        create_hive_tables.create_tables(presto_cursor, schema_name, schemas_dir, data_dir)

    # duckdb will need to know the name of each table in a hive schema, as well as the path to the parquet directory they are based on.
    tables = presto_cursor.execute(f"SHOW TABLES in {schema_name}").fetchall()
    for table, in tables:
        create_table_text = presto_cursor.execute(f"SHOW CREATE TABLE hive.{schema_name}.{table}").fetchone()
        # The data path in presto will be local to the container.
        pattern = r"external_location = 'file:/var/lib/presto/data/hive/data/integration_test/(.*)'"
        for line in create_table_text:
            matches = re.search(pattern, line)
            if matches:
                test_utils.create_duckdb_table(table, f"data/{matches.group(1)}")

    yield

    if should_create_tables:
        keep_tables = request.config.getoption("--keep-tables")
        if not keep_tables:
            create_hive_tables.drop_schema(presto_cursor, schema_name)
