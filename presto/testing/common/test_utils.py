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

import os
import json
import re

def get_queries(benchmark_type):
    with open(get_abs_file_path(f"./queries/{benchmark_type}/queries.json"), "r") as file:
        return json.load(file)


def get_abs_file_path(relative_path):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), relative_path))


def get_scale_factor_from_file(file):
    with open(get_abs_file_path(file), "r") as file:
        metadata = json.load(file)
        return metadata["scale_factor"]

def get_table_external_location(schema_name, table, presto_cursor):
    create_table_text = presto_cursor.execute(f"SHOW CREATE TABLE hive.{schema_name}.{table}").fetchone()
    test_pattern = r"external_location = 'file:/var/lib/presto/data/hive/data/integration_test/(.*)'"
    user_pattern = r"external_location = 'file:/var/lib/presto/data/hive/data/user_data/(.*)'"
    assert len(create_table_text) == 1
    test_match = re.search(test_pattern, create_table_text[0])
    external_dir =""
    if test_match:
        external_dir=get_abs_file_path(f"../integration_tests/data/{test_match.group(1)}")
    else:
        user_match = re.search(user_pattern, create_table_text[0])
        if user_match:
            external_dir=f"{os.environ['PRESTO_DATA_DIR']}/{user_match.group(1)}"
    if not os.path.isdir(external_dir):
        data_dir = get_abs_file_path("data")
        raise Exception(f"external location '{external_dir}' referenced by table hive.{schema_name}.{table} \
does not exist in {data_dir} or $PRESTO_DATA_DIR")
    return external_dir

def get_scale_factor(request, presto_cursor):
    schema_name = request.config.getoption("--schema-name")
    scale_factor = request.config.getoption("--scale-factor")
    if scale_factor is not None:
        return float(scale_factor)
    benchmark_type = request.node.obj.BENCHMARK_TYPE
    repository_path = ""
    if bool(schema_name):
        # If a schema name is specified, get the scale factor from the metadata file located
        # where the table are fetching data from.
        table = presto_cursor.execute(f"SHOW TABLES in {schema_name}").fetchone()[0]
        location = get_table_external_location(schema_name, table, presto_cursor)
        repository_path = get_abs_file_path(f"{location}/../")
    else:
        # default assumed location for metadata file.
        repository_path = get_abs_file_path(f"../integration_tests/data/{benchmark_type}")
    meta_file = f"{repository_path}/metadata.json"
    if not os.path.exists(meta_file):
        raise pytest.UsageError(f"Could not find metadata file in data repository '{repository_path}'.\n"
                                "Metadata file must be called 'metadata.json' and have the following format:\n"
                                "{\n"
                                "  \"scale_factor\": <scale_factor>\n"
                                "}\n"
                                "where <scale_factor> is a floating point number.")
    return get_scale_factor_from_file(meta_file)
