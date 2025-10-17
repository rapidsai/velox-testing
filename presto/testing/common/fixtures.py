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

import pytest
import os
import re

from . import test_utils

def get_table_external_location(schema_name, table, presto_cursor):
    create_table_text = presto_cursor.execute(f"SHOW CREATE TABLE hive.{schema_name}.{table}").fetchone()
    test_pattern = r"external_location = 'file:/var/lib/presto/data/hive/data/integration_test/(.*)'"
    user_pattern = r"external_location = 'file:/var/lib/presto/data/hive/data/user_data/(.*)'"
    assert len(create_table_text) == 1
    test_match = re.search(test_pattern, create_table_text[0])
    external_dir =""
    if test_match:
        external_dir=test_utils.get_abs_file_path(f"../integration_tests/data/{test_match.group(1)}")
    else:
        user_match = re.search(user_pattern, create_table_text[0])
        if user_match:
            external_dir=f"{os.environ['PRESTO_DATA_DIR']}/{user_match.group(1)}"
    if not os.path.isdir(external_dir):
        raise Exception(f"external location '{external_dir}' referenced by table hive.{schema_name}.{table} \
does not exist in {test_utils.get_abs_file_path("data")} or $PRESTO_DATA_DIR")
    return external_dir

@pytest.fixture(scope="module")
def get_scale_factor(request, presto_cursor):
    schema_name = request.config.getoption("--schema-name")
    benchmark_type = request.node.obj.BENCHMARK_TYPE
    repository_path = ""
    if bool(schema_name):
        # If a schema name is specified, get the scale factor from the metadata file located
        # where the table are fetching data from.
        table = presto_cursor.execute(f"SHOW TABLES in {schema_name}").fetchone()[0]
        location = get_table_external_location(schema_name, table, presto_cursor)
        repository_path = test_utils.get_abs_file_path(f"{location}/../")
    else:
        # default assumed location for metadata file.
        repository_path = test_utils.get_abs_file_path(f"../integration_tests/data/{benchmark_type}")
    meta_file = f"{repository_path}/metadata.json"
    if not os.path.exists(meta_file):
        raise pytest.UsageError(f"Could not find metadata file in data repository '{repository_path}'.\n"
                                "Metadata file must be called 'metadata.json' and have the following format:\n"
                                "{\n"
                                "  \"scale_factor\": <scale_factor>\n"
                                "}\n"
                                "where <scale_factor> is a floating point number.")
    return test_utils.get_scale_factor_from_file(meta_file)

@pytest.fixture(scope="module")
def tpch_queries(request, get_scale_factor):
    queries = test_utils.get_queries(request.node.obj.BENCHMARK_TYPE)

    scale_factor = get_scale_factor
    # The "fraction" portion of Q11 is a value that depends on scale factor
    # (it should be 0.0001 / scale_factor), whereas our query is currently hard-coded as 0.0001.
    value_ratio = 0.0001 / float(scale_factor)
    queries["Q11"] = queries["Q11"].format(SF_FRACTION=f"{value_ratio:f}")

    # Referencing the CTE defined "supplier_no" alias in the parent query causes issues on presto.
    queries["Q15"] = queries["Q15"].replace(" AS supplier_no", "").replace("supplier_no", "l_suppkey")
    return queries


@pytest.fixture(scope="module")
def tpcds_queries(request):
    return test_utils.get_queries(request.node.obj.BENCHMARK_TYPE)
