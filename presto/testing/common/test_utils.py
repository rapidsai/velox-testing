# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import os
import re

import pytest

from common.testing.test_utils import (
    get_abs_file_path,
    get_queries,  # noqa: F401
    get_scale_factor_from_file,
)


def get_table_external_location(schema_name, table, presto_cursor):
    create_table_text = presto_cursor.execute(f"SHOW CREATE TABLE hive.{schema_name}.{table}").fetchone()
    test_pattern = r"external_location = 'file:/var/lib/presto/data/hive/data/integration_test/(.*)'"
    user_pattern = r"external_location = 'file:/var/lib/presto/data/hive/data/user_data/(.*)'"
    assert len(create_table_text) == 1
    test_match = re.search(test_pattern, create_table_text[0])
    external_dir = ""
    if test_match:
        external_dir = get_abs_file_path(
            __file__, f"../../../common/testing/integration_tests/data/{test_match.group(1)}"
        )
    else:
        user_match = re.search(user_pattern, create_table_text[0])
        if user_match:
            external_dir = f"{os.environ['PRESTO_DATA_DIR']}/{user_match.group(1)}"
    if not os.path.isdir(external_dir):
        raise Exception(
            f"External location '{external_dir}' referenced by table hive.{schema_name}.{table} \
does not exist"
        )
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
        repository_path = os.path.dirname(location)
    else:
        # default assumed location for metadata file.
        repository_path = get_abs_file_path(
            __file__, f"../../../common/testing/integration_tests/data/{benchmark_type}"
        )
    meta_file = f"{repository_path}/metadata.json"
    if not os.path.exists(meta_file):
        raise pytest.UsageError(
            f"Could not find metadata file in data repository '{repository_path}'.\n"
            "Metadata file must be called 'metadata.json' and have the following format:\n"
            "{\n"
            '  "scale_factor": <scale_factor>\n'
            "}\n"
            "where <scale_factor> is a floating point number."
        )
    return get_scale_factor_from_file(meta_file)
