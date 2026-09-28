# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import json
import os
import re
import sys

import pytest

from common.testing.test_utils import (
    get_abs_file_path,
    get_queries,  # noqa: F401
)

sys.path.append(get_abs_file_path(__file__, "../../../benchmark_data_tools"))


def get_table_external_location(schema_name, table, presto_cursor):
    create_table_text = presto_cursor.execute(f"SHOW CREATE TABLE hive.{schema_name}.{table}").fetchone()
    assert len(create_table_text) == 1
    # Capture the non-empty location between two single quotes
    location_match = re.search(r"external_location = '([^']+)'", create_table_text[0])
    assert location_match, f"Table hive.{schema_name}.{table} has no external_location"
    location = location_match.group(1)
    # For S3 path
    if location.startswith("s3://"):
        return location
    # For local path
    test_match = re.search(r"^file:/var/lib/presto/data/hive/data/integration_test/(.*)$", location)
    if test_match:
        external_dir = get_abs_file_path(
            __file__, f"../../../common/testing/integration_tests/data/{test_match.group(1)}"
        )
    else:
        user_match = re.search(r"^file:/var/lib/presto/data/hive/data/user_data/(.*)$", location)
        if not user_match:
            raise Exception(
                f"Unsupported external location '{location}' referenced by table hive.{schema_name}.{table}. "
                "Only s3:// locations and file: locations under /var/lib/presto/data/hive/data/integration_test "
                "or /var/lib/presto/data/hive/data/user_data are supported."
            )
        external_dir = f"{os.environ['PRESTO_DATA_DIR']}/{user_match.group(1)}"
    if not os.path.isdir(external_dir):
        raise Exception(
            f"External location '{external_dir}' referenced by table hive.{schema_name}.{table} does not exist"
        )
    return external_dir


def read_scale_factor(metadata_uri):
    """Read the scale factor from a metadata.json at metadata_uri (local path or s3://)."""
    if metadata_uri.startswith("s3://"):
        import duckdb_utils

        metadata = json.loads(duckdb_utils.read_text(metadata_uri))
    else:
        with open(metadata_uri) as file:
            metadata = json.load(file)
    # The scale factor is either a top-level field or nested under 'options'.
    return metadata.get("scale_factor") or metadata.get("options", {}).get("scale_factor")


def get_scale_factor(request, presto_cursor):
    schema_name = request.config.getoption("--schema-name")
    scale_factor = request.config.getoption("--scale-factor")
    if scale_factor is not None:
        return float(scale_factor)
    benchmark_type = request.node.obj.BENCHMARK_TYPE
    repository_path = ""
    if bool(schema_name):
        # If a schema name is specified, get the scale factor from the metadata file located
        # where the table are fetching data from (can be local or remote).
        table = presto_cursor.execute(f"SHOW TABLES in {schema_name}").fetchone()[0]
        location = get_table_external_location(schema_name, table, presto_cursor)
        repository_path = os.path.dirname(location)
    else:
        repository_path = get_abs_file_path(
            __file__, f"../../../common/testing/integration_tests/data/{benchmark_type}"
        )
    meta_file = f"{repository_path}/metadata.json"
    try:
        return read_scale_factor(meta_file)
    except Exception as error:
        raise pytest.UsageError(
            f"Could not find metadata file in data repository '{repository_path}'.\n"
            "Metadata file must be called 'metadata.json' and have the following format:\n"
            "{\n"
            '  "scale_factor": <scale_factor>\n'
            "}\n"
            "where <scale_factor> is a floating point number."
        ) from error
