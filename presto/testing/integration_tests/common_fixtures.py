# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
import shutil
from pathlib import Path

import prestodb
import pytest

from ..common.test_utils import get_table_external_location
from . import create_hive_tables, test_utils


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
    schema_name = request.config.getoption("--schema-name") if has_schema_name else f"{benchmark_type}_test"

    should_create_tables = not has_schema_name
    if should_create_tables:
        schemas_dir = test_utils.get_abs_file_path(f"../common/schemas/{benchmark_type}")
        data_sub_directory = f"integration_test/{benchmark_type}"
        create_hive_tables.create_tables(presto_cursor, schema_name, schemas_dir, data_sub_directory)

    if not request.config.getoption("--reference-results-dir"):
        # duckdb will need to know the name of each table in a hive schema,
        # as well as the path to the parquet directory they are based on.
        tables = presto_cursor.execute(f"SHOW TABLES in {schema_name}").fetchall()
        for (table,) in tables:
            location = get_table_external_location(schema_name, table, presto_cursor)
            test_utils.create_duckdb_table(table, location)

    output_dir = Path(request.config.getoption("--output-dir"))
    user_reference_results_dir = request.config.getoption("--reference-results-dir")
    output_dir.mkdir(parents=True, exist_ok=True)

    if request.config.getoption("--store-presto-results"):
        presto_results_dir = Path(f"{output_dir}/presto_results")
        if presto_results_dir.exists():
            shutil.rmtree(presto_results_dir)
        presto_results_dir.mkdir(exist_ok=False)

    if request.config.getoption("--store-reference-results"):
        # Only manage the reference results directory if it's not being overridden by the user
        reference_results_dir = Path(f"{output_dir}/reference_results")
        if reference_results_dir.exists():
            if not user_reference_results_dir or Path(user_reference_results_dir) != reference_results_dir:
                shutil.rmtree(reference_results_dir)
        reference_results_dir.mkdir(exist_ok=False)

    yield

    if should_create_tables:
        keep_tables = request.config.getoption("--keep-tables")
        if not keep_tables:
            create_hive_tables.drop_schema(presto_cursor, schema_name)
