# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from duckdb_utils import drop_benchmark_tables
from generate_data_files import generate_data_files
from generate_table_schemas import generate_table_schemas

CHECKED_IN_SCHEMAS_DIR = Path(__file__).resolve().parents[2] / "presto" / "testing" / "common" / "schemas"


def test_generated_schemas_match_checked_in_schemas(setup_and_teardown, tmp_path):
    """Verify generated data matches the table schemas checked in for Presto integration tests."""
    data_dir_path, args = setup_and_teardown
    # Matches the data generation arguments used by the Presto integration test CI.
    args.convert_decimals_to_floats = args.benchmark_type == "tpcds"
    generate_data_files(args)
    drop_benchmark_tables()
    generate_table_schemas(args.benchmark_type, tmp_path, data_dir_name=data_dir_path)

    expected_schemas = {
        path.name: path.read_text() for path in (CHECKED_IN_SCHEMAS_DIR / args.benchmark_type).glob("*.sql")
    }
    generated_schemas = {path.name: path.read_text() for path in tmp_path.glob("*.sql")}
    assert generated_schemas == expected_schemas, (
        "Generated table schemas differ from the checked-in schemas. If the change is intended, regenerate them with "
        "presto/testing/integration_tests/scripts/generate_test_files.sh."
    )
