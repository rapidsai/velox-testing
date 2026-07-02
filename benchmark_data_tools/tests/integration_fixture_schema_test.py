# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import re
from pathlib import Path

import pyarrow.parquet as pq

EXPECTED_DECIMAL_COLUMNS = {
    "c_acctbal",
    "l_quantity",
    "l_extendedprice",
    "l_discount",
    "l_tax",
    "o_totalprice",
    "p_retailprice",
    "ps_supplycost",
    "s_acctbal",
}


def test_tpch_integration_fixtures_use_decimal_types():
    repo_root = Path(__file__).resolve().parents[2]
    fixtures_dir = repo_root / "common/testing/integration_tests/data/tpch"
    schemas_dir = repo_root / "presto/testing/common/schemas/tpch"
    schema_decimal_columns = set()
    for schema_path in schemas_dir.glob("*.sql"):
        schema_decimal_columns.update(
            re.findall(
                r"^[ \t]+(\w+)[ \t]+DECIMAL[ \t]*\([ \t]*15[ \t]*,[ \t]*2[ \t]*\)",
                schema_path.read_text(),
                flags=re.IGNORECASE | re.MULTILINE,
            )
        )

    assert schema_decimal_columns == EXPECTED_DECIMAL_COLUMNS

    seen_decimal_columns = set()

    for parquet_path in fixtures_dir.rglob("*.parquet"):
        schema = pq.ParquetFile(parquet_path).schema
        for column in schema:
            if column.name not in EXPECTED_DECIMAL_COLUMNS:
                continue
            assert column.physical_type == "INT64", f"Unexpected physical type for {column.name} in {parquet_path}"
            assert column.converted_type == "DECIMAL", f"Missing DECIMAL annotation for {column.name} in {parquet_path}"
            assert column.precision == 15, f"Unexpected precision for {column.name} in {parquet_path}"
            assert column.scale == 2, f"Unexpected scale for {column.name} in {parquet_path}"
            seen_decimal_columns.add(column.name)

    assert seen_decimal_columns == EXPECTED_DECIMAL_COLUMNS
