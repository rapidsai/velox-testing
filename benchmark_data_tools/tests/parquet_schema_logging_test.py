# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from log_parquet_schemas import log_parquet_schemas


def test_log_parquet_schemas(tmp_path, capsys):
    table_dir = tmp_path / "lineitem"
    table_dir.mkdir()
    table = pa.table(
        {
            "l_orderkey": pa.array([1], type=pa.int64()),
            "l_extendedprice": pa.array([Decimal("10.25")], type=pa.decimal128(15, 2)),
            "l_discount_float": pa.array([0.05], type=pa.float64()),
        }
    )
    pq.write_table(table, table_dir / "lineitem-1.parquet", store_decimal_as_integer=True)

    log_parquet_schemas(tmp_path)

    assert capsys.readouterr().out.splitlines() == [
        "--- parquet schema: lineitem (lineitem/lineitem-1.parquet) ---",
        "l_orderkey: physical=INT64, logical=None",
        "l_extendedprice: physical=INT64, logical=Decimal(precision=15, scale=2)",
        "l_discount_float: physical=DOUBLE, logical=None",
    ]


def test_log_parquet_schemas_rejects_mismatched_partitions(tmp_path):
    table_dir = tmp_path / "lineitem"
    table_dir.mkdir()
    pq.write_table(pa.table({"value": pa.array([1], type=pa.int64())}), table_dir / "lineitem-1.parquet")
    pq.write_table(pa.table({"value": pa.array([1.0], type=pa.float64())}), table_dir / "lineitem-2.parquet")

    with pytest.raises(ValueError, match="Parquet schema mismatch for table 'lineitem'"):
        log_parquet_schemas(tmp_path)
