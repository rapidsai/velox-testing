# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import json
import re
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import duckdb
import generate_data_files as generator
import generate_table_schemas as schemas
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session", autouse=True)
def install_tpchgen_cli():
    """These tests mock tpchgen and do not need the Docker-based installer."""


def test_partition_arguments_use_i32(tmp_path):
    with patch.object(generator.subprocess, "run") as run:
        generator.generate_partition(
            table="nation",
            partition=1,
            raw_data_path=tmp_path,
            scale_factor=0.01,
            num_partitions=1,
            verbose=False,
            approx_row_group_bytes=1024,
            convert_decimals_to_floats=False,
            codec_defs=None,
        )
    argv = run.call_args.args[0]
    assert argv[argv.index("--nationkey-type") + 1] == "i32"
    assert argv[argv.index("--regionkey-type") + 1] == "i32"


def test_inferred_ddl_and_metadata_match_key_types(tmp_path):
    types = {"i32": (pa.int32(), "INTEGER"), "i64": (pa.int64(), "BIGINT")}
    columns = {
        "customer": {"c_nationkey": "i32", "c_custkey": "i64"},
        "supplier": {"s_nationkey": "i32", "s_suppkey": "i64"},
        "nation": {"n_nationkey": "i32", "n_regionkey": "i32"},
        "region": {"r_regionkey": "i32"},
    }
    data_dir = tmp_path / "dataset"
    for table, keys in columns.items():
        table_dir = data_dir / table
        table_dir.mkdir(parents=True)
        data = pa.table(
            {
                key: pa.array([0, 1, 4 if key.endswith("regionkey") else 24], type=types[width][0])
                for key, width in keys.items()
            }
        )
        pq.write_table(data, table_dir / f"{table}.parquet")
    args = SimpleNamespace(
        benchmark_type="tpch",
        use_duckdb=False,
        scale_factor=0.01,
        convert_decimals_to_floats=False,
        data_dir_path=data_dir,
        max_rows_per_file=100,
        approx_row_group_bytes=1024,
    )
    generator.write_metadata(args)
    metadata = json.loads((data_dir / "metadata.json").read_text())
    assert metadata["tables"] == {
        table: {"columns": [{"name": key, "type": str(types[width][0])} for key, width in keys.items()]}
        for table, keys in columns.items()
    }
    try:
        schemas.generate_table_schemas("tpch", tmp_path / "schemas", data_dir)
        for table, keys in columns.items():
            ddl = (tmp_path / "schemas" / f"{table}.sql").read_text()
            ddl_columns = dict(re.findall(r'^\s+"?(\w+)"?\s+(\w+)\s+NOT NULL', ddl, flags=re.MULTILINE))
            assert ddl_columns == {key: types[width][1] for key, width in keys.items()}
            checked_in = (ROOT / "presto/testing/common/schemas/tpch" / f"{table}.sql").read_text()
            for key, width in keys.items():
                assert f"{key} {types[width][1]} NOT NULL" in checked_in
        rows = duckdb.execute(
            "SELECT n_nationkey, n_regionkey FROM read_parquet(?) ORDER BY n_nationkey",
            [str(data_dir / "nation/nation.parquet")],
        ).fetchall()
        assert rows == [(0, 0), (1, 1), (24, 4)]
    finally:
        for table in columns:
            duckdb.sql(f"DROP TABLE IF EXISTS {table}")
