# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import itertools
import json
import os
import re
import shutil
import subprocess
import sys
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
KEY_TYPES = list(itertools.product(["i32", "i64"], repeat=2))


@pytest.fixture(scope="session", autouse=True)
def install_tpchgen_cli():
    """These tests mock tpchgen and do not need the Docker-based installer."""


@pytest.mark.parametrize("nation,region", [(None, None), *KEY_TYPES])
def test_partition_arguments_and_metadata_use_selected_widths(tmp_path, nation, region):
    args = SimpleNamespace(
        benchmark_type="tpch",
        use_duckdb=False,
        scale_factor=0.01,
        convert_decimals_to_floats=False,
        data_dir_path=str(tmp_path),
        max_rows_per_file=100,
        approx_row_group_bytes=1024,
        num_threads=1,
        verbose=False,
    )
    if nation is not None:
        args.nationkey_type, args.regionkey_type = nation, region
    expected_types = (nation or "i64", region or "i64")
    partitions = {"customer": 2, "supplier": 1, "nation": 1, "region": 1}
    with (
        patch.object(generator, "get_table_sf_ratios", return_value=partitions),
        patch.object(generator, "_get_tpchgen_version", return_value="test"),
        patch.object(generator, "rearrange_directory"),
        patch.object(generator.subprocess, "run") as run,
    ):
        generator.generate_data_files_with_tpchgen(args, {"tables": []})
    assert run.call_count == sum(partitions.values())
    for call in run.call_args_list:
        argv = call.args[0]
        assert (argv[argv.index("--nationkey-type") + 1], argv[argv.index("--regionkey-type") + 1]) == expected_types
        assert argv[argv.index("--parquet-version") + 1] == "2"
    metadata = json.loads((tmp_path / "metadata.json").read_text())
    assert (metadata["nationkey_type"], metadata["regionkey_type"]) == expected_types


@pytest.mark.parametrize("nation,region", KEY_TYPES)
def test_inferred_ddl_matches_key_widths_and_readback(tmp_path, nation, region):
    types = {"i32": (pa.int32(), "INTEGER"), "i64": (pa.int64(), "BIGINT")}
    columns = {
        "customer": {"c_nationkey": nation, "c_custkey": "i64"},
        "supplier": {"s_nationkey": nation, "s_suppkey": "i64"},
        "nation": {"n_nationkey": nation, "n_regionkey": region},
        "region": {"r_regionkey": region},
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
    try:
        schemas.generate_table_schemas("tpch", tmp_path / "schemas", data_dir)
        for table, keys in columns.items():
            ddl = (tmp_path / "schemas" / f"{table}.sql").read_text()
            ddl_columns = dict(re.findall(r'^\s+"?(\w+)"?\s+(\w+)\s+NOT NULL', ddl, flags=re.MULTILINE))
            assert ddl_columns == {key: types[width][1] for key, width in keys.items()}
        rows = duckdb.execute(
            "SELECT n_nationkey, n_regionkey FROM read_parquet(?) ORDER BY n_nationkey",
            [str(data_dir / "nation/nation.parquet")],
        ).fetchall()
        assert rows == [(0, 0), (1, 1), (24, 4)]
    finally:
        for table in columns:
            duckdb.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("benchmark,use_duckdb", [("tpch", True), ("tpcds", None), ("tpcds", True)])
@pytest.mark.parametrize("nation,region", [("i32", "i64"), ("i64", "i32")])
def test_unsupported_override_preserves_existing_output(tmp_path, benchmark, use_duckdb, nation, region):
    existing = tmp_path / "keep.parquet"
    existing.write_text("existing dataset")
    args = SimpleNamespace(
        benchmark_type=benchmark,
        use_duckdb=use_duckdb,
        nationkey_type=nation,
        regionkey_type=region,
        data_dir_path=tmp_path,
    )
    with pytest.raises(ValueError, match="require the tpchgen"):
        generator.generate_data_files(args)
    assert existing.read_text() == "existing dataset"


@pytest.fixture
def setup_command(tmp_path):
    scripts = tmp_path / "presto/scripts"
    scripts.mkdir(parents=True)
    for name in ["setup_benchmark_data_and_tables.sh", "setup_benchmark_helper_check_instance_and_parse_args.sh"]:
        shutil.copyfile(ROOT / "presto/scripts" / name, scripts / name)
    runner = tmp_path / "scripts/run_py_script.sh"
    runner.parent.mkdir()
    recorder = (
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        'with open(os.environ["CALLS"], "a") as f:\n'
        '    f.write(json.dumps(sys.argv[1:]) + "\\n")\n'
    )
    for path in [runner, scripts / "setup_benchmark_tables.sh"]:
        path.write_text(recorder)
        path.chmod(0o755)
    (tmp_path / "benchmark_data_tools").mkdir()
    calls = tmp_path / "calls.jsonl"
    env = {**os.environ, "PRESTO_DATA_DIR": str(tmp_path / "data"), "CALLS": str(calls)}
    return (
        [
            "bash",
            str(scripts / "setup_benchmark_data_and_tables.sh"),
            "-b",
            "tpch",
            "-s",
            "tpch",
            "-d",
            "tpch",
            "-f",
            "1",
        ],
        env,
        calls,
    )


def test_setup_forwards_widths_then_sets_up_tables(setup_command):
    command, env, calls = setup_command
    result = subprocess.run(
        [*command, "--nationkey-type", "i32", "--regionkey-type", "i64"], env=env, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    generate, setup = [json.loads(line) for line in calls.read_text().splitlines()]
    assert generate[-4:] == ["--nationkey-type", "i32", "--regionkey-type", "i64"]
    assert setup == ["-b", "tpch", "-s", "tpch", "-d", "tpch"]


@pytest.mark.parametrize(
    "options", [["--nationkey-type"], ["--regionkey-type", "--no-docker"], ["--nationkey-type", "int32"]]
)
def test_setup_rejects_invalid_width_before_generating(setup_command, options):
    command, env, calls = setup_command
    result = subprocess.run(command + options, env=env, capture_output=True, text=True)
    assert result.returncode != 0
    assert "requires i32 or i64" in result.stderr
    assert not calls.exists()
