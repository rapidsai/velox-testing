# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import threading
import time
from pathlib import Path

import duckdb
import generate_data_files as generator
import pyarrow.parquet as pq
import pytest
from duckdb_utils import init_benchmark_tables
from generate_data_files import generate_data_files

from .common_fixtures import DataGenArgs

# Held inside each write so that concurrent writers reliably overlap in time.
_WRITER_OVERLAP_HOLD_SECONDS = 0.02


def test_max_rows_per_file_splits_tables(setup_and_teardown):
    """Validate that max_rows_per_file controls file partitioning.

    Verifies that:
    - At least one table is split into multiple files
    - Part files use contiguous 1-based names, including single-part tables
    - Every part contains between one and max_rows_per_file rows
    - The parts contain the same total number of rows as the source table
    """
    data_dir_path, args = setup_and_teardown
    # SF1 tables fit below the 100M default, so use a lower limit to exercise splitting.
    args.max_rows_per_file = 500_000
    expected_row_counts = get_expected_row_counts(args.benchmark_type, args.scale_factor)
    generate_data_files(args)

    assert_partitions_respect_max_rows_per_file(
        data_dir_path,
        args.max_rows_per_file,
        expected_row_counts,
    )


@pytest.mark.parametrize(
    ("benchmark_type", "use_duckdb"),
    [
        ("tpch", True),
        ("tpcds", False),
    ],
)
def test_duckdb_export_configuration_starts_after_materialization(
    benchmark_type,
    use_duckdb,
    tmp_path,
    monkeypatch,
):
    """Apply order-free export settings only after materialization, including writer cursors."""
    data_dir_path = tmp_path / benchmark_type
    args = build_plan_only_args(data_dir_path)
    args.benchmark_type = benchmark_type
    args.use_duckdb = use_duckdb
    args.convert_decimals_to_floats = False
    args.num_threads = 2

    observed_settings = {}
    writer_settings = []
    installed_extensions = []
    connection_configs = []
    real_connect = duckdb.connect
    real_copy_to_parquet = generator.copy_to_parquet

    class InstallConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def sql(self, statement):
            installed_extensions.append(statement)

    def connect(*connect_args, **connect_kwargs):
        if not connect_args and not connect_kwargs:
            return InstallConnection()
        connection_configs.append(connect_kwargs["config"])
        return real_connect(*connect_args, **connect_kwargs)

    def materialize_tables(materialize_args, conn):
        assert materialize_args is args
        observed_settings["materialization"] = get_duckdb_setting(conn, "preserve_insertion_order")
        conn.sql("CREATE TABLE synthetic AS SELECT i AS value FROM range(9) AS rows(i)")
        return {"synthetic": 2}

    def copy_to_parquet(select_query, file_path, rows_per_row_group, conn):
        writer_settings.append(
            (
                get_duckdb_setting(conn, "preserve_insertion_order"),
                get_duckdb_setting(conn, "threads"),
            )
        )
        return real_copy_to_parquet(select_query, file_path, rows_per_row_group, conn)

    monkeypatch.setattr(generator.duckdb, "connect", connect)
    monkeypatch.setattr(generator, "materialize_tables", materialize_tables)
    monkeypatch.setattr(generator, "copy_to_parquet", copy_to_parquet)

    generator.generate_data_files(args)

    assert installed_extensions == [f"INSTALL {benchmark_type}"]
    assert connection_configs == [{"memory_limit": "512GiB"}]
    assert observed_settings == {"materialization": "true"}
    assert writer_settings == [("false", "3")] * 3

    part_paths = sorted((data_dir_path / "synthetic").glob("*.parquet"))
    assert [path.name for path in part_paths] == [
        "synthetic-1.parquet",
        "synthetic-2.parquet",
        "synthetic-3.parquet",
    ]
    assert [
        sorted(pq.ParquetFile(path).read(columns=["value"]).column("value").to_pylist()) for path in part_paths
    ] == [
        [0, 1, 2, 3],
        [4, 5, 6, 7],
        [8],
    ]


def test_export_plan_is_one_queue_across_tables(tmp_path):
    """Validate that the export plan is a single flat queue rather than per-table batches.

    Verifies that:
    - Every part of every table is one entry of one list, in table then part order
    - Parts are named contiguously from 1, including tables that are not split
    - Split parts read disjoint, contiguous rowid ranges
    """
    args = build_plan_only_args(tmp_path)
    with duckdb.connect() as conn:
        conn.sql("CREATE TABLE alpha AS SELECT i FROM range(10) AS t(i)")
        conn.sql("CREATE TABLE beta AS SELECT i FROM range(3) AS t(i)")
        tasks = generator.plan_export_tasks(args, {}, conn)

    assert [task.table_name for task in tasks] == ["alpha", "alpha", "alpha", "beta"]
    assert [Path(task.file_path).name for task in tasks] == [
        "alpha-1.parquet",
        "alpha-2.parquet",
        "alpha-3.parquet",
        "beta-1.parquet",
    ]
    # A split table reads disjoint windows; an unsplit table reads the whole table.
    assert [task.query.partition(" WHERE ")[2] for task in tasks] == [
        "rowid >= 0 AND rowid < 4",
        "rowid >= 4 AND rowid < 8",
        "rowid >= 8 AND rowid < 12",
        "",
    ]


def test_export_writers_share_one_global_budget(setup_and_teardown, monkeypatch):
    """Validate that concurrent Parquet writers are capped for the export as a whole.

    Verifies that:
    - Writers from the whole export never exceed num_threads at once
    - More than one writer really runs at a time, so the cap is not hiding a serial export
    - Every produced file was written through the observed path
    """
    data_dir_path, args = setup_and_teardown
    # Both benchmark types must take the DuckDB export path, and be split into many parts.
    args.use_duckdb = True
    args.max_rows_per_file = 500_000
    writers = WriterObserver(generator.copy_to_parquet)
    monkeypatch.setattr(generator, "copy_to_parquet", writers)

    generate_data_files(args)

    assert writers.maximum_concurrent <= args.num_threads
    assert writers.maximum_concurrent > 1, "writers never overlapped, so the export ran serially"
    assert writers.calls == len(list(Path(data_dir_path).rglob("*.parquet")))


def test_failed_export_publishes_no_partial_file(setup_and_teardown, monkeypatch):
    """Validate that a failed export leaves no readable output behind.

    Verifies that:
    - The failure propagates instead of producing an incomplete dataset
    - The failed part has no Parquet file, and no *.partial file survives anywhere
    - Every file that does exist is a complete, readable Parquet file
    - metadata.json is absent, so the dataset is not marked complete
    """
    data_dir_path, args = setup_and_teardown
    args.use_duckdb = True
    args.max_rows_per_file = 500_000
    failing_writer = FailingWriter(generator.copy_to_parquet, fail_on_call=4)
    monkeypatch.setattr(generator, "copy_to_parquet", failing_writer)

    with pytest.raises(RuntimeError, match="injected export failure"):
        generate_data_files(args)

    assert not Path(failing_writer.failed_file_path.removesuffix(".partial")).exists()
    assert list(Path(data_dir_path).rglob("*.partial")) == []
    assert not Path(f"{data_dir_path}/metadata.json").exists()
    for file_path in Path(data_dir_path).rglob("*.parquet"):
        assert pq.ParquetFile(file_path).metadata.num_rows >= 0


class WriterObserver:
    """Record how many Parquet writes overlap, then delegate to the real writer."""

    def __init__(self, copy_to_parquet):
        self._copy_to_parquet = copy_to_parquet
        self._lock = threading.Lock()
        self._concurrent = 0
        self.maximum_concurrent = 0
        self.calls = 0

    def __call__(self, *copy_args, **copy_kwargs):
        with self._lock:
            self._concurrent += 1
            self.calls += 1
            self.maximum_concurrent = max(self.maximum_concurrent, self._concurrent)
        try:
            time.sleep(_WRITER_OVERLAP_HOLD_SECONDS)
            return self._copy_to_parquet(*copy_args, **copy_kwargs)
        finally:
            with self._lock:
                self._concurrent -= 1


class FailingWriter:
    """Fail one write without producing any file for it, and delegate the rest."""

    def __init__(self, copy_to_parquet, fail_on_call):
        self._copy_to_parquet = copy_to_parquet
        self._fail_on_call = fail_on_call
        self._lock = threading.Lock()
        self._calls = 0
        self.failed_file_path = None

    def __call__(self, select_query, file_path, *copy_args, **copy_kwargs):
        with self._lock:
            self._calls += 1
            should_fail = self._calls == self._fail_on_call
            if should_fail:
                self.failed_file_path = file_path
        if should_fail:
            raise RuntimeError("injected export failure")
        return self._copy_to_parquet(select_query, file_path, *copy_args, **copy_kwargs)


def build_plan_only_args(data_dir_path):
    """Arguments for planning alone, so no generator or benchmark data is needed."""
    return DataGenArgs(
        benchmark_type="tpcds",
        data_dir_path=str(data_dir_path),
        scale_factor=1.0,
        convert_decimals_to_floats=True,
        use_duckdb=True,
        num_threads=4,
        verbose=False,
        max_rows_per_file=4,
        keep_original_dataset=True,
        approx_row_group_bytes=128 * 1024 * 1024,
    )


def get_duckdb_setting(conn, name):
    return conn.execute("SELECT value FROM duckdb_settings() WHERE name = ?", [name]).fetchone()[0]


def get_expected_row_counts(benchmark_type, scale_factor):
    with duckdb.connect() as conn:
        init_benchmark_tables(benchmark_type, scale_factor, conn)
        return {
            table_name: conn.sql(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            for (table_name,) in conn.sql("SHOW TABLES").fetchall()
        }


def assert_partitions_respect_max_rows_per_file(data_dir_path, max_rows_per_file, expected_row_counts):
    split_table_count = 0
    for table_dir in sorted(path for path in Path(data_dir_path).iterdir() if path.is_dir()):
        table_name = table_dir.name
        file_paths = list(table_dir.glob("*.parquet"))
        num_parts = len(file_paths)

        # All tables use the same 1-indexed layout, including single-part tables.
        assert {path.name for path in file_paths} == {
            f"{table_name}-{part}.parquet" for part in range(1, num_parts + 1)
        }

        rows_per_part = [
            pq.ParquetFile(table_dir / f"{table_name}-{part}.parquet").metadata.num_rows
            for part in range(1, num_parts + 1)
        ]
        assert all(0 < rows <= max_rows_per_file for rows in rows_per_part), rows_per_part
        assert sum(rows_per_part) == expected_row_counts[table_name]

        if num_parts > 1:
            split_table_count += 1

    assert split_table_count > 0, "no table was split, so the partitioning path was not tested"
