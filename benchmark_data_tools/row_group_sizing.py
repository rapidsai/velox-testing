# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import multiprocessing
import tempfile
from concurrent.futures import ProcessPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
from duckdb_utils import copy_to_parquet, get_select_query, init_benchmark_tables

# DuckDB Parquet sizing: https://duckdb.org/docs/current/data/parquet/tips
_ROW_GROUP_GRANULARITY = 2048
_STAGE1_ROWS = 122_880

_MAX_PROBE_SCALE_FACTOR = 100
_PROBE_MEMORY_LIMIT = "16GiB"


@contextmanager
def row_group_row_count_probe(
    benchmark_type,
    target_bytes,
    target_scale_factor,
    convert_decimals_to_floats,
    data_dir_path,
):
    """Yield a callable that waits for row-count estimates from a spawned process."""
    with (
        tempfile.TemporaryDirectory(prefix=".duckdb-probe-", dir=data_dir_path) as probe_directory,
        ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context("spawn")) as executor,
    ):
        # dbgen/dsdgen use process-global state and cannot run concurrently in one process.
        future = executor.submit(
            get_row_group_row_counts,
            benchmark_type,
            target_bytes,
            target_scale_factor,
            convert_decimals_to_floats,
            probe_directory,
        )
        yield future.result


def get_row_group_row_counts(
    benchmark_type,
    target_bytes,
    target_scale_factor,
    convert_decimals_to_floats,
    probe_directory,
):
    """Measure rows per row group for every table in a throwaway dataset."""
    scale_factor = (
        target_scale_factor if target_scale_factor <= 1 else min(target_scale_factor / 10, _MAX_PROBE_SCALE_FACTOR)
    )

    row_counts = {}
    with duckdb.connect(str(Path(probe_directory) / "probe.duckdb")) as conn:
        conn.execute(f"SET memory_limit='{_PROBE_MEMORY_LIMIT}'")
        init_benchmark_tables(benchmark_type, scale_factor, conn)

        # One thread so buffers are not multiplied by the generation thread count.
        conn.execute("SET threads=1")
        for (table_name,) in conn.execute("SHOW TABLES").fetchall():
            select_query = get_select_query(table_name, convert_decimals_to_floats, conn)
            table_rows = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            rows = measure_row_group_rows(conn, select_query, table_rows, target_bytes)
            if rows is not None:
                row_counts[table_name] = rows
    return row_counts


def measure_row_group_rows(conn, select_query, table_rows, target_bytes):
    """Rows per row group so that a row group weighs about target_bytes."""
    if table_rows == 0:
        return None

    with tempfile.TemporaryDirectory() as tmp_dir:
        probe_path = Path(tmp_dir) / "probe.parquet"

        # First pass: estimate bytes/row using DuckDB's default row-group size.
        copy_to_parquet(f"{select_query} LIMIT {_STAGE1_ROWS}", probe_path, conn=conn)
        rows = rows_for_target(bytes_per_row(probe_path), target_bytes)
        if rows is None or rows >= table_rows:
            # A second write cannot fill the estimated row group or improve the result.
            return rows

        # Second pass: bytes/row changes with row-group size, so remeasure near the requested size.
        copy_to_parquet(f"{select_query} LIMIT {rows}", probe_path, rows, conn)
        refined = rows_for_target(bytes_per_row(probe_path), target_bytes)

    return rows if refined is None else refined


def bytes_per_row(path):
    """Read bytes/row from the probe's only row group."""
    row_group = pq.ParquetFile(path).metadata.row_group(0)
    return row_group.total_byte_size / row_group.num_rows


def rows_for_target(bytes_per_row, target_bytes):
    """Round to the nearest 2,048-row multiple instead of letting DuckDB round up."""
    if not bytes_per_row:
        return None
    rows = round(target_bytes / bytes_per_row / _ROW_GROUP_GRANULARITY)
    return max(rows, 1) * _ROW_GROUP_GRANULARITY
