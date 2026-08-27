# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import multiprocessing
# BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
import os
import resource
import sys
# END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
import tempfile
# BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
import time
# END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
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
    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    metrics=None,
    clock=time.monotonic,
    monotonic_origin=None,
    # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
):
    """Yield a callable that waits for row-count estimates from a spawned process."""
    with (
        tempfile.TemporaryDirectory(prefix=".duckdb-probe-", dir=data_dir_path) as probe_directory,
        ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context("spawn")) as executor,
    ):
        # dbgen/dsdgen use process-global state and cannot run concurrently in one process.
        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        submit_started = clock()
        if monotonic_origin is None:
            monotonic_origin = submit_started
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        future = executor.submit(
            get_row_group_row_counts,
            benchmark_type,
            target_bytes,
            target_scale_factor,
            convert_decimals_to_floats,
            probe_directory,
            # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
            metrics is not None,
            monotonic_origin,
            # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        )
        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        if metrics is not None:
            submit_seconds = clock() - submit_started
            metrics["submit_seconds"] = submit_seconds
            metrics["submit_interval"] = {
                "start_offset_seconds": submit_started - monotonic_origin,
                "duration_seconds": submit_seconds,
            }

            def result():
                wait_started = clock()
                try:
                    row_counts, child_metrics = future.result()
                finally:
                    completed = clock()
                    metrics["wait_seconds"] = completed - wait_started
                    metrics["complete_seconds"] = completed - submit_started
                    metrics["parent_total_interval"] = {
                        "start_offset_seconds": submit_started - monotonic_origin,
                        "duration_seconds": completed - submit_started,
                    }
                    metrics["parent_wait_interval"] = {
                        "start_offset_seconds": wait_started - monotonic_origin,
                        "duration_seconds": completed - wait_started,
                    }
                metrics["child"] = child_metrics
                return row_counts

            yield result
        else:
            # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
            yield future.result


def get_row_group_row_counts(
    benchmark_type,
    target_bytes,
    target_scale_factor,
    convert_decimals_to_floats,
    probe_directory,
    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    include_metrics=False,
    monotonic_origin=None,
    # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
):
    """Measure rows per row group for every table in a throwaway dataset."""
    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    total_started = time.monotonic()
    if monotonic_origin is None:
        monotonic_origin = total_started
    # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    scale_factor = (
        target_scale_factor if target_scale_factor <= 1 else min(target_scale_factor / 10, _MAX_PROBE_SCALE_FACTOR)
    )

    row_counts = {}
    with duckdb.connect(str(Path(probe_directory) / "probe.duckdb")) as conn:
        conn.execute(f"SET memory_limit='{_PROBE_MEMORY_LIMIT}'")
        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        generation_started = time.monotonic()
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        init_benchmark_tables(benchmark_type, scale_factor, conn)
        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        generation_seconds = time.monotonic() - generation_started
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)

        # One thread so buffers are not multiplied by the generation thread count.
        conn.execute("SET threads=1")
        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        probe_started = time.monotonic()
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        for (table_name,) in conn.execute("SHOW TABLES").fetchall():
            select_query = get_select_query(table_name, convert_decimals_to_floats, conn)
            table_rows = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            rows = _measure_row_group_rows(conn, select_query, table_rows, target_bytes)
            if rows is not None:
                row_counts[table_name] = rows
        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        probe_seconds = time.monotonic() - probe_started
        if include_metrics:
            settings = {
                name: conn.execute("SELECT value FROM duckdb_settings() WHERE name = ?", [name]).fetchone()[0]
                for name in ("threads", "memory_limit", "preserve_insertion_order")
            }
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    if not include_metrics:
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        return row_counts
    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    total_seconds = time.monotonic() - total_started
    return row_counts, {
        "generation_seconds": generation_seconds,
        "probe_seconds": probe_seconds,
        "total_seconds": total_seconds,
        "scale_factor": scale_factor,
        "settings": settings,
        "rss_hwm_bytes": _rss_hwm_bytes(),
        "pid": os.getpid(),
        "phase_intervals": {
            "proxy_generation": {
                "start_offset_seconds": generation_started - monotonic_origin,
                "duration_seconds": generation_seconds,
            },
            "proxy_probe": {
                "start_offset_seconds": probe_started - monotonic_origin,
                "duration_seconds": probe_seconds,
            },
            "proxy_child_total": {
                "start_offset_seconds": total_started - monotonic_origin,
                "duration_seconds": total_seconds,
            },
        },
    }


def _rss_hwm_bytes():
    """Return this process's peak resident set size in bytes."""
    # Linux reports KiB; macOS reports bytes.
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if sys.platform == "darwin" else value * 1024
# END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)


def _measure_row_group_rows(conn, select_query, table_rows, target_bytes):
    """Rows per row group so that a row group weighs about target_bytes."""
    if table_rows == 0:
        return None

    with tempfile.TemporaryDirectory() as tmp_dir:
        probe_path = Path(tmp_dir) / "probe.parquet"

        # First pass: estimate bytes/row using DuckDB's default row-group size.
        copy_to_parquet(f"{select_query} LIMIT {_STAGE1_ROWS}", probe_path, conn=conn)
        rows = _rows_for_target(_bytes_per_row(probe_path), target_bytes)
        if rows is None or rows >= table_rows:
            # A second write cannot fill the estimated row group or improve the result.
            return rows

        # Second pass: bytes/row changes with row-group size, so remeasure near the requested size.
        copy_to_parquet(f"{select_query} LIMIT {rows}", probe_path, rows, conn)
        refined = _rows_for_target(_bytes_per_row(probe_path), target_bytes)

    return rows if refined is None else refined


def _bytes_per_row(path):
    """Read bytes/row from the probe's only row group."""
    row_group = pq.ParquetFile(path).metadata.row_group(0)
    return row_group.total_byte_size / row_group.num_rows


def _rows_for_target(bytes_per_row, target_bytes):
    """Round to the nearest 2,048-row multiple instead of letting DuckDB round up."""
    if not bytes_per_row:
        return None
    rows = round(target_bytes / bytes_per_row / _ROW_GROUP_GRANULARITY)
    return max(rows, 1) * _ROW_GROUP_GRANULARITY
