# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from tqdm import tqdm

# Tables to sort and their sort columns for row-group pruning.
SORT_COLUMNS = {
    "tpch": {
        "lineitem": "l_shipdate",
        "orders": "o_orderdate",
    },
}

# Cap sort concurrency to avoid excessive memory usage.
# Each file is fully loaded + sorted in memory, so this limits peak RAM.
MAX_SORT_THREADS = 16

# 3 phases per file for progress tracking.
_PHASES_PER_FILE = 3  # read, sort, write


def sort_tables(data_dir_path, benchmark_type, num_threads, verbose):
    table_sort_map = SORT_COLUMNS.get(benchmark_type, {})
    if not table_sort_map:
        if verbose:
            print(f"No sort columns defined for benchmark '{benchmark_type}', skipping sort step.")
        return

    sort_threads = min(num_threads, MAX_SORT_THREADS)

    # Count total files across all tables.
    table_files = {}
    total_files = 0
    for table_name, sort_column in table_sort_map.items():
        table_dir = os.path.join(data_dir_path, table_name)
        if not os.path.isdir(table_dir):
            continue
        files = sorted(f for f in os.listdir(table_dir) if f.endswith(".parquet"))
        if files:
            table_files[table_name] = (table_dir, sort_column, files)
            total_files += len(files)

    if not table_files:
        return

    total_steps = total_files * _PHASES_PER_FILE
    with tqdm(total=total_steps, desc="Sorting (overall)", unit="step", position=0) as overall_bar:
        for table_name, (table_dir, sort_column, files) in table_files.items():
            table_steps = len(files) * _PHASES_PER_FILE

            with tqdm(total=table_steps, desc=f"  {table_name} by {sort_column}", unit="step", position=1, leave=False) as table_bar:
                def on_phase():
                    table_bar.update(1)
                    overall_bar.update(1)

                with ThreadPoolExecutor(sort_threads) as executor:
                    futures = {
                        executor.submit(_sort_parquet_file, os.path.join(table_dir, f), sort_column, on_phase): f
                        for f in files
                    }
                    for future in as_completed(futures):
                        future.result()


def _sort_parquet_file(file_path, sort_column, on_phase=None):
    parquet_file = pq.ParquetFile(file_path)
    metadata = parquet_file.metadata
    row_group_sizes = [metadata.row_group(i).num_rows for i in range(metadata.num_row_groups)]

    # Phase 1: read
    table = pq.read_table(file_path)
    if on_phase:
        on_phase()

    # Cast string columns to large_string to avoid 2 GB offset overflow in take().
    # Parquet writes large_string identically to string, so no cast back needed.
    string_col_indices = [
        i for i, f in enumerate(table.schema) if f.type == pa.string()
    ]
    if string_col_indices:
        columns = table.columns
        new_fields = list(table.schema)
        for i in string_col_indices:
            columns[i] = columns[i].cast(pa.large_string())
            new_fields[i] = new_fields[i].with_type(pa.large_string())
        table = pa.table(columns, schema=pa.schema(new_fields))

    # Phase 2: sort
    indices = pc.sort_indices(table.column(sort_column))
    sorted_columns = [pc.take(table.column(col), indices) for col in table.schema.names]
    table = pa.table(sorted_columns, schema=table.schema)
    if on_phase:
        on_phase()

    # Phase 3: write
    dir_name = os.path.dirname(file_path)
    fd, tmp_path = tempfile.mkstemp(suffix=".parquet", dir=dir_name)
    os.close(fd)
    try:
        writer = pq.ParquetWriter(tmp_path, table.schema)
        offset = 0
        for rg_size in row_group_sizes:
            writer.write_table(table.slice(offset, rg_size), row_group_size=rg_size)
            offset += rg_size
        if offset < len(table):
            writer.write_table(table.slice(offset), row_group_size=len(table) - offset)
        writer.close()
        os.replace(tmp_path, file_path)
    except Exception:
        os.unlink(tmp_path)
        raise
    if on_phase:
        on_phase()
