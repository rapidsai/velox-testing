# Copyright (c) 2025, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import pyarrow.parquet as pq
import pyarrow as pa
from pyarrow import compute as pc
import argparse
import duckdb
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from duckdb_utils import map_table_schemas

# Multi-thread file processing
def process_dir(input_dir, output_dir, num_threads, verbose, table_to_schema_map):
    with ThreadPoolExecutor(num_threads) as executor:
        futures = []
        for root, _, files in os.walk(input_dir):
            for file in files:
                if file.endswith('.parquet'):
                    input_file_path = os.path.join(root, file)
                    futures.append(executor.submit(process_file, input_file_path, output_dir, input_dir, verbose, table_to_schema_map))
        for future in futures:
            future.result()

def process_file(input_file_path, output_dir, input_dir, verbose, table_to_schema_map):
    relative_path = os.path.relpath(os.path.dirname(input_file_path), input_dir)
    output_file_path = os.path.join(output_dir, relative_path, os.path.basename(input_file_path))

    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

    # Open parquet file and derive original schema
    parquet_file = pq.ParquetFile(input_file_path)
    original_schema = parquet_file.schema_arrow

    if verbose:
        print(f"Converting {input_file_path} to {output_file_path}")

    # Determine expected types from DuckDB schema for this table
    table_name = os.path.basename(input_file_path).split('-')[0]
    assert table_name in table_to_schema_map, f"Expected table {table_name} not found in schema"
    table_schema = table_to_schema_map.get(table_name)
    expected_types = {row[0]: row[1] for row in table_schema}

    # Build output schema (cast DECIMAL->FLOAT64, optionally INT64->INT32 when DuckDB says INTEGER)
    new_fields = []
    for field in original_schema:
        new_type = field.type
        if pa.types.is_decimal(new_type):
            if verbose:
                print(f"type mismatch on col: {field.name} (decimal) casting to (float)")
            new_type = pa.float64()
        elif pa.types.is_int64(new_type) and expected_types.get(field.name) == "INTEGER":
            if verbose:
                print(f"type mismatch on col: {field.name} (int64) casting to (int32)")
            new_type = pa.int32()
        new_fields.append(pa.field(field.name, new_type))
    new_schema = pa.schema(new_fields)

    # Stream-read and write in small batches to avoid high memory usage
    writer = pq.ParquetWriter(output_file_path, new_schema)
    try:
        for batch in parquet_file.iter_batches(batch_size=65536):
            names = batch.schema.names
            casted_arrays = []
            for i, name in enumerate(names):
                arr = batch.column(i)
                if pa.types.is_decimal(arr.type):
                    arr = pc.cast(arr, pa.float64())
                elif pa.types.is_int64(arr.type) and expected_types.get(name) == "INTEGER":
                    arr = pc.cast(arr, pa.int32())
                casted_arrays.append(arr)
            casted_batch = pa.RecordBatch.from_arrays(casted_arrays, names)
            writer.write_table(pa.Table.from_batches([casted_batch]))
    finally:
        writer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Alter an exising directory of parquet files",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('--input-dir', '-i', help='Path to input Parquet files' )
    parser.add_argument('--output-dir', '-o', help='Path to output Parquet files')
    parser.add_argument('--num-threads', '-n', help='Number of threads')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose')

    args = parser.parse_args()
    table_to_schema_map = map_table_schemas(bool(args.verbose))
    process_dir(Path(args.input_dir), Path(args.output_dir), int(args.num_threads), bool(args.verbose), table_to_schema_map)
