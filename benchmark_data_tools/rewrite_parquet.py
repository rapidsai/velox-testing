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

    # Read the parquet file metadata
    parquet_file = pq.ParquetFile(input_file_path)
    schema = parquet_file.schema_arrow

    if verbose:
        print(f"Converting {input_file_path} to {output_file_path}")

    # Read the parquet file
    table = pq.read_table(input_file_path)
    table_name = os.path.basename(input_file_path).split('-')[0]
    assert table_name in table_to_schema_map, f"Expected table {table_name} not found in schema"
    table_schema = table_to_schema_map.get(table_name)

    # Convert decimal columns to double
    new_columns = []
    for col in table.columns:
        if pa.types.is_decimal(col.type):
            if verbose:
                print(f"type mismatch on col: {col._name} (decimal) casting to (float)")
            col = col.cast(pa.float64())
        elif col.type == pa.int64():
            for row in table_schema:
                if col._name == row[0] and row[1] == "INTEGER":
                    if verbose:
                        print(f"type mismatch on col: {col._name} (int64) casting to (int32)")
                    col = col.cast(pa.int32())
        new_columns.append(col)

    new_table = pa.Table.from_arrays(new_columns, schema.names)

    # Write the table back to a parquet file.
    # If we want to alter the file's properties (page_size, use_dictionary=False, column_encoding), we can do so here.
    pq.write_table(new_table, output_file_path)

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
