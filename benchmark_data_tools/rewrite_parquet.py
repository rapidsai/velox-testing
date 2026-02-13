# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import argparse
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import compute as pc


# Multi-thread file processing
def process_dir(input_dir, output_dir, num_threads, verbose, convert_decimal_to_float):
    with ThreadPoolExecutor(num_threads) as executor:
        futures = []
        for root, _, files in os.walk(input_dir):
            for file in files:
                if file.endswith(".parquet"):
                    input_file_path = os.path.join(root, file)
                    futures.append(
                        executor.submit(
                            process_file, input_file_path, output_dir, input_dir, verbose, convert_decimal_to_float
                        )
                    )
        for future in futures:
            future.result()


def process_file(input_file_path, output_dir, input_dir, verbose, convert_decimal_to_float):
    relative_path = os.path.relpath(os.path.dirname(input_file_path), input_dir)
    output_file_path = os.path.join(output_dir, relative_path, os.path.basename(input_file_path))

    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

    # Open parquet file and derive original schema
    parquet_file = pq.ParquetFile(input_file_path)
    original_schema = parquet_file.schema_arrow

    if verbose:
        print(f"Converting {input_file_path} to {output_file_path}")

    # Build output schema (cast DECIMAL->FLOAT64)
    new_fields = []
    for field in original_schema:
        new_type = field.type
        if convert_decimal_to_float and pa.types.is_decimal(new_type):
            if verbose:
                print(f"type mismatch on col: {field.name} (decimal) casting to (float)")
            new_type = pa.float64()
        new_fields.append(pa.field(field.name, new_type))
    new_schema = pa.schema(new_fields)

    # Stream-read and write in small batches to avoid high memory usage
    writer = pq.ParquetWriter(output_file_path, new_schema)
    try:
        for row_group_index in range(parquet_file.num_row_groups):
            row_group = parquet_file.read_row_group(row_group_index)
            names = row_group.schema.names
            casted_arrays = []
            for i, name in enumerate(names):
                arr = row_group.column(i)
                if convert_decimal_to_float and pa.types.is_decimal(arr.type):
                    new_type = pa.field(name, pa.float64()).type
                    arr = pc.cast(arr, new_type)
                casted_arrays.append(arr)
            orig_row_group_size = parquet_file.metadata.row_group(row_group_index).num_rows
            writer.write_table(pa.table(casted_arrays, schema=new_schema), row_group_size=orig_row_group_size)
    finally:
        writer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Alter an existing directory of parquet files", formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("-i", "--input-dir", type=Path, required=True, help="Path to input Parquet files")
    parser.add_argument("-o", "--output-dir", type=Path, required=True, help="Path to output Parquet files")
    parser.add_argument("-j", "--num-threads", type=int, help="Number of threads")
    parser.add_argument("-v", "--verbose", type=bool, action="store_true", help="Verbose")
    parser.add_argument(
        "-c",
        "--convert-decimals-to-floats",
        action="store_true",
        help="Convert all decimal columns to float column type.",
    )

    args = parser.parse_args()
    process_dir(args.input_dir, args.output_dir, args.num_threads, args.verbose, args.convert_decimals_to_floats)
