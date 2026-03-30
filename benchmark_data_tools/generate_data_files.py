#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""
Generate TPC-H datasets with specific partition configurations.

Configuration for partition counts based on scale factor.
These partition counts give us roughly 100,000,000 rows per file.

The total number of rows is determined by the scale factor and the table multipliers.
Table sizes (from Fig. 2 in https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf):
Table      Multiplier
part       200000
partsupp   800000
supplier   10000
customer   150000
lineitem   6000000
orders     1500000

With the remaining tables (nation, region) being constant.
The number of partitions equals:
  max(1, ceil(SF * multiplier / 100_000_000))
"""

import argparse
import json
import math
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
from duckdb_utils import init_benchmark_tables, is_decimal_column

# Table multipliers (rows per scale factor)
TABLE_MULTIPLIERS = {
    "customer": 150000,
    "lineitem": 6000000,
    "nation": 0,  # constant size, always 1 partition
    "orders": 1500000,
    "part": 200000,
    "partsupp": 800000,
    "region": 0,  # constant size, always 1 partition
    "supplier": 10000,
}

# Target rows per partition file
TARGET_ROWS_PER_FILE = 100_000_000

# Per-table parquet row group byte defaults
# These give approximately 1,000,000 rows per row group (maximum).
PARQUET_ROW_GROUP_BYTES_DEFAULTS = {
    "customer": 165000000,
    "lineitem": 68700000,
    "nation": 5000,
    "orders": 99000000,
    "part": 69000000,
    "partsupp": 147000000,
    "region": 5000,
    "supplier": 154000000,
}

# Default: disable compression for certain columns to match cudf-polars defaults at sf3k
UNCOMPRESSED_COLUMN_OVERRIDES = (
    "c_mktsegment,c_nationkey,l_commitdate,l_discount,l_quantity,l_receiptdate,"
    "l_shipdate,l_shipinstruct,l_shipmode,l_tax,n_nationkey,n_regionkey,o_orderdate,"
    "o_orderpriority,o_shippriority,p_brand,p_container,p_mfgr,p_size,p_type,"
    "r_regionkey,s_nationkey"
)

DELTA_BINARY_PACKED = "DELTA_BINARY_PACKED"
DELTA_LENGTH_BYTE_ARRAY = "DELTA_LENGTH_BYTE_ARRAY"
PLAIN = "PLAIN"
RLE_DICTIONARY = "RLE_DICTIONARY"
DEFAULT_COLUMN_ENCODINGS = {
    "l_comment": DELTA_LENGTH_BYTE_ARRAY,
    "ps_comment": DELTA_LENGTH_BYTE_ARRAY,
    "l_extendedprice": DELTA_BINARY_PACKED,
    "l_partkey": DELTA_BINARY_PACKED,
    "o_comment": DELTA_LENGTH_BYTE_ARRAY,
    "l_orderkey": DELTA_BINARY_PACKED,
    "o_orderkey": DELTA_BINARY_PACKED,
    "o_totalprice": DELTA_BINARY_PACKED,
    "o_custkey": DELTA_BINARY_PACKED,
    "ps_supplycost": PLAIN,
    "c_comment": DELTA_LENGTH_BYTE_ARRAY,
    "ps_partkey": DELTA_BINARY_PACKED,
    "l_suppkey": DELTA_BINARY_PACKED,
    "p_name": DELTA_LENGTH_BYTE_ARRAY,
    "p_partkey": DELTA_BINARY_PACKED,
    "c_custkey": DELTA_BINARY_PACKED,
    "c_address": DELTA_LENGTH_BYTE_ARRAY,
    "c_acctbal": DELTA_BINARY_PACKED,
    "ps_availqty": PLAIN,
    "ps_suppkey": DELTA_BINARY_PACKED,
    "p_comment": DELTA_LENGTH_BYTE_ARRAY,
    "l_receiptdate": PLAIN,
    "l_shipdate": PLAIN,
    "l_commitdate": PLAIN,
    "c_name": DELTA_LENGTH_BYTE_ARRAY,
    "c_phone": DELTA_LENGTH_BYTE_ARRAY,
    "l_linestatus": PLAIN,
    "o_orderdate": PLAIN,
    "s_suppkey": DELTA_BINARY_PACKED,
    "l_returnflag": PLAIN,
    "o_clerk": PLAIN,
    "s_address": DELTA_LENGTH_BYTE_ARRAY,
    "s_acctbal": DELTA_BINARY_PACKED,
    "s_comment": DELTA_LENGTH_BYTE_ARRAY,
    "s_name": DELTA_LENGTH_BYTE_ARRAY,
    "s_phone": DELTA_LENGTH_BYTE_ARRAY,
    "l_quantity": PLAIN,
    "l_shipinstruct": PLAIN,
    "l_shipmode": PLAIN,
    "l_discount": PLAIN,
    "l_tax": PLAIN,
    "o_orderstatus": PLAIN,
    "o_orderpriority": PLAIN,
    "o_shippriority": PLAIN,
    "c_nationkey": PLAIN,
    "c_mktsegment": PLAIN,
    "p_size": PLAIN,
    "n_nationkey": DELTA_BINARY_PACKED,
    "n_comment": DELTA_LENGTH_BYTE_ARRAY,
    "p_container": PLAIN,
    "n_name": DELTA_LENGTH_BYTE_ARRAY,
    "r_regionkey": DELTA_BINARY_PACKED,
    "r_comment": DELTA_LENGTH_BYTE_ARRAY,
    "r_name": DELTA_LENGTH_BYTE_ARRAY,
    "n_regionkey": PLAIN,
    "p_mfgr": PLAIN,
    "s_nationkey": PLAIN,
    "p_brand": PLAIN,
    "p_type": PLAIN,
    "p_retailprice": PLAIN,
    "l_linenumber": PLAIN,
}

DEFAULT_DISABLE_DICTIONARY_ENCODING_COLUMNS = [
    "l_comment",
    "ps_comment",
    "l_extendedprice",
    "l_partkey",
    "o_comment",
    "l_orderkey",
    "o_orderkey",
    "o_totalprice",
    "c_comment",
    "ps_partkey",
    "l_suppkey",
    "p_name",
    "p_partkey",
    "c_custkey",
    "c_address",
    "c_acctbal",
    "p_comment",
    "c_name",
    "c_phone",
    "s_suppkey",
    "s_address",
    "s_acctbal",
    "s_comment",
    "s_name",
    "s_phone",
    "n_nationkey",
    "n_comment",
    "n_name",
    "r_regionkey",
    "r_comment",
    "r_name",
]


def calculate_partitions(scale: int, multiplier: int) -> int:
    """
    Calculate partition count for a table at a given scale.
    Uses ceiling division: ceil(a/b) = (a + b - 1) / b
    """
    if multiplier == 0:
        # Constant-size tables (nation, region) always get 1 partition
        return 1

    total_rows = scale * multiplier
    partitions = math.ceil(total_rows / TARGET_ROWS_PER_FILE)

    # Ensure at least 1 partition
    return max(1, partitions)


def generate_partition(
    table: str,
    num_parts: int,
    part: int,
    scale: int,
    format: str,
    output_base: Path,
    temp_root: Path,
    parquet_row_group_bytes_override: int | None,
    use_upstream_compression: bool,
    use_upstream_encoding: bool,
    parquet_version: str,
    decimal_column_type: str,
    date_column_type: str,
    nationkey_type: str,
    regionkey_type: str,
    use_upstream_disable_dictionary_encoding: bool,
    no_delta_length_byte_array: bool,
) -> tuple[str, int, float]:
    """
    Generate a single partition.

    Returns:
        Tuple of (table, part, elapsed_seconds)
    """
    start_time = time.time()

    # Create a temporary directory within the output directory
    temp_dir = temp_root / f"{table}-part-{part}-{os.getpid()}"
    temp_dir.mkdir(parents=True, exist_ok=True)

    # Determine parquet row group bytes: use override if set, otherwise per-table default
    if parquet_row_group_bytes_override is not None:
        row_group_bytes = parquet_row_group_bytes_override
    else:
        row_group_bytes = PARQUET_ROW_GROUP_BYTES_DEFAULTS[table]

    print(f"  Generating partition {part} of {num_parts} for {table}...")

    # Build the command with optional flags
    cmd = [
        "tpchgen-cli",
        "-s",
        str(scale),
        "--tables",
        table,
        f"--format={format}",
        "--output-dir",
        str(temp_dir),
        "--parts",
        str(num_parts),
        "--part",
        str(part),
        "--parquet-row-group-bytes",
        str(row_group_bytes),
        "--num-threads",
        "1",
    ]

    # Add uncompressed column overrides unless using upstream compression
    if not use_upstream_compression and UNCOMPRESSED_COLUMN_OVERRIDES:
        cmd.append(f"--uncompressed-column-overrides={UNCOMPRESSED_COLUMN_OVERRIDES}")

    # Add column encoding overrides unless using upstream encoding
    # Use DELTA_LENGTH_BYTE_ARRAY for string columns (better compression than default RLE_DICTIONARY)
    if not use_upstream_encoding:
        decimal_columns_with_delta = {"c_acctbal", "l_extendedprice", "o_totalprice", "s_acctbal"}

        for col, encoding in DEFAULT_COLUMN_ENCODINGS.items():
            if decimal_column_type == "f64" and col in decimal_columns_with_delta and encoding == DELTA_BINARY_PACKED:
                encoding = PLAIN
            elif encoding == DELTA_LENGTH_BYTE_ARRAY and no_delta_length_byte_array:
                encoding = PLAIN

            cmd.append(f"--column-encoding={col}={encoding}")

    # Add disable dictionary encoding columns if specified
    if not use_upstream_disable_dictionary_encoding:
        cmd.append(f"--disable-dictionary-encoding={','.join(DEFAULT_DISABLE_DICTIONARY_ENCODING_COLUMNS)}")

    # Add column type flags
    cmd.extend(
        [
            "--decimal-column-type",
            decimal_column_type,
            "--date-column-type",
            date_column_type,
            "--nationkey-type",
            nationkey_type,
            "--regionkey-type",
            regionkey_type,
        ]
    )

    # Add parquet version
    cmd.extend(["--parquet-version", parquet_version])

    subprocess.run(cmd, check=True)

    # Move the generated file to the final location with the desired name
    table_dir = output_base / table
    table_dir.mkdir(parents=True, exist_ok=True)

    # The file will be named table/table.1.format in the temp directory
    src_file = temp_dir / table / f"{table}.{part}.{format}"
    dst_file = table_dir / f"part.{part - 1}.{format}"

    shutil.move(str(src_file), str(dst_file))
    shutil.rmtree(temp_dir)

    elapsed = time.time() - start_time
    print(f"  Finished partition {part - 1} of {num_parts} for {table} in {elapsed:.1f}s")

    return (table, part, elapsed)


def build_job_list(partition_config: dict[str, int]) -> list[tuple[str, int, int]]:
    """
    Build a flat list of all (table, num_parts, part) jobs.

    Returns:
        List of (table, num_parts, part) tuples
    """
    jobs = []
    for table, num_parts in partition_config.items():
        for part in range(1, num_parts + 1):
            jobs.append((table, num_parts, part))
    return jobs


def generate_data_files_with_duckdb(args):
    init_benchmark_tables(args.benchmark_type, args.scale_factor)

    with open(f"{args.data_dir_path}/metadata.json", "w") as file:
        json.dump({"scale_factor": args.scale_factor}, file, indent=2)
        file.write("\n")

    tables = duckdb.sql("SHOW TABLES").fetchall()
    for (table_name,) in tables:
        table_data_dir = f"{args.data_dir_path}/{table_name}"
        Path(table_data_dir).mkdir(exist_ok=False)
        duckdb.sql(
            f"COPY ({get_select_query(table_name, args.convert_decimals_to_floats)}) "
            f"TO '{table_data_dir}/{table_name}.parquet' (FORMAT parquet)"
        )


def get_select_query(table_name, convert_decimals_to_floats):
    if convert_decimals_to_floats:
        column_metadata_rows = duckdb.query(f"DESCRIBE {table_name}").fetchall()
        column_projections = [
            get_column_projection(column_metadata, convert_decimals_to_floats)
            for column_metadata in column_metadata_rows
        ]
        query = f"SELECT {','.join(column_projections)} FROM {table_name}"
    else:
        query = f"SELECT * FROM {table_name}"
    return query


def get_column_projection(column_metadata, convert_decimals_to_floats):
    col_name, col_type, *_ = column_metadata
    if convert_decimals_to_floats and is_decimal_column(col_type):
        projection = f"CAST({col_name} AS DOUBLE) AS {col_name}"
    else:
        projection = col_name
    return projection


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate benchmark parquet data files for a given scale factor. "
        "Only the TPC-H and TPC-DS benchmarks are currently supported."
    )

    subparsers = parser.add_subparsers(
        dest="benchmark_type", help="Generate benchmark data files for a given benchmark type"
    )

    tpcds_parser = subparsers.add_parser("tpcds", help="Generate TPC-DS data files")
    tpcds_parser.add_argument(
        "-d",
        "--data-dir-path",
        type=str,
        required=True,
        help="The path to the directory that will contain the benchmark data files. "
        "This directory will be created if it does not already exist.",
    )
    tpcds_parser.add_argument(
        "-s", "--scale-factor", type=float, required=True, help="The scale factor of the generated dataset."
    )
    tpcds_parser.add_argument(
        "-c",
        "--convert-decimals-to-floats",
        action="store_true",
        required=False,
        default=False,
        help="Convert all decimal columns to float column type.",
    )
    tpcds_parser.add_argument(
        "-v", "--verbose", action="store_true", required=False, default=False, help="Extra verbose logging"
    )
    tpcds_parser.add_argument(
        "--max-rows-per-file",
        type=int,
        required=False,
        default=100_000_000,
        help="Limit number of rows in each file (creates more partitions)",
    )
    tpcds_parser.add_argument(
        "-k",
        "--keep-original-dataset",
        action="store_true",
        required=False,
        default=False,
        help="Keep the original dataset that was generated before transformations",
    )
    tpcds_parser.add_argument(
        "--approx-row-group-bytes",
        type=int,
        required=False,
        default=128 * 1024 * 1024,
        help="Approximate row group size in bytes. 128MB by default.",
    )

    # subcommand for tpch

    tpch_parser = subparsers.add_parser("tpch", help="Generate TPC-H data files")

    tpch_parser.add_argument(
        "-s",
        "--scale",
        type=int,
        default=1000,
        help="Scale factor (any positive integer; default: 1000)",
    )
    tpch_parser.add_argument(
        "-f",
        "--format",
        choices=["parquet", "tbl"],
        default="parquet",
        help="Output format: parquet or tbl (default: parquet)",
    )
    tpch_parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("tpch-data"),
        help="Base output directory (default: tpch-data)",
    )
    tpch_parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        default=None,
        help="Number of parallel jobs (default: number of CPU threads)",
    )
    tpch_parser.add_argument(
        "--parquet-row-group-bytes",
        type=int,
        default=None,
        help="Override parquet row group size in bytes for all tables",
    )
    tpch_parser.add_argument(
        "--use-upstream-compression",
        action="store_true",
        help="Use upstream default compression (compress all columns)",
    )
    tpch_parser.add_argument(
        "--use-upstream-encoding",
        action="store_true",
        help="Use upstream default encoding (PLAIN/RLE_DICTIONARY for strings)",
    )
    tpch_parser.add_argument(
        "--use-upstream-parquet-version",
        action="store_true",
        help="Use upstream Parquet version (v1 instead of v2)",
    )
    tpch_parser.add_argument(
        "--use-float-type",
        action="store_true",
        help="Use f64 for decimal columns (instead of decimal128)",
    )
    tpch_parser.add_argument(
        "--use-timestamp-type",
        action="store_true",
        help="Use timestamp_ms for date columns (instead of date32)",
    )
    tpch_parser.add_argument(
        "--use-large-ids",
        action="store_true",
        help="Use i64 for nationkey/regionkey columns (instead of i32)",
    )
    tpch_parser.add_argument(
        "--use-upstream-disable-dictionary-encoding",
        action="store_true",
        help="Use upstream default disable dictionary encoding (disable dictionary encoding for all columns)",
    )
    tpch_parser.add_argument(
        "--no-delta-length-byte-array",
        action="store_true",
        help="Use PLAIN encoding (instead of DELTA_LENGTH_BYTE_ARRAY). Some engines don't support DELTA_LENGTH_BYTE_ARRAY.",
    )

    return parser.parse_args(args)


def generate_data_files_with_tpchgen(args):
    # Determine column types based on flags
    decimal_column_type = "f64" if args.use_float_type else "decimal128"
    date_column_type = "timestamp_ms" if args.use_timestamp_type else "date32"
    nationkey_type = "i64" if args.use_large_ids else "i32"
    regionkey_type = "i64" if args.use_large_ids else "i32"
    parquet_version = "v1" if args.use_upstream_parquet_version else "v2"

    # Calculate partition counts dynamically for each table
    partition_config = {}
    for table, multiplier in TABLE_MULTIPLIERS.items():
        partition_config[table] = calculate_partitions(args.scale, multiplier)

    # Display the calculated partition configuration
    print(f"Scale factor: {args.scale}")
    print("Partition configuration:")
    for table in [
        "customer",
        "lineitem",
        "nation",
        "orders",
        "part",
        "partsupp",
        "region",
        "supplier",
    ]:
        print(f"  {table}: {partition_config[table]} partition(s)")

    # Create the base output directory and temporary directory
    output_base = args.output
    output_base.mkdir(parents=True, exist_ok=True)
    temp_root = output_base / ".tmp"
    temp_root.mkdir(parents=True, exist_ok=True)

    # Build job list
    jobs = build_job_list(partition_config)
    total_jobs = len(jobs)
    print(f"Generating {total_jobs} total partitions across all tables using {args.jobs} threads...")

    try:
        # Generate all partitions in parallel across all tables
        with ThreadPoolExecutor(max_workers=args.jobs) as executor:
            futures = []
            for table, num_parts, part in jobs:
                future = executor.submit(
                    generate_partition,
                    table=table,
                    num_parts=num_parts,
                    part=part,
                    scale=args.scale,
                    format=args.format,
                    output_base=output_base,
                    temp_root=temp_root,
                    parquet_row_group_bytes_override=args.parquet_row_group_bytes,
                    use_upstream_compression=args.use_upstream_compression,
                    use_upstream_encoding=args.use_upstream_encoding,
                    parquet_version=parquet_version,
                    decimal_column_type=decimal_column_type,
                    date_column_type=date_column_type,
                    nationkey_type=nationkey_type,
                    regionkey_type=regionkey_type,
                    use_upstream_disable_dictionary_encoding=args.use_upstream_disable_dictionary_encoding,
                    no_delta_length_byte_array=args.no_delta_length_byte_array,
                )
                futures.append(future)

            # Wait for all futures to complete and handle exceptions
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error generating partition: {e}")
                    raise

    finally:
        # Cleanup temporary files
        print("Cleaning up temporary files...")
        if temp_root.exists():
            shutil.rmtree(temp_root)

    print("TPC-H data generation complete!")
    print(f"Data has been generated in: {output_base}")

    # Generate metadata.json
    script_dir = Path(__file__).parent
    inspect_script = script_dir / "inspect_tpch_parquet.py"

    print("Generating metadata.json...")

    options = {
        "scale_factor": args.scale,
        "format": args.format,
        "output_base_dir": str(output_base),
        "threads": args.jobs,
        "use_upstream_compression": args.use_upstream_compression,
        "use_upstream_encoding": args.use_upstream_encoding,
        "parquet_version": parquet_version,
        "nationkey_type": nationkey_type,
        "regionkey_type": regionkey_type,
        "decimal_column_type": decimal_column_type,
        "date_column_type": date_column_type,
        "parquet_row_group_bytes_customer": PARQUET_ROW_GROUP_BYTES_DEFAULTS["customer"],
        "parquet_row_group_bytes_lineitem": PARQUET_ROW_GROUP_BYTES_DEFAULTS["lineitem"],
        "parquet_row_group_bytes_nation": PARQUET_ROW_GROUP_BYTES_DEFAULTS["nation"],
        "parquet_row_group_bytes_orders": PARQUET_ROW_GROUP_BYTES_DEFAULTS["orders"],
        "parquet_row_group_bytes_part": PARQUET_ROW_GROUP_BYTES_DEFAULTS["part"],
        "parquet_row_group_bytes_partsupp": PARQUET_ROW_GROUP_BYTES_DEFAULTS["partsupp"],
        "parquet_row_group_bytes_region": PARQUET_ROW_GROUP_BYTES_DEFAULTS["region"],
        "parquet_row_group_bytes_supplier": PARQUET_ROW_GROUP_BYTES_DEFAULTS["supplier"],
    }

    subprocess.run(
        [
            sys.executable,
            str(inspect_script),
            str(output_base),
            "--output",
            "json",
            "--output-file",
            str(output_base / "metadata.json"),
            "--options",
            json.dumps(options),
        ],
        check=True,
    )

    print(f"Metadata written to: {output_base / 'metadata.json'}")


def main(args=None):
    parsed = parse_args(args)
    if os.path.exists(args.data_dir_path):
        shutil.rmtree(args.data_dir_path)
    Path(f"{parsed.data_dir_path}").mkdir(parents=True, exist_ok=True)

    # tpchgen is much faster, but is exclusive to generating tpch data.  Use duckdb as a fallback.
    if parsed.benchmark_type == "tpch" and not parsed.use_duckdb:
        if parsed.verbose:
            print("generating with tpchgen")
        generate_data_files_with_tpchgen(parsed)
    else:
        if parsed.verbose:
            print("generating with duckdb")
        generate_data_files_with_duckdb(parsed)


if __name__ == "__main__":
    main()
