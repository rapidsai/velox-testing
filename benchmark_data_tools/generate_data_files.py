# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import argparse
import json
import math
import os
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb
from duckdb_utils import init_benchmark_tables, is_decimal_column

_INTEGER_TYPES = frozenset(("INTEGER", "BIGINT", "SMALLINT", "TINYINT", "HUGEINT", "INT"))
_HIGH_CARD_NDV_THRESHOLD = 0.99
_SAMPLE_SF = 0.01


def generate_partition(
    table,
    partition,
    raw_data_path,
    scale_factor,
    num_partitions,
    verbose,
    approx_row_group_bytes,
    convert_decimals_to_floats,
    codec_defs,
    parquet_version,
    nationkey_type,
    regionkey_type,
):
    if verbose:
        print(f"Generating '{table}' partition: {partition}")
    Path(f"{raw_data_path}/part-{partition}").mkdir(parents=True, exist_ok=True)
    command = [
        "tpchgen-cli",
        "-T",
        table,
        "-s",
        str(scale_factor),
        "--output-dir",
        str(f"{raw_data_path}/part-{partition}"),
        "--parts",
        str(num_partitions),
        "--part",
        str(partition),
        "--format",
        "parquet",
        "--parquet-row-group-bytes",
        str(approx_row_group_bytes),
        "--parquet-version",
        parquet_version,
        "--nationkey-type",
        nationkey_type,
        "--regionkey-type",
        regionkey_type,
    ]

    if convert_decimals_to_floats:
        command.extend(["--decimal-column-type", "f64"])

    command.extend(get_tpchgen_codec_args(codec_defs, table))

    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error generating TPC-H data: {e}")


def generate_data_files(args):
    if args.codec_definitions:
        if args.benchmark_type != "tpch":
            raise ValueError("--codec-definitions is currently only supported for TPC-H benchmarks")
        if args.use_duckdb:
            raise ValueError("--codec-definitions is not supported with --use-duckdb")
        codec_defs = load_codec_definitions(args.codec_definitions)
    elif args.benchmark_type == "tpch" and not args.use_duckdb:
        codec_defs = build_default_codec_defs()
    else:
        codec_defs = None

    if os.path.exists(args.data_dir_path):
        shutil.rmtree(args.data_dir_path)
    Path(f"{args.data_dir_path}").mkdir(parents=True, exist_ok=True)

    # tpchgen is much faster, but is exclusive to generating tpch data.  Use duckdb as a fallback.
    if args.benchmark_type == "tpch" and not args.use_duckdb:
        if args.verbose:
            print("generating with tpchgen")
        generate_data_files_with_tpchgen(args, codec_defs)
    else:
        if args.verbose:
            print("generating with duckdb")
        generate_data_files_with_duckdb(args)


def generate_data_files_with_tpchgen(args, codec_defs):
    local_installs_bin = Path(__file__).resolve().parent / ".local_installs" / "bin"
    if local_installs_bin.exists():
        os.environ["PATH"] = os.pathsep.join([str(local_installs_bin), os.environ["PATH"]])

    tables_sf_ratio = get_table_sf_ratios(args.scale_factor, args.max_rows_per_file)
    raw_data_path = args.data_dir_path

    max_partitions = 1
    with ThreadPoolExecutor(args.num_threads) as executor:
        futures = []

        for table, num_partitions in tables_sf_ratio.items():
            if args.verbose:
                print(f"Generating TPC-H data for table '{table}' with {num_partitions} partitions")
            for partition in range(1, num_partitions + 1):
                futures.append(
                    executor.submit(
                        generate_partition,
                        table,
                        partition,
                        raw_data_path,
                        args.scale_factor,
                        num_partitions,
                        args.verbose,
                        args.approx_row_group_bytes,
                        args.convert_decimals_to_floats,
                        codec_defs,
                        args.parquet_version,
                        args.nationkey_type,
                        args.regionkey_type,
                    )
                )
            max_partitions = num_partitions if num_partitions > max_partitions else max_partitions

        for future in futures:
            future.result()

    rearrange_directory(raw_data_path, max_partitions)

    if args.verbose:
        print(f"Raw data created at: {raw_data_path}")

    write_metadata(args)


# This dictionary maps each table to the number of partitions it should have based on it's
# expected file size relative to the SF.
# We generate a small sample benchmark (sf-0.01) to sample the ratio of how many rows are generated.
def get_table_sf_ratios(scale_factor, max_rows):
    int_scale_factor = int(scale_factor)
    int_scale_factor = 1 if int_scale_factor < 1 else int_scale_factor
    tables_sf_ratio = {}
    init_benchmark_tables("tpch", 0.01)
    tables = duckdb.sql("SHOW TABLES").fetchall()
    for table in tables:
        stripped_table = table[0].strip("'")
        num_rows = duckdb.sql(f"SELECT COUNT (*) FROM {stripped_table}").fetchall()
        tables_sf_ratio[stripped_table] = math.ceil(int_scale_factor / (max_rows / (int(num_rows[0][0]) * 100)))
    return tables_sf_ratio


def rearrange_directory(raw_data_path, num_partitions):
    # When we generate partitioned data it will have the form <data_dir>/<partition>/<table_name>/<table_name>.parquet.
    # We want to re-arrange it to have the form <data_dir>/<table_name>/<table_name>-<partition>.parquet
    tables = os.listdir(f"{raw_data_path}/part-1")

    for table in tables:
        Path(f"{raw_data_path}/{table}").mkdir(parents=True, exist_ok=True)

    # Move the partitioned data into the new directory structure.
    for partition in range(1, num_partitions + 1):
        for table in tables:
            part_file_path = f"{raw_data_path}/part-{partition}/{table}/{table}.{partition}.parquet"
            if os.path.exists(part_file_path):
                shutil.move(part_file_path, f"{raw_data_path}/{table}/{table}-{partition}.parquet")
        part_dir_path = f"{raw_data_path}/part-{partition}"
        for dir_name in os.listdir(part_dir_path):
            os.rmdir(f"{part_dir_path}/{dir_name}")
        os.rmdir(part_dir_path)


def write_metadata(args):
    with open(f"{args.data_dir_path}/metadata.json", "w") as file:
        metadata = {
            "scale_factor": args.scale_factor,
            "approx_row_group_bytes": args.approx_row_group_bytes,
        }
        json.dump(metadata, file, indent=2)
        file.write("\n")


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


def get_tpchgen_codec_args(codec_defs, table_name):
    """Build tpchgen-cli flags from codec definitions for a specific table.

    Returns a list of CLI arguments to append to the tpchgen-cli command.
    """
    if codec_defs is None:
        return []

    table_config = None
    for table in codec_defs.get("tables", []):
        if table["name"] == table_name:
            table_config = table
            break

    if table_config is None:
        return []

    columns = table_config.get("columns", [])
    if not columns:
        return []

    args = []

    uncompressed_cols = [
        column["name"] for column in columns if column.get("compression", "").upper() == "UNCOMPRESSED"
    ]
    if uncompressed_cols:
        args.append(f"--uncompressed-column-overrides={','.join(uncompressed_cols)}")

    for column in columns:
        encoding = column.get("encoding")
        if encoding:
            if encoding.upper() == "RLE_DICTIONARY":
                raise ValueError(
                    "RLE_DICTIONARY cannot be used as a column encoding. To enable dictionary encoding, set "
                    "'dictionary' to true (or omit it) instead."
                )
            args.append(f"--column-encoding={column['name']}={encoding}")

    no_dict_cols = [column["name"] for column in columns if column.get("dictionary") is False]
    if no_dict_cols:
        args.append(f"--disable-dictionary-encoding={','.join(no_dict_cols)}")

    return args


def load_codec_definitions(path):
    """Load and validate a codec definitions JSON file.

    See codec_definition_template.json for the expected schema.
    """
    with open(path) as file:
        codec_defs = json.load(file)

    if "tables" not in codec_defs:
        raise ValueError(f"Codec definitions file must contain a 'tables' key: {path}")

    for table in codec_defs["tables"]:
        if "name" not in table:
            raise ValueError(f"Each table entry must have a 'name' key: {path}")
        for column in table.get("columns", []):
            if "name" not in column:
                raise ValueError(f"Each column entry must have a 'name' key (table '{table['name']}'): {path}")

    return codec_defs


def build_default_codec_defs():
    """Auto-generate codec definitions by introspecting TPC-H schema at a tiny SF.

    Best-performing configuration from TPC-H SF1000 benchmarks (10 iterations,
    22 queries, Presto GPU):
      - All integers:              DELTA_BINARY_PACKED, dictionary off
      - Unique strings (NDV>=99%): PLAIN, dictionary off
      - Everything else:           tpchgen-cli defaults (PLAIN + Snappy + dictionary on)

    Achieved ~11% improvement over baseline with ~15% smaller dataset.
    """
    conn = duckdb.connect()
    conn.execute(f"INSTALL tpch; LOAD tpch; CALL dbgen(sf = {_SAMPLE_SF});")

    tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
    config = {"tables": []}

    for table in sorted(tables):
        total_rows = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        assert total_rows > 0, f"Table '{table}' has no rows at SF {_SAMPLE_SF}"

        columns_meta = conn.execute(f"DESCRIBE {table}").fetchall()
        col_entries = []

        for col_name, col_type, *_ in columns_meta:
            column_type = col_type.upper()

            if column_type in _INTEGER_TYPES:
                col_entries.append(
                    {
                        "name": col_name,
                        "encoding": "DELTA_BINARY_PACKED",
                        "dictionary": False,
                    }
                )
            elif column_type == "VARCHAR":
                ndv = conn.execute(f"SELECT COUNT(DISTINCT {col_name}) FROM {table}").fetchone()[0]
                if ndv / total_rows >= _HIGH_CARD_NDV_THRESHOLD:
                    col_entries.append(
                        {
                            "name": col_name,
                            "encoding": "PLAIN",
                            "dictionary": False,
                        }
                    )

        if col_entries:
            config["tables"].append({"name": table, "columns": col_entries})

    conn.close()
    return config


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate benchmark parquet data files for a given scale factor. "
        "Only the TPC-H and TPC-DS benchmarks are currently supported."
    )
    parser.add_argument(
        "-b",
        "--benchmark-type",
        type=str,
        required=True,
        choices=["tpch", "tpcds"],
        help="The type of benchmark to generate data for.",
    )
    parser.add_argument(
        "-d",
        "--data-dir-path",
        type=str,
        required=True,
        help="The path to the directory that will contain the benchmark data files. "
        "This directory will be created if it does not already exist.",
    )
    parser.add_argument(
        "-s", "--scale-factor", type=float, required=True, help="The scale factor of the generated dataset."
    )
    parser.add_argument(
        "-c",
        "--convert-decimals-to-floats",
        action="store_true",
        required=False,
        default=False,
        help="Convert all decimal columns to float column type.",
    )
    parser.add_argument(
        "--use-duckdb", action="store_true", required=False, default=False, help="Use duckdb instead of tpchgen"
    )
    parser.add_argument(
        "-j",
        "--num-threads",
        type=int,
        required=False,
        default=4,
        help="Number of threads to generate data with tpchgen",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", required=False, default=False, help="Extra verbose logging"
    )
    parser.add_argument(
        "--max-rows-per-file",
        type=int,
        required=False,
        default=100_000_000,
        help="Limit number of rows in each file (creates more partitions)",
    )
    parser.add_argument(
        "--approx-row-group-bytes",
        type=int,
        required=False,
        default=128 * 1024 * 1024,
        help="Approximate row group size in bytes. 128MB by default.",
    )
    parser.add_argument(
        "--codec-definitions",
        type=str,
        required=False,
        default=None,
        help="Path to a JSON file specifying per-table/per-column encoding, compression, and dictionary settings.",
    )
    parser.add_argument(
        "--parquet-version",
        type=str,
        required=False,
        default="v2",
        choices=["v1", "v2"],
        help="Parquet format version. v2 enables Data Page V2 encodings (RLE_DICTIONARY, DELTA_BINARY_PACKED) "
        "that cuDF's Parquet reader relies on for efficient integer decoding. Default: v2.",
    )
    parser.add_argument(
        "--nationkey-type",
        type=str,
        required=False,
        default="i32",
        choices=["i32", "i64"],
        help="Arrow type for c_nationkey, n_nationkey, s_nationkey. Default: i32.",
    )
    parser.add_argument(
        "--regionkey-type",
        type=str,
        required=False,
        default="i32",
        choices=["i32", "i64"],
        help="Arrow type for n_regionkey, r_regionkey. Default: i32.",
    )
    args = parser.parse_args()
    generate_data_files(args)
