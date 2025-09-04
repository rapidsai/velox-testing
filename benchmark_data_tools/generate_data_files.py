import argparse
import duckdb
import json
import subprocess
import os
import shutil
import math

from duckdb_utils import init_benchmark_tables, is_decimal_column
from pathlib import Path
from rewrite_parquet import process_dir
from concurrent.futures import ThreadPoolExecutor

def generate_partition(table, partition, raw_data_path, scale_factor, num_partitions, verbose):
    if verbose:
        print(f"Generating '{table}' partition: {partition}")
    Path(f"{raw_data_path}/part-{partition}").mkdir(parents=True, exist_ok=True)
    command = [
        "tpchgen-cli",
        "-T", table,
        "-s", str(scale_factor),
        "--output-dir", str(f"{raw_data_path}/part-{partition}"),
        "--parts", str(num_partitions),
        "--part", str(partition),
        "--format", "parquet"
    ]
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error generating TPC-H data: {e}")

def generate_data_files_with_tpchgen(data_dir_path, scale_factor, convert_decimals_to_floats, num_threads, verbose):

    raw_data_path = data_dir_path + "-temp" if convert_decimals_to_floats else data_dir_path

    # This dictionary maps each table to it's expected file size relative to the SF (roughly what SF is needed to generate a 5GB for this table).
    # This is used to partition each table down to manageable file sizes.
    tables_sf_ratio = {"region": 100000, "nation": 100000, "supplier": 100000, "customer": 200, "part": 100000, "partsupp": 100, "orders": 50, "lineitem": 10}

    with ThreadPoolExecutor(num_threads) as executor:
        futures = []

        for table, ratio in tables_sf_ratio.items():
            num_partitions = math.ceil(int(scale_factor) / ratio)
            if verbose:
                print(f"Generating TPC-H data for table '{table}' with {num_partitions} partitions")
            for partition in range(1, num_partitions + 1):
                futures.append(executor.submit(generate_partition, table, partition, raw_data_path, scale_factor, num_partitions, verbose))

        for future in futures:
            future.result()

    # When we generate partitioned data it will have the form <data_dir>/<partition>/<table_name>.parquet.
    # We want to re-arrange it to have the form <data_dir>/<table_name>/<table_name>-<partition>.parquet
    parquet_files = os.listdir(f"{raw_data_path}/part-1")
    tables = []
    for p_file in parquet_files:
        tables.append(p_file.replace(".parquet", ""))
    
    for table in tables:
        Path(f"{raw_data_path}/{table}").mkdir(parents=True, exist_ok=True)

    # Move the partitioned data into the new directory structure.
    for partition in range(1, num_partitions + 1):
        for table in tables:
            if os.path.exists(f"{raw_data_path}/part-{partition}/{table}.parquet"):
                shutil.move(f"{raw_data_path}/part-{partition}/{table}.parquet",
                            f"{raw_data_path}/{table}/{table}-{partition}.parquet")
        os.rmdir(f"{raw_data_path}/part-{partition}")

    if verbose:
        print(f"Raw data created at: {raw_data_path}")

    if convert_decimals_to_floats:
        process_dir(raw_data_path, data_dir_path, num_threads, verbose)
        shutil.rmtree(raw_data_path)

    with open(f'{data_dir_path}/metadata.json', 'w') as file:
        json.dump({"scale_factor": scale_factor}, file, indent=2)
        file.write("\n")

def generate_data_files_with_duckdb(benchmark_type, data_dir_path, scale_factor, convert_decimals_to_floats):
    init_benchmark_tables(benchmark_type, scale_factor)

    with open(f'{data_dir_path}/metadata.json', 'w') as file:
        json.dump({"scale_factor": scale_factor}, file, indent=2)
        file.write("\n")

    tables = duckdb.sql("SHOW TABLES").fetchall()
    for table_name, in tables:
        table_data_dir = f"{data_dir_path}/{table_name}"
        Path(table_data_dir).mkdir(exist_ok=False)
        duckdb.sql(f"COPY ({get_select_query(table_name, convert_decimals_to_floats)}) "
                   f"TO '{table_data_dir}/{table_name}.parquet' (FORMAT parquet)")

def generate_data_files(benchmark_type, data_dir_path, scale_factor, convert_decimals_to_floats, use_duckdb, num_threads, verbose):
    Path(f"{data_dir_path}").mkdir(parents=True, exist_ok=True)
    # tpchgen is much faster, but is exclusive to generating tpch data.  Use duckdb as a fallback.
    if benchmark_type == "tpch" and not use_duckdb and float(scale_factor) >= 1:
        generate_data_files_with_tpchgen(data_dir_path, scale_factor, convert_decimals_to_floats, num_threads, verbose)
    else:
        print(f"scale factor {scale_factor} too small to use tpchgen; falling back to duckdb")
        generate_data_files_with_duckdb(benchmark_type, data_dir_path, scale_factor, convert_decimals_to_floats)

def get_select_query(table_name, convert_decimals_to_floats):
    if convert_decimals_to_floats:
        column_metadata_rows = duckdb.query(f"DESCRIBE {table_name}").fetchall()
        column_projections = [
            get_column_projection(column_metadata, convert_decimals_to_floats)
            for column_metadata in column_metadata_rows
        ]
        query = f"SELECT {",".join(column_projections)} FROM {table_name}"
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate benchmark parquet data files for a given scale factor. "
                    "Only the TPC-H and TPC-DS benchmarks are currently supported.")
    parser.add_argument("--benchmark-type", type=str, required=True, choices=["tpch", "tpcds"],
                        help="The type of benchmark to generate data for.")
    parser.add_argument("--data-dir-path", type=str, required=True,
                        help="The path to the directory that will contain the benchmark data files. "
                             "This directory will be created if it does not already exist.")
    parser.add_argument("--scale-factor", type=str, required=True,
                        choices=["0.01", "0.1", "1", "10", "100", "1000"],
                        help="The scale factor of the generated dataset.")
    parser.add_argument("--convert-decimals-to-floats", action="store_true", required=False,
                        default=False, help="Convert all decimal columns to float column type.")
    parser.add_argument("--use-duckdb", action="store_true", required=False,
                        default=False, help="Use duckdb instead of tpchgen")
    parser.add_argument("--num-threads", type=int, required=False,
                        default=4, help="Number of threads to generate data with tpchgen")
    parser.add_argument("--verbose", action="store_true", required=False,
                        default=False, help="Extra verbose logging")
    args = parser.parse_args()

    generate_data_files(args.benchmark_type, args.data_dir_path, args.scale_factor, args.convert_decimals_to_floats, args.use_duckdb, args.num_threads, args.verbose)
