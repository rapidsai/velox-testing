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

import argparse
import duckdb
import json
import subprocess
import os
import shutil
import math
import tempfile
import boto3
import sys
from botocore.exceptions import ClientError, NoCredentialsError

from duckdb_utils import init_benchmark_tables, is_decimal_column
from pathlib import Path
from rewrite_parquet import process_dir
from concurrent.futures import ThreadPoolExecutor

def generate_partition(table, partition, raw_data_path, scale_factor, num_partitions, verbose):
    if verbose: print(f"Generating '{table}' partition: {partition}")
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

def generate_data_files(args):
    if os.path.exists(args.data_dir_path):
        shutil.rmtree(args.data_dir_path)
    Path(f"{args.data_dir_path}").mkdir(parents=True, exist_ok=True) if not args.data_dir_path.startswith("s3:") else print("Using s3 for data generated")

    # tpchgen is much faster, but is exclusive to generating tpch data.  Use duckdb as a fallback.
    if args.benchmark_type == "tpch" and not args.use_duckdb:
        if args.verbose: print("generating with tpchgen")
        generate_data_files_with_tpchgen(args)
    else:
        if args.verbose: print("generating with duckdb")
        generate_data_files_with_duckdb(args)

def copy_files_to_s3(local_directory, bucket_url):
    bucket_name = bucket_url.split("/")[2]
    try:
        s3_client = boto3.client('s3', endpoint_url='http://localhost:9000')
    except NoCredentialsError:
        print("Error: AWS credentials not found. Please configure AWS credentials.")
        sys.exit(1)
    local_path = Path(local_directory)
    # Get all files in directory (recursively)
    files_to_upload = []
    for file_path in local_path.rglob('*'):
        if file_path.is_file():
            files_to_upload.append(file_path)
    if not files_to_upload:
        print(f"No files found in '{local_directory}'")
        return
    print(f"Found {len(files_to_upload)} file(s) to upload to s3://{bucket_name}")
    
    uploaded = 0
    failed = 0

    for file_path in files_to_upload:
        # Get relative path from source directory
        s3_key = "/".join(bucket_url.split("/")[3:]) if (len(bucket_url.split("/")) > 3) else ""
        s3_key += "/" + str(file_path.relative_to(local_path))

        try:
            print(f"Uploading: {s3_key} -> {bucket_url}")
            s3_client.upload_file(str(file_path), bucket_name, s3_key)
            uploaded += 1
        except ClientError as e:
            print(f"  Error uploading {file_path}: {e}")
            failed += 1

    print()
    print(f"Upload complete: {uploaded} succeeded, {failed} failed")

def generate_data_files_with_tpchgen(args):
    tables_sf_ratio = get_table_sf_ratios(args.scale_factor, args.max_rows_per_file)

    data_dir_path = args.data_dir_path if not args.data_dir_path.startswith("s3:") else args.data_dir_path.replace("s3:/", ".")

    if args.convert_decimals_to_floats:
        raw_data_path = data_dir_path + "-temp"
        if os.path.exists(raw_data_path):
            shutil.rmtree(raw_data_path)
    else:
        raw_data_path = args.data_dir_path

    max_partitions = 1
    with ThreadPoolExecutor(args.num_threads) as executor:
        futures = []

        for table, num_partitions in tables_sf_ratio.items():
            if args.verbose:
                print(f"Generating TPC-H data for table '{table}' with {num_partitions} partitions")
            for partition in range(1, num_partitions + 1):
                futures.append(executor.submit(generate_partition, table, partition, raw_data_path,
                                               args.scale_factor, num_partitions, args.verbose))
            max_partitions = num_partitions if num_partitions > max_partitions else max_partitions

        for future in futures:
            future.result()

    rearrange_directory(raw_data_path, max_partitions)

    if args.verbose: print(f"Raw data created at: {raw_data_path}")

    if args.convert_decimals_to_floats:
        process_dir(raw_data_path, data_dir_path, args.num_threads, args.verbose,
                    args.convert_decimals_to_floats)
        shutil.rmtree(raw_data_path)

    write_metadata(data_dir_path, args.scale_factor)

    if args.data_dir_path.startswith("s3:"):
        copy_files_to_s3(data_dir_path, args.data_dir_path)
        # TODO: delete at some point
        #shutil.rmtree(data_dir_path)
        


# This dictionary maps each table to the number of partitions it should have based on it's
# expected file size relative to the SF.
# We generate a small sample bechmark (sf-0.01) to sample the ratio of how many rows are generated.
def get_table_sf_ratios(scale_factor, max_rows):
    int_scale_factor = int(scale_factor)
    int_scale_factor = 1 if int_scale_factor < 1 else int_scale_factor
    tables_sf_ratio = {}
    init_benchmark_tables("tpch", 0.01)
    tables = duckdb.sql(f"SHOW TABLES").fetchall()
    for table in tables:
        stripped_table = table[0].strip('\'')
        num_rows = duckdb.sql(f"SELECT COUNT (*) FROM {stripped_table}").fetchall()
        tables_sf_ratio[stripped_table] = math.ceil(int_scale_factor / (max_rows / (int(num_rows[0][0]) * 100)))
    return tables_sf_ratio

def rearrange_directory(raw_data_path, num_partitions):
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

def write_metadata(data_dir_path, scale_factor):
    with open(f'{data_dir_path}/metadata.json', 'w') as file:
        json.dump({"scale_factor": scale_factor}, file, indent=2)
        file.write("\n")

def generate_data_files_with_duckdb(args):
    init_benchmark_tables(args.benchmark_type, args.scale_factor)

    with open(f'{args.data_dir_path}/metadata.json', 'w') as file:
        json.dump({"scale_factor": args.scale_factor}, file, indent=2)
        file.write("\n")

    tables = duckdb.sql("SHOW TABLES").fetchall()
    for table_name, in tables:
        table_data_dir = f"{args.data_dir_path}/{table_name}"
        Path(table_data_dir).mkdir(exist_ok=False)
        duckdb.sql(f"COPY ({get_select_query(table_name, args.convert_decimals_to_floats)}) "
                   f"TO '{table_data_dir}/{table_name}.parquet' (FORMAT parquet)")

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate benchmark parquet data files for a given scale factor. "
                    "Only the TPC-H and TPC-DS benchmarks are currently supported.")
    parser.add_argument("-b", "--benchmark-type", type=str, required=True, choices=["tpch", "tpcds"],
                        help="The type of benchmark to generate data for.")
    parser.add_argument("-d", "--data-dir-path", type=str, required=True,
                        help="The path to the directory that will contain the benchmark data files. "
                             "This directory will be created if it does not already exist.")
    parser.add_argument("-s", "--scale-factor", type=float, required=True,
                        help="The scale factor of the generated dataset.")
    parser.add_argument("-c", "--convert-decimals-to-floats", action="store_true", required=False,
                        default=False, help="Convert all decimal columns to float column type.")
    parser.add_argument("--use-duckdb", action="store_true", required=False,
                        default=False, help="Use duckdb instead of tpchgen")
    parser.add_argument("-j", "--num-threads", type=int, required=False,
                        default=4, help="Number of threads to generate data with tpchgen")
    parser.add_argument("-v", "--verbose", action="store_true", required=False,
                        default=False, help="Extra verbose logging")
    parser.add_argument("--max-rows-per-file", type=int, required=False,
                        default=100_000_000, help="Limit number of rows in each file (creates more partitions)")
    args = parser.parse_args()

    generate_data_files(args)
