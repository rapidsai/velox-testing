import argparse
import duckdb
import json

from duckdb_utils import init_benchmark_tables
from pathlib import Path


def generate_data_files(benchmark_type, data_dir_path, scale_factor):
    init_benchmark_tables(benchmark_type, scale_factor)

    Path(f"{data_dir_path}").mkdir(parents=True, exist_ok=True)

    with open(f'{data_dir_path}/metadata.json', 'w') as file:
        json.dump({"scale_factor": scale_factor}, file, indent=2)
        file.write("\n")

    tables = duckdb.sql("SHOW TABLES").fetchall()
    for table_name, in tables:
        table_data_dir = f"{data_dir_path}/{table_name}"
        Path(table_data_dir).mkdir(exist_ok=False)
        duckdb.sql(f"COPY (SELECT * FROM {table_name}) TO '{table_data_dir}/{table_name}.parquet' (FORMAT parquet)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate benchmark parquet data files for a given scale factor. "
                    "Only the TPC-H and TPC-DS benchmarks are currently supported.")
    parser.add_argument("--benchmark-type", type=str, required=True, choices=["tpch", "tpcds"],
                        help="The type of benchmark to generate data for.")
    parser.add_argument("--data-dir-path", type=str, required=True,
                        help="The path to the directory that will contain the benchmark data files. "
                             "This directory will be created if it does not already exist.")
    parser.add_argument("--scale-factor", type=str, required=True, choices=["0.01", "0.1", "1", "10", "100", "1000"],
                        help="The scale factor of the generated dataset.")
    args = parser.parse_args()

    generate_data_files(args.benchmark_type, args.data_dir_path, args.scale_factor)
