import argparse
import duckdb

from duckdb_utils import init_benchmark_tables
from pathlib import Path


def generate_table_schemas(benchmark_type, schemas_dir_path):
    init_benchmark_tables(benchmark_type, 0)

    Path(schemas_dir_path).mkdir(parents=True, exist_ok=True)

    tables = duckdb.sql("SHOW TABLES").fetchall()
    for table_name, in tables:
        with open(f"{schemas_dir_path}/{table_name}.sql", "w") as file:
            file.write(get_table_schema(benchmark_type, table_name))
            file.write("\n")


def get_table_schema(benchmark_type, table_name):
    column_metadata_rows = duckdb.query(f"DESCRIBE {table_name}").fetchall()
    columns_ddl_list = [
        f"{' ' * 4}{col_name} {col_type}{' NOT NULL' if nullable == 'NO' else ''}"
        for col_name, col_type, nullable, *_ in column_metadata_rows
    ]
    schema = (f"CREATE TABLE hive.{benchmark_type}_test.{table_name} "
              f"(\n{",\n".join(columns_ddl_list)}\n) "
              f"WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = 'file:{{file_path}}')")
    return schema


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate benchmark table schemas. Only the TPC-H and TPC-DS "
                    "benchmarks are currently supported.")
    parser.add_argument("--benchmark-type", type=str, required=True, choices=["tpch", "tpcds"],
                        help="The type of benchmark to generate table schemas for.")
    parser.add_argument("--schemas-dir-path", type=str, required=True,
                        help="The path to the directory that will contain the schema files. "
                             "This directory will be created if it does not already exist.")
    args = parser.parse_args()

    generate_table_schemas(args.benchmark_type, args.schemas_dir_path)
