import duckdb
import re
import os

def init_benchmark_tables(benchmark_type, scale_factor):
    tables = duckdb.sql("SHOW TABLES").fetchall()
    assert len(tables) == 0

    if benchmark_type == "tpch":
        function_name = "dbgen"
    else:
        assert benchmark_type == "tpcds"
        function_name = "dsdgen"

    duckdb.sql(f"INSTALL {benchmark_type}; LOAD {benchmark_type}; CALL {function_name}(sf = {scale_factor});")

def init_benchmark_tables_from_parquet(benchmark_type, scale_factor, data_path, schemas_path):
    tables = duckdb.sql("SHOW TABLES").fetchall()
    assert len(tables) == 0

    for file_name in os.listdir(schemas_path):
        table_name = file_name.replace(".sql", "")
        duckdb.sql(f"CREATE TABLE {table_name} AS SELECT * FROM '{data_path}/{table_name}/*.parquet';")

def is_decimal_column(column_type):
    return bool(re.match(r"^DECIMAL\(\d+,\d+\)$", column_type))
