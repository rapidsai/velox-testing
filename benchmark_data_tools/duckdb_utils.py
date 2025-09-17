import duckdb
import re

def init_benchmark_tables(benchmark_type, scale_factor):
    tables = duckdb.sql("SHOW TABLES").fetchall()
    assert len(tables) == 0

    if benchmark_type == "tpch":
        function_name = "dbgen"
    else:
        assert benchmark_type == "tpcds"
        function_name = "dsdgen"

    duckdb.sql(f"INSTALL {benchmark_type}; LOAD {benchmark_type}; CALL {function_name}(sf = {scale_factor});")

def init_benchmark_tables_from_parquet(benchmark_type, scale_factor, data_path):
    tables = duckdb.sql("SHOW TABLES").fetchall()
    assert len(tables) == 0

    if benchmark_type == "tpch":
        duckdb.sql(f"CREATE TABLE customer AS SELECT * FROM '{data_path}/customer/*.parquet';")
        duckdb.sql(f"CREATE TABLE lineitem AS SELECT * FROM '{data_path}/lineitem/*.parquet';")
        duckdb.sql(f"CREATE TABLE nation AS SELECT * FROM '{data_path}/nation/*.parquet';")
        duckdb.sql(f"CREATE TABLE orders AS SELECT * FROM '{data_path}/orders/*.parquet';")
        duckdb.sql(f"CREATE TABLE part AS SELECT * FROM '{data_path}/part/*.parquet';")
        duckdb.sql(f"CREATE TABLE partsupp AS SELECT * FROM '{data_path}/partsupp/*.parquet';")
        duckdb.sql(f"CREATE TABLE region AS SELECT * FROM '{data_path}/region/*.parquet';")
        duckdb.sql(f"CREATE TABLE supplier AS SELECT * FROM '{data_path}/supplier/*.parquet';")
    else:
        init_benchmark_tables(benchmark_type, scale_factor)

def is_decimal_column(column_type):
    return bool(re.match(r"^DECIMAL\(\d+,\d+\)$", column_type))
