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

def is_decimal_column(column_type):
    return bool(re.match(r"^DECIMAL\(\d+,\d+\)$", column_type))

def map_table_schemas(verbose):
    tables = duckdb.sql("SHOW TABLES").fetchall()
    assert len(tables) > 0 # Benchmark tables must be initialized.
    table_to_schema_map = {}
    for table_name, in tables:
        table_to_schema_map[table_name] = duckdb.sql(f"SHOW {table_name}").fetchall()
    if verbose:
        print(f"Benchmark tables: {table_to_schema_map.keys()}")
    return table_to_schema_map
