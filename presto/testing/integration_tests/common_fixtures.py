import prestodb
import pytest

from . import create_hive_tables
from . import test_utils


@pytest.fixture(scope="module")
def presto_cursor(request):
    benchmark_type = request.node.obj.BENCHMARK_TYPE
    conn = prestodb.dbapi.connect(host="localhost", port=8080, user="test_user", catalog="hive",
                                  schema=f"{benchmark_type}_test")
    return conn.cursor()


@pytest.fixture(scope="module")
def setup_and_teardown(request, presto_cursor):
    benchmark_type = request.node.obj.BENCHMARK_TYPE
    test_utils.init_duckdb_tables(benchmark_type)

    schema_name = f"{benchmark_type}_test"
    schemas_dir = test_utils.get_abs_file_path(f"schemas/{benchmark_type}")
    data_sub_directory = f"integration_test/{benchmark_type}"
    create_hive_tables.create_tables(presto_cursor, schema_name, schemas_dir, data_sub_directory)

    yield

    keep_tables = request.config.getoption("--keep-tables")
    if not keep_tables:
        create_hive_tables.drop_schema(presto_cursor, schema_name)
