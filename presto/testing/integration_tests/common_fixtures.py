import prestodb
import pytest

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
    schemas = test_utils.get_table_schemas(benchmark_type)
    test_utils.create_tables(presto_cursor, schemas, benchmark_type)
    yield
    keep_tables = request.config.getoption("--keep-tables")
    if not keep_tables:
        test_utils.drop_tables(presto_cursor, schemas, benchmark_type)
