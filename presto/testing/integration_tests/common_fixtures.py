import prestodb
import pytest

from . import test_utils


@pytest.fixture(scope="module")
def presto_cursor(request):
    benchmark_type = request.node.obj.BENCHMARK_TYPE
    hostname = request.config.getoption("--hostname")
    port = request.config.getoption("--port")
    user = request.config.getoption("--user")
    schema = request.config.getoption("--schema-name")
    schema = schema if schema else f"{benchmark_type}_test"
    conn = prestodb.dbapi.connect(host=hostname, port=port, user=user, catalog="hive", schema=schema)
    return conn.cursor()


@pytest.fixture(scope="module")
def setup_and_teardown(request, presto_cursor):
    benchmark_type = request.node.obj.BENCHMARK_TYPE
    test_utils.init_duckdb_tables(benchmark_type)

    should_create_tables = False if request.config.getoption("--schema-name") else True
    if should_create_tables:
        schemas = test_utils.get_table_schemas(benchmark_type)
        test_utils.create_tables(presto_cursor, schemas, benchmark_type)

    yield

    if should_create_tables:
        keep_tables = request.config.getoption("--keep-tables")
        if not keep_tables:
            test_utils.drop_tables(presto_cursor, schemas, benchmark_type)
