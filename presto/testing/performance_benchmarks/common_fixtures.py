import prestodb
import pytest


@pytest.fixture(scope="module")
def presto_cursor(request):
    hostname = request.config.getoption("--hostname")
    port = request.config.getoption("--port")
    user = request.config.getoption("--user")
    schema = request.config.getoption("--schema-name")
    conn = prestodb.dbapi.connect(host=hostname, port=port, user=user, catalog="hive",
                                  schema=schema)
    return conn.cursor()


@pytest.fixture(scope="session")
def benchmark_result_collector(request):
    benchmark_results = {}
    yield benchmark_results

    request.session.benchmark_results = benchmark_results


@pytest.fixture(scope="module")
def benchmark_query(request, presto_cursor, tpch_queries, benchmark_result_collector):
    iterations = request.config.getoption("--iterations")

    RAW_TIMES_KEY = "raw_times_ms"
    benchmark_result_collector[request.node.obj.BENCHMARK_TYPE] = {
        RAW_TIMES_KEY: {},
    }

    benchmark_dict = benchmark_result_collector[request.node.obj.BENCHMARK_TYPE]
    raw_times_dict = benchmark_dict[RAW_TIMES_KEY]
    assert raw_times_dict == {}

    def benchmark_query_function(query_id):
        result = [
            presto_cursor.execute(tpch_queries[query_id]).stats["elapsedTimeMillis"]
            for _ in range(iterations)
        ]
        raw_times_dict[query_id] = result

    return benchmark_query_function
