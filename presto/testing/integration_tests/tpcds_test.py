import pytest

from . import test_utils
from .common_fixtures import presto_cursor, setup_and_teardown

BENCHMARK_TYPE = "tpcds"


@pytest.fixture(scope="module")
def tpcds_queries():
    return test_utils.get_queries(BENCHMARK_TYPE)


@pytest.mark.usefixtures("setup_and_teardown")
def test_query(presto_cursor, tpcds_queries, tpcds_query_id):
    test_utils.execute_query_and_compare_results(presto_cursor, tpcds_queries, tpcds_query_id)
