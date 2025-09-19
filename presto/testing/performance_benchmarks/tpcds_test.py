from .common_fixtures import *
from ..common.fixtures import tpcds_queries

BENCHMARK_TYPE = "tpcds"


def test_query(benchmark_query, tpcds_query_id):
    benchmark_query(tpcds_query_id)
