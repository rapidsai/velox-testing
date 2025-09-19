from .common_fixtures import *
from ..common.fixtures import tpch_queries

BENCHMARK_TYPE = "tpch"

def test_query(benchmark_query, tpch_query_id):
    benchmark_query(tpch_query_id)
