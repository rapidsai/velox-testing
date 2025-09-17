import pytest

from . import test_utils
from .common_fixtures import presto_cursor, setup_and_teardown

BENCHMARK_TYPE = "tpch"


@pytest.fixture(scope="module")
def tpch_queries():
    queries = test_utils.get_queries(BENCHMARK_TYPE)
    scale_factor = test_utils.get_scale_factor(BENCHMARK_TYPE)
    value_ratio = 0.0001 / float(scale_factor)
    # Referencing the CTE defined "supplier_no" alias in the parent query causes issues on presto.
    queries["Q15"] = queries["Q15"].replace(" AS supplier_no", "").replace("supplier_no", "l_suppkey")
    queries["Q11"] = queries["Q11"].replace("0.0001000000", str(value_ratio))
    return queries


@pytest.mark.usefixtures("setup_and_teardown")
def test_query(presto_cursor, tpch_queries, tpch_query_id):
    test_utils.execute_query_and_compare_results(presto_cursor, tpch_queries, tpch_query_id)
