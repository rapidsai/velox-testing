import pytest

from . import test_utils


@pytest.fixture(scope="module")
def tpch_queries(request):
    queries = test_utils.get_queries(request.node.obj.BENCHMARK_TYPE)
    # Referencing the CTE defined "supplier_no" alias in the parent query causes issues on presto.
    queries["Q15"] = queries["Q15"].replace(" AS supplier_no", "").replace("supplier_no", "l_suppkey")
    return queries


@pytest.fixture(scope="module")
def tpcds_queries(request):
    return test_utils.get_queries(request.node.obj.BENCHMARK_TYPE)
