# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0


import pytest

from . import test_utils


@pytest.fixture(scope="module")
def tpch_queries(request, presto_cursor):
    queries = test_utils.get_queries(request.node.obj.BENCHMARK_TYPE)

    # The "fraction" portion of Q11 is a value that depends on scale factor
    # (it should be 0.0001 / scale_factor), whereas our query is currently hard-coded as 0.0001.
    value_ratio = 0.0001 / float(test_utils.get_scale_factor(request, presto_cursor))
    queries["Q11"] = queries["Q11"].format(SF_FRACTION=f"{value_ratio:.12f}")

    # Referencing the CTE defined "supplier_no" alias in the parent query causes issues on presto.
    queries["Q15"] = queries["Q15"].replace(" AS supplier_no", "").replace("supplier_no", "l_suppkey")
    return queries


@pytest.fixture(scope="module")
def tpcds_queries(request):
    return test_utils.get_queries(request.node.obj.BENCHMARK_TYPE)
