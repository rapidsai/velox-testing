# Copyright (c) 2025, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest

from . import test_utils
from .common_fixtures import presto_cursor, setup_and_teardown, get_scale_factor

BENCHMARK_TYPE = "tpch"


@pytest.fixture(scope="module")
def tpch_queries(get_scale_factor):
    scale_factor = get_scale_factor
    queries = test_utils.get_queries(BENCHMARK_TYPE)
    # The "fraction" portion of Q11 is a value that depends on scale factor
    # (it should be 0.0001 / scale_factor), whereas our query is currently hard-coded as 0.0001.
    value_ratio = 0.0001 / float(scale_factor)
    queries["Q11"] = queries["Q11"].replace("0.0001000000", str(value_ratio))
    # Referencing the CTE defined "supplier_no" alias in the parent query causes issues on presto.
    queries["Q15"] = queries["Q15"].replace(" AS supplier_no", "").replace("supplier_no", "l_suppkey")
    return queries


@pytest.mark.usefixtures("setup_and_teardown")
def test_query(presto_cursor, tpch_queries, tpch_query_id):
    test_utils.execute_query_and_compare_results(presto_cursor, tpch_queries, tpch_query_id)
