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
from ..common_fixtures import get_scale_factor

@pytest.fixture(scope="module")
def tpch_queries(request, get_scale_factor):
    queries = test_utils.get_queries(request.node.obj.BENCHMARK_TYPE)

    scale_factor = get_scale_factor
    # The "fraction" portion of Q11 is a value that depends on scale factor
    # (it should be 0.0001 / scale_factor), whereas our query is currently hard-coded as 0.0001.
    value_ratio = 0.0001 / float(scale_factor)
    queries["Q11"] = queries["Q11"].format(SF_FRACTION=f"{value_ratio:f}")

    # Referencing the CTE defined "supplier_no" alias in the parent query causes issues on presto.
    queries["Q15"] = queries["Q15"].replace(" AS supplier_no", "").replace("supplier_no", "l_suppkey")
    return queries


@pytest.fixture(scope="module")
def tpcds_queries(request):
    return test_utils.get_queries(request.node.obj.BENCHMARK_TYPE)
