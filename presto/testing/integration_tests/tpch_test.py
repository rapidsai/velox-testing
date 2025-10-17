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
from .common_fixtures import presto_cursor, setup_and_teardown
from ..common.fixtures import tpch_queries, get_scale_factor

BENCHMARK_TYPE = "tpch"


@pytest.mark.usefixtures("setup_and_teardown", "get_scale_factor")
def test_query(presto_cursor, tpch_queries, tpch_query_id):
    test_utils.execute_query_and_compare_results(presto_cursor, tpch_queries, tpch_query_id)
