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

import prestodb
import pytest

from .benchmark_keys import BenchmarkKeys
from ..common.fixtures import tpch_queries, tpcds_queries


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
def benchmark_queries(request, tpch_queries, tpcds_queries):
    if request.node.obj.BENCHMARK_TYPE == "tpch":
        return tpch_queries
    else:
        assert request.node.obj.BENCHMARK_TYPE == "tpcds"
        return tpcds_queries


@pytest.fixture(scope="module")
def benchmark_query(request, presto_cursor, benchmark_queries, benchmark_result_collector):
    iterations = request.config.getoption("--iterations")

    benchmark_result_collector[request.node.obj.BENCHMARK_TYPE] = {
        BenchmarkKeys.RAW_TIMES_KEY: {},
        BenchmarkKeys.FAILED_QUERIES_KEY: {},
    }

    benchmark_dict = benchmark_result_collector[request.node.obj.BENCHMARK_TYPE]
    raw_times_dict = benchmark_dict[BenchmarkKeys.RAW_TIMES_KEY]
    assert raw_times_dict == {}

    failed_queries_dict = benchmark_dict[BenchmarkKeys.FAILED_QUERIES_KEY]
    assert failed_queries_dict == {}

    def benchmark_query_function(query_id):
        try:
            result = [
                presto_cursor.execute(benchmark_queries[query_id]).stats["elapsedTimeMillis"]
                for _ in range(iterations)
            ]
            raw_times_dict[query_id] = result
        except Exception as e:
            failed_queries_dict[query_id] = f"{e.error_type}: {e.error_name}"
            raise

    return benchmark_query_function
