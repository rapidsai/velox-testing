# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from common.testing.test_utils import get_queries

from . import test_utils


@pytest.fixture(scope="module")
def tpch_queries(request):
    queries = get_queries(request.node.obj.BENCHMARK_TYPE)

    # The "fraction" portion of Q11 is a value that depends on scale factor
    # (it should be 0.0001 / scale_factor), whereas our query is currently hard-coded as 0.0001.
    value_ratio = 0.0001 / float(test_utils.get_scale_factor(request, request.node.obj.BENCHMARK_TYPE))
    queries["Q11"] = queries["Q11"].format(SF_FRACTION=f"{value_ratio:.12f}")

    return queries


@pytest.fixture(scope="module")
def tpcds_queries(request):
    return get_queries(request.node.obj.BENCHMARK_TYPE)


@pytest.fixture(scope="module")
def spark_session(request):
    benchmark_type = request.node.obj.BENCHMARK_TYPE
    hostname = request.config.getoption("--hostname")
    port = request.config.getoption("--port")
    spark_remote = f"sc://{hostname}:{port}"

    spark = SparkSession.builder.remote(spark_remote).appName(f"{benchmark_type} Test").getOrCreate()

    yield spark

    spark.stop()


@pytest.fixture(scope="module")
def base_setup_and_teardown(request, spark_session):
    benchmark_type = request.node.obj.BENCHMARK_TYPE
    dataset_name = request.config.getoption("--dataset-name")
    dataset_dir = test_utils.get_dataset_dir(benchmark_type, dataset_name)
    tables = tables_from_dataset_dir(dataset_dir)

    for table in tables:
        table_data_dir = f"{dataset_dir}/{table}"
        df = spark_session.read.parquet(table_data_dir)
        df.createOrReplaceTempView(table)

    yield

    for table in tables:
        try:
            spark_session.catalog.dropTempView(table)
        except Exception:
            pass


def tables_from_dataset_dir(dataset_dir):
    tables = [path.name for path in Path(dataset_dir).iterdir() if path.is_dir()]
    return tables
