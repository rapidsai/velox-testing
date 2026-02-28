# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import glob
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from common.testing.test_utils import get_abs_file_path, get_queries

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
    gluten_jar_path = get_gluten_jar_path(request.config)

    # JVM options to allow access to internal APIs required by Gluten/Netty
    # These are needed for DirectByteBuffer and sun.misc.Unsafe access in Java 9+
    # Netty requires access to DirectByteBuffer constructor and sun.misc.Unsafe
    # The error "sun.misc.Unsafe or java.nio.DirectByteBuffer.<init>(long, int) not available"
    # indicates both need to be accessible.
    java_opts = "-Dio.netty.tryReflectionSetAccessible=true"

    # TODO: Add option to get below configurations from a file.
    builder = (
        SparkSession.builder.appName(f"{benchmark_type} Test")
        .master("local[*]")
        .config("spark.jars", gluten_jar_path)
        .config("spark.plugins", "org.apache.gluten.GlutenPlugin")
        .config("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "2g")
        .config("spark.gluten.sql.columnar.forceShuffledHashJoin", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.extraJavaOptions", java_opts)
        .config("spark.executor.extraJavaOptions", java_opts)
    )

    spark = builder.getOrCreate()

    yield spark

    spark.stop()


def get_gluten_jar_path(config):
    jar_path_option = "--gluten-jar-path"
    jar_path = config.getoption(jar_path_option)
    if jar_path is None:
        default_install_dir = get_abs_file_path(__file__, "../spark-gluten-install")
        search_path = f"{default_install_dir}/gluten-*.jar"
        installed_file_paths = glob.glob(search_path)
        if len(installed_file_paths) == 0:
            raise Exception(
                f"Could not find the Gluten JAR file (searched '{search_path}'). Either specify the Gluten JAR file path using the '{jar_path_option}' option or store the Gluten JAR file in the '{default_install_dir}' directory."
            )
        if len(installed_file_paths) > 1:
            raise Exception(
                f"More than one Gluten JAR file found in the '{default_install_dir}' directory. Only one Gluten JAR file is expected for autodetection to work correctly."
            )
        jar_path = installed_file_paths[0]
    return jar_path


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
            spark_session.catalog.dropTable(table)
        except Exception:
            pass


def tables_from_dataset_dir(dataset_dir):
    tables = [path.name for path in Path(dataset_dir).iterdir() if path.is_dir()]
    return tables
