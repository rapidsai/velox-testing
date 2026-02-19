# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import os
import shutil
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


def get_abs_file_path(relative_path):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), relative_path))


sys.path.append(get_abs_file_path("../../.."))

from common.testing.duckdb_utils import create_duckdb_table  # noqa: E402


def get_gluten_jar_path():
    """Get the Gluten JAR path from GLUTEN_HOME environment variable."""
    gluten_home = os.environ.get("GLUTEN_HOME")
    if not gluten_home:
        raise EnvironmentError(
            "GLUTEN_HOME environment variable is not set. "
            "Please set it to the path containing the Gluten JAR file."
        )

    gluten_home_path = Path(gluten_home)
    if not gluten_home_path.exists():
        raise FileNotFoundError(f"GLUTEN_HOME path does not exist: {gluten_home}")

    # Look for the Gluten JAR file
    jar_files = list(gluten_home_path.glob("*.jar"))
    if not jar_files:
        # Also check in common subdirectories
        for subdir in ["jars", "package/target", "backends-velox/target"]:
            jar_files = list((gluten_home_path / subdir).glob("gluten-*.jar"))
            if jar_files:
                break

    if not jar_files:
        raise FileNotFoundError(
            f"No Gluten JAR files found in GLUTEN_HOME: {gluten_home}. "
            "Expected a JAR file matching 'gluten-*.jar'"
        )

    # Use the first matching JAR
    return str(jar_files[0])


@pytest.fixture(scope="module")
def spark_session(request):
    """Create a SparkSession with Gluten enabled."""
    gluten_jar = get_gluten_jar_path()

    spark = (
        SparkSession.builder.appName("TPC-H Gluten Integration Test")
        .master("local[*]")
        .config("spark.jars", gluten_jar)
        .config("spark.plugins", "org.apache.gluten.GlutenPlugin")
        .config("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "2g")
        .config("spark.gluten.sql.columnar.forceShuffledHashJoin", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .getOrCreate()
    )

    yield spark

    spark.stop()


@pytest.fixture(scope="module")
def setup_and_teardown(request, spark_session):
    """Setup TPC-H tables and teardown after tests."""
    benchmark_type = request.node.obj.BENCHMARK_TYPE

    # Get the data directory
    data_dir = request.config.getoption("--data-dir")
    if not data_dir:
        # Default to the presto integration test data
        data_dir = get_abs_file_path("../../../presto/testing/integration_tests/data")

    data_path = Path(data_dir) / benchmark_type
    if not data_path.exists():
        raise FileNotFoundError(f"TPC-H data directory not found: {data_path}")

    # Create temp views for each TPC-H table
    tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
    for table in tables:
        table_path = data_path / table
        if table_path.exists():
            df = spark_session.read.parquet(str(table_path))
            df.createOrReplaceTempView(table)
            create_duckdb_table(table, str(table_path))

    # Setup output directory
    output_dir = Path(request.config.getoption("--output-dir"))
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=False)

    if request.config.getoption("--store-spark-results"):
        Path(f"{output_dir}/spark_results").mkdir(exist_ok=False)
    if request.config.getoption("--store-reference-results"):
        Path(f"{output_dir}/reference_results").mkdir(exist_ok=False)

    yield

    # Cleanup: drop temp views
    for table in tables:
        try:
            spark_session.catalog.dropTempView(table)
        except Exception:
            pass
