#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Master plan TPC-H benchmark runner. Executed inside the Docker container."""

import os
import time
from contextlib import contextmanager

from pyspark.sql import SparkSession

try:
    import nvtx

    # Hex colors avoid the nvtx package's matplotlib dependency for named colors.
    @contextmanager
    def nvtx_range(name: str, color: str = "#1f77b4"):
        rng = nvtx.start_range(message=name, color=color)
        try:
            yield
        finally:
            nvtx.end_range(rng)
except ImportError:
    @contextmanager
    def nvtx_range(name: str, color: str = "#1f77b4"):
        yield

TPCH = {
    "1": """SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc, count(*) as count_order
        FROM lineitem WHERE l_shipdate <= date_sub(to_date('1998-12-01'), 90)
        GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus""",
    "3": """SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue,
        o_orderdate, o_shippriority
        FROM customer, orders, lineitem
        WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey
        AND l_orderkey = o_orderkey AND o_orderdate < to_date('1995-03-15')
        AND l_shipdate > to_date('1995-03-15')
        GROUP BY l_orderkey, o_orderdate, o_shippriority
        ORDER BY revenue DESC, o_orderdate LIMIT 10""",
}


def main():
    data_dir = os.environ["DATA_DIR"]
    queries_str = os.environ["QUERIES"]
    iterations = int(os.environ["ITERATIONS"])
    gpu = os.environ["GPU"] == "true"
    baseline = os.environ["BASELINE"] == "true"
    master = os.environ["MASTER"]
    partitions = os.environ["PARTITIONS"]

    jar_dir = "/opt/gluten/jars"
    jars = [f for f in os.listdir(jar_dir) if f.startswith("gluten-") and f.endswith(".jar")]
    if not jars:
        raise RuntimeError(f"No Gluten JAR found in {jar_dir}")
    jar = os.path.join(jar_dir, jars[0])

    use_master_plan = not baseline
    use_cudf = gpu and not baseline

    builder = (
        SparkSession.builder.appName("masterplan-bench")
        .master(master)
        .config("spark.jars", jar)
        .config("spark.plugins", "org.apache.gluten.GlutenPlugin")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "20g")
        .config("spark.driver.memory", "20g")
        .config("spark.executor.memory", "20g")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", partitions)
        .config("spark.sql.files.maxPartitionBytes", "16384mb")
        .config("spark.sql.files.openCostInBytes", str(16384 * 1024 * 1024))
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.driver.maxResultSize", "4g")
        .config("spark.ui.enabled", "false")
        .config("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        .config("spark.gluten.sql.columnar.forceShuffledHashJoin", "true")
        .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
        .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
        .config("spark.gluten.sql.columnar.veloxMasterPlan", str(use_master_plan).lower())
        .config("spark.gluten.sql.columnar.veloxExchange", "false")
        .config("spark.gluten.sql.columnar.veloxCudfExchange", "false")
        .config("spark.gluten.sql.columnar.cudf", str(use_cudf).lower())
    )

    if use_cudf:
        builder = (
            builder.config("spark.gluten.sql.columnar.backend.velox.cudf.enableTableScan", "true")
            .config("spark.gluten.sql.columnar.backend.velox.cudf.memoryResource", "arena")
            .config("spark.gluten.sql.columnar.backend.velox.cudf.memoryPercent", "95")
            .config("spark.gluten.sql.columnar.backend.velox.cudf.pinnedPoolSize", str(8 * 1024 * 1024 * 1024))
            .config("spark.gluten.sql.columnar.maxBatchSize", "1048576")
            .config("spark.gluten.sql.columnar.cudf.gpuPartition", "true")
            .config("spark.gluten.sql.columnar.cudf.convertCpuInput", "true")
        )
    else:
        builder = builder.config("spark.gluten.sql.columnar.backend.velox.IOThreads", "0")

    spark = builder.getOrCreate()

    # Load tables
    for table in ["lineitem", "orders", "customer", "nation", "region", "part", "partsupp", "supplier"]:
        path = f"{data_dir}/{table}"
        if os.path.exists(path):
            spark.read.parquet(path).createOrReplaceTempView(table)

    mode = "baseline" if baseline else ("gpu-masterplan" if gpu else "cpu-masterplan")
    query_ids = [q.strip() for q in queries_str.split(",")]

    print(f"\n{'=' * 70}")
    print(f"  Mode: {mode} | Master: {master} | Partitions: {partitions}")
    print(f"{'=' * 70}")

    results = {}
    for qid in query_ids:
        if qid not in TPCH:
            print(f"  Q{qid}: unknown query, skipping")
            continue

        query = TPCH[qid]

        with nvtx_range(f"{mode}/Q{qid}", color="#00bcd4"):  # cyan
            # Warmup
            with nvtx_range(f"{mode}/Q{qid}/warmup", color="#ffeb3b"):  # yellow
                df = spark.sql(query)
                df.collect()

            # Timed runs
            times = []
            for i in range(iterations):
                with nvtx_range(f"{mode}/Q{qid}/iter{i + 1}", color="#4caf50"):  # green
                    df = spark.sql(query)
                    t0 = time.perf_counter()
                    with nvtx_range(f"{mode}/Q{qid}/iter{i + 1}/collect", color="#e91e63"):  # magenta
                        df.collect()
                    elapsed_ms = (time.perf_counter() - t0) * 1000
                    times.append(elapsed_ms)

        results[qid] = times
        avg = sum(times) / len(times)
        mn = min(times)
        print(f"  Q{qid}: avg={avg:.1f}ms  min={mn:.1f}ms  runs={[f'{t:.1f}' for t in times]}")

    print(f"\n{'=' * 70}")
    print(f"  Summary ({mode})")
    print(f"{'=' * 70}")
    print(f"  {'Query':<8} {'Avg(ms)':>10} {'Min(ms)':>10}")
    print(f"  {'-' * 30}")
    for qid in query_ids:
        if qid in results:
            t = results[qid]
            print(f"  Q{qid:<7} {sum(t) / len(t):>10.1f} {min(t):>10.1f}")
    print()

    spark.stop()


if __name__ == "__main__":
    main()
