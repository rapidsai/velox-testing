#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Master plan TPC-H benchmark runner. Executed inside the Docker container."""

import json
import math
import os
import re
import time
import traceback
from contextlib import contextmanager
from decimal import Decimal

from pyspark.sql import SparkSession

# Path to the canonical TPC-H queries shipped in the velox-testing repo.
# The repo is bind-mounted read-only at /workspace/velox-testing inside the container.
QUERIES_JSON = "/workspace/velox-testing/common/testing/queries/tpch/queries.json"

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

def load_tpch_queries(scale_factor: float) -> dict:
    """Load all 22 TPC-H queries from the canonical queries.json.

    Returns a dict keyed by bare numeric id ("1".."22"). Q11's
    scale-factor placeholder is filled in.
    """
    with open(QUERIES_JSON) as f:
        raw = json.load(f)
    # JSON keys are "Q1".."Q22"; runner uses bare "1".."22".
    queries = {k[1:]: v for k, v in raw.items() if k.startswith("Q")}
    # Q11 has SF_FRACTION = 0.0001 / scale_factor.
    if "11" in queries:
        sf_fraction = 0.0001 / float(scale_factor)
        queries["11"] = queries["11"].format(SF_FRACTION=f"{sf_fraction:.12f}")
    return queries


def detect_scale_factor(data_dir: str) -> float:
    """Best-effort scale factor detection from the data dir path.

    Looks for a `_sf<N>` suffix (e.g. data/tpch_sf10 -> 10). Falls back
    to env override SCALE_FACTOR, then 1.
    """
    if "SCALE_FACTOR" in os.environ:
        return float(os.environ["SCALE_FACTOR"])
    m = re.search(r"_sf(\d+(?:\.\d+)?)", data_dir)
    if m:
        return float(m.group(1))
    return 1.0


def _coerce(v):
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    if hasattr(v, "isoformat"):  # date / datetime
        return v.isoformat()
    return v


def rows_equal(spark_rows, duck_rows, rel_tol=1e-6, abs_tol=1e-4):
    """Compare two result sets after canonicalizing values + sorting.

    Tolerates float drift between cuDF and DuckDB (TPC-H sums monetary
    decimals; the last few digits can differ).
    """
    if len(spark_rows) != len(duck_rows):
        return False, f"row count differs (spark={len(spark_rows)}, duck={len(duck_rows)})"

    def canon(rows):
        out = []
        for r in rows:
            cells = tuple(_coerce(c) for c in (r if isinstance(r, tuple) else tuple(r)))
            out.append(cells)
        out.sort(key=lambda t: tuple(("" if c is None else str(c)) for c in t))
        return out

    sa, sb = canon(spark_rows), canon(duck_rows)
    for i, (ra, rb) in enumerate(zip(sa, sb)):
        if len(ra) != len(rb):
            return False, f"row {i}: col count {len(ra)} vs {len(rb)}"
        for j, (va, vb) in enumerate(zip(ra, rb)):
            if va is None and vb is None:
                continue
            if va is None or vb is None:
                return False, f"row {i} col {j}: null mismatch {va!r} vs {vb!r}"
            if isinstance(va, float) or isinstance(vb, float):
                if not math.isclose(float(va), float(vb), rel_tol=rel_tol, abs_tol=abs_tol):
                    return False, f"row {i} col {j}: numeric diff {va} vs {vb}"
            else:
                if va != vb:
                    return False, f"row {i} col {j}: value diff {va!r} vs {vb!r}"
    return True, ""


def setup_duckdb(data_dir):
    import duckdb

    con = duckdb.connect()
    for table in ["lineitem", "orders", "customer", "nation", "region", "part", "partsupp", "supplier"]:
        path = f"{data_dir}/{table}"
        if os.path.exists(path):
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM '{path}/*.parquet'")
    return con


def main():
    data_dir = os.environ["DATA_DIR"]
    queries_str = os.environ["QUERIES"]
    iterations = int(os.environ["ITERATIONS"])
    gpu = os.environ["GPU"] == "true"
    baseline = os.environ["BASELINE"] == "true"
    master = os.environ["MASTER"]
    partitions = os.environ["PARTITIONS"]
    validate = os.environ.get("VALIDATE", "false") == "true"

    jar_dir = "/opt/gluten/jars"
    jars = [f for f in os.listdir(jar_dir) if f.startswith("gluten-") and f.endswith(".jar")]
    if not jars:
        raise RuntimeError(f"No Gluten JAR found in {jar_dir}")
    jar = os.path.join(jar_dir, jars[0])

    # Mode matrix:
    #   baseline=F, gpu=F  -> cpu-masterplan
    #   baseline=T, gpu=F  -> baseline           (CPU velox-spark, normal Spark stages)
    #   baseline=F, gpu=T  -> gpu-masterplan
    #   baseline=T, gpu=T  -> gpu-vanilla        (GPU velox-spark, normal Spark stages + shuffle)
    use_master_plan = not baseline
    use_cudf = gpu

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
        # Strict-GPU mode: any cuDF-to-CPU fallback becomes a hard error
        # instead of a silent fallback. Used to verify GPU coverage.
        if os.environ.get("STRICT_GPU", "false") == "true":
            builder = builder.config(
                "spark.gluten.sql.columnar.backend.velox.cudf.allowCpuFallback", "false"
            )
    else:
        builder = builder.config("spark.gluten.sql.columnar.backend.velox.IOThreads", "0")

    spark = builder.getOrCreate()

    # Load tables
    for table in ["lineitem", "orders", "customer", "nation", "region", "part", "partsupp", "supplier"]:
        path = f"{data_dir}/{table}"
        if os.path.exists(path):
            spark.read.parquet(path).createOrReplaceTempView(table)

    if baseline and gpu:
        mode = "gpu-vanilla"
    elif baseline:
        mode = "baseline"
    elif gpu:
        mode = "gpu-masterplan"
    else:
        mode = "cpu-masterplan"

    scale_factor = detect_scale_factor(data_dir)
    tpch = load_tpch_queries(scale_factor)
    query_ids = [q.strip() for q in queries_str.split(",")]

    print(f"\n{'=' * 70}")
    print(f"  Mode: {mode} | Master: {master} | Partitions: {partitions}")
    print(f"  Scale factor: {scale_factor} | Queries available: {len(tpch)} | Validate: {validate}")
    print(f"{'=' * 70}")

    duck_con = setup_duckdb(data_dir) if validate else None

    # results[qid] is either a list of float ms (success) or a string (failure summary).
    results: dict = {}
    # validation[qid] is "PASS" / "FAIL: ..." / None (skipped)
    validation: dict = {}
    for qid in query_ids:
        if qid not in tpch:
            print(f"  Q{qid}: unknown query, skipping")
            continue

        query = tpch[qid]

        try:
            with nvtx_range(f"{mode}/Q{qid}", color="#00bcd4"):  # cyan
                # Warmup (also serves as validation collect when --validate is on)
                with nvtx_range(f"{mode}/Q{qid}/warmup", color="#ffeb3b"):  # yellow
                    df = spark.sql(query)
                    if os.environ.get("EXPLAIN", "false") == "true":
                        plan = df._jdf.queryExecution().executedPlan().toString()
                        print(f"\n--- Q{qid} executed plan ---\n{plan}\n--- end Q{qid} plan ---")
                    spark_rows = df.collect()

                if validate:
                    duck_rows = duck_con.execute(query).fetchall()
                    ok, msg = rows_equal(spark_rows, duck_rows)
                    validation[qid] = "PASS" if ok else f"FAIL: {msg}"
                    print(f"  Q{qid:<3} validate {'OK ' if ok else 'FAIL'}  "
                          f"(spark={len(spark_rows)} duck={len(duck_rows)})"
                          f"{'' if ok else '  ' + msg}")
                    if not ok:
                        n_dump = min(20, len(spark_rows), len(duck_rows))
                        print(f"  Q{qid} first {n_dump} rows (spark | duck) after canonical sort:")
                        canon = lambda rows: sorted(
                            (tuple(_coerce(c) for c in r) for r in rows),
                            key=lambda t: tuple(("" if c is None else str(c)) for c in t),
                        )
                        sa, sb = canon(spark_rows)[:n_dump], canon(duck_rows)[:n_dump]
                        for i in range(n_dump):
                            mark = "  " if sa[i] == sb[i] else "!="
                            print(f"    {mark} {sa[i]}    |    {sb[i]}")

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
            print(f"  Q{qid:<3} OK    avg={avg:.1f}ms  min={mn:.1f}ms")
        except Exception as exc:  # noqa: BLE001
            err = f"{type(exc).__name__}: {exc}"
            # First line only — full Java stack traces are huge.
            err = err.split("\n", 1)[0][:300]
            results[qid] = err
            print(f"  Q{qid:<3} FAIL  {err}")
            traceback.print_exc()

    print(f"\n{'=' * 70}")
    print(f"  Summary ({mode})")
    print(f"{'=' * 70}")
    valid_hdr = f" {'Validate':<8}" if validate else ""
    print(f"  {'Query':<6} {'Status':<6} {'Avg(ms)':>10} {'Min(ms)':>10}{valid_hdr}  Notes")
    print(f"  {'-' * 80}")
    n_ok = n_fail = 0
    n_valid_pass = n_valid_fail = 0
    for qid in query_ids:
        if qid not in results:
            continue
        v = results[qid]
        vmsg = ""
        valid_cell = ""
        if validate:
            vstat = validation.get(qid, "SKIP")
            valid_cell = f" {('PASS' if vstat == 'PASS' else 'FAIL'):<8}"
            if vstat == "PASS":
                n_valid_pass += 1
            else:
                n_valid_fail += 1
                vmsg = vstat
        if isinstance(v, list):
            n_ok += 1
            avg = sum(v) / len(v)
            mn = min(v)
            print(f"  Q{qid:<5} {'OK':<6} {avg:>10.1f} {mn:>10.1f}{valid_cell}  {vmsg}")
        else:
            n_fail += 1
            print(f"  Q{qid:<5} {'FAIL':<6} {'-':>10} {'-':>10}{valid_cell}  {v}")
    print(f"  {'-' * 80}")
    print(f"  Run    : {n_ok} OK / {n_fail} FAIL / {len(query_ids)} requested")
    if validate:
        print(f"  Valid  : {n_valid_pass} PASS / {n_valid_fail} FAIL")
    print()

    spark.stop()


if __name__ == "__main__":
    main()
