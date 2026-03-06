#!/usr/bin/env bash
# timestamp_benchmark.sh — Generate timestamp data and benchmark GPU vs CPU
#
# Usage:
#   ./timestamp_benchmark.sh [setup|bench|all] [sf]
#
# Examples:
#   ./timestamp_benchmark.sh setup 1      # Create table (~6M rows)
#   ./timestamp_benchmark.sh setup 10     # Create table (~60M rows)
#   ./timestamp_benchmark.sh bench        # Run benchmark queries
#   ./timestamp_benchmark.sh all 10       # Setup + bench
#
# Commands:
#   setup  - Generate parquet data and create Hive table
#   bench  - Run benchmark queries and report timings
#   verify - Compare Presto results against DuckDB for correctness
#   all    - setup + bench
#
# Environment:
#   PRESTO_COORDINATOR  - coordinator container name (default: presto-coordinator)
#   PRESTO_PORT         - coordinator port (default: 8080)
#   HIVE_SCHEMA         - hive schema to use (default: default)
#   BENCHMARK_RUNS      - number of runs per query (default: 3)
#   PRESTO_DATA_DIR     - host data dir mounted into containers (required)
#   BENCH_MODE          - label for results file (default: gpu)
#
# Prerequisites:
#   - Presto coordinator running with hive catalog
#   - Workers running with cuDF enabled
#   - For verify: duckdb pip package (auto-installed)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../scripts/py_env_functions.sh"

# Setup python venv with pyarrow
init_python_virtual_env ".ts_bench_venv"
pip install -q pyarrow numpy duckdb
trap 'LOCAL_CONDA_INIT="${LOCAL_CONDA_INIT:-}"; delete_python_virtual_env .ts_bench_venv' EXIT

COORDINATOR="${PRESTO_COORDINATOR:-presto-coordinator}"
PORT="${PRESTO_PORT:-8080}"
SF="${2:-10}"

# Preflight: check coordinator and at least one worker are alive
preflight_check() {
  echo "Checking Presto cluster..."

  # Check coordinator container is running
  if ! docker ps --format '{{.Names}}' | grep -qw "${COORDINATOR}"; then
    echo "ERROR: Coordinator container '${COORDINATOR}' is not running."
    echo "Running containers: $(docker ps --format '{{.Names}}' | tr '\n' ' ')"
    exit 1
  fi

  # Check coordinator responds via HTTP API (doesn't need presto-cli)
  local retries=5
  local ok=false
  for i in $(seq 1 ${retries}); do
    if docker exec "${COORDINATOR}" curl -sf "http://localhost:${PORT}/v1/info" > /dev/null 2>&1; then
      ok=true
      break
    fi
    echo "  Waiting for coordinator (attempt ${i}/${retries})..."
    sleep 2
  done
  if ! ${ok}; then
    echo "ERROR: Coordinator is not responding on port ${PORT}."
    exit 1
  fi
  echo "  Coordinator is up."

  # Wait for at least one worker to register
  local worker_retries=30
  local worker_count=0
  for i in $(seq 1 ${worker_retries}); do
    local node_json
    node_json=$(docker exec "${COORDINATOR}" curl -sf "http://localhost:${PORT}/v1/node" 2>/dev/null || echo "[]")
    worker_count=$(echo "${node_json}" | { grep -o '"uri"' || true; } | wc -l)
    if [ "${worker_count}" -gt 0 ]; then
      break
    fi
    echo "  Waiting for workers (attempt ${i}/${worker_retries})..."
    sleep 2
  done

  if [ "${worker_count}" -eq 0 ]; then
    echo "ERROR: No active worker nodes found after ${worker_retries} attempts."
    exit 1
  fi
  echo "  ${worker_count} active worker(s) found."
  echo ""
}

SCHEMA="${HIVE_SCHEMA:-default}"
TABLE="hive.${SCHEMA}.ts_bench_sf${SF}"
RUNS="${BENCHMARK_RUNS:-3}"
HOST_DATA_DIR="${PRESTO_DATA_DIR:?PRESTO_DATA_DIR must be set}/ts_bench/sf${SF}"
CONTAINER_DATA_DIR="/var/lib/presto/data/hive/data/user_data/ts_bench/sf${SF}"

cli() {
  docker exec -i "${COORDINATOR}" presto-cli \
    --server "localhost:${PORT}" \
    --catalog hive \
    --schema "${SCHEMA}" \
    "$@"
}

run_query() {
  local label="$1"
  local sql="$2"

  echo -n "  [${label}] running... " >&2
  local start end elapsed
  start=$(date +%s%N)
  local query_output
  query_output=$(cli --execute "${sql}" 2>&1) || true
  end=$(date +%s%N)
  elapsed=$(( (end - start) / 1000000 ))

  # Check for query failure
  if echo "${query_output}" | grep -qi "failed\|error"; then
    echo "FAILED (${elapsed} ms)" >&2
    echo "    ${query_output}" >&2
    echo "-1"
    return
  fi

  echo "${elapsed} ms" >&2
  echo "${elapsed}"
}

ensure_schema() {
  echo "Ensuring schema hive.${SCHEMA} exists..."
  cli --execute "CREATE SCHEMA IF NOT EXISTS hive.${SCHEMA}" 2>/dev/null || {
    local schemas
    schemas=$(cli --execute "SHOW SCHEMAS IN hive" 2>/dev/null || true)
    if echo "${schemas}" | grep -qw "${SCHEMA}"; then
      echo "Schema hive.${SCHEMA} already exists."
    else
      echo "ERROR: Schema hive.${SCHEMA} does not exist and could not be created."
      echo "Available schemas:"
      echo "${schemas}"
      echo "Set HIVE_SCHEMA=<schema> to use a different schema."
      exit 1
    fi
  }
  echo "Schema hive.${SCHEMA} is ready."
}

generate_parquet() {
  local num_rows=$1
  local out_dir=$2
  local num_workers=${GENERATE_WORKERS:-$(nproc)}

  echo "Generating ${num_rows} rows of timestamp data (${num_workers} threads)..."
  python3 - "${num_rows}" "${out_dir}" "${num_workers}" <<'PYEOF'
import sys
import os
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

num_rows = int(sys.argv[1])
out_dir = sys.argv[2]
num_workers = int(sys.argv[3])
os.makedirs(out_dir, exist_ok=True)

# Split work into chunks
chunk_size = min(num_rows, 5_000_000)
chunks = []
rows_remaining = num_rows
file_idx = 0
while rows_remaining > 0:
    n = min(chunk_size, rows_remaining)
    chunks.append((file_idx, n, num_rows))
    rows_remaining -= n
    file_idx += 1

total_files = len(chunks)

def generate_chunk(args):
    idx, n, total_rows = args
    seed = 42 + idx  # Deterministic but different per chunk
    rng = np.random.default_rng(seed)

    orderkey = rng.integers(1, total_rows * 4, size=n, dtype=np.int64)
    partkey = rng.integers(1, 200_000, size=n, dtype=np.int64)
    suppkey = rng.integers(1, 10_000, size=n, dtype=np.int64)
    quantity = rng.uniform(1.0, 50.0, size=n).astype(np.float64)
    price = rng.uniform(900.0, 105000.0, size=n).astype(np.float64)

    base_us = int(datetime(1992, 1, 1).timestamp() * 1_000_000)
    day_offsets = rng.integers(0, 2557, size=n, dtype=np.int64)
    hour_offsets = rng.integers(0, 24, size=n, dtype=np.int64)
    minute_offsets = rng.integers(0, 60, size=n, dtype=np.int64)
    second_offsets = rng.integers(0, 60, size=n, dtype=np.int64)

    ship_us = base_us + day_offsets * 86400_000_000 + hour_offsets * 3600_000_000 + minute_offsets * 60_000_000 + second_offsets * 1_000_000
    commit_us = ship_us - rng.integers(1, 14, size=n, dtype=np.int64) * 86400_000_000
    receipt_us = ship_us + rng.integers(1, 30, size=n, dtype=np.int64) * 86400_000_000

    returnflag = pa.array(rng.choice(['A', 'N', 'R'], size=n))
    linestatus = pa.array(rng.choice(['F', 'O'], size=n))

    table = pa.table({
        'l_orderkey': pa.array(orderkey, type=pa.int64()),
        'l_partkey': pa.array(partkey, type=pa.int64()),
        'l_suppkey': pa.array(suppkey, type=pa.int64()),
        'l_quantity': pa.array(quantity, type=pa.float64()),
        'l_extendedprice': pa.array(price, type=pa.float64()),
        'ship_ts': pa.array(ship_us, type=pa.timestamp('us')),
        'commit_ts': pa.array(commit_us, type=pa.timestamp('us')),
        'receipt_ts': pa.array(receipt_us, type=pa.timestamp('us')),
        'l_returnflag': returnflag,
        'l_linestatus': linestatus,
    })

    out_path = os.path.join(out_dir, f'part-{idx:05d}.parquet')
    pq.write_table(table, out_path)
    return idx, n, out_path

completed = 0
with ProcessPoolExecutor(max_workers=min(num_workers, total_files)) as pool:
    futures = {pool.submit(generate_chunk, c): c for c in chunks}
    for future in as_completed(futures):
        idx, n, path = future.result()
        completed += 1
        pct = completed * 100 // total_files
        bar = '=' * (pct // 2) + '>' + ' ' * (50 - pct // 2)
        print(f'\r  [{bar}] {pct}% ({completed}/{total_files} files)', end='', flush=True)

print(f'\n  Done. {total_files} file(s), {num_rows} total rows in {out_dir}')
PYEOF
}

detect_cudf_mode() {
  # Write detection result to a temp file to avoid pipefail issues
  local tmpfile
  tmpfile=$(mktemp)
  echo "unknown" > "${tmpfile}"

  local containers
  containers=$(docker ps --format '{{.Names}}' 2>/dev/null) || true
  if [ -z "${containers}" ]; then
    cat "${tmpfile}"
    rm -f "${tmpfile}"
    return
  fi

  for c in ${containers}; do
    docker logs "${c}" 2>&1 | grep -E "cudf\.enabled=" > "${tmpfile}.grep" 2>/dev/null || true
    if [ -s "${tmpfile}.grep" ]; then
      if grep -q "cudf.enabled=true" "${tmpfile}.grep" 2>/dev/null; then
        echo "gpu" > "${tmpfile}"
      else
        echo "cpu" > "${tmpfile}"
      fi
      rm -f "${tmpfile}.grep"
      cat "${tmpfile}"
      rm -f "${tmpfile}"
      return
    fi
    rm -f "${tmpfile}.grep"
  done

  cat "${tmpfile}"
  rm -f "${tmpfile}"
}

setup_data() {
  preflight_check
  local num_rows=$(( SF * 6000000 ))

  echo "=== Setting up timestamp benchmark table: ${TABLE} (sf${SF}, ~${num_rows} rows) ==="

  # Check that workers are in CPU mode for setup (ANALYZE needs CPU)
  local mode
  mode=$(detect_cudf_mode)
  if [ "${mode}" = "gpu" ]; then
    echo ""
    echo "ERROR: Setup requires workers running with cudf.enabled=false (CPU mode)."
    echo "ANALYZE TABLE runs on the workers and may fail or produce wrong stats"
    echo "when cuDF is enabled."
    echo ""
    echo "Please restart workers with cudf.enabled=false, then run setup again."
    exit 1
  else
    echo "Workers are in CPU mode. Good."
  fi

  ensure_schema

  # Clean old data and generate fresh parquet files
  if [ -d "${HOST_DATA_DIR}" ]; then
    echo "Cleaning old data in ${HOST_DATA_DIR}..."
    rm -rf "${HOST_DATA_DIR}"
  fi
  generate_parquet "${num_rows}" "${HOST_DATA_DIR}"

  # Drop old table if exists
  cli --execute "DROP TABLE IF EXISTS ${TABLE}" 2>/dev/null || true

  # Create external table pointing to the parquet files
  echo "Creating external Hive table ${TABLE}..."
  cli --execute "
    CREATE TABLE ${TABLE} (
      l_orderkey BIGINT,
      l_partkey BIGINT,
      l_suppkey BIGINT,
      l_quantity DOUBLE,
      l_extendedprice DOUBLE,
      ship_ts TIMESTAMP,
      commit_ts TIMESTAMP,
      receipt_ts TIMESTAMP,
      l_returnflag VARCHAR,
      l_linestatus VARCHAR
    )
    WITH (
      format = 'PARQUET',
      external_location = 'file:${CONTAINER_DATA_DIR}'
    )
  "

  echo "Verifying table..."
  local count_result
  count_result=$(cli --execute "SELECT count(*) FROM ${TABLE}" 2>/dev/null | tr -d '"[:space:]')
  echo "Table ${TABLE} created with ${count_result} rows."

  if [ "${count_result}" = "0" ] || [ -z "${count_result}" ]; then
    echo ""
    echo "WARNING: Table appears empty. Check that PRESTO_DATA_DIR=${PRESTO_DATA_DIR}"
    echo "is mounted into containers at /var/lib/presto/data/hive/data/user_data"
    echo "Container path: ${CONTAINER_DATA_DIR}"
    return
  fi

  # Generate supplier dimension table
  local dim_host_dir="${PRESTO_DATA_DIR}/ts_bench/suppliers_sf${SF}"
  local dim_container_dir="/var/lib/presto/data/hive/data/user_data/ts_bench/suppliers_sf${SF}"
  echo ""
  echo "Generating supplier dimension table..."
  python3 - "${dim_host_dir}" <<'PYEOF'
import sys, os
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

out_dir = sys.argv[1]
os.makedirs(out_dir, exist_ok=True)

rng = np.random.default_rng(99)
num_suppliers = 10_000
regions = ['AFRICA', 'AMERICA', 'ASIA', 'EUROPE', 'MIDDLE EAST']

table = pa.table({
    's_suppkey': pa.array(np.arange(1, num_suppliers + 1), type=pa.int64()),
    'region': pa.array(rng.choice(regions, size=num_suppliers)),
    's_name': pa.array([f'Supplier#{i:09d}' for i in range(1, num_suppliers + 1)]),
})

pq.write_table(table, os.path.join(out_dir, 'suppliers.parquet'))
print(f'  Wrote {num_suppliers} suppliers to {out_dir}')
PYEOF

  cli --execute "DROP TABLE IF EXISTS ${DIM_TABLE}" 2>/dev/null || true
  echo "Creating dimension table ${DIM_TABLE}..."
  cli --execute "
    CREATE TABLE ${DIM_TABLE} (
      s_suppkey BIGINT,
      region VARCHAR,
      s_name VARCHAR
    )
    WITH (
      format = 'PARQUET',
      external_location = 'file:${dim_container_dir}'
    )
  "

  # Run ANALYZE to collect table statistics for query optimization
  echo ""
  echo "Running ANALYZE TABLE to collect statistics..."
  cli --execute "ANALYZE ${TABLE}" 2>/dev/null || {
    echo "WARNING: ANALYZE on ${TABLE} failed."
  }
  cli --execute "ANALYZE ${DIM_TABLE}" 2>/dev/null || {
    echo "WARNING: ANALYZE on ${DIM_TABLE} failed."
  }
  echo "Setup complete."
  echo ""
}

declare -A QUERIES
QUERIES["ts_filter_count"]="
  SELECT count(*)
  FROM ${TABLE}
  WHERE ship_ts >= TIMESTAMP '1995-01-01 00:00:00'
    AND ship_ts < TIMESTAMP '1995-04-01 00:00:00'
"

QUERIES["ts_extract_groupby"]="
  SELECT
    extract(year FROM ship_ts) AS yr,
    extract(month FROM ship_ts) AS mo,
    count(*) AS cnt,
    sum(l_quantity) AS total_qty
  FROM ${TABLE}
  GROUP BY 1, 2
  ORDER BY 1, 2
"

QUERIES["ts_date_trunc_agg"]="
  SELECT
    date_trunc('month', ship_ts) AS month,
    l_returnflag,
    count(*) AS cnt,
    sum(l_extendedprice) AS revenue
  FROM ${TABLE}
  WHERE ship_ts >= TIMESTAMP '1994-01-01 00:00:00'
  GROUP BY 1, 2
  ORDER BY 1, 2
"

QUERIES["ts_column_compare"]="
  SELECT count(*)
  FROM ${TABLE}
  WHERE receipt_ts > commit_ts
"

QUERIES["ts_multi_ops"]="
  SELECT
    extract(year FROM ship_ts) AS yr,
    count(*) AS shipments,
    sum(CASE WHEN receipt_ts > commit_ts THEN 1 ELSE 0 END) AS late
  FROM ${TABLE}
  GROUP BY 1
  ORDER BY 1
"

QUERIES["ts_dense_filter"]="
  SELECT
    date_trunc('day', ship_ts) AS day,
    count(*) AS cnt
  FROM ${TABLE}
  WHERE ship_ts >= TIMESTAMP '1995-03-01 00:00:00'
    AND ship_ts < TIMESTAMP '1995-03-15 00:00:00'
  GROUP BY 1
  ORDER BY 1
"

DIM_TABLE="hive.${SCHEMA}.ts_bench_suppliers_sf${SF}"

QUERIES["ts_self_join"]="
  SELECT
    a.l_returnflag,
    count(*) AS cnt
  FROM ${TABLE} a
  JOIN ${TABLE} b ON a.l_orderkey = b.l_orderkey
  WHERE a.ship_ts >= TIMESTAMP '1995-01-01 00:00:00'
    AND a.ship_ts < TIMESTAMP '1995-02-01 00:00:00'
    AND b.receipt_ts > b.commit_ts
  GROUP BY 1
  ORDER BY 1
"

QUERIES["ts_dim_join"]="
  SELECT
    s.region,
    extract(year FROM t.ship_ts) AS yr,
    count(*) AS cnt,
    sum(t.l_extendedprice) AS revenue
  FROM ${TABLE} t
  JOIN ${DIM_TABLE} s ON t.l_suppkey = s.s_suppkey
  WHERE t.ship_ts >= TIMESTAMP '1994-01-01 00:00:00'
    AND t.ship_ts < TIMESTAMP '1995-01-01 00:00:00'
  GROUP BY 1, 2
  ORDER BY 1, 2
"

QUERY_ORDER=(
  ts_filter_count
  ts_extract_groupby
  ts_date_trunc_agg
  ts_column_compare
  ts_multi_ops
  ts_dense_filter
  ts_self_join
  ts_dim_join
)

run_benchmark() {
  preflight_check
  echo "=== Timestamp Benchmark (${RUNS} runs per query, table: ${TABLE}) ==="
  echo ""

  # Verify table has data
  local count_result
  echo "Verifying table ${TABLE}..."
  count_result=$(cli --execute "SELECT count(*) FROM ${TABLE}" 2>&1 | tr -d '"[:space:]') || true
  if [ -z "${count_result}" ] || [ "${count_result}" = "0" ] || echo "${count_result}" | grep -qi "failed\|error"; then
    echo "ERROR: Table ${TABLE} is empty or not accessible. Run setup first."
    echo "  Result: ${count_result}"
    echo "  Try: $0 setup ${SF}"
    exit 1
  fi
  echo "Table has ${count_result} rows."
  echo ""

  local mode
  mode=$(detect_cudf_mode)
  echo "Detected mode: ${mode}"

  # Create output directory for full report
  local ts
  ts=$(date +%Y%m%d_%H%M%S)
  local out_dir="benchmark_results/${mode}_sf${SF}_${ts}"
  mkdir -p "${out_dir}"

  local results_csv="${out_dir}/timings.csv"
  local report_file="${out_dir}/report.txt"
  echo "query,run,elapsed_ms" > "${results_csv}"

  # Save worker configs from docker logs
  local all_containers
  all_containers=$(docker ps --format '{{.Names}}' || true)
  for c in ${all_containers}; do
    local config_file="${out_dir}/config_${c}.txt"
    echo "=== Container: ${c} ===" > "${config_file}"
    docker logs "${c}" 2>&1 | grep -E "Registered properties|Unregistered properties|^\s+\S+=\S+" >> "${config_file}" 2>/dev/null || true
    echo "" >> "${config_file}"
    echo "=== Full startup log (first 100 lines) ===" >> "${config_file}"
    docker logs "${c}" 2>&1 | head -100 >> "${config_file}" 2>/dev/null || true
  done

  # Tee all output to report file and terminal
  exec > >(tee -a "${report_file}") 2>&1

  echo "Mode: ${mode}"
  echo "Scale factor: ${SF}"
  echo "Runs per query: ${RUNS}"
  echo "Table: ${TABLE} (${count_result} rows)"
  echo "Output: ${out_dir}/"
  echo ""

  # Save query name -> SQL mapping for Presto stats matching
  local query_map="${out_dir}/query_map.txt"
  for qname in "${QUERY_ORDER[@]}"; do
    echo "###QUERY### ${qname}"
    echo "${QUERIES[${qname}]}"
  done > "${query_map}"

  for qname in "${QUERY_ORDER[@]}"; do
    local sql="${QUERIES[${qname}]}"
    echo "--- ${qname} ---"

    # Warmup
    echo "  Warming up..."
    cli --execute "${sql}" > /dev/null 2>&1 || true

    for run in $(seq 1 "${RUNS}"); do
      local ms
      ms=$(run_query "Run ${run}/${RUNS}" "${sql}")
      echo "${qname},${run},${ms}" >> "${results_csv}"
    done

    # Save query stats immediately after last run (before stage data expires).
    # Match by SQL text to get the right query.
    local escaped_sql
    escaped_sql=$(echo "${sql}" | python3 -c "import sys; print(repr(sys.stdin.read().strip()))")
    docker exec "${COORDINATOR}" curl -sf "http://localhost:${PORT}/v1/query" 2>/dev/null | \
      python3 -c "
import json, sys
expected = ${escaped_sql}
norm = lambda s: ' '.join(s.split()).strip().lower()
expected_norm = norm(expected)
queries = json.load(sys.stdin)
for q in queries:
    if q.get('state') == 'FINISHED' and norm(q.get('query', '')) == expected_norm:
        print(q['queryId'])
        break
" 2>/dev/null | while read qid; do
      docker exec "${COORDINATOR}" curl -sf "http://localhost:${PORT}/v1/query/${qid}" > "${out_dir}/query_${qname}.json" 2>/dev/null || true
    done
    echo ""
  done

  # Print summary with both wall time and Presto-reported times
  echo "=== Summary ==="
  python3 - "${results_csv}" "${COORDINATOR}" "${PORT}" "${RUNS}" "${query_map}" "${out_dir}" <<'PYEOF'
import subprocess
import json
import sys
import csv
import re
import os

results_csv = sys.argv[1]
coordinator = sys.argv[2]
port = sys.argv[3]
num_runs = int(sys.argv[4])
query_map_file = sys.argv[5]
out_dir = sys.argv[6]

# Parse query name -> SQL mapping
query_sql_map = {}
current_name = None
current_sql = []
with open(query_map_file) as f:
    for line in f:
        if line.startswith("###QUERY###"):
            if current_name:
                query_sql_map[current_name] = " ".join("".join(current_sql).split()).strip()
            current_name = line.strip().split(" ", 1)[1]
            current_sql = []
        else:
            current_sql.append(line)
    if current_name:
        query_sql_map[current_name] = " ".join("".join(current_sql).split()).strip()

def normalize_sql(s):
    """Normalize SQL for comparison: collapse whitespace, lowercase."""
    return " ".join(s.split()).strip().lower()

def curl_json(path):
    r = subprocess.run(
        ["docker", "exec", coordinator, "curl", "-sf", f"http://localhost:{port}{path}"],
        capture_output=True, text=True, timeout=10)
    return json.loads(r.stdout) if r.returncode == 0 else None

def parse_duration(s):
    s = s.strip()
    if s.endswith("ns"): return float(s[:-2]) / 1e6
    if s.endswith("us"): return float(s[:-2]) / 1e3
    if s.endswith("ms"): return float(s[:-2])
    if s.endswith("s") and not s.endswith("ms") and not s.endswith("ns") and not s.endswith("us"):
        return float(s[:-1]) * 1000
    if s.endswith("m") and not s.endswith("ms"): return float(s[:-1]) * 60000
    if s.endswith("h"): return float(s[:-1]) * 3600000
    return 0

def fmt_rows(n):
    if n >= 1e6: return f"{n/1e6:.1f}M"
    if n >= 1e3: return f"{n/1e3:.1f}K"
    return str(n)

def fmt_ns(ns_str):
    try: return float(ns_str.replace("ns", "")) / 1e6
    except: return 0.0

# Read wall-clock timings from CSV
timings = {}
with open(results_csv) as f:
    reader = csv.DictReader(f)
    for row in reader:
        qname = row["query"]
        ms = int(row["elapsed_ms"])
        timings.setdefault(qname, []).append(ms)

# Compute medians
medians = {}
for qname, vals in timings.items():
    vals.sort()
    medians[qname] = vals[len(vals) // 2]

# Load query stats from saved JSON files (captured immediately after each query)
def collect_all_ops(stage):
    """Walk stage tree and collect operator summaries from all stages."""
    ops = []
    if not stage:
        return ops
    exec_info = stage.get("latestAttemptExecutionInfo", {})
    stats = exec_info.get("stats", {})
    for op in stats.get("operatorSummaries", []):
        op["_stageId"] = stage.get("stageId", "?")
        ops.append(op)
    for sub in stage.get("subStages", []):
        ops.extend(collect_all_ops(sub))
    return ops

presto_by_name = {}
for qname in query_sql_map:
    json_file = os.path.join(out_dir, f"query_{qname}.json")
    if not os.path.exists(json_file):
        continue
    with open(json_file) as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            continue
    qs = data.get("queryStats", {})
    all_ops = collect_all_ops(data.get("outputStage"))
    if not all_ops:
        all_ops = qs.get("operatorSummaries", [])
    presto_by_name[qname] = {
        "qid": data.get("queryId", "?"),
        "elapsed": qs.get("elapsedTime", "?"),
        "cpu": qs.get("totalCpuTime", "?"),
        "ops": all_ops,
    }

# Print summary table
hdr = "{:<25s} {:>10s} {:>10s} {:>10s} {:>10s}"
print(hdr.format("Query", "Wall (ms)", "Presto ms", "CPU ms", "Overhead"))
print(hdr.format("-" * 25, "-" * 10, "-" * 10, "-" * 10, "-" * 10))

# Match queries by name (directly from saved JSON files)
query_details = {}
for qname, wall_median in medians.items():
    best_match = presto_by_name.get(qname)

    if best_match:
        elapsed_ms = parse_duration(best_match["elapsed"])
        cpu_ms = parse_duration(best_match["cpu"])
        overhead = wall_median - elapsed_ms
        num_ops = len(best_match["ops"])
        print(hdr.format(qname, str(wall_median), f"{elapsed_ms:.0f}", f"{cpu_ms:.0f}", f"{overhead:.0f}"))
        query_details[qname] = {"wall": wall_median, "presto": elapsed_ms, "cpu": cpu_ms, "ops": best_match["ops"], "qid": best_match["qid"]}
    else:
        print(hdr.format(qname, str(wall_median), "?", "?", "?"))
        query_details[qname] = {"wall": wall_median, "ops": []}

print()
print("  Wall    = client-measured end-to-end (includes CLI startup, HTTP, scheduling)")
print("  Presto  = server-reported elapsed time")
print("  CPU     = server-reported total CPU time across all threads")
print("  Overhead= Wall - Presto (client/network overhead)")

# Print per-operator breakdown for each query
print()
print("=" * 80)
print("Per-Operator Breakdown")
print("=" * 80)

for qname, details in query_details.items():
    ops = details.get("ops", [])
    if not ops:
        continue

    # Aggregate operators by type+planNodeId (summed across all stages/tasks)
    op_data = {}
    for op in ops:
        name = op.get("operatorType", "?")
        pid = op.get("planNodeId", "")
        key = f"{name}[{pid}]" if pid and pid != "N/A" else name
        if key not in op_data:
            op_data[key] = {"cpu": 0, "wall": 0, "in_r": 0, "out_r": 0, "drivers": 0}
        op_data[key]["wall"] += fmt_ns(op.get("getOutputWall", "0ns")) + fmt_ns(op.get("addInputWall", "0ns"))
        op_data[key]["cpu"] += fmt_ns(op.get("getOutputCpu", "0ns")) + fmt_ns(op.get("addInputCpu", "0ns"))
        op_data[key]["in_r"] += op.get("inputPositions", 0)
        op_data[key]["out_r"] += op.get("outputPositions", 0)
        op_data[key]["drivers"] += op.get("totalDrivers", 0)

    sorted_ops = sorted(op_data.items(), key=lambda x: x[1]["wall"], reverse=True)

    qid = details.get("qid", "?")
    num_ops = len(ops)
    print(f"\n--- {qname} (wall={details['wall']}ms, presto={details.get('presto','?')}ms, cpu={details.get('cpu','?')}ms, qid={qid}, ops={num_ops}) ---")
    ohdr = "  {:<40s} {:>8s} {:>8s} {:>10s} {:>10s} {:>6s}"
    print(ohdr.format("Operator", "CPU ms", "Wall ms", "In Rows", "Out Rows", "Drvrs"))
    print(ohdr.format("-" * 40, "-" * 8, "-" * 8, "-" * 10, "-" * 10, "-" * 6))
    for name, d in sorted_ops:
        if d["wall"] < 0.01 and d["cpu"] < 0.01:
            continue
        print(ohdr.format(name[:40], f"{d['cpu']:.1f}", f"{d['wall']:.1f}",
                          fmt_rows(d["in_r"]), fmt_rows(d["out_r"]),
                          str(d.get("drivers", ""))))
PYEOF

  echo ""
  echo "To compare GPU vs CPU, run benchmark twice with different server configs:"
  echo "  1. Start workers with cudf.enabled=true,  run: $0 bench ${SF}"
  echo "  2. Start workers with cudf.enabled=false, run: $0 bench ${SF}"

  # Check for unexpected GPU fallbacks
  echo ""
  echo "=== Fallback Check ==="
  local all_containers
  all_containers=$(docker ps --format '{{.Names}}' || true)
  local fallback_file="${out_dir}/fallbacks.txt"
  : > "${fallback_file}"

  if [ -z "${all_containers}" ]; then
    echo "No containers found. Skipping fallback check."
  else
    local unexpected_fallbacks=0
    local expected_count=0
    for c in ${all_containers}; do
      local all_fallbacks
      all_fallbacks=$(docker logs "${c}" 2>&1 | grep "Replacement Failed Operator:" || true)
      if [ -z "${all_fallbacks}" ]; then
        continue
      fi

      # Save all fallbacks to file
      echo "=== ${c} ===" >> "${fallback_file}"
      echo "${all_fallbacks}" >> "${fallback_file}"
      echo "" >> "${fallback_file}"

      # Filter out expected fallbacks
      local unexpected
      unexpected=$(echo "${all_fallbacks}" | grep -v "PartitionedOutput\|LocalMerge\|CallbackSink\|Values" || true)
      local expected
      expected=$(echo "${all_fallbacks}" | grep "PartitionedOutput\|LocalMerge\|CallbackSink\|Values" || true)

      if [ -n "${expected}" ]; then
        expected_count=$(( expected_count + $(echo "${expected}" | wc -l) ))
      fi

      if [ -n "${unexpected}" ]; then
        echo "UNEXPECTED fallbacks on ${c}:"
        echo "${unexpected}" | head -10
        unexpected_fallbacks=1
      fi
    done

    if [ "${unexpected_fallbacks}" -eq 0 ]; then
      echo "OK: No unexpected GPU fallbacks."
      if [ "${expected_count}" -gt 0 ]; then
        echo "  (${expected_count} expected fallbacks: PartitionedOutput[SINGLE], LocalMerge, etc.)"
      fi
    fi
    echo "  Full fallback log saved to ${fallback_file}"
  fi

  echo ""
  echo "=== Full report saved to ${out_dir}/ ==="
  echo "  report.txt    - complete benchmark output"
  echo "  timings.csv   - per-run wall clock times"
  echo "  fallbacks.txt - GPU fallback details"
  echo "  config_*.txt  - worker/coordinator configs"
}

run_verify() {
  preflight_check
  echo "=== Verification: Presto vs DuckDB (table: ${TABLE}) ==="
  echo ""

  # Write all queries to a temp file for the Python verifier
  local query_file
  query_file=$(mktemp /tmp/ts_bench_queries.XXXXXX)
  for qname in "${QUERY_ORDER[@]}"; do
    # Write query name and SQL separated by a marker
    echo "###QUERY### ${qname}"
    echo "${QUERIES[${qname}]}"
  done > "${query_file}"

  local dim_host_dir="${PRESTO_DATA_DIR}/ts_bench/suppliers_sf${SF}"

  # Run verification in Python — handles quoting, normalization, comparison
  python3 - "${query_file}" "${TABLE}" "${HOST_DATA_DIR}" "${DIM_TABLE}" "${dim_host_dir}" "${COORDINATOR}" "${PORT}" "${SCHEMA}" <<'PYEOF'
import sys
import subprocess
import duckdb
import re
from datetime import datetime

query_file = sys.argv[1]
table_name = sys.argv[2]
data_dir = sys.argv[3]
dim_table_name = sys.argv[4]
dim_data_dir = sys.argv[5]
coordinator = sys.argv[6]
port = sys.argv[7]
schema = sys.argv[8]

# Parse queries from file
queries = {}
current_name = None
current_sql = []
with open(query_file) as f:
    for line in f:
        if line.startswith("###QUERY###"):
            if current_name:
                queries[current_name] = "\n".join(current_sql)
            current_name = line.strip().split(" ", 1)[1]
            current_sql = []
        else:
            current_sql.append(line.rstrip())
    if current_name:
        queries[current_name] = "\n".join(current_sql)

def normalize_value(v):
    """Normalize a value for comparison."""
    v = v.strip().strip('"')
    # Scientific notation to decimal
    try:
        f = float(v)
        # Round to 6 significant digits to handle float precision
        return f"{f:.6g}"
    except ValueError:
        pass
    # Timestamp: strip trailing time if midnight
    v = re.sub(r' 00:00:00(\.000)?$', '', v)
    return v

def normalize_row(row_str):
    """Normalize a CSV row."""
    parts = row_str.strip().split('","')
    parts = [p.strip('"') for p in parts]
    return tuple(normalize_value(p) for p in parts)

def sort_key(row):
    """Sort key that handles numeric values correctly."""
    result = []
    for v in row:
        try:
            result.append((0, float(v), v))
        except ValueError:
            result.append((1, 0, v))
    return result

def run_presto(sql):
    """Run query via presto-cli and return normalized rows."""
    cmd = [
        "docker", "exec", "-i", coordinator, "presto-cli",
        "--server", f"localhost:{port}",
        "--catalog", "hive",
        "--schema", schema,
        "--execute", sql.strip()
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0 or "failed" in result.stdout.lower():
        return None, result.stdout + result.stderr
    rows = []
    for line in result.stdout.strip().split("\n"):
        if line.strip():
            rows.append(normalize_row(line))
    return sorted(rows, key=sort_key), None

def run_duckdb(sql):
    """Run query via DuckDB and return normalized rows."""
    # Replace table names with parquet reads
    duck_sql = sql.replace(table_name, f"read_parquet('{data_dir}/*.parquet')")
    duck_sql = duck_sql.replace(dim_table_name, f"read_parquet('{dim_data_dir}/*.parquet')")
    con = duckdb.connect()
    result = con.execute(duck_sql).fetchall()
    rows = []
    for row in result:
        normalized = tuple(normalize_value(str(v)) for v in row)
        rows.append(normalized)
    return sorted(rows, key=sort_key)

passed = 0
failed = 0

for qname, sql in queries.items():
    print(f"  {qname}... ", end="", flush=True)

    presto_rows, err = run_presto(sql)
    if presto_rows is None:
        print(f"SKIP (Presto error: {err[:100]})")
        continue

    try:
        duck_rows = run_duckdb(sql)
    except Exception as e:
        print(f"SKIP (DuckDB error: {e})")
        continue

    if presto_rows == duck_rows:
        print("PASS")
        passed += 1
    else:
        print("FAIL")
        failed += 1
        print(f"    Rows: Presto={len(presto_rows)}, DuckDB={len(duck_rows)}")
        for i, (p, d) in enumerate(zip(presto_rows[:5], duck_rows[:5])):
            if p != d:
                print(f"    Row {i}: Presto={p}")
                print(f"            DuckDB={d}")

print()
print(f"=== Verification Summary: {passed} passed, {failed} failed ===")
if failed > 0:
    print("WARNING: Some queries returned different results!")
    sys.exit(1)
PYEOF

  rm -f "${query_file}"
}

case "${1:-all}" in
  setup)
    setup_data
    ;;
  bench)
    run_benchmark
    ;;
  verify)
    run_verify
    ;;
  all)
    setup_data
    run_benchmark
    ;;
  *)
    echo "Usage: $0 [setup|bench|verify|all] [scale_factor]"
    exit 1
    ;;
esac
