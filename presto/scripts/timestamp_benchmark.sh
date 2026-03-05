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
    worker_count=$(echo "${node_json}" | grep -o '"uri"' | wc -l)
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

preflight_check
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

  echo "Generating ${num_rows} rows of timestamp data..."
  python3 - "${num_rows}" "${out_dir}" <<'PYEOF'
import sys
import os
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from datetime import datetime

num_rows = int(sys.argv[1])
out_dir = sys.argv[2]
os.makedirs(out_dir, exist_ok=True)

rng = np.random.default_rng(42)

# Generate data in chunks to manage memory
chunk_size = min(num_rows, 5_000_000)
file_idx = 0

rows_remaining = num_rows
while rows_remaining > 0:
    n = min(chunk_size, rows_remaining)

    orderkey = rng.integers(1, num_rows * 4, size=n, dtype=np.int64)
    partkey = rng.integers(1, 200_000, size=n, dtype=np.int64)
    suppkey = rng.integers(1, 10_000, size=n, dtype=np.int64)
    quantity = rng.uniform(1.0, 50.0, size=n).astype(np.float64)
    price = rng.uniform(900.0, 105000.0, size=n).astype(np.float64)

    # Timestamps: 1992-01-01 to 1998-12-31 (~2557 days) with random hours
    base_us = int(datetime(1992, 1, 1).timestamp() * 1_000_000)
    day_offsets = rng.integers(0, 2557, size=n, dtype=np.int64)
    hour_offsets = rng.integers(0, 24, size=n, dtype=np.int64)
    minute_offsets = rng.integers(0, 60, size=n, dtype=np.int64)
    second_offsets = rng.integers(0, 60, size=n, dtype=np.int64)

    ship_us = base_us + day_offsets * 86400_000_000 + hour_offsets * 3600_000_000 + minute_offsets * 60_000_000 + second_offsets * 1_000_000
    # commit ~7 days before ship, receipt ~3 days after ship
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

    out_path = os.path.join(out_dir, f'part-{file_idx:05d}.parquet')
    pq.write_table(table, out_path)
    rows_remaining -= n
    file_idx += 1
    print(f'  Wrote {out_path} ({n} rows)')

print(f'Done. {file_idx} file(s), {num_rows} total rows in {out_dir}')
PYEOF
}

setup_data() {
  local num_rows=$(( SF * 6000000 ))

  echo "=== Setting up timestamp benchmark table: ${TABLE} (sf${SF}, ~${num_rows} rows) ==="

  ensure_schema

  # Generate parquet files on host
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
  fi
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

QUERY_ORDER=(
  ts_filter_count
  ts_extract_groupby
  ts_date_trunc_agg
  ts_column_compare
  ts_multi_ops
  ts_dense_filter
)

run_benchmark() {
  echo "=== Timestamp Benchmark (${RUNS} runs per query, table: ${TABLE}) ==="
  echo ""

  # Verify table has data
  local count_result
  echo "Verifying table ${TABLE}..."
  count_result=$(cli --execute "SELECT count(*) FROM ${TABLE}" 2>&1 | tr -d '"[:space:]') || true
  echo "Count result: '${count_result}'"
  if [ -z "${count_result}" ] || [ "${count_result}" = "0" ]; then
    echo "ERROR: Table ${TABLE} is empty or not accessible. Run setup first."
    echo "Try: $0 setup ${SF}"
    exit 1
  fi
  echo "Table has ${count_result} rows."
  echo ""

  local mode="${BENCH_MODE:-gpu}"
  local results_file="timestamp_benchmark_${mode}_$(date +%Y%m%d_%H%M%S).csv"
  echo "query,run,elapsed_ms" > "${results_file}"

  echo "Mode: ${mode} (set BENCH_MODE=gpu or BENCH_MODE=cpu to label runs)"
  echo "Timing ${RUNS} runs per query against the running server config."
  echo ""

  for qname in "${QUERY_ORDER[@]}"; do
    local sql="${QUERIES[${qname}]}"
    echo "--- ${qname} ---"

    # Warmup
    echo "  Warming up..."
    cli --execute "${sql}" > /dev/null 2>&1 || true

    for run in $(seq 1 "${RUNS}"); do
      local ms
      ms=$(run_query "Run ${run}/${RUNS}" "${sql}")
      echo "${qname},${run},${ms}" >> "${results_file}"
    done
    echo ""
  done

  echo "=== Results saved to ${results_file} ==="
  echo ""

  # Print summary
  echo "=== Summary (median of ${RUNS} runs) ==="
  printf "%-25s %12s\n" "Query" "Median (ms)"
  printf "%-25s %12s\n" "-------------------------" "------------"

  for qname in "${QUERY_ORDER[@]}"; do
    local median
    median=$(grep "^${qname}," "${results_file}" | cut -d, -f3 | sort -n | sed -n "$((( RUNS + 1 ) / 2))p")

    if [ -n "${median}" ] && [ "${median}" != "-1" ] 2>/dev/null; then
      printf "%-25s %12s\n" "${qname}" "${median}"
    else
      printf "%-25s %12s\n" "${qname}" "FAILED"
    fi
  done

  echo ""
  echo "To compare GPU vs CPU, run benchmark twice with different server configs:"
  echo "  1. Start workers with cudf.enabled=true,  run: BENCH_MODE=gpu $0 bench ${SF}"
  echo "  2. Start workers with cudf.enabled=false, run: BENCH_MODE=cpu $0 bench ${SF}"

  # Check for unexpected GPU fallbacks
  echo ""
  echo "=== Fallback Check ==="
  local workers
  workers=$(docker ps --format '{{.Names}}' | grep -i worker || true)
  if [ -z "${workers}" ]; then
    echo "No worker containers found. Skipping fallback check."
  else
    local unexpected_fallbacks=0
    local expected_count=0
    for w in ${workers}; do
      # Grep for "Replacement Failed Operator:" lines which contain the operator name
      local all_fallbacks
      all_fallbacks=$(docker logs "${w}" 2>&1 | grep "Replacement Failed Operator:" || true)
      if [ -z "${all_fallbacks}" ]; then
        continue
      fi

      # Filter out expected fallbacks
      local unexpected
      unexpected=$(echo "${all_fallbacks}" | grep -v "PartitionedOutput\|LocalMerge\|CallbackSink\|Values" || true)
      local expected
      expected=$(echo "${all_fallbacks}" | grep "PartitionedOutput\|LocalMerge\|CallbackSink\|Values" || true)

      if [ -n "${expected}" ]; then
        expected_count=$(( expected_count + $(echo "${expected}" | wc -l) ))
      fi

      if [ -n "${unexpected}" ]; then
        echo "UNEXPECTED fallbacks on ${w}:"
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
  fi
}

run_verify() {
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

  # Run verification in Python — handles quoting, normalization, comparison
  python3 - "${query_file}" "${TABLE}" "${HOST_DATA_DIR}" "${COORDINATOR}" "${PORT}" "${SCHEMA}" <<'PYEOF'
import sys
import subprocess
import duckdb
import re
from datetime import datetime

query_file = sys.argv[1]
table_name = sys.argv[2]
data_dir = sys.argv[3]
coordinator = sys.argv[4]
port = sys.argv[5]
schema = sys.argv[6]

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
    return sorted(rows), None

def run_duckdb(sql):
    """Run query via DuckDB and return normalized rows."""
    # Replace table name with parquet read
    duck_sql = sql.replace(table_name, f"read_parquet('{data_dir}/*.parquet')")
    con = duckdb.connect()
    result = con.execute(duck_sql).fetchall()
    rows = []
    for row in result:
        normalized = tuple(normalize_value(str(v)) for v in row)
        rows.append(normalized)
    return sorted(rows)

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
