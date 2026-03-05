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
# Environment:
#   PRESTO_COORDINATOR  - coordinator container name (default: presto-coordinator)
#   PRESTO_PORT         - coordinator port (default: 8080)
#   HIVE_SCHEMA         - hive schema to use (default: default)
#   BENCHMARK_RUNS      - number of runs per query (default: 3)
#   PRESTO_DATA_DIR     - host data dir mounted into containers (required)
#
# Prerequisites:
#   - Presto coordinator running with hive catalog
#   - Workers running with cuDF enabled

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../scripts/py_env_functions.sh"

# Setup python venv with pyarrow
init_python_virtual_env ".ts_bench_venv"
pip install -q pyarrow numpy
trap 'delete_python_virtual_env .ts_bench_venv' EXIT

COORDINATOR="${PRESTO_COORDINATOR:-presto-coordinator}"
PORT="${PRESTO_PORT:-8080}"
SF="${2:-10}"
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
  local session="$2"
  local sql="$3"

  echo -n "  [${label}] running... "
  local start end elapsed
  start=$(date +%s%N)
  cli --session "${session}" --execute "${sql}" > /dev/null 2>&1
  end=$(date +%s%N)
  elapsed=$(( (end - start) / 1000000 ))
  echo "${elapsed} ms"
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
      external_location = 'file://${CONTAINER_DATA_DIR}'
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
  count_result=$(cli --execute "SELECT count(*) FROM ${TABLE}" 2>/dev/null | tr -d '"[:space:]')
  if [ "${count_result}" = "0" ] || [ -z "${count_result}" ]; then
    echo "ERROR: Table ${TABLE} is empty. Run setup first."
    exit 1
  fi
  echo "Table has ${count_result} rows."
  echo ""

  local results_file="timestamp_benchmark_results_$(date +%Y%m%d_%H%M%S).csv"
  echo "query,mode,run,elapsed_ms" > "${results_file}"

  for qname in "${QUERY_ORDER[@]}"; do
    local sql="${QUERIES[${qname}]}"
    echo "--- ${qname} ---"

    # Warmup
    echo "  Warming up..."
    cli --execute "${sql}" > /dev/null 2>&1 || true

    for run in $(seq 1 "${RUNS}"); do
      echo "  Run ${run}/${RUNS}:"

      # GPU
      local gpu_ms
      gpu_ms=$(run_query "GPU" "cudf.enabled=true" "${sql}")
      echo "${qname},gpu,${run},${gpu_ms}" >> "${results_file}"

      # CPU
      local cpu_ms
      cpu_ms=$(run_query "CPU" "cudf.enabled=false" "${sql}")
      echo "${qname},cpu,${run},${cpu_ms}" >> "${results_file}"
    done
    echo ""
  done

  echo "=== Results saved to ${results_file} ==="
  echo ""

  # Print summary
  echo "=== Summary (median of ${RUNS} runs) ==="
  printf "%-25s %10s %10s %10s\n" "Query" "GPU (ms)" "CPU (ms)" "Speedup"
  printf "%-25s %10s %10s %10s\n" "-------------------------" "----------" "----------" "----------"

  for qname in "${QUERY_ORDER[@]}"; do
    local gpu_median cpu_median
    gpu_median=$(grep "^${qname},gpu," "${results_file}" | cut -d, -f4 | sort -n | sed -n "$((( RUNS + 1 ) / 2))p")
    cpu_median=$(grep "^${qname},cpu," "${results_file}" | cut -d, -f4 | sort -n | sed -n "$((( RUNS + 1 ) / 2))p")

    if [ -n "${gpu_median}" ] && [ "${gpu_median}" -gt 0 ]; then
      local speedup
      speedup=$(echo "scale=2; ${cpu_median} / ${gpu_median}" | bc 2>/dev/null || echo "N/A")
      printf "%-25s %10s %10s %10sx\n" "${qname}" "${gpu_median}" "${cpu_median}" "${speedup}"
    else
      printf "%-25s %10s %10s %10s\n" "${qname}" "${gpu_median:-N/A}" "${cpu_median:-N/A}" "N/A"
    fi
  done
}

case "${1:-all}" in
  setup)
    setup_data
    ;;
  bench)
    run_benchmark
    ;;
  all)
    setup_data
    run_benchmark
    ;;
  *)
    echo "Usage: $0 [setup|bench|all] [scale_factor]"
    exit 1
    ;;
esac
