#!/usr/bin/env bash
# ohlcv_benchmark.sh — OHLCV price engine query benchmark
#
# Tests query patterns required by the price_engine.py backend:
# - Raw OHLCV with date range filter
# - Time-bucketed OHLCV aggregation (date_trunc day/week/month)
# - Daily max-high for pct-change (multi-symbol IN list + group by)
# - Distinct symbols
#
# Usage:
#   ./ohlcv_benchmark.sh [setup|bench|verify|all] [sf]
#
# Examples:
#   ./ohlcv_benchmark.sh setup 10     # Create OHLCV table (~60M rows)
#   ./ohlcv_benchmark.sh bench 10     # Run benchmark queries
#   ./ohlcv_benchmark.sh verify 10    # Verify against DuckDB
#   ./ohlcv_benchmark.sh all 10       # Setup + bench
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
#   - Workers running

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../scripts/py_env_functions.sh"

init_python_virtual_env ".ohlcv_bench_venv"
pip install -q pyarrow numpy duckdb
trap 'LOCAL_CONDA_INIT="${LOCAL_CONDA_INIT:-}"; delete_python_virtual_env .ohlcv_bench_venv' EXIT

COORDINATOR="${PRESTO_COORDINATOR:-presto-coordinator}"
PORT="${PRESTO_PORT:-8080}"
SF="${2:-10}"

# Source shared functions from timestamp_benchmark
# (detect_cudf_mode, preflight_check, cli, run_query, etc.)
# For now, inline the essentials:

detect_cudf_mode() {
  local tmpfile
  tmpfile=$(mktemp)
  echo "unknown" > "${tmpfile}"
  local containers
  containers=$(docker ps --format '{{.Names}}' 2>/dev/null) || true
  if [ -z "${containers}" ]; then
    cat "${tmpfile}"; rm -f "${tmpfile}"; return
  fi
  for c in ${containers}; do
    docker logs "${c}" 2>&1 | grep -E "cudf\.enabled=" > "${tmpfile}.grep" 2>/dev/null || true
    if [ -s "${tmpfile}.grep" ]; then
      if grep -q "cudf.enabled=true" "${tmpfile}.grep" 2>/dev/null; then
        echo "gpu" > "${tmpfile}"
      else
        echo "cpu" > "${tmpfile}"
      fi
      rm -f "${tmpfile}.grep"; cat "${tmpfile}"; rm -f "${tmpfile}"; return
    fi
    rm -f "${tmpfile}.grep"
  done
  cat "${tmpfile}"; rm -f "${tmpfile}"
}

preflight_check() {
  echo "Checking Presto cluster..."
  if ! docker ps --format '{{.Names}}' | grep -qw "${COORDINATOR}" 2>/dev/null; then
    echo "ERROR: Coordinator container '${COORDINATOR}' is not running."
    exit 1
  fi
  local retries=5 ok=false
  for i in $(seq 1 ${retries}); do
    if docker exec "${COORDINATOR}" curl -sf "http://localhost:${PORT}/v1/info" > /dev/null 2>&1; then
      ok=true; break
    fi
    echo "  Waiting for coordinator (attempt ${i}/${retries})..."
    sleep 2
  done
  if ! ${ok}; then echo "ERROR: Coordinator not responding."; exit 1; fi
  echo "  Coordinator is up."

  local worker_retries=30 worker_count=0
  for i in $(seq 1 ${worker_retries}); do
    local node_json
    node_json=$(docker exec "${COORDINATOR}" curl -sf "http://localhost:${PORT}/v1/node" 2>/dev/null || echo "[]")
    worker_count=$(echo "${node_json}" | { grep -o '"uri"' || true; } | wc -l)
    if [ "${worker_count}" -gt 0 ]; then break; fi
    echo "  Waiting for workers (attempt ${i}/${worker_retries})..."
    sleep 2
  done
  if [ "${worker_count}" -eq 0 ]; then
    echo "ERROR: No active worker nodes found."; exit 1
  fi
  echo "  ${worker_count} active worker(s) found."
  echo ""
}

SCHEMA="${HIVE_SCHEMA:-default}"
TABLE="hive.${SCHEMA}.ohlcv_prices_sf${SF}"
RUNS="${BENCHMARK_RUNS:-3}"
HOST_DATA_DIR="${PRESTO_DATA_DIR:?PRESTO_DATA_DIR must be set}/ohlcv/sf${SF}"
CONTAINER_DATA_DIR="/var/lib/presto/data/hive/data/user_data/ohlcv/sf${SF}"

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
      echo "ERROR: Schema does not exist and could not be created."
      exit 1
    fi
  }
  echo "Schema hive.${SCHEMA} is ready."
}

generate_ohlcv_data() {
  local num_rows=$1
  local out_dir=$2
  local num_workers=${GENERATE_WORKERS:-$(nproc)}

  echo "Generating ${num_rows} OHLCV rows (${num_workers} threads)..."
  python3 - "${num_rows}" "${out_dir}" "${num_workers}" <<'PYEOF'
import sys
import os
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

num_rows = int(sys.argv[1])
out_dir = sys.argv[2]
num_workers = int(sys.argv[3])
os.makedirs(out_dir, exist_ok=True)

# Symbols pool
SYMBOLS = [
    'NVDA', 'AAPL', 'GOOGL', 'MSFT', 'AMZN', 'META', 'TSLA', 'AMD',
    'INTC', 'QCOM', 'AVGO', 'CRM', 'ORCL', 'ADBE', 'NFLX', 'PYPL',
    'SQ', 'SHOP', 'SNOW', 'PLTR', 'COIN', 'UBER', 'ABNB', 'DASH',
    'RBLX', 'U', 'DDOG', 'NET', 'ZS', 'CRWD', 'MDB', 'TEAM'
]
ASSET_CLASSES = ['equity', 'crypto', 'commodity', 'fx']

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
    seed = 42 + idx
    rng = np.random.default_rng(seed)

    # Timestamps: 2020-01-01 to 2025-12-31 (~2191 days), minute-level
    base_us = int(datetime(2020, 1, 1).timestamp() * 1_000_000)
    day_offsets = rng.integers(0, 2191, size=n, dtype=np.int64)
    minute_offsets = rng.integers(0, 1440, size=n, dtype=np.int64)  # minutes in a day
    ts_us = base_us + day_offsets * 86400_000_000 + minute_offsets * 60_000_000

    symbols = pa.array(rng.choice(SYMBOLS, size=n))
    asset_class = pa.array(rng.choice(ASSET_CLASSES, size=n))

    # OHLCV data
    base_price = rng.uniform(10.0, 500.0, size=n).astype(np.float64)
    jitter = rng.uniform(-0.05, 0.05, size=n)
    open_price = base_price * (1 + jitter)
    high_price = base_price * (1 + np.abs(jitter) + rng.uniform(0, 0.03, size=n))
    low_price = base_price * (1 - np.abs(jitter) - rng.uniform(0, 0.03, size=n))
    close_price = base_price * (1 + rng.uniform(-0.03, 0.03, size=n))
    volume = rng.integers(100, 10_000_000, size=n, dtype=np.int64)

    table = pa.table({
        'asset_class': asset_class,
        'symbol': symbols,
        'timestamp': pa.array(ts_us, type=pa.timestamp('us')),
        'open': pa.array(open_price, type=pa.float64()),
        'high': pa.array(high_price, type=pa.float64()),
        'low': pa.array(low_price, type=pa.float64()),
        'close': pa.array(close_price, type=pa.float64()),
        'volume': pa.array(volume, type=pa.int64()),
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

setup_data() {
  preflight_check
  local num_rows=$(( SF * 6000000 ))

  echo "=== Setting up OHLCV table: ${TABLE} (sf${SF}, ~${num_rows} rows) ==="

  local mode
  mode=$(detect_cudf_mode)
  if [ "${mode}" = "gpu" ]; then
    echo "ERROR: Setup requires workers with cudf.enabled=false (CPU mode)."
    exit 1
  fi

  ensure_schema

  if [ -d "${HOST_DATA_DIR}" ]; then
    echo "Cleaning old data..."
    rm -rf "${HOST_DATA_DIR}"
  fi
  generate_ohlcv_data "${num_rows}" "${HOST_DATA_DIR}"

  cli --execute "DROP TABLE IF EXISTS ${TABLE}" 2>/dev/null || true

  echo "Creating external Hive table ${TABLE}..."
  cli --execute "
    CREATE TABLE ${TABLE} (
      asset_class VARCHAR,
      symbol VARCHAR,
      timestamp TIMESTAMP,
      open DOUBLE,
      high DOUBLE,
      low DOUBLE,
      close DOUBLE,
      volume BIGINT
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

  echo "Running ANALYZE..."
  cli --execute "ANALYZE ${TABLE}" 2>/dev/null || echo "WARNING: ANALYZE failed."
  echo "Setup complete."
  echo ""
}

# ============================================================================
# Queries matching price_engine.py patterns
# ============================================================================

declare -A QUERIES

# Q1: Raw OHLCV with date range filter (query_prices)
QUERIES["raw_ohlcv_range"]="
  SELECT asset_class, symbol, timestamp, open, high, low, close, volume
  FROM ${TABLE}
  WHERE symbol = 'NVDA'
    AND timestamp >= TIMESTAMP '2024-01-15 00:00:00'
    AND timestamp <= TIMESTAMP '2024-02-15 00:00:00'
"

# Q2: Time-bucketed aggregation by day (aggregate, period=day)
QUERIES["agg_daily"]="
  SELECT
    DATE_TRUNC('day', timestamp) AS bucket,
    MAX(high) AS high,
    MIN(low) AS low,
    SUM(volume) AS volume
  FROM ${TABLE}
  WHERE symbol = 'NVDA'
    AND timestamp >= TIMESTAMP '2024-01-15 00:00:00'
    AND timestamp <= TIMESTAMP '2024-02-15 00:00:00'
  GROUP BY DATE_TRUNC('day', timestamp)
  ORDER BY bucket
"

# Q3: Time-bucketed aggregation by week
QUERIES["agg_weekly"]="
  SELECT
    DATE_TRUNC('week', timestamp) AS bucket,
    MAX(high) AS high,
    MIN(low) AS low,
    SUM(volume) AS volume
  FROM ${TABLE}
  WHERE symbol = 'AAPL'
    AND timestamp >= TIMESTAMP '2024-01-01 00:00:00'
    AND timestamp <= TIMESTAMP '2024-06-30 00:00:00'
  GROUP BY DATE_TRUNC('week', timestamp)
  ORDER BY bucket
"

# Q4: Time-bucketed aggregation by month
QUERIES["agg_monthly"]="
  SELECT
    DATE_TRUNC('month', timestamp) AS bucket,
    MAX(high) AS high,
    MIN(low) AS low,
    SUM(volume) AS volume
  FROM ${TABLE}
  WHERE symbol = 'GOOGL'
    AND timestamp >= TIMESTAMP '2023-01-01 00:00:00'
    AND timestamp <= TIMESTAMP '2025-12-31 00:00:00'
  GROUP BY DATE_TRUNC('month', timestamp)
  ORDER BY bucket
"

# Q5: Daily max-high multi-symbol with IN list (daily_price_pct_change_multi)
QUERIES["daily_high_multi"]="
  SELECT
    symbol,
    DATE_TRUNC('day', timestamp) AS day,
    MAX(high) AS price_high
  FROM ${TABLE}
  WHERE symbol IN ('NVDA', 'AAPL', 'GOOGL', 'MSFT', 'AMZN', 'META', 'TSLA', 'AMD')
    AND timestamp >= TIMESTAMP '2024-01-02 00:00:00'
    AND timestamp <= TIMESTAMP '2024-06-30 00:00:00'
  GROUP BY symbol, DATE_TRUNC('day', timestamp)
  ORDER BY symbol, day
"

# Q6: Distinct symbols (symbols endpoint)
QUERIES["distinct_symbols"]="
  SELECT DISTINCT symbol
  FROM ${TABLE}
  ORDER BY symbol
"

# Q7: Broad date range aggregation (full year, all symbols)
QUERIES["yearly_agg_all"]="
  SELECT
    symbol,
    DATE_TRUNC('month', timestamp) AS month,
    COUNT(*) AS cnt,
    MAX(high) AS max_high,
    MIN(low) AS min_low,
    SUM(volume) AS total_volume
  FROM ${TABLE}
  WHERE timestamp >= TIMESTAMP '2024-01-01 00:00:00'
    AND timestamp < TIMESTAMP '2025-01-01 00:00:00'
  GROUP BY symbol, DATE_TRUNC('month', timestamp)
  ORDER BY symbol, month
"

QUERY_ORDER=(
  raw_ohlcv_range
  agg_daily
  agg_weekly
  agg_monthly
  daily_high_multi
  distinct_symbols
  yearly_agg_all
)

# ============================================================================
# Benchmark and verify (reuses same patterns as timestamp_benchmark.sh)
# ============================================================================

run_benchmark() {
  preflight_check
  echo "=== OHLCV Benchmark (${RUNS} runs per query, table: ${TABLE}) ==="
  echo ""

  local count_result
  echo "Verifying table ${TABLE}..."
  count_result=$(cli --execute "SELECT count(*) FROM ${TABLE}" 2>&1 | tr -d '"[:space:]') || true
  if [ -z "${count_result}" ] || [ "${count_result}" = "0" ] || echo "${count_result}" | grep -qi "failed\|error"; then
    echo "ERROR: Table ${TABLE} is empty or not accessible. Run setup first."
    exit 1
  fi
  echo "Table has ${count_result} rows."
  echo ""

  local mode
  mode=$(detect_cudf_mode)
  echo "Detected mode: ${mode}"

  local ts
  ts=$(date +%Y%m%d_%H%M%S)
  local out_dir="benchmark_results/ohlcv_${mode}_sf${SF}_${ts}"
  mkdir -p "${out_dir}"

  local results_csv="${out_dir}/timings.csv"
  local report_file="${out_dir}/report.txt"
  echo "query,run,elapsed_ms" > "${results_csv}"

  # Save worker configs
  local all_containers
  all_containers=$(docker ps --format '{{.Names}}' || true)
  for c in ${all_containers}; do
    local config_file="${out_dir}/config_${c}.txt"
    echo "=== Container: ${c} ===" > "${config_file}"
    docker logs "${c}" 2>&1 | grep -E "Registered properties|Unregistered properties|^\s+\S+=\S+" >> "${config_file}" 2>/dev/null || true
    echo "" >> "${config_file}"
    docker logs "${c}" 2>&1 | head -100 >> "${config_file}" 2>/dev/null || true
  done

  exec > >(tee -a "${report_file}") 2>&1

  echo "Mode: ${mode}"
  echo "Scale factor: ${SF}"
  echo "Runs per query: ${RUNS}"
  echo "Table: ${TABLE} (${count_result} rows)"
  echo "Output: ${out_dir}/"
  echo ""

  # Save query map
  local query_map="${out_dir}/query_map.txt"
  for qname in "${QUERY_ORDER[@]}"; do
    echo "###QUERY### ${qname}"
    echo "${QUERIES[${qname}]}"
  done > "${query_map}"

  for qname in "${QUERY_ORDER[@]}"; do
    local sql="${QUERIES[${qname}]}"
    echo "--- ${qname} ---"

    echo "  Warming up..."
    cli --execute "${sql}" > /dev/null 2>&1 || true

    for run in $(seq 1 "${RUNS}"); do
      local ms
      ms=$(run_query "Run ${run}/${RUNS}" "${sql}")
      echo "${qname},${run},${ms}" >> "${results_csv}"
    done

    # Save query stats
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

  # Summary
  echo "=== Summary ==="
  printf "%-25s %12s\n" "Query" "Median (ms)"
  printf "%-25s %12s\n" "-------------------------" "------------"
  for qname in "${QUERY_ORDER[@]}"; do
    local median
    median=$(grep "^${qname}," "${results_csv}" | cut -d, -f3 | sort -n | sed -n "$((( RUNS + 1 ) / 2))p")
    if [ -n "${median}" ] && [ "${median}" != "-1" ] 2>/dev/null; then
      printf "%-25s %12s\n" "${qname}" "${median}"
    else
      printf "%-25s %12s\n" "${qname}" "FAILED"
    fi
  done

  echo ""
  echo "=== Full report saved to ${out_dir}/ ==="
}

run_verify() {
  preflight_check
  echo "=== Verification: Presto vs DuckDB (table: ${TABLE}) ==="
  echo ""

  local query_file
  query_file=$(mktemp /tmp/ohlcv_queries.XXXXXX)
  for qname in "${QUERY_ORDER[@]}"; do
    echo "###QUERY### ${qname}"
    echo "${QUERIES[${qname}]}"
  done > "${query_file}"

  python3 - "${query_file}" "${TABLE}" "${HOST_DATA_DIR}" "${COORDINATOR}" "${PORT}" "${SCHEMA}" <<'PYEOF'
import sys
import subprocess
import duckdb
import re

query_file = sys.argv[1]
table_name = sys.argv[2]
data_dir = sys.argv[3]
coordinator = sys.argv[4]
port = sys.argv[5]
schema = sys.argv[6]

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
    v = v.strip().strip('"')
    try:
        f = float(v)
        return f"{f:.6g}"
    except ValueError:
        pass
    v = re.sub(r' 00:00:00(\.000)?$', '', v)
    return v

def normalize_row(row_str):
    parts = row_str.strip().split('","')
    parts = [p.strip('"') for p in parts]
    return tuple(normalize_value(p) for p in parts)

def sort_key(row):
    result = []
    for v in row:
        try:
            result.append((0, float(v), v))
        except ValueError:
            result.append((1, 0, v))
    return result

def run_presto(sql):
    cmd = ["docker", "exec", "-i", coordinator, "presto-cli",
           "--server", f"localhost:{port}", "--catalog", "hive",
           "--schema", schema, "--execute", sql.strip()]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0 or "failed" in result.stdout.lower():
        return None, result.stdout + result.stderr
    rows = []
    for line in result.stdout.strip().split("\n"):
        if line.strip():
            rows.append(normalize_row(line))
    return sorted(rows, key=sort_key), None

def run_duckdb(sql):
    duck_sql = sql.replace(table_name, f"read_parquet('{data_dir}/*.parquet')")
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
