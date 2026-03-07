#!/usr/bin/env bash
# ohlcv_benchmark.sh — OHLCV price engine query benchmark
#
# Tests query patterns required by the price_engine.py backend.
#
# Usage:
#   ./ohlcv_benchmark.sh [setup|bench|verify|all] [sf]
#
# Prerequisites:
#   - Presto coordinator running with hive catalog
#   - PRESTO_DATA_DIR environment variable set

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../scripts/py_env_functions.sh"

init_python_virtual_env ".ohlcv_bench_venv"
pip install -q pyarrow numpy duckdb
trap 'LOCAL_CONDA_INIT="${LOCAL_CONDA_INIT:-}"; delete_python_virtual_env .ohlcv_bench_venv' EXIT

COORDINATOR="${PRESTO_COORDINATOR:-presto-coordinator}"
PORT="${PRESTO_PORT:-8080}"
SF="${2:-10}"
SCHEMA="${HIVE_SCHEMA:-default}"
TABLE="hive.${SCHEMA}.ohlcv_prices_sf${SF}"
RUNS="${BENCHMARK_RUNS:-3}"
HOST_DATA_DIR="${PRESTO_DATA_DIR:?PRESTO_DATA_DIR must be set}/ohlcv/sf${SF}"
CONTAINER_DATA_DIR="/var/lib/presto/data/hive/data/user_data/ohlcv/sf${SF}"

source "${SCRIPT_DIR}/benchmark_common.sh"

# ============================================================================
# Data generation
# ============================================================================

generate_ohlcv_data() {
  local num_rows=$1
  local out_dir=$2
  local num_workers=${GENERATE_WORKERS:-$(nproc)}

  echo "Generating ${num_rows} OHLCV rows (${num_workers} threads)..."
  python3 - "${num_rows}" "${out_dir}" "${num_workers}" <<'PYEOF'
import sys, os
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

num_rows = int(sys.argv[1])
out_dir = sys.argv[2]
num_workers = int(sys.argv[3])
os.makedirs(out_dir, exist_ok=True)

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
    rng = np.random.default_rng(42 + idx)
    base_us = int(datetime(2020, 1, 1).timestamp() * 1_000_000)
    day_offsets = rng.integers(0, 2191, size=n, dtype=np.int64)
    minute_offsets = rng.integers(0, 1440, size=n, dtype=np.int64)
    ts_us = base_us + day_offsets * 86400_000_000 + minute_offsets * 60_000_000
    symbols = pa.array(rng.choice(SYMBOLS, size=n))
    asset_class = pa.array(rng.choice(ASSET_CLASSES, size=n))
    base_price = rng.uniform(10.0, 500.0, size=n).astype(np.float64)
    jitter = rng.uniform(-0.05, 0.05, size=n)
    table = pa.table({
        'asset_class': asset_class,
        'symbol': symbols,
        'timestamp': pa.array(ts_us, type=pa.timestamp('us')),
        'open': pa.array(base_price * (1 + jitter), type=pa.float64()),
        'high': pa.array(base_price * (1 + np.abs(jitter) + rng.uniform(0, 0.03, size=n)), type=pa.float64()),
        'low': pa.array(base_price * (1 - np.abs(jitter) - rng.uniform(0, 0.03, size=n)), type=pa.float64()),
        'close': pa.array(base_price * (1 + rng.uniform(-0.03, 0.03, size=n)), type=pa.float64()),
        'volume': pa.array(rng.integers(100, 10_000_000, size=n, dtype=np.int64), type=pa.int64()),
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
    echo "Cleaning old data..."; rm -rf "${HOST_DATA_DIR}"
  fi
  generate_ohlcv_data "${num_rows}" "${HOST_DATA_DIR}"

  cli --execute "DROP TABLE IF EXISTS ${TABLE}" 2>/dev/null || true
  echo "Creating external Hive table ${TABLE}..."
  cli --execute "
    CREATE TABLE ${TABLE} (
      asset_class VARCHAR, symbol VARCHAR, timestamp TIMESTAMP,
      open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume BIGINT
    ) WITH (format = 'PARQUET', external_location = 'file:${CONTAINER_DATA_DIR}')
  "

  echo "Running ANALYZE..."
  cli --execute "ANALYZE ${TABLE}" 2>/dev/null || echo "WARNING: ANALYZE failed."
  echo "Setup complete."
}

# ============================================================================
# Queries matching price_engine.py patterns
# ============================================================================

declare -A QUERIES

QUERIES["raw_ohlcv_range"]="
  SELECT asset_class, symbol, timestamp, open, high, low, close, volume
  FROM ${TABLE}
  WHERE symbol = 'NVDA'
    AND timestamp >= TIMESTAMP '2024-01-15 00:00:00'
    AND timestamp <= TIMESTAMP '2024-02-15 00:00:00'
  LIMIT 1000
"

QUERIES["agg_daily"]="
  SELECT DATE_TRUNC('day', timestamp) AS bucket,
    MAX(high) AS high, MIN(low) AS low, SUM(volume) AS volume
  FROM ${TABLE}
  WHERE symbol = 'NVDA'
    AND timestamp >= TIMESTAMP '2024-01-15 00:00:00'
    AND timestamp <= TIMESTAMP '2024-02-15 00:00:00'
  GROUP BY DATE_TRUNC('day', timestamp) ORDER BY bucket
"

QUERIES["agg_weekly"]="
  SELECT DATE_TRUNC('week', timestamp) AS bucket,
    MAX(high) AS high, MIN(low) AS low, SUM(volume) AS volume
  FROM ${TABLE}
  WHERE symbol = 'AAPL'
    AND timestamp >= TIMESTAMP '2024-01-01 00:00:00'
    AND timestamp <= TIMESTAMP '2024-06-30 00:00:00'
  GROUP BY DATE_TRUNC('week', timestamp) ORDER BY bucket
"

QUERIES["agg_monthly"]="
  SELECT DATE_TRUNC('month', timestamp) AS bucket,
    MAX(high) AS high, MIN(low) AS low, SUM(volume) AS volume
  FROM ${TABLE}
  WHERE symbol = 'GOOGL'
    AND timestamp >= TIMESTAMP '2023-01-01 00:00:00'
    AND timestamp <= TIMESTAMP '2025-12-31 00:00:00'
  GROUP BY DATE_TRUNC('month', timestamp) ORDER BY bucket
"

QUERIES["daily_high_multi"]="
  SELECT symbol, DATE_TRUNC('day', timestamp) AS day, MAX(high) AS price_high
  FROM ${TABLE}
  WHERE symbol IN ('NVDA', 'AAPL', 'GOOGL', 'MSFT', 'AMZN', 'META', 'TSLA', 'AMD')
    AND timestamp >= TIMESTAMP '2024-01-02 00:00:00'
    AND timestamp <= TIMESTAMP '2024-06-30 00:00:00'
  GROUP BY symbol, DATE_TRUNC('day', timestamp) ORDER BY symbol, day
"

QUERIES["distinct_symbols"]="
  SELECT DISTINCT symbol FROM ${TABLE} ORDER BY symbol
"

QUERIES["yearly_agg_all"]="
  SELECT symbol, DATE_TRUNC('month', timestamp) AS month,
    COUNT(*) AS cnt, MAX(high) AS max_high, MIN(low) AS min_low, SUM(volume) AS total_volume
  FROM ${TABLE}
  WHERE timestamp >= TIMESTAMP '2024-01-01 00:00:00'
    AND timestamp < TIMESTAMP '2025-01-01 00:00:00'
  GROUP BY symbol, DATE_TRUNC('month', timestamp) ORDER BY symbol, month
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
# Commands
# ============================================================================

case "${1:-all}" in
  setup)
    setup_data
    ;;
  bench)
    run_standard_benchmark "ohlcv"
    ;;
  verify)
    run_standard_verify "${TABLE}" "${HOST_DATA_DIR}"
    ;;
  all)
    setup_data
    run_standard_benchmark "ohlcv"
    ;;
  *)
    echo "Usage: $0 [setup|bench|verify|all] [scale_factor]"
    exit 1
    ;;
esac
