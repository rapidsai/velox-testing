#!/usr/bin/env bash
# bfd_benchmark.sh — BFD app-level Presto benchmark (aggregate path)
#
# Mirrors the FastAPI /api/benchmark flow:
#  1) Presto query (scan/filter/order)
#  2) Post-process aggregation (DuckDB replacement for Polars)
#
# Usage:
#   ./bfd_benchmark.sh [setup|bench|all] [sf|symbol]
#
# Environment:
#   PRESTO_COORDINATOR   - coordinator container name (default: presto-coordinator)
#   PRESTO_PORT          - coordinator port (default: 8080)
#   BFD_CATALOG          - catalog (default: hive)
#   PRESTO_SCHEMA        - schema (default: default)
#   PRESTO_USER          - Presto user (default: bfd)
#   PRESTO_DATA_DIR      - host data root (required for setup unless BFD_DATA_DIR set)
#   BENCHMARK_RUNS       - runs per query (default: 3)
#   BFD_SF                  - scale factor for setup (default: 1)
#   BFD_DATA_DIR            - host output root for setup (optional)
#   BFD_CONTAINER_DATA_DIR  - container path for Hive table (optional)
#   BFD_BASE_ROWS           - base row count per SF (default: 5000000)
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../scripts/py_env_functions.sh"
source "${SCRIPT_DIR}/benchmark_common.sh"

init_python_virtual_env ".bfd_bench_venv"
pip install -q pyarrow numpy
trap 'LOCAL_CONDA_INIT="${LOCAL_CONDA_INIT:-}"; delete_python_virtual_env .bfd_bench_venv' EXIT

COORDINATOR="${PRESTO_COORDINATOR:-presto-coordinator}"
PORT="${PRESTO_PORT:-8080}"
CATALOG="${BFD_CATALOG:-hive}"
SCHEMA="${PRESTO_SCHEMA:-default}"
PRESTO_USER="${PRESTO_USER:-bfd}"
MODE="${1:-bench}"
SF="${BFD_SF:-1}"
if [[ "${MODE}" == "setup" || "${MODE}" == "all" ]]; then
  SF="${2:-${BFD_SF:-1}}"
fi

TABLE="${CATALOG}.${SCHEMA}.prices"
RUNS="${BENCHMARK_RUNS:-3}"
BASE_ROWS="${BFD_BASE_ROWS:-5000000}"
HOST_DATA_DIR="${BFD_DATA_DIR:-}"
if [[ -z "${HOST_DATA_DIR}" && -n "${PRESTO_DATA_DIR:-}" ]]; then
  HOST_DATA_DIR="${PRESTO_DATA_DIR}/bfd_bench/sf${SF}"
fi
CONTAINER_DATA_DIR="${BFD_CONTAINER_DATA_DIR:-/var/lib/presto/data/hive/data/user_data/bfd_bench/sf${SF}}"
METADATA_DIR="${HOST_DATA_DIR:+${HOST_DATA_DIR}_meta}"
MARKET_DATA_DIR="${MARKET_DATA_DIR:-${METADATA_DIR:-}}"

generate_bfd_dataset() {
  local num_rows=$1
  local out_dir=$2
  local metadata_dir=$3
  local num_workers=${GENERATE_WORKERS:-$(nproc)}

  echo "Generating ${num_rows} BFD rows into ${out_dir} (${num_workers} workers, partitioned by year/month)..."
  python3 - "${num_rows}" "${out_dir}" "${metadata_dir}" "${num_workers}" <<'PYEOF'
import json
import os
import sys
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

num_rows = int(sys.argv[1])
out_dir = sys.argv[2]
metadata_dir = sys.argv[3]
num_workers = int(sys.argv[4])
os.makedirs(out_dir, exist_ok=True)
os.makedirs(metadata_dir, exist_ok=True)

SYMBOLS = {
    "stock": [
        "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA",
        "JPM", "V", "UNH", "XOM", "JNJ", "WMT", "PG", "MA", "HD", "CVX",
        "MRK", "ABBV", "PEP", "KO", "COST", "AVGO", "LLY", "TMO", "MCD",
        "CSCO", "ACN", "ABT", "AMD", "INTC", "QCOM", "GS", "CAT",
    ],
    "fx": [
        "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD",
        "NZDUSD", "EURGBP", "EURJPY", "GBPJPY",
    ],
    "crypto": [
        "BTC", "ETH", "SOL", "XRP", "ADA", "DOGE", "AVAX", "DOT",
    ],
    "futures": [
        "ES", "NQ", "YM", "RTY", "CL", "GC", "SI", "HG",
    ],
}

REALISTIC_PRICES = {
    "AAPL": 243.0, "MSFT": 418.0, "NVDA": 138.0, "GOOGL": 192.0,
    "AMZN": 220.0, "META": 602.0, "TSLA": 380.0,
    "JPM": 243.0, "V": 316.0, "UNH": 540.0, "XOM": 106.0,
    "JNJ": 145.0, "WMT": 92.0, "PG": 168.0, "MA": 523.0, "HD": 397.0, "CVX": 148.0,
    "MRK": 100.0, "ABBV": 182.0, "PEP": 151.0, "KO": 62.0, "COST": 920.0, "AVGO": 238.0,
    "LLY": 770.0, "TMO": 530.0, "MCD": 290.0, "CSCO": 59.0, "ACN": 356.0, "ABT": 114.0,
    "AMD": 120.0, "INTC": 20.0, "QCOM": 158.0, "GS": 580.0, "CAT": 376.0,
    "EURUSD": 1.035, "GBPUSD": 1.252, "USDJPY": 157.3,
    "USDCHF": 0.908, "AUDUSD": 0.622, "USDCAD": 1.440,
    "NZDUSD": 0.565, "EURGBP": 0.828, "EURJPY": 162.8, "GBPJPY": 196.7,
    "BTC": 94200.0, "ETH": 3350.0, "SOL": 190.0, "XRP": 2.35,
    "ADA": 0.98, "DOGE": 0.33, "AVAX": 37.0, "DOT": 7.0,
    "ES": 5950.0, "NQ": 21300.0, "YM": 42800.0, "RTY": 2050.0,
    "CL": 73.5, "GC": 2660.0, "SI": 30.5, "HG": 4.2,
}

ASSET_CLASS_TO_ID = {"stock": 0, "fx": 1, "crypto": 2, "futures": 3}

all_symbols = []
for asset, syms in SYMBOLS.items():
    for sym in syms:
        all_symbols.append((asset, sym))

symbol_to_id = {sym: idx for idx, (_, sym) in enumerate(all_symbols)}
sym_list = [sym for _, sym in all_symbols]
asset_list = [asset for asset, _ in all_symbols]
num_symbols = len(sym_list)

sym_id_arr = np.array([symbol_to_id[s] for s in sym_list], dtype=np.int32)
asset_id_arr = np.array([ASSET_CLASS_TO_ID[a] for a in asset_list], dtype=np.int8)
price_arr = np.array([REALISTIC_PRICES.get(s, 100.0) for s in sym_list], dtype=np.float64)

chunk_size = min(num_rows, 5_000_000)
chunks = []
rows_remaining = num_rows
file_idx = 0
while rows_remaining > 0:
    n = min(chunk_size, rows_remaining)
    chunks.append((file_idx, n))
    rows_remaining -= n
    file_idx += 1
total_chunks = len(chunks)

def generate_chunk(args):
    idx, n = args
    rng = np.random.default_rng(42 + idx)

    base_us = int(datetime(2020, 1, 1).timestamp() * 1_000_000)
    day_offsets = rng.integers(0, 2191, size=n, dtype=np.int64)
    minute_offsets = rng.integers(0, 1440, size=n, dtype=np.int64)
    ts_us = base_us + day_offsets * 86400_000_000 + minute_offsets * 60_000_000

    si = rng.integers(0, num_symbols, size=n)
    sym_ids = sym_id_arr[si]
    asset_ids = asset_id_arr[si]
    base_prices = price_arr[si]

    jitter = rng.uniform(-0.05, 0.05, size=n)

    ts_dt = ts_us.astype("datetime64[us]")
    years = (ts_dt.astype("datetime64[Y]").astype(int) + 1970).astype(np.int32)
    months = (ts_dt.astype("datetime64[M]").astype(int) % 12 + 1).astype(np.int32)

    table = pa.table({
        "ts": pa.array(ts_us, type=pa.timestamp("us")),
        "timestamp": pa.array(ts_us, type=pa.int64()),
        "open": pa.array((base_prices * (1 + jitter)).astype(np.float32), type=pa.float32()),
        "high": pa.array((base_prices * (1 + np.abs(jitter) + rng.uniform(0, 0.03, size=n))).astype(np.float32), type=pa.float32()),
        "low": pa.array((base_prices * (1 - np.abs(jitter) - rng.uniform(0, 0.03, size=n))).astype(np.float32), type=pa.float32()),
        "close": pa.array((base_prices * (1 + rng.uniform(-0.03, 0.03, size=n))).astype(np.float32), type=pa.float32()),
        "volume": pa.array(rng.uniform(100, 50000, size=n), type=pa.float64()),
        "symbol_id": pa.array(sym_ids, type=pa.int32()),
        "asset_class_id": pa.array(asset_ids, type=pa.int8()),
        "year": years,
        "month": months,
    })

    pq.write_to_dataset(
        table,
        root_path=out_dir,
        partition_cols=["year", "month"],
        compression="zstd",
        row_group_size=250_000,
        basename_template=f"part-{idx:05d}-{{i}}.parquet",
        existing_data_behavior="overwrite_or_ignore",
    )
    return idx, n

completed = 0
with ProcessPoolExecutor(max_workers=min(num_workers, total_chunks)) as pool:
    futures = {pool.submit(generate_chunk, c): c for c in chunks}
    for future in as_completed(futures):
        idx, n = future.result()
        completed += 1
        pct = completed * 100 // total_chunks
        bar = '=' * (pct // 2) + '>' + ' ' * (50 - pct // 2)
        print(f'\r  [{bar}] {pct}% ({completed}/{total_chunks} chunks, {n:,} rows each)', end='', flush=True)

parquet_count = sum(1 for _, _, files in os.walk(out_dir) for f in files if f.endswith(".parquet"))
print(f"\n  Done. {num_rows:,} rows, {parquet_count} parquet file(s) in {out_dir}")

id_to_symbol = {str(v): k for k, v in symbol_to_id.items()}
with open(os.path.join(metadata_dir, "symbol_map.json"), "w") as f:
    json.dump({"symbol_to_id": symbol_to_id, "id_to_symbol": id_to_symbol}, f, indent=2)
PYEOF
}

setup_data() {
  preflight_check
  local num_rows=$(( BASE_ROWS * SF ))
  echo "=== Setting up BFD table: ${TABLE} (sf${SF}, ~${num_rows} rows) ==="

  local mode
  mode=$(detect_cudf_mode)
  if [ "${mode}" = "gpu" ]; then
    echo "ERROR: Setup requires workers with cudf.enabled=false (CPU mode)."
    exit 1
  fi

  if [[ -z "${HOST_DATA_DIR}" ]]; then
    echo "ERROR: PRESTO_DATA_DIR or BFD_DATA_DIR must be set for setup."
    exit 1
  fi

  ensure_schema

  if [ -d "${HOST_DATA_DIR}" ]; then
    echo "Cleaning old data..."; rm -rf "${HOST_DATA_DIR}"
  fi
  generate_bfd_dataset "${num_rows}" "${HOST_DATA_DIR}" "${HOST_DATA_DIR}_meta"

  cli --execute "DROP TABLE IF EXISTS ${TABLE}" 2>/dev/null || true
  echo "Creating partitioned Hive table ${TABLE}..."
  cli --execute "
    CREATE TABLE ${TABLE} (
      ts TIMESTAMP, timestamp BIGINT,
      open REAL, high REAL, low REAL, close REAL,
      volume DOUBLE, symbol_id INTEGER, asset_class_id TINYINT,
      year INTEGER, month INTEGER
    ) WITH (
      format = 'PARQUET',
      external_location = 'file:${CONTAINER_DATA_DIR}',
      partitioned_by = ARRAY['year', 'month']
    )
  "

  echo "Syncing partition metadata..."
  cli --execute "CALL system.sync_partition_metadata('${SCHEMA}', 'prices', 'FULL')"

  echo "Running ANALYZE..."
  cli --execute "ANALYZE ${TABLE}"
  echo "Setup complete."
}

# ============================================================================
# Queries matching PERFORMANCE_TUNING.md query shapes
# ============================================================================

declare -A QUERIES

# Shape 1: Raw OHLCV scan (GET /api/prices)
QUERIES["raw_ohlcv_scan"]="
  SELECT timestamp, open, high, low, close, volume, symbol_id
  FROM ${TABLE}
  WHERE symbol_id IN (2)
    AND ts >= TIMESTAMP '2024-01-01 00:00:00'
    AND ts < TIMESTAMP '2024-02-01 00:00:00'
  ORDER BY symbol_id, ts
  LIMIT 1000
"

# Shape 1 variant: multi-symbol scan
QUERIES["raw_ohlcv_multi"]="
  SELECT timestamp, open, high, low, close, volume, symbol_id
  FROM ${TABLE}
  WHERE symbol_id IN (0, 1, 2, 3, 4, 5, 6)
    AND ts >= TIMESTAMP '2024-01-01 00:00:00'
    AND ts < TIMESTAMP '2024-04-01 00:00:00'
  ORDER BY symbol_id, ts
  LIMIT 5000
"

# Shape 2: Period aggregation - daily (GET /api/aggregate)
QUERIES["agg_daily"]="
  SELECT symbol_id,
         DATE_TRUNC('day', ts) AS period,
         timestamp, open, high, low, close, volume
  FROM ${TABLE}
  WHERE symbol_id IN (2)
    AND ts >= TIMESTAMP '2024-01-01 00:00:00'
    AND ts < TIMESTAMP '2024-02-01 00:00:00'
  ORDER BY symbol_id, ts
  LIMIT 5000
"

# Shape 2 variant: weekly aggregation
QUERIES["agg_weekly"]="
  SELECT symbol_id,
         DATE_TRUNC('week', ts) AS period,
         timestamp, open, high, low, close, volume
  FROM ${TABLE}
  WHERE symbol_id IN (2)
    AND ts >= TIMESTAMP '2024-01-01 00:00:00'
    AND ts < TIMESTAMP '2024-07-01 00:00:00'
  ORDER BY symbol_id, ts
  LIMIT 5000
"

# Shape 2 variant: monthly aggregation, multi-symbol
QUERIES["agg_monthly_multi"]="
  SELECT symbol_id,
         DATE_TRUNC('month', ts) AS period,
         timestamp, open, high, low, close, volume
  FROM ${TABLE}
  WHERE symbol_id IN (0, 1, 2, 3, 4, 5, 6)
    AND ts >= TIMESTAMP '2023-01-01 00:00:00'
    AND ts < TIMESTAMP '2025-01-01 00:00:00'
  ORDER BY symbol_id, ts
  LIMIT 10000
"

# Shape 3: Daily max-high (GET /api/correlation_2)
QUERIES["daily_max_high"]="
  SELECT symbol_id,
         DATE_TRUNC('day', ts) AS day,
         MAX(high) AS price_high
  FROM ${TABLE}
  WHERE symbol_id IN (2)
    AND ts >= TIMESTAMP '2024-01-01 00:00:00'
    AND ts < TIMESTAMP '2024-07-01 00:00:00'
  GROUP BY symbol_id, DATE_TRUNC('day', ts)
  ORDER BY symbol_id, DATE_TRUNC('day', ts)
  LIMIT 1000
"

# Shape 3 variant: multi-symbol daily rollup
QUERIES["daily_max_high_multi"]="
  SELECT symbol_id,
         DATE_TRUNC('day', ts) AS day,
         MAX(high) AS price_high
  FROM ${TABLE}
  WHERE symbol_id IN (0, 1, 2, 3, 4, 5, 6)
    AND ts >= TIMESTAMP '2024-01-01 00:00:00'
    AND ts < TIMESTAMP '2024-07-01 00:00:00'
  GROUP BY symbol_id, DATE_TRUNC('day', ts)
  ORDER BY symbol_id, DATE_TRUNC('day', ts)
  LIMIT 5000
"

# Full table monthly rollup
QUERIES["monthly_rollup_all"]="
  SELECT symbol_id,
         DATE_TRUNC('month', ts) AS month,
         COUNT(*) AS cnt, MAX(high) AS max_high, MIN(low) AS min_low,
         SUM(volume) AS total_volume
  FROM ${TABLE}
  WHERE ts >= TIMESTAMP '2024-01-01 00:00:00'
    AND ts < TIMESTAMP '2025-01-01 00:00:00'
  GROUP BY symbol_id, DATE_TRUNC('month', ts)
  ORDER BY symbol_id, month
  LIMIT 5000
"

QUERY_ORDER=(
  raw_ohlcv_scan
  raw_ohlcv_multi
  agg_daily
  agg_weekly
  agg_monthly_multi
  daily_max_high
  daily_max_high_multi
  monthly_rollup_all
)

case "${MODE}" in
  setup)
    setup_data
    ;;
  bench)
    run_standard_benchmark "bfd"
    ;;
  all)
    setup_data
    run_standard_benchmark "bfd"
    ;;
  *)
    echo "Usage: $0 [setup|bench|all] [sf|symbol]"
    exit 1
    ;;
esac
