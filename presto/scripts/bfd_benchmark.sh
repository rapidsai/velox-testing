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
#   SYMBOLS              - comma-separated symbols (default: NVDA)
#   SYMBOL_IDS           - comma-separated symbol ids (optional, overrides SYMBOLS)
#   START_DATE           - ISO date string (optional)
#   END_DATE             - ISO date string (optional)
#   BENCHMARK_RUNS       - runs per engine (default: 3)
#   POST_PROCESS         - true|false (default: true)
#   SYMBOL_MAP_PATH      - path to symbol_map.json (optional)
#   MARKET_DATA_DIR      - dataset root (used to find metadata/symbol_map.json)
#   DATA_DIR             - app data dir (used to find metadata/symbol_map.json)
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
pip install -q duckdb pyarrow numpy
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
if [[ "${MODE}" == "setup" || "${MODE}" == "all" ]]; then
  SYMBOLS="${SYMBOLS:-NVDA}"
else
  SYMBOLS="${SYMBOLS:-${2:-NVDA}}"
fi
SYMBOL_IDS="${SYMBOL_IDS:-}"
START_DATE="${START_DATE:-}"
END_DATE="${END_DATE:-}"
POST_PROCESS="${POST_PROCESS:-true}"
BASE_ROWS="${BFD_BASE_ROWS:-5000000}"
HOST_DATA_DIR="${BFD_DATA_DIR:-}"
if [[ -z "${HOST_DATA_DIR}" && -n "${PRESTO_DATA_DIR:-}" ]]; then
  HOST_DATA_DIR="${PRESTO_DATA_DIR}/bfd_bench/sf${SF}"
fi
CONTAINER_DATA_DIR="${BFD_CONTAINER_DATA_DIR:-/var/lib/presto/data/hive/data/user_data/bfd_bench/sf${SF}}"
METADATA_DIR="${HOST_DATA_DIR:+${HOST_DATA_DIR}_meta}"
MARKET_DATA_DIR="${MARKET_DATA_DIR:-${METADATA_DIR:-}}"

cli() {
  local session_arg=()
  if [[ -n "${1:-}" && "${1}" != -* ]]; then
    session_arg=(--session "$1")
    shift
  fi
  docker exec -i "${COORDINATOR}" presto-cli \
    --server "localhost:${PORT}" \
    --catalog "${CATALOG}" \
    --schema "${SCHEMA}" \
    --user "${PRESTO_USER}" \
    "${session_arg[@]}" \
    "$@"
}

resolve_symbol_map_path() {
  if [[ -n "${SYMBOL_MAP_PATH:-}" && -f "${SYMBOL_MAP_PATH}" ]]; then
    echo "${SYMBOL_MAP_PATH}"
    return
  fi

  local candidates=()
  if [[ -n "${MARKET_DATA_DIR:-}" ]]; then
    candidates+=("${MARKET_DATA_DIR}/metadata/symbol_map.json")
    candidates+=("${MARKET_DATA_DIR}/symbol_map.json")
  fi
  if [[ -n "${DATA_DIR:-}" ]]; then
    candidates+=("${DATA_DIR}/metadata/symbol_map.json")
    candidates+=("${DATA_DIR}/symbol_map.json")
  fi

  for path in "${candidates[@]}"; do
    if [[ -f "${path}" ]]; then
      echo "${path}"
      return
    fi
  done
  echo ""
}

resolve_symbol_ids() {
  if [[ -n "${SYMBOL_IDS}" ]]; then
    echo "${SYMBOL_IDS}"
    return
  fi

  local map_path
  map_path="$(resolve_symbol_map_path)"
  if [[ -z "${map_path}" ]]; then
    echo "ERROR: symbol_map.json not found. Set SYMBOL_IDS or SYMBOL_MAP_PATH." >&2
    exit 1
  fi

  python3 - "${SYMBOLS}" "${map_path}" <<'PYEOF'
import json
import sys

symbols = [s.strip() for s in sys.argv[1].split(",") if s.strip()]
path = sys.argv[2]
with open(path) as f:
    data = json.load(f)
mapping = data.get("symbol_to_id", {})
ids = []
missing = []
for sym in symbols:
    if sym in mapping:
        ids.append(str(int(mapping[sym])))
    else:
        missing.append(sym)
if missing:
    sys.stderr.write("WARNING: symbols not found in map: %s\n" % ", ".join(missing))
if not ids:
    sys.stderr.write("ERROR: no valid symbol ids found\n")
    sys.exit(1)
print(",".join(ids))
PYEOF
}

build_time_filters() {
  local filters=""
  if [[ -n "${START_DATE}" ]]; then
    filters+=" AND ts >= TIMESTAMP '${START_DATE}'"
  fi
  if [[ -n "${END_DATE}" ]]; then
    filters+=" AND ts <= TIMESTAMP '${END_DATE}'"
  fi
  echo "${filters}"
}


run_presto_to_csv() {
  local sql="$1"
  local out_csv="$2"
  local start end elapsed

  start=$(date +%s%N)
  if ! cli --output-format CSV_HEADER --execute "${sql}" > "${out_csv}" 2> "${out_csv}.err"; then
    end=$(date +%s%N)
    elapsed=$(( (end - start) / 1000000 ))
    echo "FAILED (${elapsed} ms)" >&2
    cat "${out_csv}.err" >&2 || true
    echo "-1"
    return
  fi
  end=$(date +%s%N)
  elapsed=$(( (end - start) / 1000000 ))
  echo "${elapsed}"
}

run_duckdb_postprocess() {
  local input_csv="$1"
  local start end elapsed
  start=$(date +%s%N)
  python3 - "${input_csv}" <<'PYEOF'
import sys
import duckdb

csv_path = sys.argv[1]
con = duckdb.connect()
con.execute("""
    CREATE TEMP TABLE t AS
    SELECT * FROM read_csv_auto(?, HEADER=true)
""", [csv_path])

con.execute("""
    SELECT
        symbol_id,
        period,
        arg_min(open, timestamp) AS open,
        max(high) AS high,
        min(low) AS low,
        arg_max(close, timestamp) AS close,
        sum(volume) AS volume
    FROM t
    GROUP BY symbol_id, period
    ORDER BY symbol_id, period
""").fetchall()
PYEOF
  end=$(date +%s%N)
  elapsed=$(( (end - start) / 1000000 ))
  echo "${elapsed}"
}

generate_bfd_dataset() {
  local num_rows=$1
  local out_dir=$2
  local metadata_dir=$3

  echo "Generating ${num_rows} BFD rows into ${out_dir} (partitioned by year/month)..."
  python3 - "${num_rows}" "${out_dir}" "${metadata_dir}" <<'PYEOF'
import json
import os
import sys
from datetime import datetime

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

num_rows = int(sys.argv[1])
out_dir = sys.argv[2]
metadata_dir = sys.argv[3]
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

rng = np.random.default_rng(42)

base_us = int(datetime(2020, 1, 1).timestamp() * 1_000_000)
day_offsets = rng.integers(0, 2191, size=num_rows, dtype=np.int64)
minute_offsets = rng.integers(0, 1440, size=num_rows, dtype=np.int64)
ts_us = base_us + day_offsets * 86400_000_000 + minute_offsets * 60_000_000

sym_indices = rng.integers(0, len(sym_list), size=num_rows)
sym_ids = np.array([symbol_to_id[sym_list[i]] for i in sym_indices], dtype=np.int32)
asset_ids = np.array([ASSET_CLASS_TO_ID[asset_list[i]] for i in sym_indices], dtype=np.int8)

base_prices = np.array([REALISTIC_PRICES.get(sym_list[i], 100.0) for i in sym_indices])
jitter = rng.uniform(-0.05, 0.05, size=num_rows)

ts_dt = ts_us.astype("datetime64[us]")
years = (ts_dt.astype("datetime64[Y]").astype(int) + 1970).astype(np.int32)
months = (ts_dt.astype("datetime64[M]").astype(int) % 12 + 1).astype(np.int32)

table = pa.table({
    "ts": pa.array(ts_us, type=pa.timestamp("us")),
    "timestamp": pa.array(ts_us, type=pa.int64()),
    "open": pa.array((base_prices * (1 + jitter)).astype(np.float32), type=pa.float32()),
    "high": pa.array((base_prices * (1 + np.abs(jitter) + rng.uniform(0, 0.03, size=num_rows))).astype(np.float32), type=pa.float32()),
    "low": pa.array((base_prices * (1 - np.abs(jitter) - rng.uniform(0, 0.03, size=num_rows))).astype(np.float32), type=pa.float32()),
    "close": pa.array((base_prices * (1 + rng.uniform(-0.03, 0.03, size=num_rows))).astype(np.float32), type=pa.float32()),
    "volume": pa.array(rng.uniform(100, 50000, size=num_rows), type=pa.float64()),
    "symbol_id": pa.array(sym_ids, type=pa.int32()),
    "asset_class_id": pa.array(asset_ids, type=pa.int8()),
    "year": pa.array(years, type=pa.int32()),
    "month": pa.array(months, type=pa.int32()),
})

print(f"  Writing partitioned dataset to {out_dir}...")
pq.write_to_dataset(
    table,
    root_path=out_dir,
    partition_cols=["year", "month"],
    compression="zstd",
    max_rows_per_file=1_000_000,
    row_group_size=250_000,
    existing_data_behavior="overwrite_or_ignore",
)

parquet_count = sum(1 for _, _, files in os.walk(out_dir) for f in files if f.endswith(".parquet"))
print(f"  Done. {num_rows:,} rows, {parquet_count} parquet file(s) in {out_dir}")

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

run_benchmark() {
  preflight_check

  local ids
  ids="$(resolve_symbol_ids)"
  local filters
  filters="$(build_time_filters)"

  local sql="
    SELECT symbol_id,
           DATE_TRUNC('week', ts) AS period,
           timestamp, open, high, low, close, volume
    FROM ${TABLE}
    WHERE symbol_id IN (${ids})
    ${filters}
    ORDER BY symbol_id, ts
  "

  local mode
  mode=$(detect_cudf_mode)
  local ts out_dir results_csv report_file
  ts=$(date +%Y%m%d_%H%M%S)
  out_dir="benchmark_results/bfd_${mode}_${ts}"
  mkdir -p "${out_dir}"
  results_csv="${out_dir}/timings.csv"
  report_file="${out_dir}/report.txt"

  echo "engine,run,presto_ms,duckdb_ms,total_ms" > "${results_csv}"

  exec > >(tee -a "${report_file}") 2>&1

  echo "=== BFD Presto Benchmark (${RUNS} runs per engine) ==="
  echo "Table: ${TABLE}"
  echo "Symbols: ${SYMBOLS}"
  echo "Symbol IDs: ${ids}"
  echo "Start: ${START_DATE:-<none>}"
  echo "End: ${END_DATE:-<none>}"
  echo "Post-process: ${POST_PROCESS}"
  echo "Output: ${out_dir}/"
  echo ""

  echo "--- ${mode} ---"
  for run in $(seq 1 "${RUNS}"); do
    local tmp_csv presto_ms duckdb_ms total_ms
    tmp_csv="$(mktemp "${out_dir}/${mode}_run${run}.XXXX.csv")"
    presto_ms=$(run_presto_to_csv "${sql}" "${tmp_csv}")
    if [[ "${presto_ms}" == "-1" ]]; then
      echo "${mode},${run},-1,-1,-1" >> "${results_csv}"
      continue
    fi

    duckdb_ms=0
    if [[ "${POST_PROCESS}" == "true" ]]; then
      duckdb_ms=$(run_duckdb_postprocess "${tmp_csv}")
    fi
    total_ms=$(( presto_ms + duckdb_ms ))
    echo "  Run ${run}/${RUNS}: presto=${presto_ms} ms, post=${duckdb_ms} ms, total=${total_ms} ms"
    echo "${mode},${run},${presto_ms},${duckdb_ms},${total_ms}" >> "${results_csv}"
  done
  echo ""

  python3 - "${results_csv}" <<'PYEOF'
import csv
import statistics
import sys

path = sys.argv[1]
rows = {}
with open(path) as f:
    r = csv.DictReader(f)
    for row in r:
        if row["presto_ms"] == "-1":
            continue
        key = row["engine"]
        rows.setdefault(key, {"presto": [], "post": [], "total": []})
        rows[key]["presto"].append(int(row["presto_ms"]))
        rows[key]["post"].append(int(row["duckdb_ms"]))
        rows[key]["total"].append(int(row["total_ms"]))

def med(vals):
    return int(statistics.median(vals)) if vals else -1

print("=== Summary (median ms) ===")
for engine, vals in rows.items():
    print(f"{engine:4s}  presto={med(vals['presto'])}  post={med(vals['post'])}  total={med(vals['total'])}")
PYEOF

  check_fallbacks "${out_dir}"
}

case "${MODE}" in
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
    echo "Usage: $0 [setup|bench|all] [sf|symbol]"
    exit 1
    ;;
esac
