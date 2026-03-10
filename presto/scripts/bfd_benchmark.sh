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
#   BENCH_ENGINE         - gpu|cpu|both (default: both)
#   POST_PROCESS         - true|false (default: true)
#   SYMBOL_MAP_PATH      - path to symbol_map.json (optional)
#   MARKET_DATA_DIR      - dataset root (used to find metadata/symbol_map.json)
#   DATA_DIR             - app data dir (used to find metadata/symbol_map.json)
#   BFD_SF                  - scale factor for setup (default: 1)
#   BFD_DATA_DIR            - host output root for setup (optional)
#   BFD_CONTAINER_DATA_DIR  - container path for Hive table (optional)
#   BFD_BASE_ROWS           - base row count per SF (default: 5000000)
#   BFD_ASSET_CLASSES       - comma list (default: stock,fx,crypto,futures)
#   BFD_SYMBOLS_PER_CLASS   - limit symbols per class (0=all, default: 0)
#   BFD_START_DATE          - start timestamp (default: 2025-01-02 09:30:00)
#   BFD_ROW_GROUP_SIZE      - parquet row group size (default: 250000)
#   BFD_MAX_ROWS_PER_FILE   - parquet rows per file (default: 1000000)
#   BFD_COMPRESSION         - parquet compression (default: zstd)
#   BFD_SEED                - RNG seed (default: 42)
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
BENCH_ENGINE="${BENCH_ENGINE:-both}"
POST_PROCESS="${POST_PROCESS:-true}"
BASE_ROWS="${BFD_BASE_ROWS:-5000000}"
HOST_DATA_DIR="${BFD_DATA_DIR:-}"
if [[ -z "${HOST_DATA_DIR}" && -n "${PRESTO_DATA_DIR:-}" ]]; then
  HOST_DATA_DIR="${PRESTO_DATA_DIR}/bfd_bench/sf${SF}"
fi
CONTAINER_DATA_DIR="${BFD_CONTAINER_DATA_DIR:-/var/lib/presto/data/hive/data/user_data/bfd_bench/sf${SF}}"
TABLE_LOCATION="${CONTAINER_DATA_DIR}/prices"
MARKET_DATA_DIR="${MARKET_DATA_DIR:-${HOST_DATA_DIR:-}}"

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
  local session_prop="$3"
  local start end elapsed

  start=$(date +%s%N)
  if ! cli "${session_prop}" --output-format CSV_HEADER --execute "${sql}" > "${out_csv}" 2> "${out_csv}.err"; then
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
  local total_rows=$1
  local out_dir=$2
  local asset_classes="${BFD_ASSET_CLASSES:-stock,fx,crypto,futures}"
  local symbols_per_class="${BFD_SYMBOLS_PER_CLASS:-0}"
  local start_date="${BFD_START_DATE:-2025-01-02 09:30:00}"
  local row_group_size="${BFD_ROW_GROUP_SIZE:-250000}"
  local max_rows_per_file="${BFD_MAX_ROWS_PER_FILE:-1000000}"
  local compression="${BFD_COMPRESSION:-zstd}"
  local seed="${BFD_SEED:-42}"

  echo "Generating ${total_rows} rows into ${out_dir}..."
  python3 - "${total_rows}" "${out_dir}" "${asset_classes}" "${symbols_per_class}" \
    "${start_date}" "${row_group_size}" "${max_rows_per_file}" "${compression}" "${seed}" <<'PYEOF'
import json
import os
import sys
import zlib
from datetime import datetime, timezone

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

total_rows = int(sys.argv[1])
out_dir = sys.argv[2]
asset_classes = [a.strip() for a in sys.argv[3].split(",") if a.strip()]
symbols_per_class = int(sys.argv[4])
start_date = sys.argv[5]
row_group_size = int(sys.argv[6])
max_rows_per_file = int(sys.argv[7])
compression = sys.argv[8]
seed = int(sys.argv[9])

ASSET_CLASSES = {
    "stock": [
        "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "BRK.B",
        "JPM", "V", "UNH", "XOM", "JNJ", "WMT", "PG", "MA", "HD", "CVX",
        "MRK", "ABBV", "PEP", "KO", "COST", "AVGO", "LLY", "TMO", "MCD",
        "CSCO", "ACN", "ABT", "DHR", "NEE", "TXN", "PM", "UPS", "MS",
        "BMY", "RTX", "HON", "QCOM", "LOW", "UNP", "INTC", "SBUX", "GS",
        "AMAT", "AMD", "CAT", "BLK", "ADP",
    ],
    "fx": [
        "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD",
        "NZDUSD", "EURGBP", "EURJPY", "GBPJPY", "AUDJPY", "CADJPY",
        "CHFJPY", "EURAUD", "EURCHF", "EURCAD", "EURNZD", "GBPAUD",
        "GBPCAD", "GBPCHF",
    ],
    "crypto": [
        "BTC", "ETH", "SOL", "XRP", "ADA", "DOGE", "AVAX", "DOT",
        "MATIC", "LINK", "UNI", "ATOM", "LTC", "BCH", "NEAR",
    ],
    "futures": [
        "ES", "NQ", "YM", "RTY", "CL", "GC", "SI", "HG",
        "ZB", "ZN", "ZC", "ZS", "ZW", "NG", "KC",
    ],
}

PRICE_RANGES = {
    "stock": (5.0, 500.0),
    "fx": (0.5, 2.0),
    "crypto": (0.01, 60000.0),
    "futures": (10.0, 5000.0),
}

REALISTIC_PRICES = {
    "AAPL": 243.0, "MSFT": 418.0, "NVDA": 138.0, "GOOGL": 192.0,
    "AMZN": 220.0, "META": 602.0, "TSLA": 380.0, "BRK.B": 457.0,
    "JPM": 243.0, "V": 316.0, "UNH": 540.0, "XOM": 106.0,
    "JNJ": 145.0, "WMT": 92.0, "PG": 168.0, "MA": 523.0,
    "HD": 397.0, "CVX": 148.0, "MRK": 100.0, "ABBV": 182.0,
    "PEP": 151.0, "KO": 62.0, "COST": 920.0, "AVGO": 238.0,
    "LLY": 770.0, "TMO": 530.0, "MCD": 290.0, "CSCO": 59.0,
    "ACN": 356.0, "ABT": 114.0, "AMD": 120.0, "INTC": 20.0,
    "QCOM": 158.0, "GS": 580.0, "CAT": 376.0, "BLK": 1010.0,
    "EURUSD": 1.035, "GBPUSD": 1.252, "USDJPY": 157.3,
    "USDCHF": 0.908, "AUDUSD": 0.622, "USDCAD": 1.440,
    "BTC": 94200.0, "ETH": 3350.0, "SOL": 190.0, "XRP": 2.35,
    "ADA": 0.98, "DOGE": 0.33, "AVAX": 37.0, "DOT": 7.0,
    "LINK": 21.0, "LTC": 105.0,
    "ES": 5950.0, "NQ": 21300.0, "YM": 42800.0, "CL": 73.5,
    "GC": 2660.0, "SI": 30.5, "NG": 3.35, "ZB": 115.0,
}

VOLUME_RANGES = {
    "stock": (100, 50000),
    "fx": (1, 500),
    "crypto": (0.001, 100.0),
    "futures": (10, 10000),
}

asset_classes = [a for a in asset_classes if a in ASSET_CLASSES]
if not asset_classes:
    raise SystemExit("No valid asset classes selected.")

symbol_lists = {}
for asset in asset_classes:
    syms = ASSET_CLASSES[asset]
    if symbols_per_class > 0:
        syms = syms[:symbols_per_class]
    symbol_lists[asset] = syms

all_symbols = [(asset, sym) for asset, syms in symbol_lists.items() for sym in syms]
if not all_symbols:
    raise SystemExit("No symbols selected.")

asset_class_to_id = {"stock": 0, "fx": 1, "crypto": 2, "futures": 3, "index": 4}
symbol_to_id = {sym: idx for idx, (_, sym) in enumerate(all_symbols)}
id_to_symbol = {str(idx): sym for sym, idx in symbol_to_id.items()}

os.makedirs(out_dir, exist_ok=True)
metadata_dir = os.path.join(out_dir, "metadata")
os.makedirs(metadata_dir, exist_ok=True)

with open(os.path.join(metadata_dir, "symbol_map.json"), "w") as f:
    json.dump({"symbol_to_id": symbol_to_id, "id_to_symbol": id_to_symbol}, f, indent=2)

with open(os.path.join(metadata_dir, "asset_map.json"), "w") as f:
    json.dump(
        {
            "asset_class_to_id": asset_class_to_id,
            "id_to_asset_class": ["stock", "fx", "crypto", "futures", "index"],
        },
        f,
        indent=2,
    )

base_ts = datetime.fromisoformat(start_date)
if base_ts.tzinfo is None:
    base_ts = base_ts.replace(tzinfo=timezone.utc)
else:
    base_ts = base_ts.astimezone(timezone.utc)
base_us = int(base_ts.timestamp() * 1_000_000)

rows_per_symbol = max(1, total_rows // len(all_symbols))
rows_written = {asset: 0 for asset in asset_classes}

file_index = {asset: 0 for asset in asset_classes}

for asset, symbol in all_symbols:
    stable_hash = zlib.crc32(symbol.encode()) % 10000
    rng = np.random.default_rng(seed + stable_hash)
    base_price = REALISTIC_PRICES.get(symbol)
    if base_price is None:
        lo, hi = PRICE_RANGES[asset]
        base_price = rng.uniform(lo, hi)

    returns = rng.normal(0, 0.0005, rows_per_symbol)
    prices = base_price * np.cumprod(1 + returns)
    noise = rng.uniform(0.998, 1.002, (rows_per_symbol, 3))
    opens = prices * noise[:, 0]
    highs = np.maximum(prices, opens) * rng.uniform(1.0, 1.003, rows_per_symbol)
    lows = np.minimum(prices, opens) * rng.uniform(0.997, 1.0, rows_per_symbol)
    closes = prices * rng.uniform(0.997, 1.003, rows_per_symbol)

    vol_lo, vol_hi = VOLUME_RANGES[asset]
    if asset == "crypto":
        volumes = rng.uniform(vol_lo, vol_hi, rows_per_symbol).astype(np.float64)
    else:
        volumes = rng.integers(int(vol_lo), int(vol_hi), rows_per_symbol).astype(np.float64)

    offsets = np.arange(rows_per_symbol, dtype=np.int64) * 60_000_000
    ts_us = base_us + offsets
    ts_dt = ts_us.astype("datetime64[us]")
    years = ts_dt.astype("datetime64[Y]").astype(int) + 1970
    months = ts_dt.astype("datetime64[M]").astype(int) % 12 + 1

    table = pa.table(
        {
            "timestamp": pa.array(ts_us, type=pa.int64()),
            "ts": pa.array(ts_us, type=pa.timestamp("us")),
            "open": pa.array(opens, type=pa.float32()),
            "high": pa.array(highs, type=pa.float32()),
            "low": pa.array(lows, type=pa.float32()),
            "close": pa.array(closes, type=pa.float32()),
            "volume": pa.array(volumes, type=pa.float64()),
            "symbol_id": pa.array(
                np.full(rows_per_symbol, symbol_to_id[symbol], dtype=np.int32),
                type=pa.int32(),
            ),
            "asset_class_id": pa.array(
                np.full(rows_per_symbol, asset_class_to_id[asset], dtype=np.int8),
                type=pa.int8(),
            ),
            "year": pa.array(years.astype(np.int16), type=pa.int16()),
            "month": pa.array(months.astype(np.int8), type=pa.int8()),
        }
    )

    asset_root = os.path.join(out_dir, "prices", asset)
    os.makedirs(asset_root, exist_ok=True)
    pq.write_to_dataset(
        table,
        root_path=asset_root,
        partition_cols=["year", "month"],
        compression=compression,
        basename_template=f"part-{file_index[asset]:05d}-{{i}}.parquet",
        existing_data_behavior="overwrite_or_ignore",
        row_group_size=row_group_size,
        max_rows_per_file=max_rows_per_file,
    )
    file_index[asset] += 1
    rows_written[asset] += rows_per_symbol

manifest = {
    "created_at_utc": datetime.now(timezone.utc).isoformat(),
    "output_root": out_dir,
    "asset_classes": asset_classes,
    "symbols_per_class": symbols_per_class,
    "rows_requested": total_rows,
    "rows_per_symbol": rows_per_symbol,
    "rows_written": rows_written,
    "row_group_size": row_group_size,
    "max_rows_per_file": max_rows_per_file,
    "compression": compression,
    "start_timestamp": base_ts.isoformat(),
    "seed": seed,
}

with open(os.path.join(out_dir, "manifest.json"), "w") as f:
    json.dump(manifest, f, indent=2)

print(f"Wrote dataset to {out_dir} ({sum(rows_written.values()):,} rows).")
PYEOF
}

setup_data() {
  preflight_check

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

  local num_rows=$(( BASE_ROWS * SF ))
  echo "=== Setting up BFD dataset (sf${SF}, ~${num_rows} rows) ==="

  if [ -d "${HOST_DATA_DIR}" ]; then
    echo "Cleaning old data..."; rm -rf "${HOST_DATA_DIR}"
  fi
  mkdir -p "${HOST_DATA_DIR}"

  generate_bfd_dataset "${num_rows}" "${HOST_DATA_DIR}"

  echo "Creating schema ${CATALOG}.${SCHEMA}..."
  cli --execute "CREATE SCHEMA IF NOT EXISTS ${CATALOG}.${SCHEMA} WITH (location = 'file://${CONTAINER_DATA_DIR}')" 2>/dev/null || true

  cli --execute "DROP TABLE IF EXISTS ${TABLE}" 2>/dev/null || true
  echo "Creating external Hive table ${TABLE}..."
  cli --execute "
    CREATE TABLE ${TABLE} (
      ts TIMESTAMP,
      timestamp BIGINT,
      open REAL,
      high REAL,
      low REAL,
      close REAL,
      volume DOUBLE,
      symbol_id INTEGER,
      asset_class_id TINYINT
    ) WITH (format = 'PARQUET', external_location = 'file://${TABLE_LOCATION}')
  "

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

  local engines=()
  case "${BENCH_ENGINE}" in
    gpu) engines=("gpu") ;;
    cpu) engines=("cpu") ;;
    both) engines=("gpu" "cpu") ;;
    *) echo "ERROR: BENCH_ENGINE must be gpu|cpu|both" >&2; exit 1 ;;
  esac

  for engine in "${engines[@]}"; do
    local session_prop=""
    if [[ "${engine}" == "gpu" ]]; then
      session_prop="cudf.enabled=true"
    else
      session_prop="cudf.enabled=false"
    fi

    echo "--- ${engine} ---"
    for run in $(seq 1 "${RUNS}"); do
      local tmp_csv presto_ms duckdb_ms total_ms
      tmp_csv="$(mktemp "${out_dir}/${engine}_run${run}.XXXX.csv")"
      presto_ms=$(run_presto_to_csv "${sql}" "${tmp_csv}" "${session_prop}")
      if [[ "${presto_ms}" == "-1" ]]; then
        echo "${engine},${run},-1,-1,-1" >> "${results_csv}"
        continue
      fi

      duckdb_ms=0
      if [[ "${POST_PROCESS}" == "true" ]]; then
        duckdb_ms=$(run_duckdb_postprocess "${tmp_csv}")
      fi
      total_ms=$(( presto_ms + duckdb_ms ))
      echo "  Run ${run}/${RUNS}: presto=${presto_ms} ms, post=${duckdb_ms} ms, total=${total_ms} ms"
      echo "${engine},${run},${presto_ms},${duckdb_ms},${total_ms}" >> "${results_csv}"
    done
    echo ""
  done

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
