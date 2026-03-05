#!/usr/bin/env bash
# timestamp_benchmark.sh — Generate timestamp data and benchmark GPU vs CPU
#
# Usage:
#   ./timestamp_benchmark.sh [setup|bench|all] [sf]
#
# Examples:
#   ./timestamp_benchmark.sh setup 10     # Create table from tpch sf10 (~60M rows)
#   ./timestamp_benchmark.sh bench        # Run benchmark queries
#   ./timestamp_benchmark.sh all 10       # Setup + bench
#
# Prerequisites:
#   - Presto coordinator running with tpch and hive catalogs
#   - Workers running with cuDF enabled

set -euo pipefail

COORDINATOR="${PRESTO_COORDINATOR:-presto-coordinator}"
PORT="${PRESTO_PORT:-8080}"
SF="${2:-10}"
TABLE="hive.default.ts_bench_sf${SF}"
RUNS="${BENCHMARK_RUNS:-3}"

cli() {
  docker exec -i "${COORDINATOR}" presto-cli \
    --server "localhost:${PORT}" \
    --catalog hive \
    --schema default \
    "$@"
}

run_query() {
  local label="$1"
  local session="$2"
  local sql="$3"

  echo "  [${label}] running..."
  local start end elapsed
  start=$(date +%s%N)
  cli --session "${session}" --execute "${sql}" > /dev/null 2>&1
  end=$(date +%s%N)
  elapsed=$(( (end - start) / 1000000 ))
  echo "  [${label}] ${elapsed} ms"
  echo "${elapsed}"
}

setup_data() {
  echo "=== Setting up timestamp benchmark table: ${TABLE} (sf${SF}) ==="
  echo "This may take a few minutes for large scale factors..."

  cli --execute "DROP TABLE IF EXISTS ${TABLE}" 2>/dev/null || true

  cli --execute "
    CREATE TABLE ${TABLE}
    WITH (format = 'PARQUET')
    AS SELECT
      l_orderkey,
      l_partkey,
      l_suppkey,
      l_quantity,
      l_extendedprice,
      CAST(l_shipdate AS TIMESTAMP) AS ship_ts,
      CAST(l_commitdate AS TIMESTAMP) AS commit_ts,
      CAST(l_receiptdate AS TIMESTAMP) AS receipt_ts,
      l_returnflag,
      l_linestatus
    FROM tpch.sf${SF}.lineitem
  "

  local row_count
  row_count=$(cli --execute "SELECT count(*) FROM ${TABLE}" 2>/dev/null | tr -d '[:space:]')
  echo "Table ${TABLE} created with ${row_count} rows."
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

  # Results file
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
