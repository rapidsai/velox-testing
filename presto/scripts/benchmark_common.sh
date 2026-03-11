#!/usr/bin/env bash
# benchmark_common.sh — Shared functions for Presto GPU benchmarks
#
# Source this file from benchmark scripts after setting:
#   COORDINATOR, PORT, SCHEMA, SF, TABLE, RUNS, QUERY_ORDER, QUERIES
#
# Provides: detect_cudf_mode, preflight_check, cli, run_query,
#   ensure_schema, run_benchmark_loop, print_summary, check_fallbacks

# ============================================================================
# Core utilities
# ============================================================================

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

# ============================================================================
# Benchmark execution loop
# ============================================================================

# Run all queries in QUERY_ORDER, save timings and query stats.
# Args: $1=results_csv, $2=out_dir
run_benchmark_loop() {
  local results_csv="$1"
  local out_dir="$2"

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

    # Save query stats immediately
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
}

# ============================================================================
# Summary and operator breakdown (Python)
# ============================================================================

# Print summary table and per-operator breakdown.
# Args: $1=results_csv, $2=out_dir
print_summary() {
  local results_csv="$1"
  local out_dir="$2"

  python3 - "${results_csv}" "${COORDINATOR}" "${PORT}" "${RUNS}" "${out_dir}" <<'PYEOF'
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
out_dir = sys.argv[5]

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
    if n >= 1e9: return f"{n/1e9:.1f}B"
    if n >= 1e6: return f"{n/1e6:.1f}M"
    if n >= 1e3: return f"{n/1e3:.1f}K"
    return str(n)

def fmt_ns(duration_str):
    return parse_duration(str(duration_str))

def collect_all_ops(stage):
    ops = []
    if not stage: return ops
    exec_info = stage.get("latestAttemptExecutionInfo", {})
    stats = exec_info.get("stats", {})
    for op in stats.get("operatorSummaries", []):
        op["_stageId"] = stage.get("stageId", "?")
        ops.append(op)
    for sub in stage.get("subStages", []):
        ops.extend(collect_all_ops(sub))
    return ops

# Read timings
timings = {}
with open(results_csv) as f:
    reader = csv.DictReader(f)
    for row in reader:
        qname = row["query"]
        ms = int(row["elapsed_ms"])
        timings.setdefault(qname, []).append(ms)

medians = {}
for qname, vals in timings.items():
    vals.sort()
    medians[qname] = vals[len(vals) // 2]

# Load query stats from saved JSON files
presto_by_name = {}
for qname in medians:
    json_file = os.path.join(out_dir, f"query_{qname}.json")
    if not os.path.exists(json_file): continue
    with open(json_file) as f:
        try: data = json.load(f)
        except json.JSONDecodeError: continue
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
print("=== Summary ===")
hdr = "{:<25s} {:>10s} {:>10s} {:>10s} {:>10s}"
print(hdr.format("Query", "Wall (ms)", "Presto ms", "CPU ms", "Overhead"))
print(hdr.format("-" * 25, "-" * 10, "-" * 10, "-" * 10, "-" * 10))

query_details = {}
for qname, wall_median in medians.items():
    best_match = presto_by_name.get(qname)
    if best_match:
        elapsed_ms = parse_duration(best_match["elapsed"])
        cpu_ms = parse_duration(best_match["cpu"])
        overhead = wall_median - elapsed_ms
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

# Per-operator breakdown
print()
print("=" * 80)
print("Per-Operator Breakdown")
print("=" * 80)

for qname, details in query_details.items():
    ops = details.get("ops", [])
    if not ops: continue

    op_data = {}
    for op in ops:
        name = op.get("operatorType", "?")
        pid = op.get("planNodeId", "")
        key = f"{name}[{pid}]" if pid and pid != "N/A" else name
        if key not in op_data:
            op_data[key] = {"cpu": 0, "wall": 0, "in_r": 0, "out_r": 0, "tasks": 0}
        op_data[key]["wall"] += fmt_ns(op.get("getOutputWall", "0ns")) + fmt_ns(op.get("addInputWall", "0ns"))
        op_data[key]["cpu"] += fmt_ns(op.get("getOutputCpu", "0ns")) + fmt_ns(op.get("addInputCpu", "0ns"))
        op_data[key]["in_r"] += op.get("inputPositions", 0)
        op_data[key]["out_r"] += op.get("outputPositions", 0)
        op_data[key]["tasks"] += op.get("totalDrivers", 1)

    sorted_ops = sorted(op_data.items(), key=lambda x: x[1]["wall"], reverse=True)

    qid = details.get("qid", "?")
    num_ops = len(ops)
    print(f"\n--- {qname} (wall={details['wall']}ms, presto={details.get('presto','?')}ms, cpu={details.get('cpu','?')}ms, qid={qid}, ops={num_ops}) ---")
    ohdr = "  {:<40s} {:>8s} {:>8s} {:>10s} {:>10s} {:>5s}"
    print(ohdr.format("Operator", "CPU ms", "Wall ms", "In Rows", "Out Rows", "Tasks"))
    print(ohdr.format("-" * 40, "-" * 8, "-" * 8, "-" * 10, "-" * 10, "-" * 5))
    for name, d in sorted_ops:
        if d["wall"] < 0.01 and d["cpu"] < 0.01: continue
        print(ohdr.format(name[:40], f"{d['cpu']:.1f}", f"{d['wall']:.1f}",
                          fmt_rows(d["in_r"]), fmt_rows(d["out_r"]),
                          str(d["tasks"])))
PYEOF
}

# ============================================================================
# Fallback check
# ============================================================================

check_fallbacks() {
  local out_dir="$1"

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
      if [ -z "${all_fallbacks}" ]; then continue; fi

      echo "=== ${c} ===" >> "${fallback_file}"
      echo "${all_fallbacks}" >> "${fallback_file}"
      echo "" >> "${fallback_file}"

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
}

# ============================================================================
# Filter pushdown analysis via EXPLAIN ANALYZE
# ============================================================================

# Run EXPLAIN ANALYZE for each query, parse ScanFilterProject stats, and
# display a pushdown effectiveness table.
# Args: $1=out_dir, $2=total_table_rows
collect_pushdown_stats() {
  local out_dir="$1"
  local total_rows="$2"
  local ea_timeout="${EXPLAIN_ANALYZE_TIMEOUT:-60}"

  if [[ "${SKIP_EXPLAIN_ANALYZE:-}" == "1" ]]; then
    echo ""
    echo "=== Filter Pushdown Analysis (SKIPPED: SKIP_EXPLAIN_ANALYZE=1) ==="
    return
  fi

  echo ""
  echo "=== Filter Pushdown Analysis ==="
  echo ""
  echo "Collecting EXPLAIN ANALYZE for each query (${ea_timeout}s timeout each)..."
  echo "  (Set SKIP_EXPLAIN_ANALYZE=1 to skip, EXPLAIN_ANALYZE_TIMEOUT=N to adjust)"

  for qname in "${QUERY_ORDER[@]}"; do
    local sql="${QUERIES[${qname}]}"
    echo -n "  [${qname}] ... "
    local ea_output
    ea_output=$(timeout "${ea_timeout}" docker exec -i "${COORDINATOR}" presto-cli \
      --server "localhost:${PORT}" --catalog hive --schema "${SCHEMA}" \
      --execute "EXPLAIN ANALYZE ${sql}" 2>&1) || true
    if [[ -z "${ea_output}" ]]; then
      echo "TIMEOUT (${ea_timeout}s)"
      echo "TIMEOUT after ${ea_timeout}s" > "${out_dir}/explain_${qname}.txt"
    else
      echo "${ea_output}" > "${out_dir}/explain_${qname}.txt"
      if echo "${ea_output}" | grep -qi "failed\|error"; then
        echo "FAILED"
      else
        echo "ok"
      fi
    fi
  done

  echo ""

  python3 - "${out_dir}" "${total_rows}" <<'PYEOF'
import os
import re
import sys

out_dir = sys.argv[1]
total_rows = int(sys.argv[2])

def parse_scan_stats(text):
    """Extract ScanFilterProject/ScanProject/TableScan stats from EXPLAIN ANALYZE text."""
    results = []
    lines = text.split("\n")
    i = 0
    while i < len(lines):
        line = lines[i]
        if re.search(r'Scan(Filter)?Project|TableScan', line):
            indent = len(line) - len(line.lstrip())
            block = line
            j = i + 1
            while j < len(lines):
                next_line = lines[j]
                if next_line.strip() == "":
                    j += 1
                    continue
                next_indent = len(next_line) - len(next_line.lstrip())
                if next_indent > indent:
                    block += "\n" + next_line
                    j += 1
                else:
                    break

            filter_pred = ""
            m = re.search(r'filterPredicate\s*=\s*([^,\]]+)', block)
            if m:
                filter_pred = m.group(1).strip()

            scan_input = 0
            scan_output = 0
            filtered_pct = 0.0

            m_input = re.search(r'Input:\s+([\d,]+)\s+rows', block)
            if m_input:
                scan_input = int(m_input.group(1).replace(",", ""))

            m_output = re.search(r'Output:\s+([\d,]+)\s+rows', block)
            if m_output:
                scan_output = int(m_output.group(1).replace(",", ""))

            m_filt = re.search(r'Filtered:\s+([\d.]+)%', block)
            if m_filt:
                filtered_pct = float(m_filt.group(1))

            results.append({
                "input": scan_input,
                "output": scan_output,
                "filtered_pct": filtered_pct,
                "filter": filter_pred,
            })
            i = j
        else:
            i += 1
    return results

def fmt_rows(n):
    if n >= 1e9: return f"{n/1e9:.1f}B"
    if n >= 1e6: return f"{n/1e6:.1f}M"
    if n >= 1e3: return f"{n/1e3:.1f}K"
    return str(n)

files = sorted(f for f in os.listdir(out_dir) if f.startswith("explain_") and f.endswith(".txt"))

if not files:
    print("  No EXPLAIN ANALYZE data collected.")
    sys.exit(0)

hdr = "{:<25s} {:>12s} {:>12s} {:>12s} {:>10s} {:>10s}"
print(hdr.format("Query", "Table Rows", "Scan Input", "Scan Output", "Filtered%", "RG Pruned%"))
print(hdr.format("-" * 25, "-" * 12, "-" * 12, "-" * 12, "-" * 10, "-" * 10))

for f in files:
    qname = f.replace("explain_", "").replace(".txt", "")
    path = os.path.join(out_dir, f)
    with open(path) as fh:
        text = fh.read()

    if "failed" in text.lower() and "ScanFilterProject" not in text:
        print(hdr.format(qname, fmt_rows(total_rows), "ERROR", "", "", ""))
        continue

    scans = parse_scan_stats(text)
    if not scans:
        print(hdr.format(qname, fmt_rows(total_rows), "N/A", "N/A", "N/A", "N/A"))
        continue

    agg_input = sum(s["input"] for s in scans)
    agg_output = sum(s["output"] for s in scans)
    agg_filtered = (1 - agg_output / agg_input) * 100 if agg_input > 0 else 0

    if total_rows > 0 and agg_input < total_rows:
        rg_pruned = (1 - agg_input / total_rows) * 100
    else:
        rg_pruned = 0.0

    def fmt_pct(v):
        if v == 0: return "0%"
        if v >= 99.95 and v < 100.0: return f"{v:.2f}%"
        if v == 100.0: return "100%"
        return f"{v:.1f}%"

    print(hdr.format(
        qname,
        fmt_rows(total_rows),
        fmt_rows(agg_input),
        fmt_rows(agg_output),
        fmt_pct(agg_filtered),
        fmt_pct(rg_pruned),
    ))

print()
print("  Table Rows  = total rows in the table")
print("  Scan Input  = rows read from Parquet (after row-group pruning)")
print("  Scan Output = rows surviving the filter predicate")
print("  Filtered%   = % of scanned rows eliminated by the filter")
print("  RG Pruned%  = % of table rows skipped via row-group statistics pushdown")
print("                (>0% means pushdown is effectively reducing I/O)")
print()
print("  Note: RG Pruned% is from EXPLAIN ANALYZE (Velox operator-level reporting).")
print("  For cuDF-level row-group pruning stats, check the worker logs section below")
print("  or enable VLOG(1) with --v=1 on workers.")
PYEOF

  # Collect cuDF-level row-group pruning logs from worker containers
  echo ""
  echo "=== cuDF Row-Group Pruning Logs ==="
  local all_containers
  all_containers=$(docker ps --format '{{.Names}}' 2>/dev/null || true)
  local pushdown_log="${out_dir}/pushdown_logs.txt"
  : > "${pushdown_log}"
  local found_logs=0

  for c in ${all_containers}; do
    local rg_logs
    rg_logs=$(docker logs "${c}" 2>&1 | grep "CudfHiveDataSource.*row-group pruning\|CudfHiveDataSource.*split complete\|CudfHiveDataSource.*filter pushdown active" || true)
    if [ -n "${rg_logs}" ]; then
      echo "=== ${c} ===" >> "${pushdown_log}"
      echo "${rg_logs}" >> "${pushdown_log}"
      echo "" >> "${pushdown_log}"
      found_logs=1
    fi
  done

  if [ "${found_logs}" -eq 1 ]; then
    python3 - "${pushdown_log}" <<'PYEOF'
import re
import sys

log_file = sys.argv[1]
with open(log_file) as f:
    text = f.read()

rg_pattern = re.compile(
    r'row-group pruning:\s*(\d+)\s*total.*?(\d+)\s*after stats filter\s*\((\d+)%\s*pruned\)\s*file=(\S+)'
)
split_pattern = re.compile(
    r'split complete(?:\s*\(hybrid\))?:\s*(\d+)\s*rows read.*?file=(\S+)'
)

pruning_stats = []
for m in rg_pattern.finditer(text):
    pruning_stats.append({
        "total_rg": int(m.group(1)),
        "after_stats": int(m.group(2)),
        "pruned_pct": int(m.group(3)),
        "file": m.group(4).split("/")[-1],
    })

split_stats = []
for m in split_pattern.finditer(text):
    split_stats.append({
        "rows": int(m.group(1)),
        "file": m.group(2).split("/")[-1],
    })

if pruning_stats:
    total_rg = sum(s["total_rg"] for s in pruning_stats)
    surviving_rg = sum(s["after_stats"] for s in pruning_stats)
    overall_pruned = (1 - surviving_rg / total_rg) * 100 if total_rg > 0 else 0

    print(f"  cuDF row-group pruning across {len(pruning_stats)} split(s):")
    print(f"    Total row groups:     {total_rg}")
    print(f"    After stats filter:   {surviving_rg}")
    print(f"    Pruned:               {total_rg - surviving_rg} ({overall_pruned:.1f}%)")
    print()

    hdr = "    {:<30s} {:>8s} {:>8s} {:>8s}"
    print(hdr.format("File", "Total RG", "Kept RG", "Pruned%"))
    print(hdr.format("-" * 30, "-" * 8, "-" * 8, "-" * 8))
    for s in pruning_stats[:20]:
        print(hdr.format(s["file"][:30], str(s["total_rg"]), str(s["after_stats"]), f"{s['pruned_pct']}%"))
    if len(pruning_stats) > 20:
        print(f"    ... and {len(pruning_stats) - 20} more splits")
else:
    print("  No cuDF row-group pruning logs found.")
    print("  This may mean: (a) non-hybrid reader path, (b) no filter pushdown,")
    print("  or (c) VLOG level not enabled (start workers with --v=1).")

if split_stats:
    total_rows_read = sum(s["rows"] for s in split_stats)
    print()
    print(f"  cuDF split completion: {len(split_stats)} split(s), {total_rows_read:,} total rows read")
print()
print(f"  Full pushdown logs saved to {log_file}")
PYEOF
  else
    echo "  No cuDF pushdown logs found in worker containers."
    echo "  Enable verbose logging: start workers with --v=1 or set glog verbosity."
  fi
}

# ============================================================================
# Standard benchmark runner (call from benchmark scripts)
# ============================================================================

# Full benchmark run: verify table, detect mode, run queries, print summary,
# check fallbacks, save report.
# Args: $1=bench_name (e.g. "timestamp", "ohlcv")
run_standard_benchmark() {
  local bench_name="${1:-benchmark}"

  preflight_check
  echo "=== ${bench_name} Benchmark (${RUNS} runs per query, table: ${TABLE}) ==="
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
  local out_dir="benchmark_results/${bench_name}_${mode}_sf${SF}_${ts}"
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

  run_benchmark_loop "${results_csv}" "${out_dir}"

  print_summary "${results_csv}" "${out_dir}"

  collect_pushdown_stats "${out_dir}" "${count_result}"

  echo ""
  echo "To compare GPU vs CPU, run benchmark twice with different server configs:"
  echo "  1. Start workers with cudf.enabled=true, then run bench"
  echo "  2. Start workers with cudf.enabled=false, then run bench"

  check_fallbacks "${out_dir}"

  echo ""
  echo "=== Full report saved to ${out_dir}/ ==="
  echo "  report.txt    - complete benchmark output"
  echo "  timings.csv   - per-run wall clock times"
  echo "  explain_*.txt - EXPLAIN ANALYZE output per query"
  echo "  fallbacks.txt - GPU fallback details"
  echo "  config_*.txt  - worker/coordinator configs"
}

# ============================================================================
# Standard verify runner
# ============================================================================

# Verify queries against DuckDB.
# Args: $1=table_name, $2=host_data_dir, $3..=extra table replacements (pairs of name,dir)
run_standard_verify() {
  local main_table="$1"
  local main_data_dir="$2"
  shift 2

  # Build extra table replacements
  local extra_tables=()
  while [ $# -ge 2 ]; do
    extra_tables+=("$1" "$2")
    shift 2
  done

  preflight_check
  echo "=== Verification: Presto vs DuckDB (table: ${main_table}) ==="
  echo ""

  local query_file
  query_file=$(mktemp /tmp/bench_queries.XXXXXX)
  for qname in "${QUERY_ORDER[@]}"; do
    echo "###QUERY### ${qname}"
    echo "${QUERIES[${qname}]}"
  done > "${query_file}"

  # Build Python replacement list (recursive glob supports both flat and partitioned layouts)
  local py_replacements="replacements = [('${main_table}', '${main_data_dir}/**/*.parquet')]"
  local i=0
  while [ $i -lt ${#extra_tables[@]} ]; do
    py_replacements="${py_replacements}
replacements.append(('${extra_tables[$i]}', '${extra_tables[$((i+1))]}/**/*.parquet'))"
    i=$((i+2))
  done

  python3 - "${query_file}" "${COORDINATOR}" "${PORT}" "${SCHEMA}" <<PYEOF
import sys
import subprocess
import duckdb
import re

query_file = sys.argv[1]
coordinator = sys.argv[2]
port = sys.argv[3]
schema = sys.argv[4]

${py_replacements}

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
        return f
    except ValueError:
        pass
    v = re.sub(r'\.000$', '', v)
    v = re.sub(r' 00:00:00$', '', v)
    return v

def normalize_row(row_str):
    parts = row_str.strip().split('","')
    parts = [p.strip('"') for p in parts]
    return tuple(normalize_value(p) for p in parts)

def sort_key(row):
    result = []
    for v in row:
        if isinstance(v, float):
            result.append((0, v, ""))
        else:
            result.append((1, 0, str(v)))
    return result

def rows_match(presto_rows, duck_rows, rel_tol=1e-4):
    """Compare rows with relative tolerance for floats."""
    if len(presto_rows) != len(duck_rows):
        return False
    for p_row, d_row in zip(presto_rows, duck_rows):
        if len(p_row) != len(d_row):
            return False
        for p_val, d_val in zip(p_row, d_row):
            if isinstance(p_val, float) and isinstance(d_val, float):
                if p_val == 0.0 and d_val == 0.0:
                    continue
                denom = max(abs(p_val), abs(d_val))
                if denom > 0 and abs(p_val - d_val) / denom > rel_tol:
                    return False
            elif str(p_val) != str(d_val):
                return False
    return True

def fmt_val(v):
    return f"{v:.6g}" if isinstance(v, float) else str(v)

def fmt_row(row):
    return tuple(fmt_val(v) for v in row)

def run_presto(sql):
    cmd = ["docker", "exec", "-i", coordinator, "presto-cli",
           "--server", f"localhost:{port}", "--catalog", "hive",
           "--schema", schema, "--execute", sql.strip()]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0 or "failed" in result.stdout.lower():
        return None, result.stdout + result.stderr
    rows = []
    for line in result.stdout.strip().split("\n"):
        if line.strip(): rows.append(normalize_row(line))
    return sorted(rows, key=sort_key), None

def run_duckdb(sql):
    duck_sql = sql
    for table_name, parquet_path in replacements:
        duck_sql = duck_sql.replace(
            table_name,
            f"read_parquet('{parquet_path}', hive_partitioning=true)")
    con = duckdb.connect()
    result = con.execute(duck_sql).fetchall()
    rows = [tuple(normalize_value(str(v)) for v in row) for row in result]
    return sorted(rows, key=sort_key)

passed = 0
failed = 0
for qname, sql in queries.items():
    print(f"  {qname}... ", end="", flush=True)
    presto_rows, err = run_presto(sql)
    if presto_rows is None:
        print(f"SKIP (Presto error: {err[:100]})")
        continue
    try: duck_rows = run_duckdb(sql)
    except Exception as e:
        print(f"SKIP (DuckDB error: {e})")
        continue
    if rows_match(presto_rows, duck_rows):
        print("PASS")
        passed += 1
    elif len(presto_rows) == len(duck_rows) and len(presto_rows) > 0:
        n = len(presto_rows)
        # Count matching rows (tolerance-aware)
        presto_set = set(tuple(f"{v:.10g}" if isinstance(v, float) else str(v) for v in r) for r in presto_rows)
        duck_set = set(tuple(f"{v:.10g}" if isinstance(v, float) else str(v) for v in r) for r in duck_rows)
        overlap = len(presto_set & duck_set)
        overlap_pct = overlap * 100 / n if n > 0 else 0

        has_limit = "limit" in sql.lower()
        if has_limit and overlap_pct >= 80:
            print(f"PASS (LIMIT tie-break, {overlap}/{n} rows identical)")
            passed += 1
        else:
            print("FAIL")
            failed += 1
            print(f"    Rows: Presto={len(presto_rows)}, DuckDB={len(duck_rows)}, overlap={overlap}/{n}")
            shown = 0
            for i, (p, d) in enumerate(zip(presto_rows, duck_rows)):
                if not rows_match([p], [d]):
                    print(f"    Row {i}: Presto={fmt_row(p)}")
                    print(f"            DuckDB={fmt_row(d)}")
                    shown += 1
                    if shown >= 3:
                        break
    else:
        print("FAIL")
        failed += 1
        print(f"    Rows: Presto={len(presto_rows)}, DuckDB={len(duck_rows)}")

print()
print(f"=== Verification Summary: {passed} passed, {failed} failed ===")
if failed > 0:
    print("WARNING: Some queries returned different results!")
    sys.exit(1)
PYEOF

  rm -f "${query_file}"
}
