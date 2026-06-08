#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# ==============================================================================
# Presto TPC-H Benchmark Sweep
# ==============================================================================
# Runs launch-run.sh (and optionally post_results.py) over the Cartesian product
# of every sweep dimension provided. Dimensions default to a single-element list
# so behavior is backward-compatible with the previous nodes × scale-factors
# sweep.
#
# Sweep dimensions (each accepts a space-separated value list):
#   -n, --nodes                    Node counts                  (default: "8")
#   -s, --scale-factors            TPC-H scale factors          (default: "30000")
#   --driver-counts                task.max-drivers-per-task    (property)
#   --kvikio-nthreads              KVIKIO_NTHREADS              (env var)
#   --batch-sizes                  cudf.batch_size_min_threshold (property)
#   --ucx-buffer-sizes             exchange.max-buffer-size AND sink.max-buffer-size
#                                  (both properties set to the same value)
#   --libcudf-stream-pool-sizes    LIBCUDF_KERNEL_STREAM_POOL_SIZE (env var;
#                                  rename via --env-overrides if engine reads
#                                  a different variable)
#
# Generic catch-alls (each accepts "KEY=v1,v2 KEY2=v3,v4 ..."):
#   --config-overrides             Property overrides for etc_worker/config_native.properties
#   --env-overrides                Env-var overrides written into per-run worker.env
#
# Other:
#   -i, --iterations               Iterations per query         (default: 3)
#   --cache-state                  Override cache state (default derived: 1 iter
#                                  -> lukewarm, 2+ -> warm)
#   -w, --worker-image <name>      Worker container image (pass-through to
#                                  launch-run.sh; overrides cluster default)
#   -c, --coord-image <name>       Coordinator container image (pass-through)
#   -g, --num-workers-per-node <n> Workers per node (pass-through; overrides
#                                  cluster default)
#   --no-post                      Skip post_results.py for every run
#   --continue-on-failure          Continue the sweep when a run produces any
#                                  failed queries (default: bail out)
#   --repeats <n>                  Repeat the entire sweep <n> times (default: 1).
#                                  Useful for flake-rate / stability studies.
#   -v, --verbose                  Stream launch-run.sh / post_results.py output
#                                  live (default: capture and only dump on failure)
#
# Required only when posting (i.e. when --no-post is NOT set):
#   --sku-name, --storage-configuration-name,
#   --velox-branch, --velox-repo, --presto-branch, --presto-repo
#
# Examples:
#   ./run-sweep.sh -n "5" -s "10000" --driver-counts "4 8 12" --no-post
#   ./run-sweep.sh -n "5" -s "10000" --ucx-buffer-sizes "32MB 64MB 128MB" \
#                  --batch-sizes "100000 200000" --no-post
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
source "${SCRIPT_DIR}/defaults.env"

# ------------------------------------------------------------------------------
# Argument parsing
# ------------------------------------------------------------------------------

NODE_COUNTS=(8)
SCALE_FACTORS=(30000)
DRIVER_COUNTS=()
KVIKIO_NTHREADS_LIST=()
BATCH_SIZES=()
UCX_BUFFER_SIZES=()
STREAM_POOL_SIZES=()
GENERIC_CONFIG_OVERRIDES=""   # raw string, parsed below
GENERIC_ENV_OVERRIDES=""      # raw string, parsed below
ITERATIONS=3
NO_POST=0
CONTINUE_ON_FAILURE=0
REPEATS=1
VERBOSE=0
WORKER_IMAGE=""
COORD_IMAGE=""
NUM_WORKERS_PER_NODE=""

SKU_NAME=""
STORAGE_CONFIGURATION_NAME=""
CACHE_STATE=""
VELOX_BRANCH=""
PRESTO_BRANCH=""
VELOX_REPO=""
PRESTO_REPO=""

usage() {
    sed -n '5,54p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sku-name)                    SKU_NAME="$2";                    shift 2 ;;
        --storage-configuration-name)  STORAGE_CONFIGURATION_NAME="$2";  shift 2 ;;
        --cache-state)                 CACHE_STATE="$2";                 shift 2 ;;
        --velox-branch)                VELOX_BRANCH="$2";                shift 2 ;;
        --presto-branch)               PRESTO_BRANCH="$2";               shift 2 ;;
        --velox-repo)                  VELOX_REPO="$2";                  shift 2 ;;
        --presto-repo)                 PRESTO_REPO="$2";                 shift 2 ;;
        -n|--nodes)                    read -ra NODE_COUNTS           <<< "$2"; shift 2 ;;
        -s|--scale-factors)            read -ra SCALE_FACTORS         <<< "$2"; shift 2 ;;
        --driver-counts)               read -ra DRIVER_COUNTS         <<< "$2"; shift 2 ;;
        --kvikio-nthreads)             read -ra KVIKIO_NTHREADS_LIST  <<< "$2"; shift 2 ;;
        --batch-sizes)                 read -ra BATCH_SIZES           <<< "$2"; shift 2 ;;
        --ucx-buffer-sizes)            read -ra UCX_BUFFER_SIZES      <<< "$2"; shift 2 ;;
        --libcudf-stream-pool-sizes)   read -ra STREAM_POOL_SIZES     <<< "$2"; shift 2 ;;
        --config-overrides)            GENERIC_CONFIG_OVERRIDES="$2";    shift 2 ;;
        --env-overrides)               GENERIC_ENV_OVERRIDES="$2";       shift 2 ;;
        -i|--iterations)               ITERATIONS="$2";                  shift 2 ;;
        -w|--worker-image)             WORKER_IMAGE="$2";                shift 2 ;;
        -c|--coord-image)              COORD_IMAGE="$2";                 shift 2 ;;
        -g|--num-workers-per-node)     NUM_WORKERS_PER_NODE="$2";        shift 2 ;;
        --no-post)                     NO_POST=1;                        shift   ;;
        --continue-on-failure)         CONTINUE_ON_FAILURE=1;            shift   ;;
        --repeats)                     REPEATS="$2";                     shift 2 ;;
        -v|--verbose)                  VERBOSE=1;                        shift   ;;
        -h|--help)                     usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
    esac
done

# Required args only matter when we're going to post.
if (( NO_POST == 0 )); then
    for req in SKU_NAME STORAGE_CONFIGURATION_NAME VELOX_BRANCH PRESTO_BRANCH VELOX_REPO PRESTO_REPO; do
        [[ -n "${!req}" ]] || { echo "Error: --${req//_/-} is required (or pass --no-post)"; exit 1; }
    done
fi

if [[ -z "${CACHE_STATE}" ]]; then
    [[ "${ITERATIONS}" -eq 1 ]] && CACHE_STATE="lukewarm" || CACHE_STATE="warm"
fi

# Seconds to wait between runs to allow the previous job's cudf exchange UCX
# sockets to release their ports (10003, 10013, ...).  These ports are
# deterministic (http_port+3 per worker) so a new job on the same nodes will
# collide if the previous job's containers haven't fully torn down yet.
INTER_RUN_SLEEP=90

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

# Parse a generic-overrides string of the form "KEY=v1,v2 KEY2=v3,v4" into
# parallel arrays of (key, csv_values). Whitespace separates key=values groups;
# comma separates the values for one key.
#
# Usage:
#   declare -a KEYS VALS
#   parse_generic_overrides "$str" KEYS VALS
parse_generic_overrides() {
    local input="$1"
    local -n keys_out="$2"
    local -n vals_out="$3"
    keys_out=()
    vals_out=()
    [[ -z "$input" ]] && return 0
    local pair k v
    for pair in $input; do
        [[ -z "$pair" || "$pair" != *=* ]] && continue
        k="${pair%%=*}"
        v="${pair#*=}"
        keys_out+=("$k")
        vals_out+=("$v")
    done
}

# Compact label fragment for a knob: <prefix><value> when the swept set has
# more than one value, otherwise the empty string. Keeps default-only run-dir
# names short.
#
# Usage: label=$(label_fragment <prefix> <swept_count> <value>)
label_fragment() {
    local prefix="$1" count="$2" value="$3"
    if (( count > 1 )); then
        # Replace anything not [A-Za-z0-9] so it's filesystem-safe.
        local clean="${value//[^A-Za-z0-9]/}"
        printf "_%s%s" "$prefix" "$clean"
    fi
}

# Sentinel for "knob not swept — use engine default". Must be non-empty so
# `read -ra` doesn't drop it as a trailing field. Compared explicitly below
# before emitting overrides.
SWEEP_DEFAULT="__default__"

# Default a swept array to a singleton sentinel when no values were provided
# on the CLI. Iterating over the empty array would skip the entire Cartesian
# branch; the sentinel lets us iterate exactly once with the engine's default.
default_singleton() {
    local -n arr="$1"
    if (( ${#arr[@]} == 0 )); then
        arr=("$SWEEP_DEFAULT")
    fi
}

default_singleton DRIVER_COUNTS
default_singleton KVIKIO_NTHREADS_LIST
default_singleton BATCH_SIZES
default_singleton UCX_BUFFER_SIZES
default_singleton STREAM_POOL_SIZES

# --repeats N is plumbed as a synthetic env-override "REPEAT=1,2,...,N". The
# cross-product machinery then multiplies the run count by N, each repeat gets
# a distinct OUTPUT_DIR (_REPEAT_<n> suffix), and the engine just sees an
# inert REPEAT=<n> env var in worker.env that it ignores.
if [[ ! "${REPEATS}" =~ ^[0-9]+$ ]] || (( REPEATS < 1 )); then
    echo "Error: --repeats must be a positive integer; got '${REPEATS}'" >&2
    exit 1
fi
if (( REPEATS > 1 )); then
    repeat_csv=$(seq -s, 1 "${REPEATS}")
    if [[ -n "${GENERIC_ENV_OVERRIDES}" ]]; then
        GENERIC_ENV_OVERRIDES="REPEAT=${repeat_csv} ${GENERIC_ENV_OVERRIDES}"
    else
        GENERIC_ENV_OVERRIDES="REPEAT=${repeat_csv}"
    fi
fi

# Parse generic overrides up-front. Each key becomes its own sweep dimension.
declare -a CFG_OV_KEYS CFG_OV_VALCSV
parse_generic_overrides "$GENERIC_CONFIG_OVERRIDES" CFG_OV_KEYS CFG_OV_VALCSV
declare -a ENV_OV_KEYS ENV_OV_VALCSV
parse_generic_overrides "$GENERIC_ENV_OVERRIDES" ENV_OV_KEYS ENV_OV_VALCSV

# ------------------------------------------------------------------------------
# Cross-product enumeration
#
# Build an array of "combo records": one string per run, with TAB-separated
# fields encoding every swept value. The producer-consumer split keeps the
# main loop straightforward at the cost of a small precomputation pass.
# ------------------------------------------------------------------------------

# Each combo string format (tab-separated):
#   SF<TAB>N<TAB>DC<TAB>KVK<TAB>BS<TAB>UXC<TAB>SP<TAB>cfg_kv;cfg_kv;...<TAB>env_kv;env_kv;...
# where the trailing two are semicolon-joined "key=value" lists for the
# generic overrides (one entry per generic override key).

combos=("")
expand_dim() {
    # $1 = label, $2... = values
    local label="$1"; shift
    local -a values=("$@")
    local -a out=()
    local c v
    for c in "${combos[@]}"; do
        for v in "${values[@]}"; do
            if [[ -z "$c" ]]; then
                out+=("$v")
            else
                out+=("$c"$'\t'"$v")
            fi
        done
    done
    combos=("${out[@]}")
}

expand_dim SF  "${SCALE_FACTORS[@]}"
expand_dim N   "${NODE_COUNTS[@]}"
expand_dim DC  "${DRIVER_COUNTS[@]}"
expand_dim KVK "${KVIKIO_NTHREADS_LIST[@]}"
expand_dim BS  "${BATCH_SIZES[@]}"
expand_dim UXC "${UCX_BUFFER_SIZES[@]}"
expand_dim SP  "${STREAM_POOL_SIZES[@]}"

# Expand each generic-override dimension. We push the per-key "key=value"
# string into the combo so the consumer just has to join the trailing fields.
for i in "${!CFG_OV_KEYS[@]}"; do
    key="${CFG_OV_KEYS[$i]}"
    IFS=',' read -ra vlist <<< "${CFG_OV_VALCSV[$i]}"
    expanded=()
    for v in "${vlist[@]}"; do expanded+=("${key}=${v}"); done
    expand_dim "CFG_${key}" "${expanded[@]}"
done
for i in "${!ENV_OV_KEYS[@]}"; do
    key="${ENV_OV_KEYS[$i]}"
    IFS=',' read -ra vlist <<< "${ENV_OV_VALCSV[$i]}"
    expanded=()
    for v in "${vlist[@]}"; do expanded+=("${key}=${v}"); done
    expand_dim "ENV_${key}" "${expanded[@]}"
done

total=${#combos[@]}
run=0

# Counts used to decide whether each dim contributes a label fragment.
COUNT_SF=${#SCALE_FACTORS[@]}
COUNT_N=${#NODE_COUNTS[@]}
COUNT_DC=${#DRIVER_COUNTS[@]}
COUNT_KVK=${#KVIKIO_NTHREADS_LIST[@]}
COUNT_BS=${#BATCH_SIZES[@]}
COUNT_UXC=${#UCX_BUFFER_SIZES[@]}
COUNT_SP=${#STREAM_POOL_SIZES[@]}

# ------------------------------------------------------------------------------
# Sweep
# ------------------------------------------------------------------------------

# Per-iteration worker.env files must live on a filesystem visible to the
# compute nodes (pyxis bind-mounts them into the worker container). /tmp is
# node-local, so we stage under SCRIPT_DIR which is on the same shared FS as
# the default worker.env that launch-run.sh consumes.
SWEEP_TMPDIR="$(mktemp -d -p "${SCRIPT_DIR}" .sweep-tmp.XXXXXX)"
trap 'rm -rf "${SWEEP_TMPDIR}"' EXIT

for combo in "${combos[@]}"; do
    run=$(( run + 1 ))
    # Decode fields. First seven are the well-known knobs; the rest are the
    # generic config/env override "key=value" strings, one per generic key.
    IFS=$'\t' read -ra fields <<< "$combo"
    SF="${fields[0]}"
    N="${fields[1]}"
    DC="${fields[2]}"
    KVK="${fields[3]}"
    BS="${fields[4]}"
    UXC="${fields[5]}"
    SP="${fields[6]}"
    # Generic override key=value pairs come after the seven well-known fields.
    cfg_count=${#CFG_OV_KEYS[@]}
    env_count=${#ENV_OV_KEYS[@]}
    cfg_start=7
    cfg_end=$(( cfg_start + cfg_count ))
    env_start=$cfg_end
    env_end=$(( env_start + env_count ))

    # Treat the sentinel as "unset" — only emit overrides for real values.
    is_set() { [[ -n "$1" && "$1" != "$SWEEP_DEFAULT" ]]; }

    # Build the CONFIG_OVERRIDES string for launch-run.sh (semicolon-joined).
    cfg_ov_pairs=()
    is_set "$DC"  && cfg_ov_pairs+=("task.max-drivers-per-task=${DC}")
    is_set "$BS"  && cfg_ov_pairs+=("cudf.batch_size_min_threshold=${BS}")
    if is_set "$UXC"; then
        cfg_ov_pairs+=("exchange.max-buffer-size=${UXC}")
        cfg_ov_pairs+=("sink.max-buffer-size=${UXC}")
    fi
    for (( i = cfg_start; i < cfg_end; i++ )); do
        cfg_ov_pairs+=("${fields[$i]}")
    done
    cfg_ov_str=""
    if (( ${#cfg_ov_pairs[@]} > 0 )); then
        cfg_ov_str=$(IFS=';'; echo "${cfg_ov_pairs[*]}")
    fi

    # Build the per-iteration worker.env. Start from the base file, append
    # any env-var overrides for this combination.
    sweep_worker_env="${SWEEP_TMPDIR}/worker_env_${run}"
    cp "${SCRIPT_DIR}/worker.env" "${sweep_worker_env}"
    # Remove any base-env lines we're about to overwrite so the rendered file
    # has a single canonical assignment per var (set -a; source last-wins
    # would also work, but a clean file is easier to debug).
    strip_var() {
        local var="$1"
        sed -i "/^${var}=/d" "${sweep_worker_env}"
    }
    if is_set "$KVK"; then strip_var KVIKIO_NTHREADS;                 echo "KVIKIO_NTHREADS=${KVK}"                >> "${sweep_worker_env}"; fi
    if is_set "$SP";  then strip_var LIBCUDF_KERNEL_STREAM_POOL_SIZE; echo "LIBCUDF_KERNEL_STREAM_POOL_SIZE=${SP}" >> "${sweep_worker_env}"; fi
    for (( i = env_start; i < env_end; i++ )); do
        kv="${fields[$i]}"
        strip_var "${kv%%=*}"
        echo "${kv}" >> "${sweep_worker_env}"
    done

    # Output-dir suffix: only knobs that are actually swept appear, to keep
    # default-only run-dir names readable.
    # SF and N are always part of the dir prefix below, so don't echo them
    # in the suffix too (avoids result_sf1000_n2_sf1000_n2_dc2_... noise).
    suffix=""
    suffix+="$(label_fragment dc  "$COUNT_DC"  "$DC")"
    suffix+="$(label_fragment kvk "$COUNT_KVK" "$KVK")"
    suffix+="$(label_fragment bs  "$COUNT_BS"  "$BS")"
    suffix+="$(label_fragment uxc "$COUNT_UXC" "$UXC")"
    suffix+="$(label_fragment sp  "$COUNT_SP"  "$SP")"
    for (( i = cfg_start; i < cfg_end; i++ )); do
        # fields[i] is "key=val"; strip key= prefix for a short tag.
        kv="${fields[$i]}"
        suffix+="_$(echo "${CFG_OV_KEYS[$(( i - cfg_start ))]}" | tr -c 'A-Za-z0-9' '_')${kv##*=}"
    done
    for (( i = env_start; i < env_end; i++ )); do
        kv="${fields[$i]}"
        suffix+="_$(echo "${ENV_OV_KEYS[$(( i - env_start ))]}" | tr -c 'A-Za-z0-9' '_')${kv##*=}"
    done
    # Always include SF and N in the dir name so things stay searchable, even
    # when the sweep is one-dimensional.
    OUTPUT_DIR="${RESULTS_BASE}/result_sf${SF}_n${N}${suffix}"

    if (( VERBOSE )); then
        echo "========================================"
        echo "Run ${run}/${total}: ${OUTPUT_DIR##*/}"
        echo "  nodes=${N} scale_factor=${SF}"
        is_set "$DC"  && echo "  task.max-drivers-per-task=${DC}"
        is_set "$KVK" && echo "  KVIKIO_NTHREADS=${KVK}"
        is_set "$BS"  && echo "  cudf.batch_size_min_threshold=${BS}"
        is_set "$UXC" && echo "  exchange/sink.max-buffer-size=${UXC}"
        is_set "$SP"  && echo "  LIBCUDF_KERNEL_STREAM_POOL_SIZE=${SP}"
        for (( i = cfg_start; i < cfg_end; i++ )); do echo "  cfg-override: ${fields[$i]}"; done
        for (( i = env_start; i < env_end; i++ )); do echo "  env-override: ${fields[$i]}"; done
        echo "  output: ${OUTPUT_DIR}"
        echo "========================================"
    else
        echo "[${run}/${total}] starting ${OUTPUT_DIR##*/} ..."
    fi
    run_start_ts=$(date +%s)

    rm -rf "${OUTPUT_DIR}"

    launch_args=(
        -n "${N}"
        -s "${SF}"
        -i "${ITERATIONS}"
        -o "${OUTPUT_DIR}"
        --worker-env-file "${sweep_worker_env}"
    )
    [[ -n "${WORKER_IMAGE}"          ]] && launch_args+=(-w "${WORKER_IMAGE}")
    [[ -n "${COORD_IMAGE}"           ]] && launch_args+=(-c "${COORD_IMAGE}")
    [[ -n "${NUM_WORKERS_PER_NODE}"  ]] && launch_args+=(-g "${NUM_WORKERS_PER_NODE}")
    [[ -n "${cfg_ov_str}" ]] && launch_args+=(--config-overrides "${cfg_ov_str}")

    # In quiet mode (default), capture launch-run.sh output so we don't drown
    # the user in slurm bash -x traces and per-query timings on every success.
    # On failure (non-zero exit OR query failures), dump the captured log so
    # the user has full context for the failure.
    launch_log="${SWEEP_TMPDIR}/launch_${run}.log"
    launch_rc=0
    if (( VERBOSE )); then
        "${SCRIPT_DIR}/launch-run.sh" "${launch_args[@]}" || launch_rc=$?
    else
        "${SCRIPT_DIR}/launch-run.sh" "${launch_args[@]}" > "${launch_log}" 2>&1 || launch_rc=$?
    fi
    if (( launch_rc != 0 )); then
        if (( ! VERBOSE )); then
            echo "" >&2
            echo "--- launch-run.sh output (run ${run}/${total}, exit ${launch_rc}) ---" >&2
            cat "${launch_log}" >&2
        fi
        if (( CONTINUE_ON_FAILURE == 1 )); then
            echo "launch-run.sh failed for ${OUTPUT_DIR##*/}; --continue-on-failure set, marching on." >&2
        else
            echo "Stopping sweep: launch-run.sh failed for ${OUTPUT_DIR##*/}." >&2
            exit 1
        fi
    fi

    # Bail out if any query failed. launch-run.sh exits 0 as long as the slurm
    # job's state is COMPLETED — pytest's per-query failures get swallowed
    # because the batch script keeps running for nsys cleanup etc. So we check
    # benchmark_result.json's failed_queries directly. --continue-on-failure
    # opts back into "ignore and march on" for explore-the-state-space sweeps.
    check_run_success() {
        local out_dir="$1"
        local result_json="${out_dir}/benchmark_result.json"
        if [[ ! -f "$result_json" ]]; then
            echo "Stopping sweep: ${result_json} not produced — the run did not complete." >&2
            return 1
        fi
        # Python emits: first line = total failure count (or "ERROR: ..."),
        # subsequent lines = per-benchmark detail. Bash captures everything,
        # reads the first line as the count, prints the rest to stderr.
        local output count detail
        output=$(python3 - "${result_json}" <<'PY'
import json, sys
path = sys.argv[1]
try:
    d = json.load(open(path))
except Exception as e:
    print(f"ERROR: {e}")
    sys.exit(0)
total = 0
details = []
for k, v in d.items():
    if isinstance(v, dict) and "failed_queries" in v:
        fq = v.get("failed_queries") or {}
        if fq:
            details.append(f"  {k}: {len(fq)} failed: {sorted(fq.keys())}")
        total += len(fq)
print(total)
for line in details:
    print(line)
PY
)
        count="${output%%$'\n'*}"
        detail="${output#*$'\n'}"
        if [[ "$count" == ERROR:* ]]; then
            echo "Stopping sweep: could not parse ${result_json} (${count})." >&2
            return 1
        fi
        if (( count > 0 )); then
            [[ "$detail" != "$count" ]] && echo "${detail}" >&2
            echo "Stopping sweep: ${count} query failure(s) in ${out_dir##*/}." >&2
            return 1
        fi
        return 0
    }

    if ! check_run_success "${OUTPUT_DIR}"; then
        # Dump the captured launch log on query failures too — gives the user
        # the full pytest trace / per-query error context that check_run_success
        # itself only summarizes.
        if (( ! VERBOSE )) && [[ -s "${launch_log}" ]]; then
            echo "" >&2
            echo "--- launch-run.sh output (run ${run}/${total}, query failure) ---" >&2
            cat "${launch_log}" >&2
        fi
        if (( CONTINUE_ON_FAILURE == 1 )); then
            echo "(--continue-on-failure set; sweep continues.)" >&2
        else
            echo "Pass --continue-on-failure to keep going through failed runs." >&2
            exit 1
        fi
    fi

    if (( NO_POST == 0 )); then
        post_log="${SWEEP_TMPDIR}/post_${run}.log"
        post_args=(
            -p "${VT_ROOT}/benchmark_reporting_tools/post_results.py"
            "${OUTPUT_DIR}"
            --sku-name "${SKU_NAME}"
            --storage-configuration-name "${STORAGE_CONFIGURATION_NAME}"
            --cache-state "${CACHE_STATE}"
            --benchmark-name "tpch-rs-${SF}"
            --velox-branch "${VELOX_BRANCH}"
            --presto-branch "${PRESTO_BRANCH}"
            --velox-repo "${VELOX_REPO}"
            --presto-repo "${PRESTO_REPO}"
        )
        post_rc=0
        if (( VERBOSE )); then
            echo ""
            echo "Posting results for sf=${SF} n=${N}..."
            "${VT_ROOT}/scripts/run_py_script.sh" "${post_args[@]}" || post_rc=$?
        else
            "${VT_ROOT}/scripts/run_py_script.sh" "${post_args[@]}" > "${post_log}" 2>&1 || post_rc=$?
        fi
        if (( post_rc != 0 )); then
            if (( ! VERBOSE )); then
                echo "" >&2
                echo "--- post_results.py output (run ${run}/${total}, exit ${post_rc}) ---" >&2
                cat "${post_log}" >&2
            fi
            if (( CONTINUE_ON_FAILURE == 1 )); then
                echo "post_results.py failed for ${OUTPUT_DIR##*/}; --continue-on-failure set, marching on." >&2
            else
                echo "Stopping sweep: post_results.py failed for ${OUTPUT_DIR##*/}." >&2
                exit 1
            fi
        fi
    fi

    run_elapsed=$(( $(date +%s) - run_start_ts ))
    if (( VERBOSE )); then
        echo ""
        echo "Done: ${OUTPUT_DIR##*/} (${run_elapsed}s)"
    else
        echo "[${run}/${total}] OK ${OUTPUT_DIR##*/} (${run_elapsed}s)"
    fi

    if (( run < total )); then
        if (( VERBOSE )); then
            echo "Waiting ${INTER_RUN_SLEEP}s for worker UCX ports to be released before next run..."
        fi
        sleep "${INTER_RUN_SLEEP}"
    fi
    (( VERBOSE )) && echo ""
done

echo "========================================"
echo "Sweep complete: ${total} runs finished."
echo "========================================"
