#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# ==============================================================================
# launcher_common.sh — shared cluster-config resolution for the launch-*.sh
# and run_interactive.sh scripts.
# ==============================================================================
# Sourced (not executed). Assumes defaults.env has already been sourced so that
# ~/.cluster_config.env values (CLUSTER_GPU_*, CLUSTER_CPU_*) are in the env.
#
# Public functions:
#   resolve_cluster_variant <gpu|cpu>
#       Reads CLUSTER_{GPU,CPU}_* and populates the generic CLUSTER_DEFAULT_*,
#       CLUSTER_CPUS_PER_TASK, CLUSTER_NUM_WORKERS_PER_NODE, CLUSTER_TIME_*,
#       CLUSTER_DEFAULT_PORT, CLUSTER_UCX_NET_DEVICES, CLUSTER_USE_NUMA,
#       CLUSTER_EXTRA_MOUNTS, CLUSTER_NUMA_GPUS_PER_NODE, CLUSTER_LIB*_PATH
#       variables, plus COORD_IMAGE / WORKER_IMAGE. Pre-existing values are
#       preserved (so command-line flags and shell exports still win).
#
#   build_cluster_sbatch_args [<time-value>]
#       Sets the global CLUSTER_SBATCH_ARGS array with --cpus-per-task,
#       --partition, --account, and (if provided non-empty) --time. Caller
#       passes the array to sbatch.
#
# Shared defaults:
#   WORKER_ENV_FILE — path to the env file bind-mounted into worker
#   containers (sourced by run_worker in functions.sh). Defaults to
#   worker.env next to this file. Both the benchmark and analyze flows
#   inherit this default so run_worker's mount line is never unbound.
# ==============================================================================

_launcher_common_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${WORKER_ENV_FILE:=${_launcher_common_dir}/worker.env}"
: "${VT_ROOT:=$(cd "${_launcher_common_dir}/../../.." && pwd -P)}"
unset _launcher_common_dir

# ----------------------------------------------------------------------------
# Pre-flight checks
# ----------------------------------------------------------------------------
# Each helper prints an actionable error pointing at the command that would
# satisfy the missing prerequisite, then exits 1. Callers invoke them after
# arg parsing and resolve_cluster_variant but before sbatch submission so
# failures surface immediately instead of inside a queued job.
#
# On clusters where some filesystems are mounted only on compute nodes (e.g.
# /scratch on NVL72), the host-side check would always fail.  Set
# CLUSTER_COMPUTE_ONLY_PATHS in cluster_config.env to a space-separated list
# of path prefixes that live on compute-only mounts; preflights against those
# paths are skipped on the host and deferred to the compute-side checks in
# functions.sh / the .slurm scripts.

# _path_is_compute_only <path>
# Return 0 if <path> begins with any prefix listed in
# CLUSTER_COMPUTE_ONLY_PATHS (space-separated), else 1.
_path_is_compute_only() {
    local path="$1" prefix
    [[ -z "${CLUSTER_COMPUTE_ONLY_PATHS:-}" ]] && return 1
    for prefix in ${CLUSTER_COMPUTE_ONLY_PATHS}; do
        [[ -n "${prefix}" && "${path}" == "${prefix}"* ]] && return 0
    done
    return 1
}

# preflight_image <image_name_or_path> [<hint>]
# Verify a .sqsh container image is on disk. Accepts a bare basename
# (resolved against IMAGE_DIR with a .sqsh suffix), a basename ending in
# .sqsh, or an absolute path.
preflight_image() {
    local image="$1" hint="${2:-}" image_path
    if [[ "${image}" == /* ]]; then
        image_path="${image}"
    elif [[ "${image}" == *.sqsh ]]; then
        image_path="${IMAGE_DIR:?IMAGE_DIR not set in cluster_config.env}/${image}"
    else
        image_path="${IMAGE_DIR:?IMAGE_DIR not set in cluster_config.env}/${image}.sqsh"
    fi
    if _path_is_compute_only "${image_path}"; then
        echo "Note: skipping host-side preflight for ${image_path} (compute-only path)" >&2
        return 0
    fi
    if [[ ! -f "${image_path}" ]]; then
        echo "Error: container image not found at ${image_path}" >&2
        [[ -n "${hint}" ]] && echo "       To fix:  ${hint}" >&2
        exit 1
    fi
}

# preflight_dir <path> <description> [<hint>]
# Verify a directory exists. <description> is used in the error message.
preflight_dir() {
    local path="$1" desc="$2" hint="${3:-}"
    if [[ -z "${path}" ]]; then
        echo "Error: ${desc} path is not set" >&2
        [[ -n "${hint}" ]] && echo "       To fix:  ${hint}" >&2
        exit 1
    fi
    if _path_is_compute_only "${path}"; then
        echo "Note: skipping host-side preflight for ${desc} at ${path} (compute-only path)" >&2
        return 0
    fi
    if [[ ! -d "${path}" ]]; then
        echo "Error: ${desc} directory not found at ${path}" >&2
        [[ -n "${hint}" ]] && echo "       To fix:  ${hint}" >&2
        exit 1
    fi
}

# ----------------------------------------------------------------------------
# Job monitoring / output
# ----------------------------------------------------------------------------

# print_monitor_hints <job_id> <out_file> <err_file> [<extra hint>...]
# Prints the "Monitor with:" block. Extra positional args become additional
# "  <hint>" lines — useful for launcher-specific log paths (coord.log etc.).
print_monitor_hints() {
    local job_id="$1" out_file="$2" err_file="$3"
    shift 3
    echo "Monitor with:"
    echo "  squeue -j ${job_id}"
    echo "  tail -f ${out_file}"
    echo "  tail -f ${err_file}"
    local hint
    for hint in "$@"; do
        echo "  ${hint}"
    done
}

# wait_for_job <job_id> [<poll_interval_seconds>]
# Block until the job leaves the squeue queue, then query sacct for the
# final state. Default poll interval is 5s.
#
# Sets two globals for the caller (so they can decide what to display before
# acting on the result):
#   JOB_STATE     — sacct State (COMPLETED, FAILED, TIMEOUT, CANCELLED, …).
#                   Defaults to UNKNOWN if sacct doesn't return a row in time.
#   JOB_EXIT_CODE — sacct ExitCode in "N:M" format (N is the script exit code,
#                   M is the signal that killed it). "0:0" means clean exit.
#
# Does NOT return non-zero on failure (so set -e in the caller doesn't skip
# the post-completion display). Callers should test JOB_STATE / JOB_EXIT_CODE
# after show_job_output has run.
wait_for_job() {
    local job_id="$1" interval="${2:-5}"
    while squeue -j "${job_id}" 2>/dev/null | grep -q "${job_id}"; do
        sleep "${interval}"
    done

    # sacct may take a moment to register the job after it leaves the queue.
    local row=""
    local _i
    for _i in 1 2 3 4 5; do
        row=$(sacct -j "${job_id}" -X -n -P -o State,ExitCode 2>/dev/null | head -1)
        [[ -n "${row}" ]] && break
        sleep 1
    done
    if [[ -n "${row}" ]]; then
        JOB_STATE="${row%%|*}"
        JOB_EXIT_CODE="${row##*|}"
    else
        JOB_STATE="UNKNOWN"
        JOB_EXIT_CODE="?:?"
    fi
}

# show_job_output <out_file> <err_file> [<extra_log_path>] [<extra_log_label>]
# Prints the success/failure footer (using JOB_STATE / JOB_EXIT_CODE set by
# wait_for_job) plus the slurm stdout file.  The stderr file is always cat'd
# when non-empty — failures often go to stderr only, so silently dropping it
# loses the actual error.  If an extra log path is provided, that file is
# also displayed under the given label (e.g. "CLI log").
show_job_output() {
    local out_file="$1" err_file="${2:-}" extra_log="${3:-}" extra_label="${4:-CLI log}"
    echo ""
    if [[ "${JOB_STATE:-}" == "COMPLETED" ]]; then
        echo "Job completed (state: ${JOB_STATE}, exit: ${JOB_EXIT_CODE})"
    elif [[ -n "${JOB_STATE:-}" ]]; then
        echo "Job FAILED (state: ${JOB_STATE}, exit: ${JOB_EXIT_CODE})"
    else
        echo "Job completed!"   # wait_for_job not used; legacy path
    fi
    echo ""
    echo "Showing job stdout:"
    echo "========================================"
    cat "${out_file}" 2>/dev/null || echo "No stdout available"
    if [[ -n "${err_file}" && -s "${err_file}" ]]; then
        echo ""
        echo "Showing job stderr:"
        echo "========================================"
        cat "${err_file}"
    fi
    if [[ -n "${extra_log}" ]]; then
        echo ""
        echo "Showing ${extra_label}:"
        cat "${extra_log}" 2>/dev/null || echo "No ${extra_label} available"
    fi
}

# preflight_metastore <scale_factor> [<hint>]
# Verify an analyzed Hive metastore for the given SF exists either locally
# (${VT_ROOT}/.hive_metastore/tpchsf<SF>) or in the shared root (when
# HIVE_METASTORE_VERSION + HIVE_METASTORE_SHARED_ROOT are configured).
preflight_metastore() {
    local sf="$1" hint="${2:-}"
    local local_path="${VT_ROOT}/.hive_metastore/tpchsf${sf}"
    local shared_path=""
    if [[ -n "${HIVE_METASTORE_VERSION:-}" && -n "${HIVE_METASTORE_SHARED_ROOT:-}" ]]; then
        shared_path="${HIVE_METASTORE_SHARED_ROOT}/${HIVE_METASTORE_VERSION}/tpchsf${sf}"
    fi
    # An "available" candidate path is one we can verify from the host (i.e.
    # not on a compute-only mount).  Compute-only candidates count as
    # "unverifiable here" -- we can't say they're missing.
    local local_unverifiable=0 shared_unverifiable=0
    _path_is_compute_only "${local_path}" && local_unverifiable=1
    [[ -n "${shared_path}" ]] && _path_is_compute_only "${shared_path}" && shared_unverifiable=1

    # If every candidate is on a compute-only mount, defer the check entirely.
    if [[ "${local_unverifiable}" == "1" ]] && \
       { [[ -z "${shared_path}" ]] || [[ "${shared_unverifiable}" == "1" ]]; }; then
        echo "Note: skipping host-side metastore preflight for SF${sf} (compute-only paths)" >&2
        return 0
    fi

    # Otherwise: a verifiable candidate exists. Treat unverifiable candidates
    # as "maybe present" -- they only block failure, not success.
    if { [[ "${local_unverifiable}" == "1" ]] || [[ -d "${local_path}" ]]; } || \
       { [[ -n "${shared_path}" ]] && { [[ "${shared_unverifiable}" == "1" ]] || [[ -d "${shared_path}" ]]; }; }; then
        return 0
    fi
    echo "Error: analyzed Hive metastore for SF${sf} not found." >&2
    echo "       Looked in:  ${local_path}" >&2
    [[ -n "${shared_path}" ]] && echo "                   ${shared_path}" >&2
    [[ -n "${hint}" ]] && echo "       To fix:  ${hint}" >&2
    exit 1
}

# Indirect-expansion helper: assign the variant-specific source var into a
# generic destination, but only if the destination is currently empty.
# Usage: _resolve_var <dest> <src> [<fallback>]
_resolve_var() {
    local dest="$1" src="$2" fallback="${3:-}"
    if [[ -z "${!dest:-}" ]]; then
        printf -v "${dest}" '%s' "${!src:-${fallback}}"
    fi
}

# requires_value <flag-name> <value>
# Guard for case-statement arms expecting a non-empty next argument.
# Exits 1 if the value is missing or begins with '-' (i.e. the next flag).
# Usage:   -s|--scale-factor) requires_value "$1" "${2:-}"; SCALE_FACTOR="$2"; shift 2 ;;
requires_value() {
    [[ -n "${2:-}" && "${2:0:1}" != "-" ]] || { echo "Error: $1 requires a value" >&2; exit 1; }
}

resolve_cluster_variant() {
    local variant="${1:-gpu}"
    local prefix
    case "${variant}" in
        gpu) prefix="CLUSTER_GPU" ;;
        cpu) prefix="CLUSTER_CPU" ;;
        *) echo "Error: resolve_cluster_variant expects 'gpu' or 'cpu', got '${variant}'" >&2; return 1 ;;
    esac

    _resolve_var CLUSTER_DEFAULT_PARTITION    "${prefix}_PARTITION"
    _resolve_var CLUSTER_DEFAULT_ACCOUNT      "${prefix}_ACCOUNT"
    _resolve_var CLUSTER_CPUS_PER_TASK        "${prefix}_CPUS_PER_TASK"
    _resolve_var CLUSTER_NUM_WORKERS_PER_NODE "${prefix}_NUM_WORKERS_PER_NODE"
    _resolve_var CLUSTER_TIME_BENCHMARK       "${prefix}_TIME_BENCHMARK"
    _resolve_var CLUSTER_TIME_ANALYZE         "${prefix}_TIME_ANALYZE"
    _resolve_var CLUSTER_DEFAULT_PORT         "${prefix}_DEFAULT_PORT"
    _resolve_var CLUSTER_UCX_NET_DEVICES      "${prefix}_UCX_NET_DEVICES"
    _resolve_var CLUSTER_EXTRA_MOUNTS         "${prefix}_EXTRA_MOUNTS"
    _resolve_var COORD_IMAGE                  "${prefix}_DEFAULT_COORD_IMAGE"
    _resolve_var WORKER_IMAGE                 "${prefix}_DEFAULT_WORKER_IMAGE"

    if [[ "${variant}" == "gpu" ]]; then
        _resolve_var CLUSTER_USE_NUMA                  CLUSTER_GPU_USE_NUMA                  1
        _resolve_var CLUSTER_NUMA_GPUS_PER_NODE        CLUSTER_GPU_NUMA_GPUS_PER_NODE        1
        _resolve_var CLUSTER_LIBCUDA_HOST_PATH         CLUSTER_GPU_LIBCUDA_HOST_PATH
        _resolve_var CLUSTER_LIBCUDA_CONTAINER_PATH    CLUSTER_GPU_LIBCUDA_CONTAINER_PATH
        _resolve_var CLUSTER_LIBNVIDIA_ML_HOST_PATH    CLUSTER_GPU_LIBNVIDIA_ML_HOST_PATH
        _resolve_var CLUSTER_LIBNVIDIA_ML_CONTAINER_PATH CLUSTER_GPU_LIBNVIDIA_ML_CONTAINER_PATH
    else
        _resolve_var CLUSTER_USE_NUMA CLUSTER_CPU_USE_NUMA 0
    fi
}

# build_common_export_vars
# Populate the global EXPORT_VARS string with variables common to both the
# benchmark and analyze sbatch jobs.  Always-set vars (SCALE_FACTOR, SCRIPT_DIR,
# image names, etc.) are added unconditionally; optional CLUSTER_* and
# HIVE_METASTORE_* vars are appended only when non-empty.
#
# Callers append mode-specific extras (e.g. NUM_ITERATIONS for benchmark,
# DATA override for analyze) to EXPORT_VARS after this returns.
build_common_export_vars() {
    EXPORT_VARS="ALL,SCALE_FACTOR=${SCALE_FACTOR},SCRIPT_DIR=${SCRIPT_DIR}"
    EXPORT_VARS+=",NUM_GPUS_PER_NODE=${NUM_GPUS_PER_NODE},WORKER_IMAGE=${WORKER_IMAGE},COORD_IMAGE=${COORD_IMAGE}"
    EXPORT_VARS+=",USE_NUMA=${USE_NUMA},VARIANT_TYPE=${VARIANT_TYPE}"
    EXPORT_VARS+=",WORKER_ENV_FILE=${WORKER_ENV_FILE}"
    EXPORT_VARS+=",CLUSTER_DEFAULT_PORT=${CLUSTER_DEFAULT_PORT}"
    local v
    for v in CLUSTER_UCX_NET_DEVICES CLUSTER_NUMA_GPUS_PER_NODE \
             CLUSTER_LIBCUDA_HOST_PATH CLUSTER_LIBCUDA_CONTAINER_PATH \
             CLUSTER_LIBNVIDIA_ML_HOST_PATH CLUSTER_LIBNVIDIA_ML_CONTAINER_PATH \
             CLUSTER_EXTRA_MOUNTS CLUSTER_CONFIG \
             HIVE_METASTORE_VERSION HIVE_METASTORE_SHARED_ROOT; do
        [[ -n "${!v:-}" ]] && EXPORT_VARS+=",${v}=${!v}"
    done
}

build_cluster_sbatch_args() {
    local time_val="${1:-}"
    CLUSTER_SBATCH_ARGS=()
    [[ -n "${time_val}" ]]                    && CLUSTER_SBATCH_ARGS+=(--time="${time_val}")
    [[ -n "${CLUSTER_CPUS_PER_TASK:-}" ]]     && CLUSTER_SBATCH_ARGS+=(--cpus-per-task="${CLUSTER_CPUS_PER_TASK}")
    [[ -n "${CLUSTER_DEFAULT_PARTITION:-}" ]] && CLUSTER_SBATCH_ARGS+=(--partition="${CLUSTER_DEFAULT_PARTITION}")
    [[ -n "${CLUSTER_DEFAULT_ACCOUNT:-}" ]]   && CLUSTER_SBATCH_ARGS+=(--account="${CLUSTER_DEFAULT_ACCOUNT}")
    return 0
}
