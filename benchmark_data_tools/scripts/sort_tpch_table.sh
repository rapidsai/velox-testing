#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_DATA_TOOLS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
SORT_SCRIPT="${BENCHMARK_DATA_TOOLS_DIR}/sort_partition_table.py"

RAPIDS_IMAGE="${RAPIDS_IMAGE:-rapidsai/base:26.06-cuda13-py3.13}"
GPU_DEVICE="${GPU_DEVICE:-0}"

# TPC-H tables rewritten by this wrapper, with fixed partition + secondary keys.
TABLES=(orders lineitem)
declare -A PARTITION_KEY=(
    [orders]=o_orderdate
    [lineitem]=l_shipdate
)
declare -A SECONDARY_ORDER_KEY=(
    [orders]=o_orderkey
    [lineitem]=l_orderkey
)

show_help() {
    cat <<EOF
Usage: $(basename "$0") --source DIR --output DIR [options]

Build a TPC-H dataset with orders and lineitem range-partitioned and sorted:
  orders:   partition o_orderdate, secondary o_orderkey
  lineitem: partition l_shipdate,  secondary l_orderkey

Sibling tables are cloned once into --output; only orders and lineitem are
rewritten. Sorting runs inside a RAPIDS GPU container via sort_partition_table.py.

Required:
  --source DIR     Existing TPC-H dataset directory
  --output DIR     New dataset directory (must not exist unless --resume)

Options:
  --copy-mode MODE   How to clone sibling tables: symlink|copy (default: symlink)
  --resume           Resume rewriting tables; skip files that already exist
  --dry-run          Print partition plans only; do not write output

Environment:
  RAPIDS_IMAGE   Container image (default: rapidsai/base:26.06-cuda13-py3.13)
  GPU_DEVICE     CUDA device index passed to docker (default: 0)
  RAID_MOUNT     Host path bind-mounted at /raid inside the container
                 (default: /raid)

Examples:
  $(basename "$0") --source /path/to/sf100 --output /path/to/sf100_sorted --dry-run
  $(basename "$0") --source /path/to/sf100 --output /path/to/sf100_sorted
  $(basename "$0") --source /path/to/sf100 --output /path/to/sf100_sorted --resume
EOF
}

is_rewritten_table() {
    local name="$1" table
    for table in "${TABLES[@]}"; do
        if [[ "${name}" == "${table}" ]]; then
            return 0
        fi
    done
    return 1
}

clone_siblings() {
    local source="$1" output="$2" mode="$3"
    local child name resolved

    if [[ -e "${output}" ]]; then
        echo "ERROR: output dataset already exists: ${output}" >&2
        exit 1
    fi
    mkdir -p "${output}"

    shopt -s nullglob
    for child in "${source}"/*; do
        name="$(basename "${child}")"
        if is_rewritten_table "${name}"; then
            continue
        fi
        resolved="$(realpath "${child}")"
        case "${mode}" in
            symlink)
                ln -s "${resolved}" "${output}/${name}"
                ;;
            copy)
                cp -a "${resolved}" "${output}/${name}"
                ;;
            *)
                echo "ERROR: unsupported --copy-mode: ${mode} (use symlink or copy)" >&2
                exit 1
                ;;
        esac
    done
    shopt -u nullglob
}

run_sort_table() {
    local source_table="$1" output_table="$2" partition_key="$3" secondary_key="$4"
    shift 4

    docker run --rm \
        --gpus "device=${GPU_DEVICE}" \
        -v "${SORT_SCRIPT}:/workspace/sort_partition_table.py:ro" \
        -v "${RAID_MOUNT}:${RAID_MOUNT}" \
        -w /workspace \
        "${RAPIDS_IMAGE}" \
        python sort_partition_table.py \
        --source-table "${source_table}" \
        --output-table "${output_table}" \
        --partition-key "${partition_key}" \
        --secondary-order-key "${secondary_key}" \
        "$@"
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    show_help
    exit 0
fi

SOURCE=""
OUTPUT=""
COPY_MODE="symlink"
RESUME=false
DRY_RUN=false
PYTHON_FLAGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --source)
            SOURCE="$2"
            shift 2
            ;;
        --output)
            OUTPUT="$2"
            shift 2
            ;;
        --copy-mode)
            COPY_MODE="$2"
            shift 2
            ;;
        --resume)
            RESUME=true
            PYTHON_FLAGS+=(--resume)
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            PYTHON_FLAGS+=(--dry-run)
            shift
            ;;
        *)
            echo "ERROR: unknown argument: $1" >&2
            show_help
            exit 1
            ;;
    esac
done

if [[ -z "${SOURCE}" || -z "${OUTPUT}" ]]; then
    echo "ERROR: --source and --output are required" >&2
    show_help
    exit 1
fi

SOURCE="$(realpath "${SOURCE}")"
OUTPUT="$(realpath -m "${OUTPUT}")"
RAID_MOUNT="${RAID_MOUNT:-/raid}"

for table in "${TABLES[@]}"; do
    if [[ ! -d "${SOURCE}/${table}" ]]; then
        echo "ERROR: source table does not exist: ${SOURCE}/${table}" >&2
        exit 1
    fi
done

if [[ "${DRY_RUN}" == false ]]; then
    if [[ "${RESUME}" == true ]]; then
        for table in "${TABLES[@]}"; do
            if [[ ! -d "${OUTPUT}/${table}" ]]; then
                echo "ERROR: --resume requires existing output table: ${OUTPUT}/${table}" >&2
                exit 1
            fi
        done
    else
        clone_siblings "${SOURCE}" "${OUTPUT}" "${COPY_MODE}"
    fi
fi

for table in "${TABLES[@]}"; do
    echo "===== sorting ${table} (${PARTITION_KEY[${table}]}, ${SECONDARY_ORDER_KEY[${table}]}) ====="
    run_sort_table \
        "${SOURCE}/${table}" \
        "${OUTPUT}/${table}" \
        "${PARTITION_KEY[${table}]}" \
        "${SECONDARY_ORDER_KEY[${table}]}" \
        "${PYTHON_FLAGS[@]+"${PYTHON_FLAGS[@]}"}"
done
