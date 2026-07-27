#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Open presto-cli against the coordinator node of a running Slurm job.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

source ./defaults.env
source ./launcher_common.sh

VARIANT_TYPE="${VARIANT_TYPE:-${CLUSTER_DEFAULT_VARIANT:-gpu}}"
resolve_cluster_variant "${VARIANT_TYPE}" >/dev/null 2>&1 || true

IMAGE="${IMAGE:-${COORD_IMAGE:-}}"
PORT="${PORT:-${CLUSTER_DEFAULT_PORT:-9200}}"
CATALOG="${CATALOG:-hive}"
SCHEMA="${SCHEMA:-}"
PRESTO_USER="${PRESTO_USER:-${USER:-prestouser}}"

JOB_ID=""
SQL_FILE=""
EXTRA_ARGS=()

usage() {
    cat <<EOF
Usage: $0 [options] <job_id> [-- extra presto-cli args]

Options:
  -s, --schema <schema>   Schema to pass to presto-cli
  -f, --file <sql_file>  Execute a SQL file instead of opening an interactive REPL
  -i, --image <image>    Coordinator/CLI container image
                         Accepts an absolute path, a .sqsh basename, or an image name
                         resolved under IMAGE_DIR from cluster_config.env
  -p, --port <port>      Presto coordinator HTTP port (default: ${PORT})
  -c, --catalog <name>   Catalog (default: ${CATALOG})
  -u, --user <name>      Presto user (default: ${PRESTO_USER})
  -h, --help             Show this help

Examples:
  $0 -s tpchsf10000 5236
  $0 -s tpchsf10000 5243 -f analyze_tables.sql
  IMAGE=/path/to/images/coordinator.sqsh $0 5236
EOF
}

require_value() {
    local flag="$1"
    local value="${2:-}"
    if [[ -z "${value}" || "${value}" == -* ]]; then
        echo "Error: ${flag} requires a value" >&2
        exit 2
    fi
}

resolve_image_path() {
    local image="$1"
    if [[ -z "${image}" ]]; then
        echo ""
    elif [[ "${image}" == /* ]]; then
        echo "${image}"
    elif [[ "${image}" == *.sqsh ]]; then
        if [[ -z "${IMAGE_DIR:-}" ]]; then
            echo "Error: IMAGE_DIR is not set; pass an absolute image path with -i" >&2
            exit 2
        fi
        echo "${IMAGE_DIR}/${image}"
    else
        if [[ -z "${IMAGE_DIR:-}" ]]; then
            echo "Error: IMAGE_DIR is not set; pass an absolute image path with -i" >&2
            exit 2
        fi
        echo "${IMAGE_DIR}/${image}.sqsh"
    fi
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -s|--schema)
            require_value "$1" "${2:-}"
            SCHEMA="$2"
            shift 2
            ;;
        -f|--file)
            require_value "$1" "${2:-}"
            SQL_FILE="$2"
            shift 2
            ;;
        -i|--image)
            require_value "$1" "${2:-}"
            IMAGE="$2"
            shift 2
            ;;
        -p|--port)
            require_value "$1" "${2:-}"
            PORT="$2"
            shift 2
            ;;
        -c|--catalog)
            require_value "$1" "${2:-}"
            CATALOG="$2"
            shift 2
            ;;
        -u|--user)
            require_value "$1" "${2:-}"
            PRESTO_USER="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --)
            shift
            EXTRA_ARGS+=("$@")
            break
            ;;
        -*)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
        *)
            if [[ -z "${JOB_ID}" ]]; then
                JOB_ID="$1"
            else
                EXTRA_ARGS+=("$1")
            fi
            shift
            ;;
    esac
done

if [[ -z "${JOB_ID}" ]]; then
    usage >&2
    exit 2
fi

IMAGE="$(resolve_image_path "${IMAGE}")"
if [[ -z "${IMAGE}" ]]; then
    echo "Error: coordinator image is not set. Pass -i, set IMAGE, or configure CLUSTER_${VARIANT_TYPE^^}_DEFAULT_COORD_IMAGE." >&2
    exit 2
fi

MOUNT_ARGS=()
if [[ -n "${SQL_FILE}" ]]; then
    if [[ "${SQL_FILE}" != /* ]]; then
        SQL_FILE="${PWD}/${SQL_FILE}"
    fi
    SQL_DIR="$(cd "$(dirname "${SQL_FILE}")" && pwd -P)"
    SQL_FILE="${SQL_DIR}/$(basename "${SQL_FILE}")"
    if [[ ! -r "${SQL_FILE}" ]]; then
        echo "Error: SQL file not readable: ${SQL_FILE}" >&2
        exit 1
    fi
    EXTRA_ARGS=(--file "${SQL_FILE}" "${EXTRA_ARGS[@]}")
    MOUNT_ARGS=(--container-mounts="${SQL_DIR}:${SQL_DIR}")
fi

NODELIST="$(squeue -j "${JOB_ID}" -h -o '%N' | awk 'NR==1{print $1}')"
if [[ -z "${NODELIST}" || "${NODELIST}" == "(null)" ]]; then
    echo "Could not find allocated nodes for job ${JOB_ID}. Is it running?" >&2
    exit 1
fi

COORD="$(scontrol show hostnames "${NODELIST}" | head -1)"
if [[ -z "${COORD}" ]]; then
    echo "Could not resolve coordinator node from node list: ${NODELIST}" >&2
    exit 1
fi

SERVER="http://${COORD}:${PORT}"

echo "Job:         ${JOB_ID}"
echo "Coordinator: ${COORD}"
echo "Server:      ${SERVER}"
echo "Image:       ${IMAGE}"
echo "Catalog:     ${CATALOG}"
[[ -n "${SCHEMA}" ]] && echo "Schema:      ${SCHEMA}"
[[ -n "${SQL_FILE}" ]] && echo "SQL file:    ${SQL_FILE}"
echo

SRUN_TTY_ARGS=()
if [[ -z "${SQL_FILE}" ]]; then
    SRUN_TTY_ARGS=(--pty)
fi

srun --jobid "${JOB_ID}" -w "${COORD}" --ntasks=1 --overlap \
    "${SRUN_TTY_ARGS[@]}" \
    --container-image="${IMAGE}" \
    "${MOUNT_ARGS[@]}" \
    --export=ALL,PRESTO_SERVER="${SERVER}",PRESTO_CATALOG="${CATALOG}",PRESTO_SCHEMA="${SCHEMA}",PRESTO_USER_NAME="${PRESTO_USER}" \
    -- bash -lc '
        set -euo pipefail
        export PAGER="${PAGER:-cat}"
        export PRESTO_PAGER="${PRESTO_PAGER:-cat}"
        args=(presto-cli --server "$PRESTO_SERVER" --catalog "$PRESTO_CATALOG" --user "$PRESTO_USER_NAME")
        if [[ -n "${PRESTO_SCHEMA:-}" ]]; then
            args+=(--schema "$PRESTO_SCHEMA")
        fi
        exec "${args[@]}" "$@"
    ' presto-cli "${EXTRA_ARGS[@]}"
