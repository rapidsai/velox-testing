#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# ==============================================================================
# TPC-H Data Generation Launcher
# ==============================================================================
# Submits a SLURM job to generate TPC-H parquet data using tpchgen-rs.
#
# Usage:
#   ./launch-gen-data.sh [--scale-factor <sf>] [--output-dir <path>] [--parallelism <n>] [additional sbatch options]
#
# To change the container image, override IMAGE_DIR or set IMAGE before running.
# Pre-pull the image with: ./pull_ghcr_image.sh ghcr.io/rapidsai/velox-testing-images:tpchgen-cli
# ==============================================================================

set -e

cd "$(dirname "$0")"

module load slurm 2>/dev/null || true

source "$(dirname "$0")/defaults.env"
source "$(dirname "$0")/launcher_common.sh"

SCALE_FACTOR="100"
# OUTPUT_DIR defaults to ${DATA}/tpch-rs-${SCALE_FACTOR} after arg parsing so
# the -s flag is honored. Override via -o or by exporting OUTPUT_DIR.
PARALLELISM="100"
# NODELIST defaults to empty -- Slurm picks any available node.
# Override via env or -N/--nodelist to pin.
NODELIST="${NODELIST:-}"
EXTRA_ARGS=()

usage() {
    cat <<EOF
Usage: $0 [OPTIONS] [-- <additional sbatch options>]

Submits a SLURM job to generate TPC-H parquet data using tpchgen-rs.

Options:
  -s, --scale-factor <sf>   TPC-H scale factor (default: ${SCALE_FACTOR})
  -o, --output-dir <path>   Output directory for generated parquet data
                            (default: \${DATA}/tpch-rs-\${SCALE_FACTOR},
                             where DATA is from your cluster_config.env)
  -j, --parallelism <n>     tpchgen-rs parallelism (default: ${PARALLELISM})
  -N, --nodelist <nodes>    Slurm node(s) to run on (default: any available)
  -h, --help                Show this help message and exit

Any arguments after -- are passed directly to sbatch.

Cluster config (~/.cluster_config.env or \$CLUSTER_CONFIG):
  Partition and account default to CLUSTER_CPU_PARTITION / CLUSTER_CPU_ACCOUNT
  (data gen is CPU-only). Override either by exporting CLUSTER_DEFAULT_PARTITION
  or CLUSTER_DEFAULT_ACCOUNT before invoking, or pass -- --partition=... --account=...

Environment overrides:
  IMAGE_DIR / IMAGE         Container image location. Pre-pull with:
                            ./pull_ghcr_image.sh ghcr.io/rapidsai/velox-testing-images:tpchgen-cli
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -s|--scale-factor) requires_value "$1" "${2:-}"; SCALE_FACTOR="$2"; shift 2 ;;
        -o|--output-dir)   requires_value "$1" "${2:-}"; OUTPUT_DIR="$2"; shift 2 ;;
        -j|--parallelism)  requires_value "$1" "${2:-}"; PARALLELISM="$2"; shift 2 ;;
        -N|--nodelist)     requires_value "$1" "${2:-}"; NODELIST="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        --) shift; break ;;
        *) EXTRA_ARGS+=("$1"); shift ;;
    esac
done

# Default OUTPUT_DIR now that SCALE_FACTOR is finalized.
if [[ -z "${OUTPUT_DIR:-}" ]]; then
    [[ -n "${DATA:-}" ]] || { echo "Error: DATA not set in cluster_config.env — can't pick a default output dir.  Pass -o <path> or set DATA." >&2; exit 1; }
    OUTPUT_DIR="${DATA}/tpch-rs-${SCALE_FACTOR}"
fi

# Data generation is CPU-only.
resolve_cluster_variant cpu
[[ -z "${CLUSTER_CPUS_PER_TASK}" ]] && { echo "Error: CLUSTER_CPU_CPUS_PER_TASK not set in cluster_config.env"; exit 1; }
build_cluster_sbatch_args

# Pre-flight: the slurm script defaults IMAGE to this same path; keep them in sync.
: "${IMAGE:=${IMAGE_DIR}/velox-testing-images-tpchgen-cli.sqsh}"
preflight_image "${IMAGE}" \
    "./pull_ghcr_image.sh ghcr.io/rapidsai/velox-testing-images:tpchgen-cli"

echo "Submitting TPC-H data generation job..."
echo "  Scale factor: $SCALE_FACTOR"
echo "  Output dir:   $OUTPUT_DIR"
echo "  Parallelism:  $PARALLELISM"
echo "  Node:         ${NODELIST:-<any available>}"
echo "  Partition:    ${CLUSTER_DEFAULT_PARTITION:-<sbatch default>}"
echo "  Account:      ${CLUSTER_DEFAULT_ACCOUNT:-<sbatch default>}"
echo "  CPUs/task:    ${CLUSTER_CPUS_PER_TASK}"
echo ""

OUT_FMT="gen-tpch-data_sf${SCALE_FACTOR}_%j.out"
ERR_FMT="gen-tpch-data_sf${SCALE_FACTOR}_%j.err"

NODELIST_ARG=()
if [[ -n "${NODELIST}" ]]; then
    NODELIST_ARG=(--nodelist="${NODELIST}")
fi

# Slurm copies the submitted script to its per-job spool dir before executing,
# so the .slurm can't compute SCRIPT_DIR from BASH_SOURCE.  Pass it explicitly
# so the job can locate defaults.env / cluster_config.env back at the repo.
SCRIPT_DIR="$PWD"
JOB_ID=$(sbatch \
  --nodes=1 \
  "${NODELIST_ARG[@]}" \
  "${CLUSTER_SBATCH_ARGS[@]}" \
  --export="ALL,SCRIPT_DIR=${SCRIPT_DIR},SCALE_FACTOR=${SCALE_FACTOR},OUTPUT_DIR=${OUTPUT_DIR},PARALLELISM=${PARALLELISM},IMAGE=${IMAGE}" \
  --output="${OUT_FMT}" \
  --error="${ERR_FMT}" \
  "${EXTRA_ARGS[@]}" \
  gen-tpch-data.slurm | awk '{print $NF}')

OUT_FILE="${OUT_FMT//%j/${JOB_ID}}"
ERR_FILE="${ERR_FMT//%j/${JOB_ID}}"

echo "Job submitted with ID: $JOB_ID"
echo ""
print_monitor_hints "${JOB_ID}" "${OUT_FILE}" "${ERR_FILE}"
echo ""
echo "Waiting for job to complete..."
wait_for_job "${JOB_ID}"

echo ""
echo "Output files:"
ls -lh "${OUT_FILE}" "${ERR_FILE}" 2>/dev/null || echo "No output files found"
show_job_output "${OUT_FILE}" "${ERR_FILE}"
[[ "${JOB_STATE}" == "COMPLETED" ]] || exit 1
