#!/bin/bash
#SBATCH --job-name=presto-analyze-tables
#SBATCH --ntasks-per-node=1
#SBATCH --mem=0
#SBATCH --exclusive
# Note: --time, --cpus-per-task, --partition, and --account are passed as sbatch
# CLI flags by launch-analyze-tables.sh (sourced from cluster_config.env) so they
# can vary per cluster/partition without editing this file.

# ==============================================================================
# Presto Analyze Tables Job Script
# ==============================================================================
# Starts coordinator + workers, registers TPC-H tables, and runs ANALYZE TABLE
# to collect statistics for the given scale factor.

source "${SCRIPT_DIR}/slurm_common.sh"

# Dataset subdirectory name under DATA for table registration.
: "${DATASET_NAME:=tpch-rs-${SCALE_FACTOR}}"
export DATASET_NAME

# ANALYZE-specific UCX knob: cap rendezvous rails at 1 to avoid the multi-rail
# rendezvous path that has shown stability issues during table registration.
export UCX_MAX_RNDV_RAILS=1

echo "Dataset name: ${DATASET_NAME}"
echo "========================================"

bash "${SCRIPT_DIR}/run-analyze-tables.sh"
