#!/bin/bash
#SBATCH --job-name=presto-alter-tables
#SBATCH --ntasks-per-node=1
#SBATCH --mem=0
#SBATCH --exclusive
# Note: --time, --cpus-per-task, --partition, --account are passed as sbatch
# CLI flags by launch-alter-tables.sh (sourced from cluster_config.env).

# ==============================================================================
# Presto Alter Tables Job Script
# ==============================================================================
# Starts coordinator + workers (cudf disabled, like the analyze flow), then
# runs the SQL file in ${ALTER_SQL_FILE} against the hive schema
# tpchsf${SCALE_FACTOR}.

source "${SCRIPT_DIR}/slurm_common.sh"

# Mirror analyze's UCX knob: cap rendezvous rails for stability on
# metastore-only workloads.
export UCX_MAX_RNDV_RAILS=1

echo "Scale factor : ${SCALE_FACTOR}"
echo "Schema name  : tpchsf${SCALE_FACTOR}"
echo "SQL file     : ${ALTER_SQL_FILE}"
echo "========================================"

bash "${SCRIPT_DIR}/run-alter-tables.sh"
