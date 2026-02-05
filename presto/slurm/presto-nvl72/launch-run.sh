#!/bin/bash
# ==============================================================================
# Presto TPC-H Benchmark Launcher
# ==============================================================================
# Simple launcher script to submit the presto benchmark job to slurm
#
# Usage:
#   ./launch-run.sh [additional sbatch options]
#
# To change configuration, edit run-presto-benchmarks.slurm directly
# ==============================================================================

set -e

# Change to script directory
cd "$(dirname "$0")"

# Clean up old output files
rm -f result_dir/* logs/* *.out *.err 2>/dev/null || true
mkdir -p result_dir logs

echo "Submitting Presto TPC-H benchmark job..."
echo "Configuration is set in run-presto-benchmarks.slurm"
echo ""

# Submit job
JOB_ID=$(sbatch "$@" run-presto-benchmarks.slurm | awk '{print $NF}')
#JOB_ID=$(sbatch "$@" create-presto-benchmarks.slurm | awk '{print $NF}')

echo "Job submitted with ID: $JOB_ID"
echo ""
echo "Monitor job with:"
echo "  squeue -j $JOB_ID"
echo "  tail -f presto-tpch-run_${JOB_ID}.out"
echo ""
echo "Waiting for job to complete..."

# Wait for job to finish
while squeue -j $JOB_ID 2>/dev/null | grep -q $JOB_ID; do
    sleep 5
done

echo ""
echo "Job completed!"
echo ""
echo "Output files:"
ls -lh presto-tpch-run_${JOB_ID}.{out,err} 2>/dev/null || echo "No output files found"
echo ""
echo "Showing job output:"
echo "========================================"
cat presto-tpch-run_${JOB_ID}.out 2>/dev/null || echo "No output available"
