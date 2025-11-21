#!/bin/bash
#SBATCH --time=00:20:00

source ../../scripts/common.sh
source slurm_functions.sh
source setup_coord.sh

i=0
for node_id in $(scontrol show hostnames "$SLURM_JOB_NODELIST"); do
    for gpu_id in $(seq 0 $(( $NUM_GPUS_PER_NODE - 1 )) ); do
	run_worker "${gpu_id}" "gpu" "${node_id}" "$i"
	i=$(($i + 1))
    done
done

wait_for_workers_to_register $NUM_WORKERS

run_queries 5 $SCALE_FACTOR
fetch_query_results
parse_results 5
