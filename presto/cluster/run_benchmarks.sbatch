#!/bin/bash
#SBATCH --time=00:20:00

source ../../scripts/common.sh
source slurm_functions.sh
source setup_coord.sh

for i in $(seq 0 $(( $NUM_WORKERS - 1 )) ); do
    run_worker $i "gpu"
done

wait_for_workers_to_register $NUM_WORKERS

run_queries 5
fetch_query_results
parse_results 5
