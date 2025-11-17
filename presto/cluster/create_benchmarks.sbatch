#!/bin/bash
#SBATCH --time=00:25:00

source ../../scripts/common.sh
source slurm_functions.sh
source setup_coord.sh

for i in $(seq 0 $(( $NUM_WORKERS - 1 )) ); do
    # Workers need to be CPU to run ANALYZE on tables.
    run_worker $i "cpu"
done

wait_for_workers_to_register $NUM_WORKERS

create_schema
