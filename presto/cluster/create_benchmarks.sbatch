#!/bin/bash
#SBATCH --time=00:25:00

source ../../scripts/common.sh
source slurm_functions.sh

[ -z "$SLURM_JOB_NAME" ] && echo_error "required argument '--job-name' not specified" && exit 1
[ -z "$SLURM_JOB_ACCOUNT" ] && echo_error "required argument '--account' not specified" && exit 1
[ -z "$SLURM_JOB_PARTITION" ] && echo_error "required argument '--partition' not specified" && exit 1
#[ -z "$SLURM_TIMELIMIT" ] && echo_error "required argument '--time' not specified" && exit 1
[ -z "$SLURM_NTASKS_PER_NODE" ] && echo_error "required argument '--ntasks-per-node' not specified" && exit 1
[ -z "$SLURM_NNODES" ] && echo_error "required argument '--nodes' not specified" && exit 1

NUM_WORKERS=1
WORKSPACE="/lustre/fsw/datascience_rapids_cugraphgnn/misiug/NewVersion"
DATA="/lustre/fsw/datascience_rapids_cugraphgnn/misiug/datasets"
LOGS="${WORKSPACE}/velox-testing-main/presto/cluster/"
CONFIGS="${WORKSPACE}/velox-testing-main/presto/docker/config/generated"
# Right now we assume one node that everything will run on.
# To support more nodes we just need to split the nodelist and assign the coord/each worker to a separate node.
# This will also require custom configs for each worker.
NODE=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -1)
COORD=${NODE}
CUDF_LIB=/usr/lib64/presto-native-libs
if [ "${NUM_WORKERS}" -eq "1" ]; then
    SINGLE_NODE_EXECUTION=true
else
    SINGLE_NODE_EXECUTION=false
fi

[ ! -d "$WORKSPACE" ] && echo_error "WORKSPACE must be a valid directory" && exit 1
[ ! -d "$DATA" ] && echo_error "DATA must be a valid directory" && exit 1

validate_config_directory

run_coordinator

wait_until_coordinator_is_running

for i in $(seq 0 $(( $NUM_WORKERS - 1 )) ); do
    # Workers need to be CPU to run ANALYZE on tables.
    run_worker $i "cpu"
done

wait_for_workers_to_register $NUM_WORKERS

create_schema
