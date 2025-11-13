#!/bin/bash

source ../../common.sh
source slurm_functions.sh

[ -z "$SLURM_JOB_NAME" ] && echo_error "required argument '--job-name' not specified" && exit 1
[ -z "$SLURM_JOB_ACCOUNT" ] && echo_error "required argument '--account' not specified" && exit 1
[ -z "$SLURM_JOB_PARTITION" ] && echo_error "required argument '--partition' not specified" && exit 1
[ -z "$SLURM_TIMEOUT" ] && echo_error "required argument '--time' not specified" && exit 1
[ -z "$SLURM_NTASKS_PER_NODE" ] && echo_error "required argument '--ntasks-per-node' not specified" && exit 1
[ -z "$SLURM_NNODES" ] && echo_error "required argument '--nodes' not specified" && exit 1

NUM_WORKERS=1
validate_environment_preconditions WORKSPACE NUM_WORKERS

[ ! -d "$WORKSPACE" ] && echo_error "WORKSPACE must be a valid directory" && exit 1

if [ "${NUM_WORKERS}" -eq "1" ]; then
    SINGLE_NODE_EXECUTION=true
else
    SINGLE_NODE_EXECUTION=false
fi

validate_config_directory

#CONFIG="velox-testing-main/presto/docker/config/generated"
# These scripts will need to edit the configs to specify the addresses of the
# coordinator/nodes (which will change based on which nodes are allocated when running SLURM jobs).
# Since we don't want to edit the source configs, copy them over to a
# new config directory that will be edited and used when running the contianers.
SOURCE_CONFIGS="${WORKSPACE}/TemplateConfigs"
CONFIGS="${WORKSPACE}/Configs"
# Right now we assume one node that everything will run on.
# To support more nodes we just need to split the nodelist and assign the coord/each worker to a separate node.
# This will also require custom configs for each worker.
NODE=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -1)
COORD=${NODE}
CUDF_LIB=/usr/lib64/presto-native-libs

run_coordinator

wait_until_coordinator_is_running

for i in {0..${NUM_WORKERS}}; do
    run_worker $i
done

wait_for_workers_to_register

run_queries

fetch_query_results

parse_results
