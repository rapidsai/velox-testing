# Outlined steps to be run from an sbatch script.
# These steps should verify the context we are running in
# and start the coordinator.

[ -z "$SLURM_JOB_NAME" ] && echo "required argument '--job-name' not specified" && exit 1
[ -z "$SLURM_JOB_ACCOUNT" ] && echo "required argument '--account' not specified" && exit 1
[ -z "$SLURM_JOB_PARTITION" ] && echo "required argument '--partition' not specified" && exit 1
#[ -z "$SLURM_TIMELIMIT" ] && echo_error "required argument '--time' not specified" && exit 1
[ -z "$SLURM_NTASKS_PER_NODE" ] && echo "required argument '--ntasks-per-node' not specified" && exit 1
[ -z "$SLURM_NNODES" ] && echo "required argument '--nodes' not specified" && exit 1
[ -z "$NUM_NODES" ] && echo "NUM_WORKERS must be set" && exit 1
[ -z "$NUM_GPUS_PER_NODE" ] && echo "NUM_GPUS_PER_NODE env variable must be set" && exit 1

NUM_WORKERS=$(( $NUM_NODES * $NUM_GPUS_PER_NODE ))
LOGS="${WORKSPACE}/velox-testing/presto/cluster/"
CONFIGS="${WORKSPACE}/velox-testing/presto/docker/config/generated"
# Right now we assume one node that everything will run on.
# To support more nodes we just need to split the nodelist and assign the coord/each worker to a separate node.
# This will also require custom configs for each worker.
COORD=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -1)
CUDF_LIB=/usr/lib64/presto-native-libs
if [ "${NUM_WORKERS}" -eq "1" ]; then
    SINGLE_NODE_EXECUTION=true
else
    SINGLE_NODE_EXECUTION=false
fi

[ ! -d "$WORKSPACE" ] && echo "WORKSPACE must be a valid directory" && exit 1
[ ! -d "$DATA" ] && echo "DATA must be a valid directory" && exit 1

validate_config_directory

run_coordinator

wait_until_coordinator_is_running
