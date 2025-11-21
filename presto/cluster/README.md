These are a set of scripts to run presto in a Slurm cluster.

# Dispatch a job to create SF1k tables and track job progress.
SCALE_FACTOR=1000 WORKSPACE=<workspace_path> DATA=<data_path> NUM_NODES=1 NUM_GPUS_PER_NODE=1 ./dispatch.sh create --job-name <job_name> -A <account> -p <partition>

# Dispatch a job to run SF1k benchmark (after creation) and track job progress.
SCALE_FACTOR=1000 WORKSPACE=<workspace_path> DATA=<data_path> NUM_NODES=<num_nodes> NUM_GPUS_PER_NODE=<num_gpus> ./dispatch.sh run --job-name <job_name> -A <account> -p <partition>