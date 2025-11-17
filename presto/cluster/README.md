These are a set of scripts to run presto in a Slurm cluster.

# Dispatch a job to create SF1k tables and track job progress.
WORKSPACE=<workspace_path> DATA=<data_path> ./dispatch.sh create --job-name <job_name> -A <account> -p <platform>

# Dispatch a job to run SF1k benchmark (after creation) and track job progress.
WORKSPACE=<workspace_path> DATA=<data_path> ./dispatch.sh run --job-name <job_name> -A <account> -p <platform>