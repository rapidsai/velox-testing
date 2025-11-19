# This is a convenient wrapper for the run_benchmarks.sh and create_benchmarks.sh that
# will track the current job for the user.

#!/bin/bash
rm *.log
rm *.out

[ $# -ge 1 ] && echo "$0 expected first argument is 'create/run'" && exit 1
JOB_TYPE="$1"
[ "$JOB_TYPE" == "create" ] && [ "$JOB_TYPE" == "run" ] && echo "parameter must be create or run" && exit 1
shift 1

[ -z "$NUM_NODES" ] && echo "NUM_NODES env variable must be set" && exit 1
[ -z "$NUM_GPUS_PER_NODE" ] && echo "NUM_GPUS_PER_NODE env variable must be set" && exit 1

NUM_WORKERS=$(( $NUM_NODES * $NUM_GPUS_PER_NODE ))

sbatch "$@" --nodes=${NUM_NODES} --ntasks-per-node=${NUM_GPUS_PER_NODE} ${JOB_TYPE}_benchmarks.sbatch;

echo "Waiting for jobs to finish..."
while :; do
    line=$(squeue | grep $(whoami))
    [ -z "$line" ] && break
    printf "\r%s" "$line"
    sleep 5
done
echo ""
cat *.out
