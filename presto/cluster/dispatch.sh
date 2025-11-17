# This is a convenient wrapper for the run_benchmarks.sh and create_benchmarks.sh that
# will track the current job for the user.

#!/bin/bash
rm *.log
rm *.out

[ $# -ne 1 ] && echo_error "$0 expected one argument for 'create/run'"
JOB_TYPE="$1"
[ "$JOB_TYPE" == "create" ] && [ "$JOB_TYPE" == "run" ] && echo_error "parameter must be create or run"
shift 1

sbatch "$@" --nodes=1 --ntasks-per-node=10 ${JOB_TYPE}_benchmarks.sbatch;

echo "Waiting for jobs to finish..."
while :; do
    line=$(squeue | grep $(whoami))
    [ -z "$line" ] && break
    printf "\r%s" "$line"
    sleep 5
done
echo ""
cat *.out
