#!/bin/bash

set -e
# Run ldconfig once
echo ldconfig

START_COORDINATOR_NORMALIZED="$(echo "${START_COORDINATOR}" | tr '[:upper:]' '[:lower:]')"
# Optionally start Java coordinator in the same container
if [[ "${START_COORDINATOR_NORMALIZED}" == "1" || "${START_COORDINATOR_NORMALIZED}" == "true" || "${START_COORDINATOR_NORMALIZED}" == "on" ]]; then
  PRESTO_JAVA_HOME=${PRESTO_JAVA_HOME:-/opt/presto-server-java}
  PRESTO_HOME="${PRESTO_JAVA_HOME}" "${PRESTO_JAVA_HOME}/bin/launcher" run &
fi

if [ $# -eq 0 ]; then
  presto_server --etc-dir="/opt/presto-server/etc/" &
else
# Launch workers in parallel, each pinned to a different GPU
# The GPU IDs are passed as command-line arguments
for gpu_id in "$@"; do
  CUDA_VISIBLE_DEVICES=$gpu_id presto_server --etc-dir="/opt/presto-server/etc${gpu_id}" &
done
fi

# Wait for all background processes
wait
