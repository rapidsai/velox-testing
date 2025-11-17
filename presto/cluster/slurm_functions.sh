#!/bin/bash

# Takes a list of environment variables.  Checks that each one is set and of non-zero length.
function validate_environment_preconditions {
    local missing=()
    for var in "$@"; do
        # -z "${!var+x}" => unset; -z "${!var}" => empty
        if [[ -z "${!var+x}" || -z "${!var}" ]]; then
            missing+=("$var")
        fi
    done
    if ((${#missing[@]})); then
        echo_error "required env var ${missing[*]} not set"
  fi
}

# Execute script through the coordinator image (used for coordinator and cli executables)
function run_coord_image {
    [ $# -ne 1 ] && echo_error "$0 expected one argument for '<script>' and one for '<coord/cli>'"
    validate_environment_preconditions LOGS CONFIGS WORKSPACE COORD DATA
    local script=$1
    local type=$2
    [ "$type" != "coord" ] && [ "$type" != "cli" ] && echo_error "coord type must be coord/cli"
    local log_file="${type}.log"

    local coord_image="${WORKSPACE}/presto-coordinator.sqsh"
    [ ! -f "${coord_image}" ] && echo_error "coord image does not exist at ${coord_image}"

    mkdir -p ${WORKSPACE}/.hive_metastore

RUN_CMD="srun -w $COORD --ntasks=1 --overlap \
--container-image=${coord_image} \
--container-mounts=${WORKSPACE}:/workspace,\
${DATA}:/data,\
${CONFIGS}/etc_common:/opt/presto-server/etc,\
${CONFIGS}/etc_coordinator/node.properties:/opt/presto-server/etc/node.properties,\
${CONFIGS}/etc_coordinator/config_native.properties:/opt/presto-server/etc/config.properties,\
${WORKSPACE}/.hive_metastore:/var/lib/presto/data/hive/metastore \
-- bash -lc \"${script}\" > ${LOGS}/${log_file} 2>&1"

# Coordinator runs as a background process, whereas we want to wait for cli
# so that the job will finish when the cli is done (terminating background
# processes like the coordinator and workers).
if [ "${type}" == "coord" ]; then
    $RUN_CMD &
else
    $RUN_CMD
}

#function run_cli_image {
#    [ $# -ne 1 ] && echo_error "$0 expected one argument for '<script>'"
#    validate_environment_preconditions LOGS CONFIGS WORKSPACE COORD DATA
#    local script=$1
#    local log_file="cli.log"
#
#    mkdir -p ${WORKSPACE}/.hive_metastore
#
#    srun -w $COORD --ntasks=1 --overlap \
#--container-image=${WORKSPACE}/presto-coordinator.sqsh \
#--container-mounts=${WORKSPACE}:/workspace,\
#${DATA}:/data,\
#${CONFIGS}/etc_common:/opt/presto-server/etc,\
#${CONFIGS}/etc_coordinator/node.properties:/opt/presto-server/etc/node.properties,\
#${CONFIGS}/etc_coordinator/config_native.properties:/opt/presto-server/etc/config.properties,\
#${WORKSPACE}/.hive_metastore:/var/lib/presto/data/hive/metastore \
#-- bash -lc "${script}" > ${LOGS}/${log_file} 2>&1
#}

# Runs a coordinator on a specific node with default configurations.
# Overrides the config files with the coord node and other needed updates.
function run_coordinator {
    validate_environment_preconditions CONFIGS SINGLE_NODE_EXECUTION NODE
    local coord_config="${CONFIGS}/etc_coordinator/config_native.properties"
    # Replace placeholder in configs
    sed -i "s+discovery\.uri.*+discovery\.uri=http://${COORD}:8080+g" ${coord_config}
    sed -i "s+single-node-execution-enabled.*+single-node-execution-enabled=${SINGLE_NODE_EXECUTION}+g" ${coord_config}

    run_coord_image "/opt/presto-server/bin/launcher run" "coord"
}

# Runs a worker on a given node with custom configuration files which are generated as necessary.
function run_worker {
    [ $# -ne 2 ] && echo_error "$0 expected arguments 'worker_id' and 'worker_type'"
    validate_environment_preconditions LOGS CONFIGS WORKSPACE COORD SINGLE_NODE_EXECUTION NODE CUDF_LIB DATA

    local worker_id=$1
    local worker_type=$2
    [ "$worker_type" != "cpu" ] && [ "$worker_type" != "gpu" ] && echo_error "worker type must be gpu/cpu"
    if [ "$worker_type" == "cpu" ]; then
	NUM_DRIVERS=64
    else
	NUM_DRIVERS=4
    fi

    local worker_image="${WORKSPACE}/presto-native-worker-${worker_type}.sqsh"
    [ ! -f "${worker_image}" ] && echo_error "worker image does not exist at ${worker_image}"

    # Make a copy of the worker config that can be given a unique id for this worker.
    cp -r "${CONFIGS}/etc_worker" "${CONFIGS}/etc_worker_${worker_id}"
    local worker_config="${CONFIGS}/etc_worker_${worker_id}/config_native.properties"
    local worker_node="${CONFIGS}/etc_worker_${worker_id}/node.properties"
    local worker_data="${WORKSPACE}/worker_data_${worker_id}"

    # Create unique configuration/data files for each worker:
    # Give each worker a unique port.
    sed -i "s+http-server\.http\.port.*+http-server\.http\.port=900${worker_id}+g" ${worker_config}
    # Update discovery based on which node the coordinator is running on.
    sed -i "s+discovery\.uri.*+discovery\.uri=http://${COORD}:8080+g" ${worker_config}
    sed -i "s+single-node-execution-enabled.*+single-node-execution-enabled=${SINGLE_NODE_EXECUTION}+g" ${worker_config}
    sed -i "s+task.max-drivers-per-task.*+task.max-drivers-per-task=${NUM_DRIVERS}+g" ${worker_config}

    # Give each worker a unique id.
    sed -i "s+node\.id.*+node\.id=worker_${worker_id}+g" ${worker_node}

    # Create unique data dir per worker.
    mkdir -p ${worker_data}
    mkdir -p ${WORKSPACE}/.hive_metastore

    # Run the worker with the new configs.
    CUDA_VISIBLE_DIVICES=${worker_id} srun -w $NODE --ntasks=1 --overlap \
--container-image=${worker_image} \
--container-mounts=${WORKSPACE}:/workspace,\
${DATA}:/data,\
${CONFIGS}/etc_common:/opt/presto-server/etc,\
${worker_node}:/opt/presto-server/etc/node.properties,\
${worker_config}:/opt/presto-server/etc/config.properties,\
${worker_data}:/var/lib/presto/data,\
${WORKSPACE}/.hive_metastore:/var/lib/presto/data/hive/metastore \
--container-env=LD_LIBRARY_PATH="$CUDF_LIB:$LD_LIBRARY_PATH" \
-- bash -lc "presto_server --etc-dir=/opt/presto-server/etc" > ${LOGS}/worker_${worker_id}.log 2>&1 &
}

# Run a cli node that will connect to the coordinator and run queries that setup a
# tpch schema based on the create_schema.sql file.
function create_schema {
    run_coord_image "/opt/presto-cli --server ${COORD}:8080 --catalog hive --schema default < /workspace/create_schema.sql" "cli"
}

# Run a cli node that will connect to the coordinator and run queries from queries.sql
# Results are stored in cli.log.
function run_queries {
    [ $# -ne 1 ] && echo_error "$0 expected one argument for '<iterations>'"
    local num_iterations=$1
    awk -v n="$num_iterations" '{ for (i=1; i<=n; i++) print }' "${WORKSPACE}/queries.sql" > ${WORKSPACE}/iterating_queries.sql
    run_coord_image "/opt/presto-cli --server ${COORD}:8080 --catalog hive --schema tpch1k < /workspace/iterating_queries.sql" "cli"
    rm ${WORKSPACE}/iterating_queries.sql
}

# Check if the coordinator is running via curl.  Fail after 10 retries.
function wait_until_coordinator_is_running {
    validate_environment_preconditions COORD LOGS
    local state="INACTIVE"
    for i in {1..10}; do
        state=$(curl -s http://${COORD}:8080/v1/info/state || true)
        if [[ "$state" == "\"ACTIVE\"" ]]; then
            echo_success "$SLURM_JOB_ID coord started.  state: $state"  >> $LOGS/out.log
	    return 0
        fi
        sleep 5
    done
    echo_error "$SLURM_JOB_ID coord did not start.  state: $state"  >> $LOGS/out.log
}

# Check N nodes are registered with the coordinator.  Fail after 10 retries.
function wait_for_workers_to_register {
    validate_environment_preconditions LOGS COORD
    [ $# -ne 1 ] && echo_error "$0 expected one argument for 'expected number of workers'"
    local expected_num_workers=$1
    local num_workers=0
    for i in {1..10}; do
        num_workers=$(curl -s http://${COORD}:8080/v1/node | jq length)
        if (( $num_workers == $expected_num_workers )); then
            echo_success "$SLURM_JOB_ID worker registered. num_nodes: $num_workers"  >> $LOGS/out.log
	    return 0
        fi
        sleep 5
    done
    echo_error "$SLURM_JOB_ID worker registered. num_nodes: $num_workers"  >> $LOGS/out.log
}

function validate_file_exists {
    [ ! -f "$1" ] && echo_error "$1 must exist in CONFIGS directory" && exit 1
}

function validate_config_directory {
    validate_environment_preconditions CONFIGS
    validate_file_exists "${CONFIGS}/etc_common/jvm.config"
    validate_file_exists "${CONFIGS}/etc_common/log.properties"
    validate_file_exists "${CONFIGS}/etc_coordinator/config_native.properties"
    validate_file_exists "${CONFIGS}/etc_coordinator/node.properties"
    validate_file_exists "${CONFIGS}/etc_worker/config_native.properties"
    validate_file_exists "${CONFIGS}/etc_worker/node.properties"
}

# Reads from cli.log to get the query IDs and fetches their results from the coordinator
# The results are stored in results.log
function fetch_query_results {
    validate_environment_preconditions LOGS COORD
    echo "" > ${LOGS}/results.log
    while IFS= read -r query_line; do
        if [[ $query_line =~ Query[[:space:]]+([^,]+), ]]; then
	    query_id="${BASH_REMATCH[1]}"
	    curl -s http://${COORD}:8080/v1/query/$query_id >> ${LOGS}/results.log
        fi
    done < <(grep "Query [^ ]*," "${LOGS}/cli.log")
}

# Append sum/avg rows to the csv.
function append_non_failed_sum_and_avg() {
  local file="$1"
  awk -F',' -v OFS=',' '
    function trim(s){ gsub(/^[ \t"]+|[ \t"]+$/,"",s); return s }
    NR==1 { next }  # skip header
    {
      st = trim($2)
      el = trim($3)+0
      ex = trim($4)+0
      cu = trim($5)+0
      if (st != "FAILED") {
        s_el += el; s_ex += ex; s_cu += cu; n++
      }
    }
    END {
      # sums for NON-FAILED
      printf "%s,%s,%.6f,%.6f,%.6f\n", "SUM", "NON_FAILED", s_el+0, s_ex+0, s_cu+0
      # averages for NON-FAILED
      if (n > 0) {
        printf "%s,%s,%.6f,%.6f,%.6f\n", "AVG", "NON_FAILED", s_el/n, s_ex/n, s_cu/n
      } else {
        printf "%s,%s,%.6f,%.6f,%.6f\n", "AVG", "NON_FAILED", 0, 0, 0
      }
    }
  ' "$file" >> "$file"
}

# Parses results.log for the relevant timing data, storing the results in summary.csv.
# Assumes the queries are in order, and numbers them accordingly.
function parse_results {
    [ $# -ne 1 ] && echo_error "$0 expected one argument for '<iterations>'"
    local num_iterations=$1

    validate_environment_preconditions LOGS
    echo "query,state,elapsed_ms,execution_ms,cpu_ms" > ${LOGS}/summary.csv
    cat ${LOGS}/results.log | jq -r '
      def durms:
        if (type=="number") then .
        else capture("(?<v>[0-9.]+)\\s*(?<u>ms|s|m|h)") as $c
          | ($c.v|tonumber) * (if $c.u=="ms" then 1 elif $c.u=="s" then 1000 elif $c.u=="m" then 60000 else 3600000 end)
        end;

      . as $r
      | [
          ($r.state // "UNKNOWN"),
          ($r.queryStats.elapsedTimeMillis    // ($r.queryStats.elapsedTime    | durms) // 0),
          ($r.queryStats.executionTimeMillis  // ($r.queryStats.executionTime  | durms) // 0),
          ($r.queryStats.totalCpuTimeMillis   // ($r.queryStats.totalCpuTime   | durms) // 0)
        ]
      | @csv' | awk -v n="${num_iterations:-0}" '
      	BEGIN { if (n <= 0) { print "error: num_iterations must be > 0" > "/dev/stderr"; exit 1 } }
      	{ printf("%2d,%s\n", int((NR-1)/n), $0) }
      ' >> ${LOGS}/summary.csv
    #| awk '{printf("%2d,%s\n", int(NR / $num_iterations) + 1, $0)}' >> ${LOGS}/summary.csv
    #append_non_failed_sum_and_avg ${LOGS}/summary.csv
}
