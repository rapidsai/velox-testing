#!/bin/bash

# Usage: require_env VAR1 VAR2 ...
# Fails (exit 1) if any listed env var is unset or empty; prints missing names to stderr.
function validate_environment_preconditions {
    #local missing=() v
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
    [ $# -ne 2 ] && echo_error "$0 expected one argument for '<script>' and one for '<coord/cli>'"
    validate_environment_preconditions CONFIGS WORKSPACE COORD
    local script=$1
    local log_file="$2.log"
    srun -w $COORD --ntasks=1 --overlap \
--container-image=${WORKSPACE}/presto-coordinator.sqsh \
--container-mounts=${WORKSPACE}:/workspace,\
${CONFIGS}/etc_common:/opt/presto-server/etc,\
${CONFIGS}/etc_coordinator/node.properties:/opt/presto-server/etc/node.properties,\
${CONFIGS}/etc_coordinator/config_native.properties:/opt/presto-server/etc/config.properties,\
${WORKSPACE}/.hive_metastore:/var/lib/presto/data/hive/metastore \
-- bash -lc "${script}" > ${WORKSPACE}/${log_file} 2>&1 &
}

# Runs a coordinator on a specific node with default configurations.
function run_coordinator {
    validate_environment_preconditions CONFIGS SOURCE_CONFIGS SINGLE_NODE_EXECUTION NODE
    local coord_config="${CONFIGS}/etc_coordinator/config_native.properties"
    # Replace placeholder in configs
    cp ${SOURCE_CONFIGS}/etc_coordinator/config_native.properties ${coord_config}
    sed -i "s+discovery\.uri.*+discovery\.uri=http://${COORD}:8080+g" ${coord_config}
    sed -i "s+single-node-execution-enabled.*+single-node-execution-enabled=${SINGLE_NODE_EXECUTION}+g" ${coord_config}

    run_coord_image "/opt/presto-server/bin/launcher run" "coord"
}

# Runs a worker on a given node with custom configuration files which are generated as necessary.
function run_worker {
    [ $# -ne 1 ] && echo_error "$0 expected one argument for 'worker_id'"
    validate_environment_preconditions CONFIGS WORKSPACE SOURCE_CONFIGS COORD SINGLE_NODE_EXECUTION NODE CUDF_LIB

    local worker_id=$1
    local worker_config="${CONFIGS}/etc_worker_${worker_id}/config_native.properties"
    local worker_node="${CONFIGS}/etc_worker_${worker_id}/node.properties"
    local worker_data="${WORKSPACE}/worker_data_${worker_id}"

    # Create unique configuration/data files for each worker:
    cp ${SOURCE_CONFIGS}/etc_worker/config_native.properties ${worker_config}
    # Give each worker a unique port.
    sed -i "s+http-server\.http\.port.*+http-server\.http\.port=900${worker_id}+g" ${worker_config}
    # Update discovery based on which node the coordinator is running on.
    sed -i "s+discovery\.uri.*+discovery\.uri=http://${COORD}:8080+g" ${worker_config}
    sed -i "s+single-node-execution-enabled.*+single-node-execution-enabled=${SINGLE_NODE_EXECUTION}+g" ${coord_config}

    cp ${SOURCE_CONFIGS}/etc_worker/node.properties ${worker_node}
    # Give each worker a unique id.
    sed -i "s+node\.id.*+node\.id=worker_${worker_id}+g" ${worker_node}

    # Create unique data dir per worker.
    mkdir ${worker_data}

    # Run the worker with the new configs.
    CUDA_VISIBLE_DIVICES=${worker_id} srun -w $NODE --ntasks=1 --overlap \
--container-image=${WORKSPACE}/worker-all-arch.sqsh \
--container-mounts=${WORKSPACE}:/workspace,\
${CONFIGS}/etc_common:/opt/presto-server/etc,\
${worker_node}:/opt/presto-server/etc/node.properties,\
${worker_config}:/opt/presto-server/etc/config.properties,\
${worker_data}:/var/lib/presto/data,\
${WORKSPACE}/.hive_metastore:/var/lib/presto/data/hive/metastore \
--container-env=LD_LIBRARY_PATH="$CUDF_LIB:$LD_LIBRARY_PATH" \
-- bash -lc "presto_server --etc-dir=/opt/presto-server/etc" > ${WORKSPACE}/worker_${worker_id}.log 2>&1 &
}

# Run a cli node that will connect to the coordinator and run queries that setup a
# tpch schema based on the create_schema.sql file.
function create_schema {
    run_coord_iamge "/opt/presto-cli --server ${COORD}:8080 --catalog hive --schema default < /workspace/create_schema.sql" "cli"
}

# Run a cli node that will connect to the coordinator and run queries from queries.sql
# Results are stored in cli.log.
function run_queries {
    run_coord_image "/opt/presto-cli --server ${COORD}:8080 --catalog hive --schema tpch1k < /workspace/queries.sql" "cli"
}

function wait_until_coordinator_is_running {
    validate_environment_preconditions WORKSPACE COORD
    local state="INACTIVE"
    for i in {1..20}; do
        state=$(curl -s http://${COORD}:8080/v1/info/state || true)
        echo "$SLURM_JOB_ID coord state: $state" >> ${WORKSPACE}/out.log
        if [[ "$state" == "\"ACTIVE\"" ]]; then
            echo_success "$SLURM_JOB_ID coord started.  state: $state" >> ${WORKSPACE}/out.log
	    return 0
        fi
        sleep 2
    done
    echo_error "$SLURM_JOB_ID coord did not start.  state: $state" >> ${WORKSPACE}/out.log
}

function wait_for_workers_to_register {
    validate_environment_preconditions WORKSPACE COORD
    [ $# -ne 1 ] && echo_error "$0 expected one argument for 'expected number of workers'"
    local expected_num_workers=$1
    local num_workers=0
    for i in {1..20}; do
        num_workers=$(curl -s http://${COORD}:8080/v1/node | jq length)
        if (( $num_workers == $expected_num_workers )); then
            echo_success "$SLURM_JOB_ID worker registered. num_nodes: $num_nodes" >> ${WORKSPACE}/out.log
	    return 0
        fi
        sleep 2
    done
    echo_error "$SLURM_JOB_ID worker registered. num_nodes: $num_nodes" >> ${WORKSPACE}/out.log
}

function validate_config_file_exists {
    validate_environment_preconditions CONFIGS
    [ ! -f "${CONFIGS}/$1" ] && echo_error "$1 must exist in CONFIGS directory" && exit 1
}

function validate_config_directory {
    validate_environment_preconditions CONFIGS
    validate_config_file_exists "etc_common/jvm.config"
    validate_config_file_exists "etc_common/log.properties"
    validate_config_file_exists "etc_coordinator/config_native.properties"
    validate_config_file_exists "etc_coordinator/node.properties"
    validate_config_file_exists "etc_worker/config_native.properties"
    validate_config_file_exists "etc_worker/node.properties"
}

# Reads from cli.log to get the query IDs and fetches their results from the coordinator
# The results are stored in results.log
function fetch_query_results {
    validate_environment_preconditions WORKSPACE COORD
    echo "" > ${WORKSPACE}/results.log
    while IFS= read -r query_line; do
        if [[ $query_line =~ Query[[:space:]]+([^,]+), ]]; then
	    query_id="${BASH_REMATCH[1]}"
	    curl -s http://${COORD}:8080/v1/query/$query_id >> ${WORKSPACE}/results.log
        fi
    done < <(grep "Query [^ ]*," "${WORKSPACE}/cli.log")
}

# Parses results.log for the relevant timing data, storing the results in summary.csv.
# Assumes the queries are in order, and numbers them accordingly.
function parse_results {
    validate_environment_preconditions WORKSPACE
    echo "query,state,elapsed_ms,execution_ms,queued_ms,cpu_ms" > ${WORKSPACE}/summary.csv
    cat ${WORKSPACE}/results.log | jq -r '
      def durms:
        if (type=="number") then .
        else capture("(?<v>[0-9.]+)\\s*(?<u>ms|s|m|h)") as $c
          | ($c.v|tonumber) * (if $c.u=="ms" then 1 elif $c.u=="s" then 1000 elif $c.u=="m" then 60000 else 3600000 end)
        end;

      . as $r
      | [
          $r.state,
          ($r.queryStats.elapsedTimeMillis    // ($r.queryStats.elapsedTime    | durms)),
          ($r.queryStats.executionTimeMillis  // ($r.queryStats.executionTime  | durms)),
          ($r.queryStats.queuedTimeMillis     // ($r.queryStats.queuedTime     | durms)),
          ($r.queryStats.totalCpuTimeMillis   // ($r.queryStats.totalCpuTime   | durms))
        ]
      | @csv' | awk '{printf("%2d,%s\n", NR, $0)}' >> ${WORKSPACE}/summary.csv
}
