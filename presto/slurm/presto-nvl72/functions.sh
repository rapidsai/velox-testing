#!/bin/bash

# UCX Configuration
export UCX_TLS=^ib,ud:aux,sm
export UCX_MAX_RNDV_RAILS=1
export UCX_RNDV_PIPELINE_ERROR_HANDLING=y
export UCX_TCP_KEEPINTVL=1ms
export UCX_KEEPALIVE_INTERVAL=1ms


# Image directory for presto container images (can be overridden via environment)
IMAGE_DIR="${IMAGE_DIR:-${WORKSPACE}/images}"

# Logs directory for presto execution logs (can be overridden via environment)
# Default to logs/ in the same directory as this script
SCRIPT_DIR_FUNCS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOGS="${LOGS:-${SCRIPT_DIR_FUNCS}/logs}"

# Validates job preconditions and assigns default values for presto execution.
function setup {
    [ -z "$SLURM_JOB_NAME" ] && echo "required argument '--job-name' not specified" && exit 1
    [ -z "$SLURM_JOB_ACCOUNT" ] && echo "required argument '--account' not specified" && exit 1
    [ -z "$SLURM_JOB_PARTITION" ] && echo "required argument '--partition' not specified" && exit 1
    [ -z "$SLURM_NNODES" ] && echo "required argument '--nodes' not specified" && exit 1
    [ -z "$NUM_NODES" ] && echo "NUM_WORKERS must be set" && exit 1
    [ -z "$NUM_GPUS_PER_NODE" ] && echo "NUM_GPUS_PER_NODE env variable must be set" && exit 1
    [ ! -d "$WORKSPACE" ] && echo "WORKSPACE must be a valid directory" && exit 1
    [ ! -d "$DATA" ] && echo "DATA must be a valid directory" && exit 1

    NUM_WORKERS=$(( $NUM_NODES * $NUM_GPUS_PER_NODE ))
    mkdir -p ${LOGS}
    # Only set CONFIGS if not already set (allow override from environment)
    #CONFIGS="${CONFIGS:-${WORKSPACE}/config/generated/gpu}"
    #CONFIGS="${CONFIGS:-${WORKSPACE}/config/generated/cpu}"
    CONFIGS="${CONFIGS:-${WORKSPACE}/config/generated/${VARIANT_TYPE}}"
    COORD=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -1)
    PORT=9200
    CUDF_LIB=/usr/lib64/presto-native-libs
    if [ "${NUM_WORKERS}" -eq "1" ]; then
	SINGLE_NODE_EXECUTION=true
    else
	SINGLE_NODE_EXECUTION=false
    fi

    if [ ! -d ${WORKSPACE}/velox-testing ]; then
        git clone -b misiug/cluster https://github.com/rapidsai/velox-testing.git ${WORKSPACE}/velox-testing
        #sed -i "s/python3 /python3.12 /g" ${WORKSPACE}/velox-testing/scripts/py_env_functions.sh
    fi

    [ ! -d ${CONFIGS} ] && generate_configs

    validate_config_directory
}

function generate_configs {
    mkdir -p ${CONFIGS}
    pushd ${WORKSPACE}/velox-testing/presto/scripts
    #VARIANT_TYPE=cpu ./generate_presto_config.sh
    #VARIANT_TYPE=gpu ./generate_presto_config.sh
    OVERWRITE_CONFIG=true ./generate_presto_config.sh
    popd
    mv ${WORKSPACE}/velox-testing/presto/docker/config/generated/${VARIANT_TYPE}/* ${CONFIGS}/
    #mv ${WORKSPACE}/velox-testing/presto/docker/config/generated/gpu/* ${CONFIGS}/
    #mv ${WORKSPACE}/velox-testing/presto/docker/config/generated/cpu/* ${CONFIGS}/
    echo "--add-modules=java.management,jdk.management" >> ${CONFIGS}/etc_common/jvm.config
    echo "-Dcom.sun.management.jmxremote=false" >> ${CONFIGS}/etc_common/jvm.config
    echo "-XX:-UseContainerSupport" >> ${CONFIGS}/etc_common/jvm.config
}

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
    [ $# -ne 2 ] && echo_error "$0 expected one argument for '<script>' and one for '<coord/cli>'"
    validate_environment_preconditions LOGS CONFIGS WORKSPACE COORD DATA
    local script=$1
    local type=$2
    [ "$type" != "coord" ] && [ "$type" != "cli" ] && echo_error "coord type must be coord/cli"
    local log_file="${type}.log"

    local coord_image="${IMAGE_DIR}/presto-coordinator.sqsh"
    [ ! -f "${coord_image}" ] && echo_error "coord image does not exist at ${coord_image}"

    mkdir -p ${WORKSPACE}/.hive_metastore

    # Coordinator runs as a background process, whereas we want to wait for cli
    # so that the job will finish when the cli is done (terminating background
    # processes like the coordinator and workers).
    if [ "${type}" == "coord" ]; then
        srun -w $COORD --ntasks=1 --overlap \
--container-image=${coord_image} \
--export=ALL,JAVA_HOME=/usr/lib/jvm/jre-17-openjdk \
--container-env=JAVA_HOME=/usr/lib/jvm/jre-17-openjdk \
--container-env=PATH=/usr/lib/jvm/jre-17-openjdk/bin:$PATH \
--container-mounts=${WORKSPACE}:/workspace,\
${DATA}:/data,\
${CONFIGS}/etc_common:/opt/presto-server/etc,\
${CONFIGS}/etc_coordinator/node.properties:/opt/presto-server/etc/node.properties,\
${CONFIGS}/etc_coordinator/config_native.properties:/opt/presto-server/etc/config.properties,\
${CONFIGS}/etc_coordinator/catalog/hive.properties:/opt/presto-server/etc/catalog/hive.properties,\
${WORKSPACE}/.hive_metastore:/var/lib/presto/data/hive/metastore \
-- bash -lc "unset JAVA_HOME; export JAVA_HOME=/usr/lib/jvm/jre-17-openjdk; export PATH=/usr/lib/jvm/jre-17-openjdk/bin:\$PATH; ${script}" >> ${LOGS}/${log_file} 2>&1 &
    else
        srun -w $COORD --ntasks=1 --overlap \
--container-image=${coord_image} \
--export=ALL,JAVA_HOME=/usr/lib/jvm/jre-17-openjdk \
--container-env=JAVA_HOME=/usr/lib/jvm/jre-17-openjdk \
--container-env=PATH=/usr/lib/jvm/jre-17-openjdk/bin:$PATH \
--container-mounts=${WORKSPACE}:/workspace,\
${DATA}:/data,\
${CONFIGS}/etc_common:/opt/presto-server/etc,\
${CONFIGS}/etc_coordinator/node.properties:/opt/presto-server/etc/node.properties,\
${CONFIGS}/etc_coordinator/config_native.properties:/opt/presto-server/etc/config.properties,\
${CONFIGS}/etc_coordinator/catalog/hive.properties:/opt/presto-server/etc/catalog/hive.properties,\
${WORKSPACE}/.hive_metastore:/var/lib/presto/data/hive/metastore \
-- bash -lc "unset JAVA_HOME; export JAVA_HOME=/usr/lib/jvm/jre-17-openjdk; export PATH=/usr/lib/jvm/jre-17-openjdk/bin:\$PATH; ${script}" >> ${LOGS}/${log_file} 2>&1
    fi
}

# Runs a coordinator on a specific node with default configurations.
# Overrides the config files with the coord node and other needed updates.
function run_coordinator {
    validate_environment_preconditions CONFIGS SINGLE_NODE_EXECUTION
    local coord_config="${CONFIGS}/etc_coordinator/config_native.properties"
    # Replace placeholder in configs
    sed -i "s+discovery\.uri.*+discovery\.uri=http://${COORD}:${PORT}+g" ${coord_config}
    sed -i "s+http-server\.http\.port=.*+http-server\.http\.port=${PORT}+g" ${coord_config}
    sed -i "s+single-node-execution-enabled.*+single-node-execution-enabled=${SINGLE_NODE_EXECUTION}+g" ${coord_config}

read -r -d '' COORD_SCRIPT <<'EOS' || true
set -euo pipefail
unset CONFIG NODE_CONFIG PRESTO_ETC JAVA_TOOL_OPTIONS JDK_JAVA_OPTIONS _JAVA_OPTIONS

export JAVA_HOME=/usr
export PATH=/usr/bin:$PATH
/opt/presto-server/bin/launcher run & srv=$!

# wait for JVM to appear
for i in {1..60}; do
  pid="$(pgrep -fa java | awk '/com\.facebook\.presto\.server\.PrestoServer/{print $1; exit}' || true)"
  [ -n "${pid:-}" ] && break
  sleep 1
done

echo "JAVA PID: ${pid:-not-found}"
if [ -n "${pid:-}" ]; then
  echo "---- JVM cmdline args ----"
  xargs -0 -a "/proc/$pid/cmdline" printf "%s\n" | tr ' ' '\n' \
    | grep -E '^-D(node\.config|config)=|--add-modules|^-Xmx' || true

  if command -v jcmd >/dev/null 2>&1; then
    echo "---- jcmd VM.system_properties (filtered) ----"
    jcmd "$pid" VM.system_properties \
      | grep -E '(^ -Dnode\.config=|^ -Dconfig=|^ -Dhttp-server|^ -Dcom\.sun\.management| -XX:-?UseContainerSupport)' || true
  fi
fi
echo "DONE JAVA PID"

wait "$srv"
EOS

run_coord_image "$COORD_SCRIPT" "coord"
}

# Runs a worker on a given node with custom configuration files which are generated as necessary.
function run_worker {
    [ $# -ne 4 ] && echo_error "$0 expected arguments 'gpu_id', 'image', 'node_id', and 'worker_id'"
    validate_environment_preconditions LOGS CONFIGS WORKSPACE COORD SINGLE_NODE_EXECUTION CUDF_LIB DATA

    local gpu_id=$1
    local image=$2
    local node=$3
    local worker_id=$4
    local worker_two_digit=$(printf "%02d\n" "$worker_id")
    echo "running worker ${worker_id} with image ${image} on node ${node} with gpu_id ${gpu_id}"
    if [ "$image" == "presto-native-worker-cpu" ]; then
	NUM_DRIVERS=64
    elif (( $NUM_WORKERS > 1 )); then
	NUM_DRIVERS=1
    else
	NUM_DRIVERS=2
    fi

    local worker_image="${IMAGE_DIR}/${image}.sqsh"
    [ ! -f "${worker_image}" ] && echo_error "worker image does not exist at ${worker_image}"

    # Make a copy of the worker config that can be given a unique id for this worker.
    rm -rf "${CONFIGS}/etc_worker_${worker_id}"
    cp -r "${CONFIGS}/etc_worker" "${CONFIGS}/etc_worker_${worker_id}"
    local worker_config="${CONFIGS}/etc_worker_${worker_id}/config_native.properties"
    local worker_node="${CONFIGS}/etc_worker_${worker_id}/node.properties"
    local worker_hive="${CONFIGS}/etc_worker_${worker_id}/catalog/hive.properties"
    local worker_data="${SCRIPT_DIR_FUNCS}/worker_data_${worker_id}"

    # Create unique configuration/data files for each worker:
    # Give each worker a unique port.
    sed -i "s+http-server\.http\.port.*+http-server\.http\.port=10${worker_two_digit}0+g" ${worker_config}
    # If we are using cudf exchange then the port number is hard coded (in current velox) to port # + 3
    sed -i "s+cudf\.exchange\.server\.port=.*+cudf\.exchange\.server\.port=10${worker_two_digit}3+g" ${worker_config}
    # Update discovery based on which node the coordinator is running on.
    sed -i "s+discovery\.uri.*+discovery\.uri=http://${COORD}:${PORT}+g" ${worker_config}
    sed -i "s+single-node-execution-enabled.*+single-node-execution-enabled=${SINGLE_NODE_EXECUTION}+g" ${worker_config}
    sed -i "s+task.max-drivers-per-task.*+task.max-drivers-per-task=${NUM_DRIVERS}+g" ${worker_config}
    # Give each worker a unique id.
    sed -i "s+node\.id.*+node\.id=worker_${worker_id}+g" ${worker_node}

    # Create unique data dir per worker.
    mkdir -p ${worker_data}
    mkdir -p ${WORKSPACE}/.hive_metastore

    # Need to fix this to run with cpu nodes as well.
    # Run the worker with the new configs.
    # Use --overlap to allow multiple srun commands from same job
    # Don't use --gres=gpu:1 here since the job already allocated GPUs
    # Set CUDA_VISIBLE_DEVICES explicitly in bash command to override SLURM default
    srun -N1 -w $node --ntasks=1 --overlap \
--container-image=${worker_image} \
--export=ALL \
--container-env=LD_LIBRARY_PATH="/usr/lib64/presto-native-libs:/usr/local/lib:/usr/lib64" \
--container-mounts=${WORKSPACE}:/workspace,\
${DATA}:/data,\
${CONFIGS}/etc_common:/opt/presto-server/etc,\
${worker_node}:/opt/presto-server/etc/node.properties,\
${worker_config}:/opt/presto-server/etc/config.properties,\
${worker_hive}:/opt/presto-server/etc/catalog/hive.properties,\
${worker_data}:/var/lib/presto/data,\
${WORKSPACE}/.hive_metastore:/var/lib/presto/data/hive/metastore \
--container-env=LD_LIBRARY_PATH="$CUDF_LIB:$LD_LIBRARY_PATH" \
--container-env=GLOG_vmodule=IntraNodeTransferRegistry=3,ExchangeOperator=3 \
--container-env=GLOG_logtostderr=1 \
-- /bin/bash -c "export CUDA_VISIBLE_DEVICES=${gpu_id}; echo \"CUDA_VISIBLE_DEVICES=\$CUDA_VISIBLE_DEVICES\"; echo \"--- Environment Variables ---\"; set | grep -E 'UCX_|CUDA_VISIBLE_DEVICES'; nvidia-smi -L; /usr/bin/presto_server --etc-dir=/opt/presto-server/etc" > ${LOGS}/worker_${worker_id}.log 2>&1 &
}

function setup_benchmark {
    echo "setting up benchmark"
    [ $# -ne 1 ] && echo_error "$0 expected one argument for 'scale factor'"
    local scale_factor=$1
    local data_path="/data/date-scale-${scale_factor}"

    # Create tables and run ANALYZE
    # ANALYZE may fail on very large datasets due to resource constraints, but tables will still be usable
    run_coord_image "export PORT=$PORT; export HOSTNAME=$COORD; export PRESTO_DATA_DIR=/data; yum install python3.12 -y; yum install jq -y; cd /workspace/velox-testing/presto/scripts; ./setup_benchmark_tables.sh -b tpch -d date-scale-${scale_factor} -s tpchsf${scale_factor}; ./analyze_tables.sh --port $PORT --hostname $COORD -s tpchsf${scale_factor}" "cli"

    # Alternative: Use register_benchmark.sh which creates tables WITHOUT running ANALYZE
    #run_coord_image "export COORD=${COORD}:${PORT}; export SCHEMA=tpchsf${scale_factor}; cd /workspace/velox-testing/presto/scripts; ./register_benchmark.sh register -l ${data_path} -s tpchsf${scale_factor} -c ${COORD}:${PORT}" "cli"
}

# Run a cli node that will connect to the coordinator and run queries from queries.sql
# Results are stored in cli.log.
function run_queries {
    echo "running queries"
    [ $# -ne 2 ] && echo_error "$0 expected two arguments for '<iterations>' and '<scale_factor>'"
    local num_iterations=$1
    local scale_factor=$2
    run_coord_image "export PORT=$PORT; export HOSTNAME=$COORD; export PRESTO_DATA_DIR=/data; yum install python3.12 jq -y > /dev/null; cd /workspace/velox-testing/presto/scripts; ./run_benchmark.sh -b tpch -s tpchsf${scale_factor} -i ${num_iterations} --hostname ${COORD} --port $PORT -o /workspace/veloxtesting/slurm_scripts/result_dir" "cli"
}

# Check if the coordinator is running via curl.  Fail after 10 retries.
function wait_until_coordinator_is_running {
    echo "waiting for coordinator to be accessible"
    validate_environment_preconditions COORD LOGS
    local state="INACTIVE"
    for i in {1..10}; do
        state=$(curl -s http://${COORD}:${PORT}/v1/info/state || true)
        if [[ "$state" == "\"ACTIVE\"" ]]; then
            echo "coord started.  state: $state"
	    return 0
        fi
        sleep 5
    done
    echo_error "coord did not start.  state: $state"
}

# Check N nodes are registered with the coordinator.  Fail after 60 retries (5 minutes).
function wait_for_workers_to_register {
    validate_environment_preconditions LOGS COORD
    [ $# -ne 1 ] && echo_error "$0 expected one argument for 'expected number of workers'"
    echo "waiting for $1 workers to register"
    local expected_num_workers=$1
    local num_workers=0
    for i in {1..60}; do
        num_workers=$(curl -s http://${COORD}:${PORT}/v1/node | jq length)
        if (( $num_workers == $expected_num_workers )); then
            echo "workers registered. num_nodes: $num_workers"
	    return 0
        fi
        sleep 5
    done
    echo_error "workers failed to register. num_nodes: $num_workers"
}

function validate_file_exists {
    if [ ! -f "$1" ]; then
        echo_error "$1 must exist in CONFIGS directory"
    fi
}

function validate_config_directory {
    validate_environment_preconditions CONFIGS
    validate_file_exists "${CONFIGS}/etc_common/jvm.config"
    validate_file_exists "${CONFIGS}/etc_common/log.properties"
    validate_file_exists "${CONFIGS}/etc_coordinator/config_native.properties"
    validate_file_exists "${CONFIGS}/etc_coordinator/node.properties"
    validate_file_exists "${CONFIGS}/etc_worker/config_native.properties"
    validate_file_exists "${CONFIGS}/etc_worker/node.properties"
    echo "configs are valid"
}

function tpch_summary_to_csv() {
  local in="$1" out="${2:-}"
  awk -v outFile="$out" '
    function trim(s){ gsub(/^[[:space:]]+|[[:space:]]+$/, "", s); return s }
    function emit(line){ if (outFile!="") print line > outFile; else print line }

    BEGIN{ inblk=0; header_done=0 }

    # Enter the summary block after this header line
    /tpch[[:space:]]+Benchmark[[:space:]]+Summary/ { inblk=1; header_done=0; next }

    # Leave the block when we hit a terminating line (e.g., "22 passed in ...")
    inblk && /passed in/ { inblk=0; exit }

    !inblk { next }

    /^[[:space:]]*$/ { next }                # skip empty
    /^[[:space:]]*-+$/ { next }               # skip separator lines
    index($0, "|")==0 { next }                # only lines with columns

    {
      n = split($0, a, /\|/)
      for (i=1; i<=n; i++) a[i]=trim(a[i])

      # Header row
      if (!header_done && $0 ~ /Query[[:space:]]+ID/) {
        line=""
        for (i=1; i<=n; i++) if (length(a[i])) line=(line?line ",":line) a[i]
        emit(line)
        header_done=1
        next
      }

      # Data rows (Q1, Q2, ...)
      cols=0; delete col
      for (i=1; i<=n; i++) if (length(a[i])) col[++cols]=a[i]
      if (cols>=2 && col[1] ~ /^Q[0-9]+$/) {
        # Emit exactly 6 columns: Query ID, Avg, Min, Max, Median, GMean (if present)
        emit(col[1] "," col[2] "," col[3] "," col[4] "," col[5] "," col[6])
      }
    }
  ' "$in"
}

function generate_json() {
    local kind="single-node"
    if (( $NUM_WORKERS > 1 )); then
	kind="${NUM_WORKERS}-node"
    fi
    local timestamp=$(date +"%Y-%m-%dT%H:%M:%SZ")
    local gpu=$(grep "GPU 0: NVIDIA [^ ]* " ${OUTPUT_PREFIX}/logs/worker_0.log | sed "s/GPU 0: NVIDIA \([^ ]*\) .*/\1/g")
    echo "GPU = $gpu"

    jq --null-input \
       --arg kind "$kind" \
       --arg benchmark "tpch" \
       --arg timestamp "$timestamp" \
       --arg num_workers "$NUM_WORKERS" \
       --arg scale_factor "$SCALE_FACTOR" \
       --arg num_drivers "$NUM_DRIVERS" \
       --arg image_name "$WORKER_IMAGE" \
       --arg gpu_name "$gpu" \
       --arg engine_name "velox" \
  '{
    kind: $kind,
    benchmark: $benchmark,
    timestamp: $timestamp,
    execution_number: 1,
    n_workers: ($num_workers | tonumber),
    scale_factor: ($scale_factor | tonumber),
    gpu_count: ($num_workers | tonumber),
    num_drivers: ($num_drivers | tonumber),
    worker_image: $image_name,
    gpu_name: $gpu_name,
    engine: $engine_name
  }' > ${OUTPUT_PREFIX}/benchmark.json
}

# Create a new output directory within the date structure.
# This will be an incremented value based on what is already present.
function create_output_prefix() {
    [ $# -ne 1 ] && echo_error "$0 expected arguments 'output_dir'"
    local output_dir=$1
    pushd $output_dir
    for ((i=1; i<=99; i++)); do
	local candidate=$(printf "%02d" "$i")
	if [[ ! -e "$candidate" ]]; then
	    OUTPUT_PREFIX=$(printf "%02d" "$i")
	    break
	fi
    done
    echo "$PWD output_prefix: $OUTPUT_PREFIX"
    popd
}

# Push results to gitlab.
function push_csv() {
    local results_dir="${SCRIPT_DIR_FUNCS}/results_dir"
    local timestamp="$(date +%Y%m%d_%H%M%S)"
    local run_dir="${results_dir}/run_${timestamp}_scale${SCALE_FACTOR}"

    echo "Collecting results to: ${run_dir}"
    mkdir -p ${run_dir}

    # Copy result_dir if it exists
    if [ -d "${SCRIPT_DIR_FUNCS}/result_dir" ]; then
        cp -r ${SCRIPT_DIR_FUNCS}/result_dir ${run_dir}/
    fi

    # Copy logs
    if [ -d "${LOGS}" ]; then
        cp -r ${LOGS} ${run_dir}/
    fi

    # Copy slurm output files from the job directory
    if [ -n "${SLURM_JOB_ID}" ]; then
        cp ${SCRIPT_DIR_FUNCS}/presto-tpch-run_${SLURM_JOB_ID}.out ${run_dir}/ 2>/dev/null || true
        cp ${SCRIPT_DIR_FUNCS}/presto-tpch-run_${SLURM_JOB_ID}.err ${run_dir}/ 2>/dev/null || true
    fi

    # Copy configs
    mkdir -p ${run_dir}/configs
    cp ${CONFIGS}/etc_coordinator/config_native.properties ${run_dir}/configs/coordinator.config 2>/dev/null || true
    cp ${CONFIGS}/etc_worker_0/config_native.properties ${run_dir}/configs/worker.config 2>/dev/null || true

    echo "Results saved to: ${run_dir}"
}
