#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Validates job preconditions and assigns default values for presto execution.
function setup {
    [ -z "$SLURM_JOB_NAME" ] && echo "required argument '--job-name' not specified" && exit 1
    [ -z "$SLURM_JOB_ACCOUNT" ] && echo "required argument '--account' not specified" && exit 1
    [ -z "$SLURM_JOB_PARTITION" ] && echo "required argument '--partition' not specified" && exit 1
    [ -z "$SLURM_NNODES" ] && echo "required argument '--nodes' not specified" && exit 1
    [ -z "$IMAGE_DIR" ] && echo "IMAGE_DIR must be set" && exit 1
    [ -z "$LOGS" ] && echo "LOGS must be set" && exit 1
    [ -z "$CONFIGS" ] && echo "CONFIGS must be set" && exit 1
    [ -z "$NUM_NODES" ] && echo "NUM_NODES must be set" && exit 1
    [ -z "$NUM_GPUS_PER_NODE" ] && echo "NUM_GPUS_PER_NODE env variable must be set" && exit 1
    [ ! -d "$VT_ROOT" ] && echo "VT_ROOT must be a valid directory" && exit 1
    [ ! -d "$DATA" ] && echo "DATA must be a valid directory" && exit 1

    if [ ! -d ${VT_ROOT}/.hive_metastore ]; then
        echo "Copying hive metastore from data source."
        copy_hive_metastore
    else
        echo "Hive metastore already exists.  Reusing."
    fi

    [ ! -d ${VT_ROOT}/.hive_metastore/tpchsf${SCALE_FACTOR} ] && echo "Schema for SF ${SCALE_FACTOR} does not exist in hive metastore." && exit 1

    generate_configs

    validate_config_directory
}

function generate_configs {
    mkdir -p ${CONFIGS}
    pushd ${VT_ROOT}/presto/scripts
    OVERWRITE_CONFIG=true ./generate_presto_config.sh
    popd
    # These options are require to run in some cluster contexts.
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
    validate_environment_preconditions LOGS CONFIGS VT_ROOT COORD DATA COORD_IMAGE
    local script=$1
    local type=$2
    [ "$type" != "coord" ] && [ "$type" != "cli" ] && echo_error "coord type must be coord/cli"
    local log_file="${type}.log"

    local coord_image="${IMAGE_DIR}/${COORD_IMAGE}.sqsh"
    [ ! -f "${coord_image}" ] && echo_error "coord image does not exist at ${coord_image}"

    # Coordinator runs as a background process, whereas we want to wait for cli
    # so that the job will finish when the cli is done (terminating background
    # processes like the coordinator and workers).
    if [ "${type}" == "coord" ]; then
        srun -w $COORD --ntasks=1 --overlap \
--container-image=${coord_image} \
--export=ALL,JAVA_HOME=/usr/lib/jvm/jre-17-openjdk \
--container-env=JAVA_HOME=/usr/lib/jvm/jre-17-openjdk \
--container-env=PATH=/usr/lib/jvm/jre-17-openjdk/bin:$PATH \
--container-mounts=${VT_ROOT}:/workspace,\
${CONFIGS}/etc_common:/opt/presto-server/etc,\
${CONFIGS}/etc_coordinator/node.properties:/opt/presto-server/etc/node.properties,\
${CONFIGS}/etc_coordinator/config_native.properties:/opt/presto-server/etc/config.properties,\
${CONFIGS}/etc_coordinator/catalog/hive.properties:/opt/presto-server/etc/catalog/hive.properties,\
${DATA}:/var/lib/presto/data/hive/data/user_data,\
${VT_ROOT}/.hive_metastore:/var/lib/presto/data/hive/metastore \
-- bash -lc "unset JAVA_HOME; export JAVA_HOME=/usr/lib/jvm/jre-17-openjdk; export PATH=/usr/lib/jvm/jre-17-openjdk/bin:\$PATH; ${script}" >> ${LOGS}/${log_file} 2>&1 &
    else
        srun -w $COORD --ntasks=1 --overlap \
--container-image=${coord_image} \
--export=ALL,JAVA_HOME=/usr/lib/jvm/jre-17-openjdk \
--container-env=JAVA_HOME=/usr/lib/jvm/jre-17-openjdk \
--container-env=PATH=/usr/lib/jvm/jre-17-openjdk/bin:$PATH \
--container-mounts=${VT_ROOT}:/workspace,\
${CONFIGS}/etc_common:/opt/presto-server/etc,\
${CONFIGS}/etc_coordinator/node.properties:/opt/presto-server/etc/node.properties,\
${CONFIGS}/etc_coordinator/config_native.properties:/opt/presto-server/etc/config.properties,\
${CONFIGS}/etc_coordinator/catalog/hive.properties:/opt/presto-server/etc/catalog/hive.properties,\
${DATA}:/var/lib/presto/data/hive/data/user_data,\
${VT_ROOT}/.hive_metastore:/var/lib/presto/data/hive/metastore \
-- bash -lc "unset JAVA_HOME; export JAVA_HOME=/usr/lib/jvm/jre-17-openjdk; export PATH=/usr/lib/jvm/jre-17-openjdk/bin:\$PATH; ${script}" >> ${LOGS}/${log_file} 2>&1
    fi
}

# Runs a coordinator on a specific node with default configurations.
# Overrides the config files with the coord node and other needed updates.
function run_coordinator {
    validate_environment_preconditions CONFIGS
    local coord_config="${CONFIGS}/etc_coordinator/config_native.properties"

    # Update configs with assigned node address and port.
    sed -i "s+discovery\.uri.*+discovery\.uri=http://${COORD}:${PORT}+g" ${coord_config}
    sed -i "s+http-server\.http\.port=.*+http-server\.http\.port=${PORT}+g" ${coord_config}

    mkdir -p ${VT_ROOT}/.hive_metastore

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
    validate_environment_preconditions LOGS CONFIGS VT_ROOT COORD CUDF_LIB DATA

    local gpu_id=$1 image=$2 node=$3 worker_id=$4
    echo "running worker ${worker_id} with image ${image} on node ${node} with gpu_id ${gpu_id}"

    local worker_image="${IMAGE_DIR}/${image}.sqsh"
    [ ! -f "${worker_image}" ] && echo_error "worker image does not exist at ${worker_image}"

    # Make a copy of the worker config that can be given a unique id for this worker.
    local worker_config="${CONFIGS}/etc_worker_${worker_id}/config_native.properties"
    local worker_node="${CONFIGS}/etc_worker_${worker_id}/node.properties"
    local worker_hive="${CONFIGS}/etc_worker_${worker_id}/catalog/hive.properties"
    
    # Create worker_data directory and unique data dir per worker
    mkdir -p ${SCRIPT_DIR}/worker_data
    local worker_data="${SCRIPT_DIR}/worker_data/worker_${worker_id}"
    mkdir -p ${worker_data}
    mkdir -p ${worker_data}/hive/data/user_data

    # Each worker needs to be told how to access the coordianator
    sed -i "s+discovery\.uri.*+discovery\.uri=http://${COORD}:${PORT}+g" ${worker_config}
    mkdir -p ${VT_ROOT}/.hive_metastore
    
    # Create profiles directory for profiling output
    mkdir -p ${SCRIPT_DIR}/profiles
    
    # Create worker info directory and save worker info for profiling commands
    mkdir -p ${SCRIPT_DIR}/worker_info
    local worker_info_file="${SCRIPT_DIR}/worker_info/worker_${worker_id}.info"
    echo "WORKER_NODE=${node}" > "${worker_info_file}"
    echo "WORKER_IMAGE=${image}" >> "${worker_info_file}"
    
    # Build container mounts
    local container_mounts="${VT_ROOT}:/workspace,\
${CONFIGS}/etc_common:/opt/presto-server/etc,\
${worker_node}:/opt/presto-server/etc/node.properties,\
${worker_config}:/opt/presto-server/etc/config.properties,\
${worker_hive}:/opt/presto-server/etc/catalog/hive.properties,\
${worker_data}:/var/lib/presto/data,\
${DATA}:/var/lib/presto/data/hive/data/user_data,\
${VT_ROOT}/.hive_metastore:/var/lib/presto/data/hive/metastore,\
${SCRIPT_DIR}/profiles:/presto_profiles,\
${SCRIPT_DIR}/worker_info:/worker_info"
    
    # Build the presto server command
    local presto_cmd="/usr/bin/presto_server --etc-dir=/opt/presto-server/etc"
    
    # If profiling is enabled, start presto_server and then attach nsys profiling
    # We use nsys start/stop instead of nsys profile because:
    # 1. nsys stop can be called explicitly to write the file even if process is killed
    # 2. This gives us more control over when profiling stops and files are written
    if [ "${ENABLE_PROFILING:-false}" == "true" ]; then
        local target_output="/presto_profiles/worker_${worker_id}.nsys-rep"
        local stop_signal_file="/presto_profiles/stop_profiling_${worker_id}"
        # Build as a single-line command with semicolons to avoid parsing issues
        presto_cmd="mkdir -p /presto_profiles /worker_info && if command -v nsys >/dev/null 2>&1; then echo 'Starting presto_server for worker ${worker_id}' >&2; /usr/bin/presto_server --etc-dir=/opt/presto-server/etc & presto_pid=\$!; echo \$presto_pid > /worker_info/worker_${worker_id}_pid.txt; sleep 20; echo 'Starting nsys profiling for worker ${worker_id} (PID: '\$presto_pid')' >&2; nsys start --gpu-metrics-devices=cuda-visible -o ${target_output} 2>&1 || echo 'WARNING: nsys start failed' >&2; trap 'echo \"Stopping nsys profiling (trap)...\" >&2; nsys stop 2>&1 || true; rm -f ${stop_signal_file} 2>/dev/null || true' EXIT TERM INT; (echo 'Monitoring loop started for worker ${worker_id}' >&2; while true; do if [ -f ${stop_signal_file} ]; then echo 'Stop signal file detected for worker ${worker_id}, stopping nsys profiling...' >&2; nsys stop 2>&1 || echo 'WARNING: nsys stop failed' >&2; rm -f ${stop_signal_file} 2>/dev/null || true; echo 'nsys profiling stopped for worker ${worker_id}' >&2; break; fi; sleep 1; done) & monitor_pid=\$!; echo 'Waiting for presto_server (PID: '\$presto_pid')...' >&2; wait \$presto_pid; kill \$monitor_pid 2>/dev/null || true; else echo 'WARNING: nsys not found in container, running without profiling' >&2; /usr/bin/presto_server --etc-dir=/opt/presto-server/etc; fi"
    fi

    # Need to fix this to run with cpu nodes as well.
    # Run the worker with the new configs.
    # Use --overlap to allow multiple srun commands from same job
    # Don't use --gres=gpu:1 here since the job already allocated GPUs
    # Set CUDA_VISIBLE_DEVICES explicitly in bash command to override SLURM default
    srun -N1 -w $node --ntasks=1 --overlap \
        --container-image=${worker_image} \
        --export=ALL \
        --container-env=LD_LIBRARY_PATH="/usr/lib64/presto-native-libs:/usr/local/lib:/usr/lib64" \
        --container-env=LD_LIBRARY_PATH="$CUDF_LIB:$LD_LIBRARY_PATH" \
        --container-env=GLOG_vmodule=IntraNodeTransferRegistry=3,ExchangeOperator=3 \
        --container-env=GLOG_logtostderr=1 \
        --container-mounts=${container_mounts} \
        -- /bin/bash -c "export CUDA_VISIBLE_DEVICES=${gpu_id}; echo \"CUDA_VISIBLE_DEVICES=\$CUDA_VISIBLE_DEVICES\"; echo \"--- Environment Variables ---\"; set | grep -E 'UCX_|CUDA_VISIBLE_DEVICES'; nvidia-smi -L; ${presto_cmd}" > ${LOGS}/worker_${worker_id}.log 2>&1 &
}

function copy_hive_metastore {
    cp -r /mnt/data/tpch-rs/HIVE-METASTORE-MG-260313 ${VT_ROOT}/.hive_metastore
}

#./analyze_tables.sh --port $PORT --hostname $HOSTNAME -s tpchsf${scale_factor}
function setup_benchmark {
    echo "setting up benchmark"
    [ $# -ne 1 ] && echo_error "$0 expected one argument for 'scale factor'"
    local scale_factor=$1
    local data_path="/data/date-scale-${scale_factor}"
    run_coord_image "export PORT=$PORT; export HOSTNAME=$COORD; export PRESTO_DATA_DIR=/var/lib/presto/data/hive/data/user_data; yum install python3.12 -y; yum install jq -y; cd /workspace/presto/scripts; ./setup_benchmark_tables.sh -b tpch -d date-scale-${scale_factor} -s tpchsf${scale_factor} --skip-analyze-tables --no-docker; " "cli"

    # Copy the hive metastore from a local copy.  This means we don't have to create
    # or analyze the tables.
    for dataset in $(ls ${SCRIPT_DIR}/ANALYZED_HIVE_METASTORE); do
	if [[ -d ${VT_ROOT}/.hive_metastore/${dataset} ]]; then
	    echo "replacing dataset metadata: $dataset"
	    cp -r ${SCRIPT_DIR}/ANALYZED_HIVE_METASTORE/${dataset} ${VT_ROOT}/.hive_metastore/
	    for table in $(ls ${VT_ROOT}/.hive_metastore/${dataset}); do
		# Need to remove checksum file (it will be recreated).
		if [ -f ${VT_ROOT}/.hive_metastore/${dataset}/${table}/..prestoSchema.crc ]; then
		    rm ${VT_ROOT}/.hive_metastore/${dataset}/${table}/..prestoSchema.crc
		fi
	    done
        fi
    done
}

# Run a cli node that will connect to the coordinator and run queries from queries.sql
# Results are stored in cli.log.
function run_queries {
    echo "running queries"
    [ $# -ne 2 ] && echo_error "$0 expected two arguments for '<iterations>' and '<scale_factor>'"
    local num_iterations=$1
    local scale_factor=$2
    
    # Build profiling command
    # Note: When profiling is enabled, workers are already wrapped with nsys launch
    # So we don't need per-query profiling - profiles are created for the entire worker lifetime
    local benchmark_cmd="./run_benchmark.sh -q 1 -b tpch -s tpchsf${scale_factor} -i ${num_iterations} \
        --hostname ${COORD} --port $PORT -o /workspace/presto/slurm/presto-nvl72/result_dir --skip-drop-cache"
    
    echo "ENABLE_PROFILING=${ENABLE_PROFILING:-false}"
    if [ "${ENABLE_PROFILING:-false}" == "true" ]; then
        echo "Profiling enabled - workers are wrapped with nsys launch"
        echo "Profiles will be created for entire worker lifetime (not per-query)"
        echo "Profile files will be at: ${SCRIPT_DIR}/profiles/worker_*.nsys-rep"
        # Don't add --profile flag since we're using nsys launch on workers instead
    else
        echo "Profiling disabled (ENABLE_PROFILING=${ENABLE_PROFILING:-false})"
    fi
    echo "Benchmark command: ${benchmark_cmd}"
    
    # We currently skip dropping cache because it requires docker (not available on the cluster).
    # Note: SCRIPT_DIR must be set to the slurm directory so profiler functions can find worker info files
    # Also mount the host's /usr/bin so srun might be accessible (though this may not work in all setups)
    run_coord_image "export PORT=$PORT; \
    export HOSTNAME=$COORD; \
    export PRESTO_DATA_DIR=/var/lib/presto/data/hive/data/user_data; \
    export SCRIPT_DIR=/workspace/presto/slurm/presto-nvl72; \
    export VT_ROOT=/workspace; \
    export IMAGE_DIR=${IMAGE_DIR}; \
    export NUM_WORKERS=${NUM_WORKERS}; \
    export SLURM_JOB_NODELIST=${SLURM_JOB_NODELIST}; \
    yum install python3.12 jq -y > /dev/null; \
    cd /workspace/presto/scripts; \
    ${benchmark_cmd}" "cli"
}

# Signal workers to stop profiling (nsys stop) and wait for profile files to be generated.
# This does NOT shut down workers or presto servers - that happens after the job completes.
function stop_workers {
    validate_environment_preconditions COORD SCRIPT_DIR
    
    if [ "${ENABLE_PROFILING:-false}" != "true" ]; then
        echo "Profiling not enabled, skipping stop_workers"
        return 0
    fi
    
    echo "Signaling workers to stop profiling (nsys stop)..."
    
    # Ensure profiles directory exists
    mkdir -p ${SCRIPT_DIR}/profiles
    
    # Find all worker info files to determine which workers exist
    local worker_info_dir="${SCRIPT_DIR}/worker_info"
    if [ ! -d "${worker_info_dir}" ]; then
        echo "Warning: Worker info directory not found at ${worker_info_dir}, cannot stop profiling"
        return 1
    fi
    
    local worker_info_files=($(find "${worker_info_dir}" -name "worker_*.info" -type f 2>/dev/null | sort -V))
    
    if [ ${#worker_info_files[@]} -eq 0 ]; then
        echo "Warning: No worker info files found in ${worker_info_dir}, cannot stop profiling"
        return 1
    fi
    
    # Signal each worker to run nsys stop by creating a stop signal file
    # The worker script periodically checks for this file and runs nsys stop when it finds it
    local worker_ids=()
    for worker_info_file in "${worker_info_files[@]}"; do
        # Extract worker_id from filename (e.g., worker_0.info -> 0)
        local worker_id=$(basename "$worker_info_file" | sed 's/^worker_//; s/\.info$//')
        worker_ids+=("$worker_id")
        
        local stop_signal_file="${SCRIPT_DIR}/profiles/stop_profiling_${worker_id}"
        
        echo "  Creating stop signal for worker ${worker_id} at ${stop_signal_file}..."
        touch "${stop_signal_file}"
        if [ -f "${stop_signal_file}" ]; then
            echo "    Stop signal file created successfully"
        else
            echo "    WARNING: Failed to create stop signal file"
        fi
    done
    
    # Give workers a moment to detect the signal files and run nsys stop
    echo "  Waiting 5 seconds for workers to detect stop signals and run nsys stop..."
    sleep 5
    
    # Wait for profile files to be generated
    echo "Waiting for profile files to be generated..."
    echo "  Checking in directory: ${SCRIPT_DIR}/profiles"
    echo "  Looking for ${#worker_ids[@]} profile files"
    local max_wait=120  # Maximum wait time in seconds (increased for nsys stop to complete)
    local wait_interval=2  # Check every 2 seconds
    local waited=0
    local all_profiles_ready=false
    local last_ready_count=0
    
    while [ $waited -lt $max_wait ]; do
        local profiles_ready=0
        local profiles_missing=()
        
        for worker_id in "${worker_ids[@]}"; do
            local profile_file="${SCRIPT_DIR}/profiles/worker_${worker_id}.nsys-rep"
            if [ -f "$profile_file" ] && [ -s "$profile_file" ]; then
                ((profiles_ready++))
            else
                profiles_missing+=("$worker_id")
            fi
        done
        
        if [ $profiles_ready -eq ${#worker_ids[@]} ]; then
            all_profiles_ready=true
            break
        fi
        
        # Print progress whenever the count changes, or every 5 seconds
        if [ $profiles_ready -ne $last_ready_count ] || [ $((waited % 5)) -eq 0 ]; then
            echo "  Waiting for profiles... (${profiles_ready}/${#worker_ids[@]} ready, waited ${waited}s)"
            if [ ${#profiles_missing[@]} -gt 0 ] && [ ${#profiles_missing[@]} -le 10 ]; then
                echo "    Missing profiles for workers: ${profiles_missing[*]}"
            elif [ ${#profiles_missing[@]} -gt 10 ]; then
                echo "    Missing profiles for ${#profiles_missing[@]} workers"
            fi
            last_ready_count=$profiles_ready
        fi
        
        sleep $wait_interval
        waited=$((waited + wait_interval))
    done
    
    # Final check - list what we actually found
    echo "Final profile file check:"
    local found_files=($(find "${SCRIPT_DIR}/profiles" -name "worker_*.nsys-rep" -type f 2>/dev/null | sort -V))
    echo "  Found ${#found_files[@]} profile file(s) in ${SCRIPT_DIR}/profiles"
    if [ ${#found_files[@]} -gt 0 ]; then
        for file in "${found_files[@]}"; do
            local size=$(du -h "$file" 2>/dev/null | cut -f1 || echo "unknown")
            echo "    $(basename "$file"): ${size}"
        done
    fi
    
    if [ "$all_profiles_ready" = true ]; then
        echo "All profile files generated successfully:"
        for worker_id in "${worker_ids[@]}"; do
            local profile_file="${SCRIPT_DIR}/profiles/worker_${worker_id}.nsys-rep"
            if [ -f "$profile_file" ]; then
                local size=$(du -h "$profile_file" | cut -f1)
                echo "  worker_${worker_id}.nsys-rep: ${size}"
            fi
        done
    else
        echo "Warning: Not all profile files were generated within ${max_wait}s"
        echo "  Generated: ${profiles_ready}/${#worker_ids[@]}"
        for worker_id in "${worker_ids[@]}"; do
            local profile_file="${SCRIPT_DIR}/profiles/worker_${worker_id}.nsys-rep"
            if [ ! -f "$profile_file" ] || [ ! -s "$profile_file" ]; then
                echo "    Missing: worker_${worker_id}.nsys-rep"
            fi
        done
    fi
    
    echo "Profiling stopped (workers and presto servers remain running)"
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
    local results_dir="${SCRIPT_DIR}/results_dir"
    local timestamp="$(date +%Y%m%d_%H%M%S)"
    local run_dir="${results_dir}/run_${timestamp}_scale${SCALE_FACTOR}"

    echo "Collecting results to: ${run_dir}"
    mkdir -p ${run_dir}

    # Copy result_dir if it exists
    if [ -d "${results_dir}" ]; then
        cp -r ${results_dir} ${run_dir}/
    fi

    # Copy logs
    if [ -d "${LOGS}" ]; then
        cp -r ${LOGS} ${run_dir}/
    fi

    # Copy profiles if they exist
    if [ -d "${SCRIPT_DIR}/profiles" ] && [ "$(ls -A ${SCRIPT_DIR}/profiles 2>/dev/null)" ]; then
        cp -r ${SCRIPT_DIR}/profiles ${run_dir}/ 2>/dev/null || true
    fi

    # Copy slurm output files from the job directory
    if [ -n "${SLURM_JOB_ID}" ]; then
        cp ${SCRIPT_DIR}/presto-tpch-run_${SLURM_JOB_ID}.out ${run_dir}/ 2>/dev/null || true
        cp ${SCRIPT_DIR}/presto-tpch-run_${SLURM_JOB_ID}.err ${run_dir}/ 2>/dev/null || true
    fi

    # Copy configs
    mkdir -p ${run_dir}/configs
    cp ${CONFIGS}/etc_coordinator/config_native.properties ${run_dir}/configs/coordinator.config 2>/dev/null || true
    cp ${CONFIGS}/etc_worker_0/config_native.properties ${run_dir}/configs/worker.config 2>/dev/null || true

    # Clean up worker info directory
    rm -rf ${SCRIPT_DIR}/worker_info 2>/dev/null || true

    echo "Results saved to: ${run_dir}"
}
