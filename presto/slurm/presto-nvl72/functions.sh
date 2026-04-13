#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Validates job preconditions and assigns default values for presto execution.
function setup {
    [ -z "${SLURM_JOB_NAME:-}" ] && echo "required argument '--job-name' not specified" && exit 1
    [ -z "${SLURM_JOB_ACCOUNT:-}" ] && echo "warning: '--account' not specified"
    [ -z "${SLURM_JOB_PARTITION:-}" ] && echo "warning: '--partition' not specified"
    [ -z "${SLURM_NNODES:-}" ] && echo "required argument '--nodes' not specified" && exit 1
    [ -z "$IMAGE_DIR" ] && echo "IMAGE_DIR must be set" && exit 1
    [ -z "$LOGS" ] && echo "LOGS must be set" && exit 1
    [ -z "$CONFIGS" ] && echo "CONFIGS must be set" && exit 1
    [ -z "$NUM_NODES" ] && echo "NUM_NODES must be set" && exit 1
    [ -z "$NUM_GPUS_PER_NODE" ] && echo "NUM_GPUS_PER_NODE env variable must be set" && exit 1
    [ ! -d "$VT_ROOT" ] && echo "VT_ROOT must be a valid directory" && exit 1
    [ ! -d "$DATA" ] && echo "DATA must be a valid directory" && exit 1

    #if [ ! -d ${VT_ROOT}/.hive_metastore ]; then
    #    echo "Copying hive metastore from data source."
    #    copy_hive_metastore
    #else
    #    echo "Hive metastore already exists.  Reusing."
    #fi

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

    # Provide a writable base data directory for the coordinator so that the
    # Presto launcher can create /var/lib/presto/data/var (PID file, etc.).
    # Workers do the same via worker_data_N; without this mount the squash
    # image filesystem is read-only and the launcher fails with EROFS.
    local coord_data="${SCRIPT_DIR}/coord_data"
    mkdir -p "${coord_data}"

    # Miniforge is installed at ${VT_ROOT}/miniforge3. Its conda/python scripts
    # have shebangs hardcoded to the host-absolute install path. We bind-mount
    # miniforge at that same absolute path inside the container so shebangs
    # resolve correctly regardless of where /workspace points.
    local miniforge_dir="${VT_ROOT}/miniforge3"
    local extra_mounts=""
    if [ -d "${miniforge_dir}" ]; then
        extra_mounts=",${miniforge_dir}:${miniforge_dir}"
    fi
    if [ -d "/scratch" ]; then
        extra_mounts="${extra_mounts},/scratch:/scratch"
    fi

    # Coordinator runs as a background process, whereas we want to wait for cli
    # so that the job will finish when the cli is done (terminating background
    # processes like the coordinator and workers).
    if [ "${type}" == "coord" ]; then
        srun -w $COORD --ntasks=1 --overlap \
--container-image=${coord_image} \
--container-remap-root \
--export=ALL \
--container-mounts=${VT_ROOT}:/workspace,\
${coord_data}:/var/lib/presto/data,\
${CONFIGS}/etc_common:/opt/presto-server/etc,\
${CONFIGS}/etc_coordinator/node.properties:/opt/presto-server/etc/node.properties,\
${CONFIGS}/etc_coordinator/config_native.properties:/opt/presto-server/etc/config.properties,\
${CONFIGS}/etc_coordinator/catalog/hive.properties:/opt/presto-server/etc/catalog/hive.properties,\
${DATA}:/var/lib/presto/data/hive/data/user_data,\
${VT_ROOT}/.hive_metastore:/var/lib/presto/data/hive/metastore${extra_mounts} \
-- bash -lc "unset JAVA_HOME; export JAVA_HOME=/usr/lib/jvm/jre-17-openjdk; export PATH=/usr/lib/jvm/jre-17-openjdk/bin:\$PATH; ${script}" >> ${LOGS}/${log_file} 2>&1 &
    else
        srun -w $COORD --ntasks=1 --overlap \
--container-remap-root \
--container-image=${coord_image} \
--export=ALL \
--container-mounts=${VT_ROOT}:/workspace,\
${coord_data}:/var/lib/presto/data,\
${CONFIGS}/etc_common:/opt/presto-server/etc,\
${CONFIGS}/etc_coordinator/node.properties:/opt/presto-server/etc/node.properties,\
${CONFIGS}/etc_coordinator/config_native.properties:/opt/presto-server/etc/config.properties,\
${CONFIGS}/etc_coordinator/catalog/hive.properties:/opt/presto-server/etc/catalog/hive.properties,\
${DATA}:/var/lib/presto/data/hive/data/user_data,\
${VT_ROOT}/.hive_metastore:/var/lib/presto/data/hive/metastore${extra_mounts} \
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
    # Assign NUMA node based on GPU ID: GPUs 0-1 → node 0, GPUs 2-3 → node 1, etc.
    local numa_node=$((gpu_id / 2))
    echo "running worker ${worker_id} with image ${image} on node ${node} with gpu_id ${gpu_id} numa_node ${numa_node}"

    local worker_image="${IMAGE_DIR}/${image}.sqsh"
    [ ! -f "${worker_image}" ] && echo_error "worker image does not exist at ${worker_image}"

    # Make a copy of the worker config that can be given a unique id for this worker.
    local worker_config="${CONFIGS}/etc_worker_${worker_id}/config_native.properties"
    local worker_node="${CONFIGS}/etc_worker_${worker_id}/node.properties"
    local worker_hive="${CONFIGS}/etc_worker_${worker_id}/catalog/hive.properties"
    local worker_data="${SCRIPT_DIR}/worker_data_${worker_id}"

    # Each worker needs to be told how to access the coordianator
    sed -i "s+discovery\.uri.*+discovery\.uri=http://${COORD}:${PORT}+g" ${worker_config}

    # Create unique data dir per worker.
    mkdir -p ${worker_data}
    mkdir -p ${worker_data}/hive/data/user_data
    mkdir -p ${VT_ROOT}/.hive_metastore

    local vt_cufile_log_dir="/var/log/cufile"
    local vt_cufile_log="${vt_cufile_log_dir}/cufile_worker_${worker_id}.log"

    local gds_mounts=""
    function add_gds_sys_path {
        local path="${1:?Path argument missing}"
        local read_only="${2:-0}"

        # System file path must exist
        if [[ ! -e ${path} ]]; then
            echo "${path} required by GDS does not exist"
            exit 1
        fi

        # If gds_mounts is not empty, append a comma
        [[ -n "${gds_mounts}" ]] && gds_mounts+=","

        # Append path
        gds_mounts+="${path}:${path}"
        if [[ "${read_only}" == "1" ]]; then
            gds_mounts+=":ro"
        fi
    }

    if [[ "${ENABLE_GDS}" == "1" ]]; then
        # Add GDS-required system paths
        add_gds_sys_path "/run/udev" 1
        add_gds_sys_path "/dev/infiniband"
        add_gds_sys_path "/etc/cufile.json" 1
        for dev in /dev/nvidia-fs*; do
            # If file exists, append the path, otherwise, exit the loop
            [[ -e "${dev}" ]] || continue
            add_gds_sys_path "${dev}"
        done
    fi

    local nsys_bin=""
    local nsys_launch_opts=""
    local vt_nsys_report_dir="/var/log/nsys"
    if [[ "${ENABLE_NSYS}" == "1" && "${worker_id}" == "0" ]]; then
        nsys_bin="/opt/nvidia/nsight-systems-cli/2026.2.1/bin/nsys"
        nsys_launch_opts="-t nvtx,cuda,osrt,ucx \
        --cuda-memory-usage=true \
        --cuda-um-cpu-page-faults=true \
        --cuda-um-gpu-page-faults=true"
    fi

    # The parent SLURM job allocates --gres=gpu:NUM_GPUS_PER_NODE so all GPU kernel
    # capabilities are already set up for the job cgroup.  Do NOT use --gres=gpu:1
    # on the step: it restricts the step's cgroup to one GPU and then nvidia-container-cli
    # rejects NVIDIA_VISIBLE_DEVICES values for other GPUs as "unknown device".
    #
    # NVIDIA_VISIBLE_DEVICES=all triggers the enroot 98-nvidia.sh hook which calls
    # nvidia-container-cli configure --device=all --compute.  This mounts all GPU
    # devices and all required host driver libraries (580.105.08: libcuda, libnvidia-
    # gpucomp, libnvidia-nvvm, libnvidia-ptxjitcompiler, libnvidia-ml, etc.) and runs
    # ldconfig inside the container.  The manual libcuda bind-mount then overrides the
    # compat library with the host driver so cudaMallocAsync works.
    # CUDA_VISIBLE_DEVICES=${gpu_id} inside the container restricts each worker to
    # its assigned GPU while still allowing the CUDA driver to enumerate all devices.
    # export GLOG_vmodule=IntraNodeTransferRegistry=3,ExchangeOperator=3
    # export GLOG_logtostderr=1
    srun -N1 -w $node --ntasks=1 --overlap \
--container-image=${worker_image} \
--container-remap-root \
--export=ALL,NVIDIA_VISIBLE_DEVICES=all,NVIDIA_DRIVER_CAPABILITIES=compute,utility \
--container-mounts=${VT_ROOT}:/workspace,\
${CONFIGS}/etc_common:/opt/presto-server/etc,\
${worker_node}:/opt/presto-server/etc/node.properties,\
${worker_config}:/opt/presto-server/etc/config.properties,\
${worker_hive}:/opt/presto-server/etc/catalog/hive.properties,\
${worker_data}:/var/lib/presto/data,\
${DATA}:/var/lib/presto/data/hive/data/user_data,\
${VT_ROOT}/.hive_metastore:/var/lib/presto/data/hive/metastore,\
${LOGS}:${vt_cufile_log_dir},\
${LOGS}:${vt_nsys_report_dir},\
/usr/lib/aarch64-linux-gnu/libcuda.so.580.105.08:/usr/local/cuda-13.0/compat/libcuda.so.1,\
/usr/lib/aarch64-linux-gnu/libnvidia-ml.so.580.105.08:/usr/local/lib/libnvidia-ml.so.1\
${gds_mounts:+,${gds_mounts}} \
-- /bin/bash -c "
export LD_LIBRARY_PATH=\"${CUDF_LIB}:${LD_LIBRARY_PATH}\"
if [[ '${ENABLE_GDS}' == '1' ]]; then
    export KVIKIO_COMPAT_MODE=OFF
    export CUFILE_LOGFILE_PATH=${vt_cufile_log}
    export CUFILE_LOGGING_LEVEL=INFO
fi
if [[ '${VARIANT_TYPE}' == 'gpu' ]]; then export CUDA_VISIBLE_DEVICES=${gpu_id}; fi
echo \"Worker ${worker_id}: CUDA_VISIBLE_DEVICES=\${CUDA_VISIBLE_DEVICES:-none}, NUMA_NODE=${numa_node}\"
echo \"Worker ${worker_id}: ENABLE_GDS=\${ENABLE_GDS:-unset}\"
echo \"Worker ${worker_id}: ENABLE_NSYS=\${ENABLE_NSYS:-unset}\"
echo \"Worker ${worker_id}: KVIKIO_COMPAT_MODE=\${KVIKIO_COMPAT_MODE:-unset}\"
echo \"Worker ${worker_id}: CUFILE_LOGFILE_PATH=\${CUFILE_LOGFILE_PATH:-unset}\"

if [[ -n '${nsys_bin}' ]]; then
    (
        echo \"Worker ${worker_id}: nsys subshell started\"
        if [[ -n '${QUERIES:-}' ]]; then
            IFS=',' read -ra qlist <<< '${QUERIES}'
        else
            qlist=({1..22})
        fi
        for qnum in \"\${qlist[@]}\"; do
            qid=\"Q\${qnum}\"
            while [[ ! -f ${vt_nsys_report_dir}/.nsys_start_token_\${qid} ]]; do
                read -t 2 -r _ <<< '' || true
            done
            echo \"Worker ${worker_id}: start token found for \${qid}\"
            rm ${vt_nsys_report_dir}/.nsys_start_token_\${qid}
            ${nsys_bin} start -o ${vt_nsys_report_dir}/nsys_worker_${worker_id}_\${qid} -f true; echo \"Worker ${worker_id}: nsys start exit code: \$?\"
            echo \"Worker ${worker_id}: post-start token created for \${qid}\"
            touch ${vt_nsys_report_dir}/.nsys_started_token_\${qid}

            while [[ ! -f ${vt_nsys_report_dir}/.nsys_stop_token_\${qid} ]]; do
                read -t 2 -r _ <<< '' || true
            done
            echo \"Worker ${worker_id}: stop token found for \${qid}\"
            rm ${vt_nsys_report_dir}/.nsys_stop_token_\${qid}
            ${nsys_bin} stop; echo \"Worker ${worker_id}: nsys stop exit code: \$?\"
        done
        echo \"Worker ${worker_id}: nsys subshell done, all queries profiled\"
    ) &

    echo \"Worker ${worker_id}: Nsight System program at ${nsys_bin}\"
    echo \"Worker ${worker_id}: running nsys launch\"
    ${nsys_bin} launch ${nsys_launch_opts} /usr/bin/presto_server --etc-dir=/opt/presto-server/etc
    echo \"Worker ${worker_id}: nsys launch exited with code: \$?\"
else
    if [[ '${USE_NUMA}' == '1' ]]; then
        numactl --cpubind=${numa_node} --membind=${numa_node} /usr/bin/presto_server --etc-dir=/opt/presto-server/etc
    else
        /usr/bin/presto_server --etc-dir=/opt/presto-server/etc
    fi
fi

" > ${LOGS}/worker_${worker_id}.log 2>&1 &
}

function copy_hive_metastore {
    cp -r "${HIVE_METASTORE_SOURCE:-/mnt/data/tpch-rs/HIVE-METASTORE-MG-260313}" ${VT_ROOT}/.hive_metastore
}

#./analyze_tables.sh --port $PORT --hostname $HOSTNAME -s tpchsf${scale_factor}
function setup_benchmark {
    echo "setting up benchmark"
    [ $# -ne 1 ] && echo_error "$0 expected one argument for 'scale factor'"
    local scale_factor=$1
    local data_path="/data/date-scale-${scale_factor}"
    run_coord_image "export PRESTO_DATA_DIR=/var/lib/presto/data/hive/data/user_data; yum install python3.12 -y; yum install jq -y; cd /workspace/presto/scripts; ./setup_benchmark_tables.sh -H $COORD -p $PORT -b tpch -d date-scale-${scale_factor} -s tpchsf${scale_factor} --skip-analyze-tables --no-docker; " "cli"

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
    local extra_args=()
    [[ "${ENABLE_METRICS}" == "1" ]] && extra_args+=("-m")
    [[ "${ENABLE_NSYS}" == "1" ]] && extra_args+=("-p" "--profile-script-path" "/workspace/presto/slurm/presto-nvl72/profiler_functions.sh")
    [[ -n "${QUERIES:-}" ]] && extra_args+=("-q" "${QUERIES}")

    source "${SCRIPT_DIR}/defaults.env"
    # We currently skip dropping cache because it requires docker (not available on the cluster).
    run_coord_image "export PORT=$PORT; \
    export HOSTNAME=$COORD; \
    export PRESTO_DATA_DIR=/var/lib/presto/data/hive/data/user_data; \
    export MINIFORGE_HOME=/workspace/miniforge3; \
    export HOME=/workspace; \
    cd /workspace/presto/scripts; \
    ./run_benchmark.sh -b tpch -s tpchsf${scale_factor} -i ${num_iterations} ${extra_args[*]} \
        --hostname ${COORD} --port $PORT -o /workspace/presto/slurm/presto-nvl72/result_dir --skip-drop-cache; \
    echo 'Validating query results...'; \
    MINIFORGE_HOME=/workspace/miniforge3 /workspace/scripts/run_py_script.sh \
        -p /workspace/benchmark_reporting_tools/validate_results.py \
        /workspace/presto/slurm/presto-nvl72/result_dir/query_results \
        --expected-dir ${EXPECTED_RESULTS_BASE}/scale-${scale_factor} \
        || echo 'Warning: result validation reported failures'" "cli"
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

function collect_results {
    local result_dir="${SCRIPT_DIR}/result_dir"

    echo "Copying configs to ${result_dir}/configs/..."
    mkdir -p "${result_dir}/configs"
    cp "${CONFIGS}/etc_coordinator/config_native.properties" "${result_dir}/configs/coordinator.config"
    cp "${CONFIGS}/etc_worker_0/config_native.properties"    "${result_dir}/configs/worker.config"

    echo "Copying logs to ${result_dir}/..."
    cp "${LOGS}"/*.log "${result_dir}/"

    if [[ "${ENABLE_NSYS}" == "1" ]]; then
        echo "Copying nsys reports to ${result_dir}/..."
        cp "${LOGS}"/*.nsys-rep "${result_dir}/"
    fi
}

function inject_benchmark_metadata {
    local result_file="${SCRIPT_DIR}/result_dir/benchmark_result.json"
    if [ ! -f "${result_file}" ]; then
        echo "Warning: ${result_file} not found, skipping metadata injection"
        return
    fi

    local kind="multi-node"
    if (( NUM_WORKERS == 1 )); then
        kind="single-node"
    fi

    local timestamp
    timestamp=$(date +"%Y-%m-%dT%H:%M:%SZ")

    local gpu_name
    gpu_name=$(nvidia-smi --query-gpu=gpu_name --format=csv,noheader -i 0 2>/dev/null | head -1) || true
    gpu_name="${gpu_name:-unknown}"

    local num_drivers
    num_drivers=$(grep "^task\.max-drivers-per-task=" "${CONFIGS}/etc_worker/config_native.properties" 2>/dev/null \
                  | cut -d= -f2) || true
    num_drivers="${num_drivers:-2}"

    local cudf_enabled
    cudf_enabled=$(grep "^cudf\.enabled=" "${CONFIGS}/etc_worker/config_native.properties" 2>/dev/null \
                   | cut -d= -f2) || true
    local engine
    if [[ "${cudf_enabled}" == "true" ]]; then
        engine="presto-velox-gpu"
    else
        engine="presto-velox-cpu"
    fi

    local worker_image_path="${IMAGE_DIR}/${WORKER_IMAGE}.sqsh"
    local image_digest
    echo "Computing SHA256 of ${worker_image_path}..."
    image_digest=$(sha256sum "${worker_image_path}" | awk '{print $1}') || true
    image_digest="${image_digest:-unknown}"
    echo "Image digest: ${image_digest}"

    local tmp_file
    tmp_file=$(mktemp)
    jq --arg kind "$kind" \
       --arg timestamp "$timestamp" \
       --argjson n_workers "$NUM_WORKERS" \
       --argjson node_count "$NUM_NODES" \
       --argjson scale_factor "$SCALE_FACTOR" \
       --argjson gpu_count "$NUM_WORKERS" \
       --arg gpu_name "$gpu_name" \
       --argjson num_drivers "$num_drivers" \
       --arg worker_image "$WORKER_IMAGE" \
       --arg image_digest "$image_digest" \
       --arg engine "$engine" \
       '.context += {
           kind: $kind,
           timestamp: $timestamp,
           n_workers: $n_workers,
           node_count: $node_count,
           scale_factor: $scale_factor,
           gpu_count: $gpu_count,
           gpu_name: $gpu_name,
           num_drivers: $num_drivers,
           worker_image: $worker_image,
           image_digest: $image_digest,
           engine: $engine
       }' "${result_file}" > "${tmp_file}" && mv "${tmp_file}" "${result_file}"
    echo "Injected benchmark metadata into ${result_file}"
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
       --arg num_nodes "$NUM_NODES" \
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
    node_count: ($num_nodes | tonumber),
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

    # Copy slurm output files from the job directory
    if [ -n "${SLURM_JOB_ID}" ]; then
        cp ${SCRIPT_DIR}/presto-tpch-run_${SLURM_JOB_ID}.out ${run_dir}/ 2>/dev/null || true
        cp ${SCRIPT_DIR}/presto-tpch-run_${SLURM_JOB_ID}.err ${run_dir}/ 2>/dev/null || true
    fi

    # Copy configs
    mkdir -p ${run_dir}/configs
    cp ${CONFIGS}/etc_coordinator/config_native.properties ${run_dir}/configs/coordinator.config 2>/dev/null || true
    cp ${CONFIGS}/etc_worker_0/config_native.properties ${run_dir}/configs/worker.config 2>/dev/null || true

    echo "Results saved to: ${run_dir}"
}
