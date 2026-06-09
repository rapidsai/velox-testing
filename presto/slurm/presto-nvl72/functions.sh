#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Echo the ",<host_path>:<host_path>" bind-mount fragment for the host's
# miniforge3 install if it exists, else nothing.  Used by every container
# that needs to run Python via init_python_virtual_env / run_py_script.sh:
# miniforge's conda + python scripts have shebangs hardcoded to the host
# install path, so the same absolute path has to resolve inside the
# container.  Pair this with `MINIFORGE_HOME=/workspace/miniforge3` in the
# --export (which uses the separate VT_ROOT:/workspace mount).
miniforge_mount_arg() {
    local miniforge_dir="${VT_ROOT}/miniforge3"
    if [[ -d "${miniforge_dir}" ]]; then
        printf ',%s:%s' "${miniforge_dir}" "${miniforge_dir}"
    fi
    # Always succeed: callers like `extra_mounts="$(miniforge_mount_arg)"` run
    # under `set -e`, where a non-zero return code from this helper would
    # silently kill the calling script.
    return 0
}

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

    # If sharing is opted in (HIVE_METASTORE_VERSION set) and the local
    # snapshot is missing, try to populate it from the shared location.
    if [[ -n "${HIVE_METASTORE_VERSION:-}" && ! -d "${VT_ROOT}/.hive_metastore/tpchsf${SCALE_FACTOR}" ]]; then
        populate_hive_metastore_from_shared
    fi

    if [ ! -d "${VT_ROOT}/.hive_metastore/tpchsf${SCALE_FACTOR}" ]; then
        echo "Schema for SF ${SCALE_FACTOR} is not present in ${VT_ROOT}/.hive_metastore."
        if [[ -n "${HIVE_METASTORE_VERSION:-}" ]]; then
            echo "Shared slot $(shared_metastore_slot) is also empty; publish one by running launch-analyze-tables.sh with the same HIVE_METASTORE_VERSION set."
        else
            echo "Run launch-analyze-tables.sh -s ${SCALE_FACTOR} first, or set HIVE_METASTORE_VERSION to consume a pre-published snapshot."
        fi
        exit 1
    fi

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
    apply_config_overrides
}

# Apply per-run property overrides to the freshly-generated configs.
# Reads CONFIG_OVERRIDES="key1=val1;key2=val2;..." (semicolon-separated because
# commas are reserved by sbatch --export). Used by run-sweep.sh to permute
# knobs.
#
# Policy:
#   * Worker (etc_worker_*/config_native.properties): replace if present,
#     append if absent. The native C++ worker recognizes cudf.* keys via
#     CudfConfig.h and tolerates a few unrecognized ones.
#   * Coordinator (etc_coordinator/config_native.properties): replace-ONLY.
#     The coord on the GPU variant is Java Presto, whose config loader
#     refuses to start when it sees unknown properties. Appending e.g.
#     cudf.batch_size_min_threshold to the coord config bricks the run.
function apply_config_overrides {
    local overrides="${CONFIG_OVERRIDES:-}"
    [[ -z "$overrides" ]] && return 0
    local IFS=';'
    for pair in $overrides; do
        [[ -z "$pair" ]] && continue
        local key="${pair%%=*}"
        local val="${pair#*=}"
        for f in "${CONFIGS}"/etc_worker_*/config_native.properties; do
            [[ -f "$f" ]] || continue
            if grep -q "^${key}=" "$f"; then
                sed -i "s|^${key}=.*|${key}=${val}|" "$f"
            else
                echo "${key}=${val}" >> "$f"
            fi
        done
        local coord_cfg="${CONFIGS}/etc_coordinator/config_native.properties"
        if [[ -f "$coord_cfg" ]] && grep -q "^${key}=" "$coord_cfg"; then
            sed -i "s|^${key}=.*|${key}=${val}|" "$coord_cfg"
        fi
        echo "Applied config override: ${key}=${val}"
    done
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

    local extra_mounts
    extra_mounts="$(miniforge_mount_arg)"
    if [[ -n "${CLUSTER_EXTRA_MOUNTS:-}" ]]; then
        extra_mounts="${extra_mounts},${CLUSTER_EXTRA_MOUNTS}"
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
${LOGS}:/var/log/nsys,\
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
    : "${ENABLE_GDS:=0}"
    : "${ENABLE_NSYS:=0}"
    # Migrate legacy NSYS_WORKER_ID (single int) → NSYS_WORKER_IDS (csv).
    : "${NSYS_WORKER_IDS:=${NSYS_WORKER_ID:-0}}"

    [ $# -ne 4 ] && echo_error "$0 expected arguments 'gpu_id', 'image', 'node_id', and 'worker_id'"
    validate_environment_preconditions LOGS CONFIGS VT_ROOT COORD CUDF_LIB DATA

    local gpu_id=$1 image=$2 node=$3 worker_id=$4
    # Pair each worker with the CPU NUMA domain its GPU is attached to.
    # CLUSTER_NUMA_GPUS_PER_NODE controls how many GPUs share each NUMA domain
    # (e.g., 2 means GPUs 0-1 on NUMA 0, GPUs 2-3 on NUMA 1).
    local numa_node=$((gpu_id / ${CLUSTER_NUMA_GPUS_PER_NODE:-1}))
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

    local vt_worker_env_file="/var/worker_env_file"
    local vt_cufile_log_dir="/var/log/cufile"
    local vt_cufile_log="${vt_cufile_log_dir}/cufile_worker_${worker_id}.log"

    # GDS (GPU Direct Storage) is a GPU-only feature.  On CPU variant, skip
    # the cufile/nvidia-fs bind-mounts even when ENABLE_GDS=1 — /etc/cufile.json
    # does not exist on CPU hosts and pyxis would fail container start.
    local gds_mounts=""
    if [[ "${ENABLE_GDS}" == "1" && "${VARIANT_TYPE}" == "gpu" ]]; then
        export MELLANOX_VISIBLE_DEVICES=all
        gds_mounts="/etc/cufile.json:/etc/cufile.json:ro"
        for dev in /dev/nvidia-fs*; do
            if [[ -e "${dev}" ]]; then
                gds_mounts+=",${dev}:${dev}"
            fi
        done
    fi

    local nsys_bin=""
    local nsys_launch_opts=""
    local vt_nsys_report_dir="/var/log/nsys"
    if [[ "${ENABLE_NSYS}" == "1" && ",${NSYS_WORKER_IDS}," == *",${worker_id},"* ]]; then
        # nsys must be on PATH inside the worker container. The recommended approach is
        # a symlink in the image build:
        # ln -sf /opt/nvidia/nsight-systems-cli/<version>/bin/nsys /usr/local/bin/nsys
        nsys_bin="nsys"
        # Trace flags for `nsys launch` come from NSYS_LAUNCH_OPTS, set by
        # launch-run.sh's --nsys-launch-opts (default declared there as
        # "-t nvtx,cuda"). Empty fallback if a caller forgot to export it —
        # nsys then uses its own built-in default trace set.
        nsys_launch_opts="${NSYS_LAUNCH_OPTS:-}"
    fi

    # To re-enable verbose GLOG logging, add these flags to the srun call below
    # (note: move them inside -- bash -c "..." as exports, not --container-env,
    # since pyxis ignores key=value in --container-env):
    #   GLOG_vmodule=IntraNodeTransferRegistry=3,ExchangeOperator=3
    #   GLOG_logtostderr=1
    # Warning: GLOG_logtostderr=1 generates very large logs that can fill the disk.

    # The parent SLURM job allocates --gres=gpu:NUM_GPUS_PER_NODE so all GPU kernel
    # capabilities are already set up for the job cgroup.  Do NOT use --gres=gpu:1
    # on the step: it restricts the step's cgroup to one GPU and then nvidia-container-cli
    # rejects NVIDIA_VISIBLE_DEVICES values for other GPUs as "unknown device".
    #
    # NVIDIA_VISIBLE_DEVICES=all triggers the enroot 98-nvidia.sh hook which calls
    # nvidia-container-cli configure --device=all --compute.  This mounts all GPU
    # devices and all required host driver libraries and runs ldconfig inside the
    # container.  The manual libcuda bind-mount (when set) overrides the compat
    # library with the host driver so cudaMallocAsync works.
    # CUDA_VISIBLE_DEVICES=${gpu_id} inside the container restricts each worker to
    # its assigned GPU while still allowing the CUDA driver to enumerate all devices.
    local driver_mounts=""
    if [[ -n "${CLUSTER_LIBCUDA_HOST_PATH:-}" && -n "${CLUSTER_LIBCUDA_CONTAINER_PATH:-}" ]]; then
        driver_mounts="${driver_mounts},${CLUSTER_LIBCUDA_HOST_PATH}:${CLUSTER_LIBCUDA_CONTAINER_PATH}"
    fi
    if [[ -n "${CLUSTER_LIBNVIDIA_ML_HOST_PATH:-}" && -n "${CLUSTER_LIBNVIDIA_ML_CONTAINER_PATH:-}" ]]; then
        driver_mounts="${driver_mounts},${CLUSTER_LIBNVIDIA_ML_HOST_PATH}:${CLUSTER_LIBNVIDIA_ML_CONTAINER_PATH}"
    fi
    local worker_extra_mounts=""
    if [[ -n "${CLUSTER_EXTRA_MOUNTS:-}" ]]; then
        worker_extra_mounts=",${CLUSTER_EXTRA_MOUNTS}"
    fi

    # NVIDIA_VISIBLE_DEVICES triggers enroot's 98-nvidia.sh hook, which requires
    # nvidia-container-cli on the host.  CPU-only nodes typically don't have it,
    # so the hook fails container start.  Skip the export on the CPU variant.
    local nvidia_env=""
    if [[ "${VARIANT_TYPE}" == "gpu" ]]; then
        nvidia_env=",NVIDIA_VISIBLE_DEVICES=all,NVIDIA_DRIVER_CAPABILITIES=compute,utility"
    fi

    # Notes on nsys profiling
    #
    # In the worker container here, we use `nsys launch` to start the presto_server,
    # and later use `nsys start` and `nsys stop` to control the per-query profiling
    # session, and each query gets its own .nsys-rep file.
    # The queries are initiated in a separate cli container. So the challenge is how to
    # communicate between these two containers, so that the worker container knows the
    # exact time a query begins and ends, so as to execute `nsys start` and `nsys stop`
    # accordingly.
    #
    # Here we use file-token handshakes:
    # - A background process in the worker container:
    #   - Waits for .nsys_start_token_w<id>_<qid>
    #   - Runs `nsys start`
    #   - Creates .nsys_started_token_w<id>_<qid>
    #   - Waits for .nsys_stop_token_w<id>_<qid>
    #   - Runs `nsys stop`
    # - The pytest process in the cli container:
    #   - Creates .nsys_start_token_w<id>_<qid>
    #   - Waits for .nsys_started_token_w<id>_<qid>
    #   - Runs the query
    #   - When the query ends, creates .nsys_stop_token_w<id>_<qid>
    # - The slurm batch script (run-presto-benchmarks.sh) waits for nsys report
    #   generation to complete after pytest exits. See wait_for_nsys_report_generation
    #   for details.
    #
    # The token files live in /var/log/nsys, the same host directory bind-mounted
    # into both the worker and cli containers.

    srun -N1 -w $node --ntasks=1 --overlap \
--container-image=${worker_image} \
--container-remap-root \
--export=ALL${nvidia_env} \
--container-mounts=${VT_ROOT}:/workspace,\
${CONFIGS}/etc_common:/opt/presto-server/etc,\
${worker_node}:/opt/presto-server/etc/node.properties,\
${worker_config}:/opt/presto-server/etc/config.properties,\
${worker_hive}:/opt/presto-server/etc/catalog/hive.properties,\
${worker_data}:/var/lib/presto/data,\
${DATA}:/var/lib/presto/data/hive/data/user_data,\
${VT_ROOT}/.hive_metastore:/var/lib/presto/data/hive/metastore,\
${WORKER_ENV_FILE}:${vt_worker_env_file},\
${LOGS}:${vt_cufile_log_dir},\
${LOGS}:${vt_nsys_report_dir}${driver_mounts}${gds_mounts:+,${gds_mounts}}${worker_extra_mounts} \
-- /bin/bash -c "
export LD_LIBRARY_PATH='${CUDF_LIB}':/usr/local/lib:\${LD_LIBRARY_PATH:-}

set -a
source ${vt_worker_env_file}
set +a

if [[ '${VARIANT_TYPE}' == 'gpu' ]]; then export CUDA_VISIBLE_DEVICES=${gpu_id}; fi

if [[ '${ENABLE_GDS}' == '1' ]]; then
    export KVIKIO_COMPAT_MODE=OFF
    export CUFILE_LOGFILE_PATH=${vt_cufile_log}
    export CUFILE_LOGGING_LEVEL=INFO
else
    export KVIKIO_COMPAT_MODE=ON
fi

echo \"Worker ${worker_id}: CUDA_VISIBLE_DEVICES=\${CUDA_VISIBLE_DEVICES:-none}, NUMA_NODE=${numa_node}\"
echo \"Worker ${worker_id}: ENABLE_GDS=\${ENABLE_GDS:-unset}\"
echo \"Worker ${worker_id}: ENABLE_NSYS=\${ENABLE_NSYS:-unset}\"
echo \"Worker ${worker_id}: KVIKIO_COMPAT_MODE=\${KVIKIO_COMPAT_MODE:-unset}\"
echo \"Worker ${worker_id}: CUFILE_LOGFILE_PATH=\${CUFILE_LOGFILE_PATH:-unset}\"
echo \"Worker ${worker_id}: KVIKIO_TASK_SIZE=\${KVIKIO_TASK_SIZE:-unset}\"
echo \"Worker ${worker_id}: KVIKIO_NTHREADS=\${KVIKIO_NTHREADS:-unset}\"

if [[ -n '${nsys_bin}' ]]; then
    (
        echo \"Worker ${worker_id}: nsys subshell started\"
        if [[ -n '${QUERIES:-}' ]]; then
            IFS=',' read -ra qlist <<< '${QUERIES:-}'
        else
            qlist=({1..22})
        fi
        # PROFILE_ITERATIONS = csv of 0-based iter indices, e.g. \"1\" or \"0,1\".
        # When unset, iter_list=(\"\") — a single empty-string sentinel — which
        # gives the legacy one-session-per-query behavior (no _iterN suffix).
        if [[ -n \"\${PROFILE_ITERATIONS:-}\" ]]; then
            IFS=',' read -ra iter_list <<< \"\${PROFILE_ITERATIONS}\"
        else
            iter_list=(\"\")
        fi
        for qnum in \"\${qlist[@]}\"; do
            for iter in \"\${iter_list[@]}\"; do
                if [[ -z \"\${iter}\" ]]; then
                    qid=\"Q\${qnum}\"
                else
                    qid=\"Q\${qnum}_iter\${iter}\"
                fi
                while [[ ! -f ${vt_nsys_report_dir}/.nsys_start_token_w${worker_id}_\${qid} ]]; do
                    sleep 2
                done
                echo \"Worker ${worker_id}: start token found for \${qid}\"
                rm ${vt_nsys_report_dir}/.nsys_start_token_w${worker_id}_\${qid}
                ${nsys_bin} start -o ${vt_nsys_report_dir}/nsys_worker_w${worker_id}_\${qid} -f true
                echo \"Worker ${worker_id}: nsys start exit code: \$?\"
                echo \"Worker ${worker_id}: post-start token created for \${qid}\"
                touch ${vt_nsys_report_dir}/.nsys_started_token_w${worker_id}_\${qid}

                while [[ ! -f ${vt_nsys_report_dir}/.nsys_stop_token_w${worker_id}_\${qid} ]]; do
                    sleep 2
                done
                echo \"Worker ${worker_id}: stop token found for \${qid}\"
                rm ${vt_nsys_report_dir}/.nsys_stop_token_w${worker_id}_\${qid}
                ${nsys_bin} stop
                echo \"Worker ${worker_id}: nsys stop exit code: \$?\"
            done
        done
        echo \"Worker ${worker_id}: nsys subshell done, all sessions profiled\"
    ) &

    echo \"Worker ${worker_id}: Nsight System program at ${nsys_bin}\"
    echo \"Worker ${worker_id}: running nsys launch\"
    if [[ '${USE_NUMA}' == '1' ]]; then
        ${nsys_bin} launch ${nsys_launch_opts} numactl --cpubind=${numa_node} --membind=${numa_node} /usr/bin/presto_server --etc-dir=/opt/presto-server/etc
    else
        ${nsys_bin} launch ${nsys_launch_opts} /usr/bin/presto_server --etc-dir=/opt/presto-server/etc
    fi
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

# ----------------------------------------------------------------------------
# Cluster lifecycle
# ----------------------------------------------------------------------------
# Start the coordinator, wait for it to come up, fan out NUM_GPUS_PER_NODE
# workers per node across $SLURM_JOB_NODELIST, then block until all
# $NUM_WORKERS register.  Used by both the benchmark and analyze flows.
function start_cluster {
    echo "Starting Presto coordinator on ${COORD}..."
    run_coordinator
    wait_until_coordinator_is_running

    echo "Starting ${NUM_WORKERS} Presto workers across ${NUM_NODES} nodes..."
    local worker_id=0 node gpu_id
    for node in $(scontrol show hostnames "$SLURM_JOB_NODELIST"); do
        for gpu_id in $(seq 0 $((NUM_GPUS_PER_NODE - 1))); do
            echo "  Starting worker ${worker_id} on node ${node} GPU ${gpu_id}"
            run_worker "${gpu_id}" "$WORKER_IMAGE" "${node}" "$worker_id"
            worker_id=$((worker_id + 1))
        done
    done

    echo "Waiting for ${NUM_WORKERS} workers to register with coordinator..."
    wait_for_workers_to_register "$NUM_WORKERS"
}

# ----------------------------------------------------------------------------
# Shared Hive metastore: publish/populate
# ----------------------------------------------------------------------------
# When HIVE_METASTORE_VERSION is set, analyze runs publish their post-ANALYZE
# tpchsf<SF> tree under $HIVE_METASTORE_SHARED_ROOT/<version>/tpchsf<SF>/, and
# subsequent benchmark runs populate from that snapshot instead of re-analyzing.
# Paths inside a .prestoSchema file are container-relative
# (file:/var/lib/presto/data/hive/data/user_data/scale-<SF>/<table>), so a
# single snapshot works for any user whose DATA bind-mount lands on the same
# in-container path.

# Echo the absolute path of the shared slot for the current (version, SF).
# Empty output => sharing is not opted in.
function shared_metastore_slot {
    [[ -z "${HIVE_METASTORE_VERSION:-}" ]] && return 0
    [[ -z "${HIVE_METASTORE_SHARED_ROOT:-}" || -z "${SCALE_FACTOR:-}" ]] && return 0
    echo "${HIVE_METASTORE_SHARED_ROOT}/${HIVE_METASTORE_VERSION}/tpchsf${SCALE_FACTOR}"
}

# Copy the shared snapshot for the current (version, SF) into the local
# .hive_metastore.  Skipped when the shared slot does not exist; the caller
# still has to have the tpchsf<SF> tree in .hive_metastore one way or another
# (a local analyze run also produces one).
function populate_hive_metastore_from_shared {
    local slot
    slot=$(shared_metastore_slot)
    if [[ -z "${slot}" || ! -d "${slot}" ]]; then
        echo "No shared metastore snapshot at ${slot:-<sharing disabled>}; skipping populate."
        return 0
    fi
    local dest="${VT_ROOT}/.hive_metastore/tpchsf${SCALE_FACTOR}"
    echo "Populating ${dest} from shared snapshot ${slot}"
    mkdir -p "${VT_ROOT}/.hive_metastore"
    rsync -a --delete "${slot}/" "${dest}/"
}

# If the shared slot for the current (version, SF) is empty, atomically publish
# the just-analyzed .hive_metastore/tpchsf<SF> tree into it.  Uses a staging
# directory under the same parent so the final rename is atomic on the shared
# filesystem; concurrent publishers from different jobs race harmlessly.
function publish_hive_metastore_to_shared {
    local slot
    slot=$(shared_metastore_slot)
    if [[ -z "${slot}" ]]; then
        echo "HIVE_METASTORE_VERSION not set; skipping publish."
        return 0
    fi
    local src="${VT_ROOT}/.hive_metastore/tpchsf${SCALE_FACTOR}"
    if [[ ! -d "${src}" ]]; then
        echo "Nothing to publish: ${src} does not exist."
        return 0
    fi
    if [[ -d "${slot}" ]]; then
        echo "Shared slot already populated at ${slot}; skipping publish."
        return 0
    fi
    local parent staging
    parent="$(dirname "${slot}")"
    staging="${parent}/.staging-${SLURM_JOB_ID:-$$}-tpchsf${SCALE_FACTOR}"
    mkdir -p "${parent}"
    rm -rf "${staging}"
    echo "Publishing ${src} -> ${slot} (via ${staging})"
    rsync -a "${src}/" "${staging}/"
    # Racy between the is-empty check and the rename; mv -T rejects overwriting
    # a non-empty dir, which is the protection we want.  If another publisher
    # wins, drop our staging copy.
    if ! mv -T "${staging}" "${slot}" 2>/dev/null; then
        echo "Another publisher populated ${slot} first; discarding staging copy."
        rm -rf "${staging}"
    fi
}

# Run a cli node that will connect to the coordinator and run queries from queries.sql
# Results are stored in cli.log.
function run_queries {
    echo "running queries"
    [ $# -ne 2 ] && echo_error "$0 expected two arguments for '<iterations>' and '<scale_factor>'"
    local num_iterations=$1
    local scale_factor=$2

    # SCRIPT_DIR is host-side; translate to its container-side equivalent
    # (VT_ROOT is bind-mounted as /workspace) so the path stays correct even
    # if this directory is renamed away from `presto-nvl72`.
    local container_script_dir="${SCRIPT_DIR/${VT_ROOT}//workspace}"

    local extra_args=()
    [[ "${ENABLE_METRICS}" == "1" ]] && extra_args+=("-m")
    [[ "${ENABLE_NSYS}" == "1" ]] && extra_args+=("-p" "--profile-script-path" "${container_script_dir}/profiler_functions.sh")
    [[ -n "${QUERIES:-}" ]] && extra_args+=("-q" "${QUERIES}")

    source "${SCRIPT_DIR}/defaults.env"

    # The upstream coordinator image ships without jq, which
    # run_benchmark.sh's wait_for_worker_node_registration requires.
    # yum/dnf cannot install it at runtime because the container root is
    # a read-only squashfs (/var/cache/dnf is read-only).  Stage a
    # statically-linked jq under VT_ROOT (which is bind-mounted into the
    # container as /workspace) and prepend that to PATH.  The download
    # is cached across runs so the cost is paid once.
    local jq_cache="${VT_ROOT}/.cache/bin"
    local jq_arch
    case "$(uname -m)" in
        aarch64|arm64) jq_arch="arm64" ;;
        x86_64|amd64)  jq_arch="amd64" ;;
        *) echo_error "unsupported arch for jq download: $(uname -m)" ;;
    esac
    if [ ! -x "${jq_cache}/jq" ]; then
        echo "Staging static jq (${jq_arch}) at ${jq_cache}/jq"
        mkdir -p "${jq_cache}"
        curl -sSL "https://github.com/jqlang/jq/releases/download/jq-1.7.1/jq-linux-${jq_arch}" \
            -o "${jq_cache}/jq"
        chmod +x "${jq_cache}/jq"
    fi

    # Result validation is intentionally not wired here yet: that belongs
    # to PR #275 (upstream validate_results.py) and will be hooked up
    # after the PR merges and this branch is rebased.
    # Cache-drop is skipped because it requires docker (not available on
    # the cluster).
    run_coord_image "export PATH=/workspace/.cache/bin:\$PATH; \
    export PORT=$PORT; \
    export HOSTNAME=$COORD; \
    export PRESTO_DATA_DIR=/var/lib/presto/data/hive/data/user_data; \
    export MINIFORGE_HOME=/workspace/miniforge3; \
    export HOME=/workspace; \
    cd /workspace/presto/scripts; \
    ./run_benchmark.sh -b tpch -s tpchsf${scale_factor} -i ${num_iterations} ${extra_args[*]} \
        --hostname ${COORD} --port $PORT -o ${container_script_dir}/result_dir --skip-drop-cache" "cli"
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

function collect_results {
    local result_dir="${SCRIPT_DIR}/result_dir"

    echo "Copying configs to ${result_dir}/configs/..."
    mkdir -p "${result_dir}/configs"
    cp "${CONFIGS}/etc_coordinator/config_native.properties" "${result_dir}/configs/coordinator.config"
    cp "${CONFIGS}/etc_worker_0/config_native.properties"    "${result_dir}/configs/worker.config"

    echo "Copying logs to ${result_dir}/..."
    cp "${LOGS}"/*.log "${result_dir}/"
    cp "${LOGS}"/*.out "${LOGS}"/*.err "${result_dir}/" 2>/dev/null || true
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

    local num_drivers
    num_drivers=$(grep "^task\.max-drivers-per-task=" "${CONFIGS}/etc_worker/config_native.properties" 2>/dev/null \
                  | cut -d= -f2) || true
    num_drivers="${num_drivers:-2}"

    local cudf_enabled
    cudf_enabled=$(grep "^cudf\.enabled=" "${CONFIGS}/etc_worker/config_native.properties" 2>/dev/null \
                   | cut -d= -f2) || true
    local engine gpu_count gpu_name
    if [[ "${cudf_enabled}" == "true" ]]; then
        engine="presto-velox-gpu"
        gpu_count="${NUM_WORKERS}"
        gpu_name=$(nvidia-smi --query-gpu=gpu_name --format=csv,noheader -i 0 2>/dev/null | head -1) || true
        gpu_name="${gpu_name:-unknown}"
    else
        engine="presto-velox-cpu"
        gpu_count=0
        gpu_name="N/A"
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
       --argjson gpu_count "$gpu_count" \
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

function wait_for_nsys_report_generation {
    # Notes on nsys profiling
    #
    # Wait for nsys to finish flushing .nsys-rep files before the job exits.
    # `nsys stop` only signals the daemon; the report is written asynchronously
    # and can take seconds to minutes. If SLURM tears down the container first,
    # partial reports are lost.
    #
    # We poll file sizes and consider a report done when its size hasn't
    # changed for 3 consecutive iterations (15s). The .qdstrm fallback covers
    # the case where nsys hasn't yet finalized into .nsys-rep: a stable
    # .qdstrm is better than nothing if we hit the 10-minute ceiling.

    if [[ "${ENABLE_NSYS}" == "1" ]]; then
        echo "Waiting for nsys report generation..."
        if [[ -n "${QUERIES:-}" ]]; then
            IFS=',' read -ra qlist <<< "${QUERIES}"
        else
            qlist=({1..22})
        fi
        local -a wid_list
        IFS=',' read -ra wid_list <<< "${NSYS_WORKER_IDS}"
        # Mirror the worker subshell's iter handling: empty PROFILE_ITERATIONS
        # means single empty-string sentinel (= legacy one-file-per-query).
        local -a iter_list
        if [[ -n "${PROFILE_ITERATIONS:-}" ]]; then
            IFS=',' read -ra iter_list <<< "${PROFILE_ITERATIONS}"
        else
            iter_list=("")
        fi
        local expected=$((${#qlist[@]} * ${#wid_list[@]} * ${#iter_list[@]}))

        declare -A prev_sizes
        local stable_count=0
        for i in {1..120}; do
            local all_stable=true
            for wid in "${wid_list[@]}"; do
                for qnum in "${qlist[@]}"; do
                    for iter in "${iter_list[@]}"; do
                        if [[ -z "${iter}" ]]; then
                            qtag="Q${qnum}"
                        else
                            qtag="Q${qnum}_iter${iter}"
                        fi
                        report="${LOGS}/nsys_worker_w${wid}_${qtag}.nsys-rep"
                        fallback="${LOGS}/nsys_worker_w${wid}_${qtag}.qdstrm"
                        if [[ -f "$report" ]]; then
                            target="$report"
                        elif [[ -f "$fallback" ]]; then
                            target="$fallback"
                        else
                            echo "    w${wid} ${qtag}: no file yet"
                            all_stable=false
                            continue
                        fi
                        cur_size=$(stat -c%s "$target" 2>/dev/null || echo 0)
                        prev=${prev_sizes["$target"]:-0}
                        echo "    w${wid} ${qtag}: cur=${cur_size} prev=${prev}"
                        if (( cur_size == 0 || cur_size != prev )); then
                            all_stable=false
                        fi
                        prev_sizes["$target"]=$cur_size
                    done
                done
            done
            echo "  all_stable=${all_stable} stable_count=${stable_count}"
            if $all_stable; then
                stable_count=$((stable_count + 1))
                if (( stable_count >= 3 )); then
                    echo "All ${expected} nsys reports stable."
                    break
                fi
            else
                stable_count=0
            fi
            sleep 5
        done

        echo "Copying nsys reports to ${SCRIPT_DIR}/result_dir/..."
        cp "${LOGS}"/*.nsys-rep "${SCRIPT_DIR}/result_dir/"
    fi
}
