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
    # GB200 NVL72 compute tray: 2 Grace CPUs x 2 Blackwell GPUs per CPU.
    # CPU NUMA 0 = cores 0-71 (Grace 0, GPUs 0-1).
    # CPU NUMA 1 = cores 72-143 (Grace 1, GPUs 2-3).
    # Pairs each worker with the CPU socket its GPU is attached to over NVLink-C2C.
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
    # devices and all required host driver libraries (580.105.08: libcuda, libnvidia-
    # gpucomp, libnvidia-nvvm, libnvidia-ptxjitcompiler, libnvidia-ml, etc.) and runs
    # ldconfig inside the container.  The manual libcuda bind-mount then overrides the
    # compat library with the host driver so cudaMallocAsync works.
    # CUDA_VISIBLE_DEVICES=${gpu_id} inside the container restricts each worker to
    # its assigned GPU while still allowing the CUDA driver to enumerate all devices.
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
/usr/lib/aarch64-linux-gnu/libcuda.so.580.105.08:/usr/local/cuda-13.0/compat/libcuda.so.1,\
/usr/lib/aarch64-linux-gnu/libnvidia-ml.so.580.105.08:/usr/local/lib/libnvidia-ml.so.1 \
-- /bin/bash -c "
export LD_LIBRARY_PATH='${CUDF_LIB}':\${LD_LIBRARY_PATH:-}
if [[ '${VARIANT_TYPE}' == 'gpu' ]]; then export CUDA_VISIBLE_DEVICES=${gpu_id}; fi
echo \"Worker ${worker_id}: CUDA_VISIBLE_DEVICES=\${CUDA_VISIBLE_DEVICES:-none}, NUMA_NODE=${numa_node}\"
if [[ '${USE_NUMA}' == '1' ]]; then
    numactl --cpubind=${numa_node} --membind=${numa_node} /usr/bin/presto_server --etc-dir=/opt/presto-server/etc
else
    /usr/bin/presto_server --etc-dir=/opt/presto-server/etc
fi" > ${LOGS}/worker_${worker_id}.log 2>&1 &
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
    ./run_benchmark.sh -b tpch -s tpchsf${scale_factor} -i ${num_iterations} \
        --hostname ${COORD} --port $PORT -o /workspace/presto/slurm/presto-nvl72/result_dir --skip-drop-cache" "cli"
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

