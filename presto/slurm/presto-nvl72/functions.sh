#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

PRESTO_SCRIPT_FUNCTIONS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../scripts" && pwd)"
source "${PRESTO_SCRIPT_FUNCTIONS_DIR}/common_functions.sh"

# Track only the srun clients started by this batch shell. Cleanup never uses a
# broad pkill, so it cannot terminate another user's process or an unrelated
# process in the allocation.
COORD_SRUN_PID=""
declare -a WORKER_SRUN_PIDS=()
declare -a WORKER_SRUN_IDS=()
declare -a WORKER_SRUN_NODES=()
declare -A CPU_NUMA_NODE_BY_WORKER_SLOT=()
declare -A CLUSTER_PORT_SPECS_BY_NODE=()
CLUSTER_STOPPED=0

# Echo the ",<host_path>:<host_path>" bind-mount fragment for the host's
# miniforge3 install if it exists, else nothing.  Used by every container
# that needs to run Python via init_python_virtual_env / run_py_script.sh:
# miniforge's conda + python scripts have shebangs hardcoded to the host
# install path, so the same absolute path has to resolve inside the
# container.  Pair this with `MINIFORGE_HOME=/workspace/miniforge3` in the
# --export (which uses the separate VT_ROOT:/workspace mount).
# Resolve a container image name or path to an absolute .sqsh path.
# Accepts: bare name (appends .sqsh under IMAGE_DIR), name ending in .sqsh
# (prepends IMAGE_DIR), or an absolute path (used as-is).
resolve_image_path() {
    local image="$1"
    if [[ "${image}" == /* ]]; then
        echo "${image}"
    elif [[ "${image}" == *.sqsh ]]; then
        echo "${IMAGE_DIR}/${image}"
    else
        echo "${IMAGE_DIR}/${image}.sqsh"
    fi
}

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
}

function resolve_node_ipv4_for_interface {
    [ $# -ne 2 ] && echo_error "$0 expected arguments 'node' and 'interface'"
    local node=$1 iface=$2
    srun -N1 -w "${node}" --ntasks=1 --overlap \
        /bin/bash -lc "ip -o -4 addr show dev '${iface}' | awk '{print \$4}' | cut -d/ -f1 | head -n 1"
}

# Resolve an RDMA device (for example mlx5_0:1) to its Linux netdev and IPv4
# address on a particular allocated node. This is deliberately performed on
# the compute node: NVL72 odd/even trays attach to different leaf groups, and
# interface names need not be globally encoded in the cluster configuration.
function resolve_node_ipv4_for_ucx_device {
    [ $# -ne 2 ] && echo_error "$0 expected arguments 'node' and 'ucx_device'"
    local node=$1 rdma_device=${2%%:*}
    [[ "${rdma_device}" =~ ^[[:alnum:]_.-]+$ ]] || return 1

    srun -N1 -w "${node}" --ntasks=1 --overlap \
        /bin/bash -lc '
            rdma_device=$1
            net_dir="/sys/class/infiniband/${rdma_device}/device/net"
            [[ -d "${net_dir}" ]] || exit 1
            shopt -s nullglob
            net_paths=("${net_dir}"/*)
            ((${#net_paths[@]} > 0)) || exit 1
            iface="${net_paths[0]##*/}"
            address="$(ip -o -4 addr show dev "${iface}" scope global \
                | sed -n "1{s/.* inet \\([^/ ]*\\).*/\\1/p;}")"
            [[ -n "${address}" ]] || exit 1
            printf "%s %s\\n" "${iface}" "${address}"
        ' _ "${rdma_device}"
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

    local coord_image
    coord_image=$(resolve_image_path "${COORD_IMAGE}")
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
        COORD_SRUN_PID=$!
        echo "Coordinator srun client PID: ${COORD_SRUN_PID}"
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
    : "${ENABLE_PERF:=0}"
    : "${PERF_WORKER_ID:=0}"
    # Migrate legacy NSYS_WORKER_ID (single int) → NSYS_WORKER_IDS (csv).
    : "${NSYS_WORKER_IDS:=${NSYS_WORKER_ID:-0}}"

    [ $# -ne 4 ] && echo_error "$0 expected arguments 'local_worker_id', 'image', 'node_id', and 'worker_id'"
    validate_environment_preconditions LOGS CONFIGS VT_ROOT COORD CUDF_LIB DATA

    # The first argument is the worker's local slot on its host. For GPU jobs
    # that slot is also the CUDA device id; for CPU jobs it is only an index
    # into node-local interface/device lists. It restarts at zero on each host.
    local local_worker_id=$1 image=$2 node=$3 worker_id=$4
    local gpu_id="${local_worker_id}"
    local numa_node=""
    if [[ "${VARIANT_TYPE}" == "gpu" ]]; then
        # Pair each worker with the CPU NUMA domain its GPU is attached to.
        # On GB200, GPUs 0-1 have CPU affinity 0-71 (node 0) and GPUs 2-3
        # have CPU affinity 72-143 (node 1). The GPU NUMA IDs reported by
        # nvidia-smi are HBM nodes and are intentionally not used here.
        numa_node=$((gpu_id / ${CLUSTER_NUMA_GPUS_PER_NODE:-1}))
    elif [[ "${USE_NUMA}" == "1" ]]; then
        numa_node="${CPU_NUMA_NODE_BY_WORKER_SLOT["${node}:${local_worker_id}"]:-}"
        [[ -n "${numa_node}" ]] || \
            echo_error "Worker ${worker_id}: CPU NUMA placement was not prepared for ${node} local slot ${local_worker_id}"
    fi
    if [[ "${VARIANT_TYPE}" == "gpu" ]]; then
        echo "running worker ${worker_id} with image ${image} on node ${node} with gpu_id ${gpu_id} numa_node ${numa_node}"
    elif [[ "${USE_NUMA}" == "1" ]]; then
        echo "running worker ${worker_id} with image ${image} on node ${node} with local_slot ${local_worker_id} cpu_numa_node ${numa_node}"
    else
        echo "running worker ${worker_id} with image ${image} on node ${node} with local_slot ${local_worker_id} (NUMA unbound)"
    fi

    local worker_image
    worker_image=$(resolve_image_path "${image}")
    [ ! -f "${worker_image}" ] && echo_error "worker image does not exist at ${worker_image}"

    # Make a copy of the worker config that can be given a unique id for this worker.
    local worker_config="${CONFIGS}/etc_worker_${worker_id}/config_native.properties"
    local worker_node="${CONFIGS}/etc_worker_${worker_id}/node.properties"
    local worker_hive="${CONFIGS}/etc_worker_${worker_id}/catalog/hive.properties"
    local worker_data="${SCRIPT_DIR}/worker_data_${worker_id}"

    # CPU UCX peers derive the destination listener port from the advertised
    # Prestissimo HTTP task URL as HTTP+3. Derive the local listener from the
    # same generated config rather than putting a single static port in the
    # shared worker.env file; this stays correct for every worker ID.
    local cpu_ucx_port=""
    if [[ "${VARIANT_TYPE}" == "cpu" ]]; then
        local worker_http_port
        worker_http_port="$(awk -F= '
            /^[[:space:]]*http-server\.http\.port[[:space:]]*=/ {
                gsub(/[[:space:]]/, "", $2)
                print $2
                exit
            }
        ' "${worker_config}")"
        if [[ ! "${worker_http_port}" =~ ^[0-9]+$ ]] || (( worker_http_port < 1 || worker_http_port > 65532 )); then
            echo_error "Worker ${worker_id}: invalid or missing http-server.http.port in ${worker_config}"
        fi
        cpu_ucx_port=$((worker_http_port + 3))
        echo "  Worker ${worker_id} CPU UCX listener: HTTP ${worker_http_port} + 3 = ${cpu_ucx_port}"
    fi

    # Each worker needs to be told how to access the coordianator
    sed -i "s+discovery\.uri.*+discovery\.uri=http://${COORD}:${PORT}+g" ${worker_config}

    # Resolve a worker-specific UCX device from its local slot before entering
    # the container. The same list is reused independently on every allocated
    # node, so scheduling remains node-agnostic. Preserve the legacy per-GPU
    # list and the generic UCX_NET_DEVICES multi-rail value as fallbacks.
    local worker_ucx_net_device="${UCX_NET_DEVICES:-${CLUSTER_UCX_NET_DEVICES:-}}"
    local ucx_net_devices_by_local_worker="${CLUSTER_UCX_NET_DEVICES_BY_LOCAL_WORKER:-}"
    local ucx_net_devices_by_gpu="${CLUSTER_UCX_NET_DEVICES_BY_GPU:-}"
    if [[ -n "${ucx_net_devices_by_local_worker}" ]]; then
        IFS=',' read -ra ucx_net_device_entries <<< "${ucx_net_devices_by_local_worker}"
        worker_ucx_net_device="${ucx_net_device_entries[${local_worker_id}]:-}"
        [[ -n "${worker_ucx_net_device}" ]] || \
            echo_error "CLUSTER_UCX_NET_DEVICES_BY_LOCAL_WORKER has no entry for local worker ${local_worker_id}"
    elif [[ -n "${ucx_net_devices_by_gpu}" ]]; then
        IFS=',' read -ra ucx_net_device_entries <<< "${ucx_net_devices_by_gpu}"
        worker_ucx_net_device="${ucx_net_device_entries[${gpu_id}]:-}"
        [[ -n "${worker_ucx_net_device}" ]] || \
            echo_error "CLUSTER_UCX_NET_DEVICES_BY_GPU has no entry for GPU ${gpu_id}"
    fi
    [[ -n "${worker_ucx_net_device}" ]] && \
        echo "  Worker ${worker_id} UCX device: ${worker_ucx_net_device}"

    # Keep an explicit interface override when supplied. Otherwise CPU UCX
    # derives the advertised address from the first selected RDMA device on
    # each actual host. For a multi-rail list this first device is also the
    # listener/bootstrap rail; UCX remains free to use the full device list for
    # data movement. The legacy per-GPU interface list remains available to GPU
    # runs and to callers that intentionally want fixed local-slot pairing.
    local internal_address_interface="${PRESTO_INTERNAL_ADDRESS_INTERFACE:-${CLUSTER_INTERNAL_ADDRESS_INTERFACE:-}}"
    local internal_address_interface_by_gpu="${PRESTO_INTERNAL_ADDRESS_INTERFACE_BY_GPU:-${CLUSTER_INTERNAL_ADDRESS_INTERFACE_BY_GPU:-}}"
    if [[ -n "${internal_address_interface_by_gpu}" ]]; then
        local -a internal_address_interfaces
        IFS=',' read -ra internal_address_interfaces <<< "${internal_address_interface_by_gpu}"
        internal_address_interface="${internal_address_interfaces[${gpu_id}]:-}"
        [[ -n "${internal_address_interface}" ]] || \
            echo_error "PRESTO_INTERNAL_ADDRESS_INTERFACE_BY_GPU/CLUSTER_INTERNAL_ADDRESS_INTERFACE_BY_GPU has no entry for GPU ${gpu_id}"
    fi

    local internal_address="" address_source=""
    if [[ -n "${internal_address_interface}" ]]; then
        internal_address="$(resolve_node_ipv4_for_interface "${node}" "${internal_address_interface}")"
        [[ -n "${internal_address}" ]] || \
            echo_error "Could not resolve ${internal_address_interface} IPv4 address on ${node}"
        address_source="interface ${internal_address_interface}"
    elif [[ "${VARIANT_TYPE}" == "cpu" && "${worker_ucx_net_device}" == mlx5_* ]]; then
        local bootstrap_ucx_device="${worker_ucx_net_device%%,*}"
        local discovered_address
        discovered_address="$(resolve_node_ipv4_for_ucx_device "${node}" "${bootstrap_ucx_device}")" || \
            echo_error "Worker ${worker_id}: could not map ${bootstrap_ucx_device} to an IPv4 netdev on ${node}"
        read -r internal_address_interface internal_address <<< "${discovered_address}"
        [[ -n "${internal_address_interface}" && -n "${internal_address}" ]] || \
            echo_error "Worker ${worker_id}: incomplete RDMA address discovery for ${bootstrap_ucx_device} on ${node}"
        address_source="auto ${bootstrap_ucx_device} -> ${internal_address_interface}"
    fi

    if [[ -n "${internal_address}" ]]; then
        if grep -q "^node\.internal-address=" "${worker_node}"; then
            sed -i "s+^node\.internal-address=.*+node.internal-address=${internal_address}+g" "${worker_node}"
        else
            echo "node.internal-address=${internal_address}" >> "${worker_node}"
        fi
        echo "  Worker ${worker_id} internal address: ${internal_address} (${address_source})"
    fi

    # Create unique data dir per worker.
    mkdir -p ${worker_data}
    mkdir -p ${worker_data}/hive/data/user_data
    mkdir -p ${VT_ROOT}/.hive_metastore

    local vt_worker_env_file="/var/worker_env_file"
    local vt_cufile_log_dir="/var/log/cufile"
    local vt_cufile_log="${vt_cufile_log_dir}/cufile_worker_${worker_id}.log"
    local container_script_dir="${SCRIPT_DIR/${VT_ROOT}//workspace}"
    [[ "${container_script_dir}" == /workspace/* ]] || \
        echo_error "SCRIPT_DIR must be below VT_ROOT (${VT_ROOT}) so the NUMA launcher is visible in the worker container: ${SCRIPT_DIR}"
    local vt_numa_launcher="${container_script_dir}/numa-worker-launch.sh"
    if [[ "${USE_NUMA:-0}" == "1" ]]; then
        [[ -f "${SCRIPT_DIR}/numa-worker-launch.sh" ]] || \
            echo_error "NUMA worker launcher not found: ${SCRIPT_DIR}/numa-worker-launch.sh"
    fi

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
    local publish_perf_pid=0
    if [[ "${ENABLE_NSYS}" == "1" && ",${NSYS_WORKER_IDS}," == *",${worker_id},"* ]]; then
        # nsys must be on PATH inside the worker container. The recommended approach is
        # a symlink in the image build:
        # ln -sf /opt/nvidia/nsight-systems-cli/<version>/bin/nsys /usr/local/bin/nsys
        nsys_bin="nsys"
        # nsys then uses its own built-in default trace set.
        nsys_launch_opts="${NSYS_LAUNCH_OPTS:-}"
    fi
    if [[ "${ENABLE_PERF}" == "1" && "${worker_id}" == "${PERF_WORKER_ID}" ]]; then
        publish_perf_pid=1
        # Remove a PID from a prior failed job before the new container starts.
        rm -f "${LOGS}/.presto_worker_pid_w${worker_id}"
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
    # nvidia-container-cli on the host. CPU-only nodes typically do not have it,
    # so this export remains GPU-only.
    local nvidia_env=""
    if [[ "${VARIANT_TYPE}" == "gpu" ]]; then
        nvidia_env=",NVIDIA_VISIBLE_DEVICES=all,NVIDIA_DRIVER_CAPABILITIES=compute,utility"
    fi

    # Enroot's Mellanox hook exposes /dev/infiniband and the matching providers.
    # GPU jobs default to the historical "all" behavior. Selecting an mlx5
    # device is also an explicit CPU opt-in: expose the matching RDMA devices
    # and providers without requiring a second, redundant topology setting.
    # Ordinary CPU-only hosts using UCX auto/TCP still avoid the hook.
    if [[ -n "${CLUSTER_MELLANOX_VISIBLE_DEVICES:-}" ]]; then
        export MELLANOX_VISIBLE_DEVICES="${CLUSTER_MELLANOX_VISIBLE_DEVICES}"
    elif [[ "${VARIANT_TYPE}" == "gpu" || "${worker_ucx_net_device}" == *mlx5_* ]]; then
        export MELLANOX_VISIBLE_DEVICES=all
    else
        unset MELLANOX_VISIBLE_DEVICES
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

    # Pyxis/Enroot share the host network and IPC namespaces unless explicitly
    # asked to unshare them (and do not create a separate PID namespace here).
    # That already matches Compose's network/ipc/pid host settings required by
    # UCX same-host transports; do not add --container-unshare=ipc to this step.
    UCX_NET_DEVICES="${worker_ucx_net_device}" \
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
set -a
source ${vt_worker_env_file}
set +a

if [[ \"\${PRESTO_WORKER_UCX_LOCAL_LIB_FIRST:-0}\" == \"1\" ]]; then
    export LD_LIBRARY_PATH=/usr/local/lib:'${CUDF_LIB}':\${LD_LIBRARY_PATH:-}
else
    export LD_LIBRARY_PATH='${CUDF_LIB}':/usr/local/lib:\${LD_LIBRARY_PATH:-}
fi

if [[ '${VARIANT_TYPE}' == 'cpu' ]]; then
    if [[ -n \"\${VELOX_UCX_CPU_PORT:-}\" && \"\${VELOX_UCX_CPU_PORT}\" != '${cpu_ucx_port}' ]]; then
        echo \"Worker ${worker_id}: overriding VELOX_UCX_CPU_PORT=\${VELOX_UCX_CPU_PORT} with generated HTTP+3 port ${cpu_ucx_port}\"
    fi
    export VELOX_UCX_CPU_EXCHANGE=\"\${VELOX_UCX_CPU_EXCHANGE:-1}\"
    export VELOX_UCX_CPU_PORT='${cpu_ucx_port}'
    if [[ \"\${VERIFY_CPU_UCX:-0}\" == '1' && \"\${VELOX_UCX_CPU_EXCHANGE}\" != '1' ]]; then
        echo \"Worker ${worker_id}: --verify-cpu-ucx requires VELOX_UCX_CPU_EXCHANGE=1\" >&2
        exit 2
    fi
    echo \"Worker ${worker_id}: CPU UCX enabled=\${VELOX_UCX_CPU_EXCHANGE}, port=\${VELOX_UCX_CPU_PORT}\"
    echo \"Worker ${worker_id}: UCX_TLS=\${UCX_TLS:-unset}, UCX_NET_DEVICES=\${UCX_NET_DEVICES:-unset}\"
    echo \"Worker ${worker_id}: UCX_MODULE_DIR=\${UCX_MODULE_DIR:-unset}\"
fi

if [[ '${VARIANT_TYPE}' == 'gpu' ]]; then export CUDA_VISIBLE_DEVICES=${gpu_id}; fi

if [[ '${ENABLE_GDS}' == '1' ]]; then
    export KVIKIO_COMPAT_MODE=OFF
    export CUFILE_LOGFILE_PATH=${vt_cufile_log}
    export CUFILE_LOGGING_LEVEL=INFO
else
    export KVIKIO_COMPAT_MODE=ON
fi

echo \"Worker ${worker_id}: CUDA_VISIBLE_DEVICES=\${CUDA_VISIBLE_DEVICES:-none}, NUMA_NODE=${numa_node}\"
echo \"Worker ${worker_id}: UCX_NET_DEVICES=\${UCX_NET_DEVICES:-unset}\"
echo \"Worker ${worker_id}: ENABLE_GDS=\${ENABLE_GDS:-unset}\"
echo \"Worker ${worker_id}: ENABLE_NSYS=\${ENABLE_NSYS:-unset}\"
echo \"Worker ${worker_id}: ENABLE_PERF=\${ENABLE_PERF:-unset}\"
echo \"Worker ${worker_id}: KVIKIO_COMPAT_MODE=\${KVIKIO_COMPAT_MODE:-unset}\"
echo \"Worker ${worker_id}: CUFILE_LOGFILE_PATH=\${CUFILE_LOGFILE_PATH:-unset}\"
echo \"Worker ${worker_id}: KVIKIO_TASK_SIZE=\${KVIKIO_TASK_SIZE:-unset}\"
echo \"Worker ${worker_id}: KVIKIO_NTHREADS=\${KVIKIO_NTHREADS:-unset}\"

if [[ '${VARIANT_TYPE}' == 'cpu' && \"\${VERIFY_CPU_UCX:-0}\" == '1' ]]; then
    echo \"Worker ${worker_id}: LD_LIBRARY_PATH=\${LD_LIBRARY_PATH:-unset}\"
    if command -v ucx_info >/dev/null 2>&1; then
        echo \"Worker ${worker_id}: UCX runtime version\"
        ucx_info -v
    else
        echo \"Worker ${worker_id}: ucx_info not found\" >&2
    fi
    if command -v ldd >/dev/null 2>&1; then
        echo \"Worker ${worker_id}: presto_server UCX linkage\"
        ldd /usr/bin/presto_server | grep -E 'libuc[mpxt]|not found' || true
    fi
fi

if [[ -n '${nsys_bin}' ]]; then
    (
        echo \"Worker ${worker_id}: nsys subshell started\"
        if [[ -n '${QUERIES:-}' ]]; then
            IFS=',' read -ra qlist <<< '${QUERIES:-}'
        else
            qlist=({1..22})
        fi
        for qnum in \"\${qlist[@]}\"; do
            qid=\"Q\${qnum}\"
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
        echo \"Worker ${worker_id}: nsys subshell done, all queries profiled\"
    ) &

    echo \"Worker ${worker_id}: Nsight System program at ${nsys_bin}\"
    echo \"Worker ${worker_id}: running nsys launch\"
    if [[ '${USE_NUMA}' == '1' ]]; then
        ${nsys_bin} launch ${nsys_launch_opts} /bin/bash ${vt_numa_launcher} ${numa_node} /usr/bin/presto_server --etc-dir=/opt/presto-server/etc
    else
        ${nsys_bin} launch ${nsys_launch_opts} /usr/bin/presto_server --etc-dir=/opt/presto-server/etc
    fi
    echo \"Worker ${worker_id}: nsys launch exited with code: \$?\"
else
    if [[ '${publish_perf_pid}' == '1' ]]; then
        perf_pid_file='${vt_nsys_report_dir}/.presto_worker_pid_w${worker_id}'
        rm -f \"\${perf_pid_file}\"
        if [[ '${USE_NUMA}' == '1' ]]; then
            /bin/bash ${vt_numa_launcher} ${numa_node} /usr/bin/presto_server --etc-dir=/opt/presto-server/etc &
        else
            /usr/bin/presto_server --etc-dir=/opt/presto-server/etc &
        fi
        server_pid=\$!
        printf '%s\n' \"\${server_pid}\" > \"\${perf_pid_file}.tmp\"
        mv \"\${perf_pid_file}.tmp\" \"\${perf_pid_file}\"
        echo \"Worker ${worker_id}: published presto_server host PID \${server_pid} for perf\"
        wait \"\${server_pid}\"
        server_rc=\$?
        rm -f \"\${perf_pid_file}\"
        exit \"\${server_rc}\"
    elif [[ '${USE_NUMA}' == '1' ]]; then
        /bin/bash ${vt_numa_launcher} ${numa_node} /usr/bin/presto_server --etc-dir=/opt/presto-server/etc
    else
        /usr/bin/presto_server --etc-dir=/opt/presto-server/etc
    fi
fi

" > ${LOGS}/worker_${worker_id}.log 2>&1 &
    WORKER_SRUN_PIDS+=("$!")
    WORKER_SRUN_IDS+=("${worker_id}")
    WORKER_SRUN_NODES+=("${node}")
    echo "  Worker ${worker_id} srun client PID: ${WORKER_SRUN_PIDS[$((${#WORKER_SRUN_PIDS[@]} - 1))]}"
}

# ----------------------------------------------------------------------------
# Host perf sidecar
# ----------------------------------------------------------------------------

# PID of the background srun that hosts perf-worker-sampler.sh. This is kept in
# the batch shell so success and failure paths can both finalize captures before
# Slurm tears down the allocation.
PERF_SIDECAR_SRUN_PID=""

function start_perf_sampler {
    [[ "${ENABLE_PERF:-0}" == "1" ]] || return 0

    local worker_id="${PERF_WORKER_ID:-0}"
    local sampler="${SCRIPT_DIR}/perf-worker-sampler.sh"
    local sharded_recorder="${SCRIPT_DIR}/perf-record-sharded.sh"
    local postprocess_script="${SCRIPT_DIR}/process-perf-captures.sh"
    local result_dir="${RESULT_DIR:-${SCRIPT_DIR}/result_dir}"
    local output_dir="${result_dir}/profiles/perf"
    local ready_file="${LOGS}/.perf_sidecar_ready_w${worker_id}"
    local error_file="${LOGS}/.perf_sidecar_error_w${worker_id}"
    local done_file="${LOGS}/.perf_sidecar_done_w${worker_id}"
    local sampler_log="${LOGS}/perf_worker_${worker_id}.log"

    [[ "${worker_id}" == "0" ]] || echo_error "This perf path currently supports worker 0 only"
    [[ -f "${sampler}" ]] || echo_error "Perf sampler not found: ${sampler}"
    [[ -f "${sharded_recorder}" ]] || echo_error "Sharded perf recorder not found: ${sharded_recorder}"
    [[ -f "${postprocess_script}" ]] || echo_error "Perf postprocessor not found: ${postprocess_script}"

    mkdir -p "${output_dir}"
    rm -f \
        "${ready_file}" \
        "${error_file}" \
        "${done_file}" \
        "${LOGS}/.perf_sidecar_shutdown_w${worker_id}" \
        "${LOGS}"/.perf_start_w"${worker_id}"_* \
        "${LOGS}"/.perf_started_w"${worker_id}"_* \
        "${LOGS}"/.perf_stop_w"${worker_id}"_* \
        "${LOGS}"/.perf_finished_w"${worker_id}"_* \
        "${LOGS}"/.perf_error_w"${worker_id}"_*

    echo "Starting host perf sidecar for worker ${worker_id} on ${COORD}..."
    srun -N1 -w "${COORD}" --ntasks=1 --overlap --export=ALL \
        bash "${sampler}" \
            --worker-id "${worker_id}" \
            --control-dir "${LOGS}" \
            --output-dir "${output_dir}" \
            --postprocess-script "${postprocess_script}" \
            --postprocess "${PERF_POSTPROCESS:-0}" \
            --event "${PERF_EVENT:-cpu-clock:u}" \
            --frequency "${PERF_FREQUENCY:-19}" \
            --call-graph "${PERF_CALL_GRAPH:-dwarf,8192}" \
            --user-regs "${PERF_USER_REGS:-auto}" \
            --nofile-limit "${PERF_NOFILE_LIMIT:-65536}" \
            --threads-per-shard "${PERF_THREADS_PER_SHARD:-128}" \
            --record-stop-timeout "${PERF_RECORD_STOP_TIMEOUT:-120}" \
        >"${sampler_log}" 2>&1 &
    PERF_SIDECAR_SRUN_PID=$!

    local waited
    for waited in {1..180}; do
        if [[ -f "${ready_file}" ]]; then
            echo "Host perf sidecar is ready for worker ${worker_id}."
            return 0
        fi
        if [[ -s "${error_file}" ]]; then
            cat "${error_file}" >&2
            echo_error "Host perf preflight failed; see ${sampler_log}"
        fi
        if ! kill -0 "${PERF_SIDECAR_SRUN_PID}" 2>/dev/null; then
            wait "${PERF_SIDECAR_SRUN_PID}" || true
            PERF_SIDECAR_SRUN_PID=""
            echo_error "Host perf sidecar exited before becoming ready; see ${sampler_log}"
        fi
        sleep 1
    done
    echo_error "Timed out waiting for host perf sidecar; see ${sampler_log}"
}

function stop_perf_sampler {
    [[ "${ENABLE_PERF:-0}" == "1" ]] || return 0

    local worker_id="${PERF_WORKER_ID:-0}"
    local shutdown_file="${LOGS}/.perf_sidecar_shutdown_w${worker_id}"
    local done_file="${LOGS}/.perf_sidecar_done_w${worker_id}"
    local error_file="${LOGS}/.perf_sidecar_error_w${worker_id}"
    local sampler_log="${LOGS}/perf_worker_${worker_id}.log"
    local finalize_timeout="${PERF_FINALIZE_TIMEOUT:-0}"
    local sidecar_rc=0
    local timeout_rc=0

    if [[ -z "${PERF_SIDECAR_SRUN_PID}" && -f "${done_file}" ]]; then
        if [[ -s "${error_file}" ]]; then
            cat "${error_file}" >&2
            return 1
        fi
        return 0
    fi
    [[ "${finalize_timeout}" =~ ^[0-9]+$ ]] || {
        echo "Invalid PERF_FINALIZE_TIMEOUT=${finalize_timeout}; expected a non-negative integer" >&2
        return 1
    }

    touch "${shutdown_file}"

    if [[ -n "${PERF_SIDECAR_SRUN_PID}" ]]; then
        if (( finalize_timeout > 0 )); then
            if command -v timeout >/dev/null 2>&1; then
                timeout --foreground "${finalize_timeout}s" bash -c '
                    while kill -0 "$1" 2>/dev/null; do
                        sleep 1
                    done
                ' bash "${PERF_SIDECAR_SRUN_PID}" || timeout_rc=$?
            else
                local deadline=$((SECONDS + finalize_timeout))
                while kill -0 "${PERF_SIDECAR_SRUN_PID}" 2>/dev/null && (( SECONDS < deadline )); do
                    sleep 1
                done
                if kill -0 "${PERF_SIDECAR_SRUN_PID}" 2>/dev/null; then
                    timeout_rc=124
                fi
            fi
            if (( timeout_rc == 124 )); then
                echo "Timed out after ${finalize_timeout}s waiting for perf artifact publication; terminating its srun. See ${sampler_log}" >&2
                kill -TERM "${PERF_SIDECAR_SRUN_PID}" 2>/dev/null || true
            elif (( timeout_rc != 0 )); then
                echo "Error while waiting for perf artifact publication (status ${timeout_rc}); see ${sampler_log}" >&2
                kill -TERM "${PERF_SIDECAR_SRUN_PID}" 2>/dev/null || true
            fi
        fi
        wait "${PERF_SIDECAR_SRUN_PID}" || sidecar_rc=$?
        PERF_SIDECAR_SRUN_PID=""
    fi
    if [[ ! -f "${done_file}" ]]; then
        echo "Perf sidecar did not publish its completion token; see ${sampler_log}" >&2
        return 1
    fi
    if [[ -s "${error_file}" ]]; then
        cat "${error_file}" >&2
        return 1
    fi
    if (( sidecar_rc != 0 )); then
        echo "Perf sidecar srun exited with status ${sidecar_rc}; see ${sampler_log}" >&2
        return "${sidecar_rc}"
    fi
    echo "Host perf raw captures published. Artifacts: ${RESULT_DIR:-${SCRIPT_DIR}/result_dir}/profiles/perf/worker_${worker_id}"
}

# ----------------------------------------------------------------------------
# Cluster lifecycle
# ----------------------------------------------------------------------------

# Return success only while a tracked child exists and is not a zombie waiting
# for this shell to reap it.
function tracked_process_is_running {
    local pid="${1:-}" state=""
    [[ "${pid}" =~ ^[1-9][0-9]*$ ]] || return 1
    kill -0 "${pid}" 2>/dev/null || return 1
    state="$(ps -o stat= -p "${pid}" 2>/dev/null | tr -d '[:space:]')"
    [[ -n "${state}" && "${state}" != Z* ]]
}

# Discover the CPU-bearing NUMA nodes independently on every selected host.
# The generated worker configs are homogeneous, so reject a heterogeneous
# allocation instead of silently applying resource limits tuned for another
# node's topology.
function prepare_cpu_numa_layout {
    CPU_NUMA_NODE_BY_WORKER_SLOT=()
    [[ "${VARIANT_TYPE:-gpu}" == "cpu" ]] || return 0

    if [[ "${USE_NUMA:-0}" != "1" ]]; then
        echo "CPU NUMA binding is disabled; workers may use every CPU and allowed memory node on their host."
        return 0
    fi
    local common_functions="${PRESTO_SCRIPT_FUNCTIONS_DIR}/common_functions.sh"
    local reference_signature="" node output line
    for node in $(scontrol show hostnames "${SLURM_JOB_NODELIST}"); do
        if ! output="$(
            srun -N1 -w "${node}" --ntasks=1 --overlap \
                /bin/bash -s -- "${common_functions}" "${NUM_GPUS_PER_NODE}" <<'EOS'
set -euo pipefail
source "$1"
workers_per_host="$2"
discover_cpu_numa_topology

cpu_ids="$(IFS=,; printf '%s' "${CPU_NUMA_NODE_IDS[*]}")"
cpu_counts="$(IFS=,; printf '%s' "${CPU_NUMA_NODE_CPU_COUNTS[*]}")"
cpu_memory="$(IFS=,; printf '%s' "${CPU_NUMA_NODE_MEMORY_GB[*]}")"
memory_ids="$(IFS=,; printf '%s' "${MEMORY_NUMA_NODE_IDS[*]}")"
printf 'TOPOLOGY|%s|%s|%s|%s|%s\n' \
    "${VISIBLE_NUMA_NODE_COUNT}" "${cpu_ids}" "${cpu_counts}" \
    "${cpu_memory}" "${memory_ids}"

for ((slot = 0; slot < workers_per_host; ++slot)); do
    printf 'MAP|%s|%s\n' \
        "${slot}" "$(cpu_numa_node_for_worker "${slot}" "${workers_per_host}")"
done
EOS
        )"; then
            echo "${output}" >&2
            echo_error "Could not discover CPU NUMA topology on ${node}"
        fi

        local signature="" visible="" cpu_ids="" cpu_counts="" cpu_memory="" memory_ids=""
        local slot="" cpu_node="" map_count=0 mapping=""
        while IFS= read -r line; do
            case "${line}" in
                TOPOLOGY\|*)
                    IFS='|' read -r _ visible cpu_ids cpu_counts cpu_memory memory_ids <<< "${line}"
                    signature="${visible}|${cpu_ids}|${cpu_counts}|${cpu_memory}|${memory_ids}"
                    ;;
                MAP\|*)
                    IFS='|' read -r _ slot cpu_node <<< "${line}"
                    if [[ ! "${slot}" =~ ^[0-9]+$ || ! "${cpu_node}" =~ ^[0-9]+$ ]]; then
                        echo_error "Invalid CPU NUMA mapping returned by ${node}: ${line}"
                    fi
                    CPU_NUMA_NODE_BY_WORKER_SLOT["${node}:${slot}"]="${cpu_node}"
                    mapping+="${mapping:+,}${slot}->${cpu_node}"
                    map_count=$((map_count + 1))
                    ;;
                "")
                    ;;
                *)
                    echo "CPU NUMA discovery output from ${node}: ${line}"
                    ;;
            esac
        done <<< "${output}"

        [[ -n "${signature}" ]] || echo_error "CPU NUMA discovery on ${node} returned no topology record"
        (( map_count == NUM_GPUS_PER_NODE )) || \
            echo_error "CPU NUMA discovery on ${node} returned ${map_count} worker mappings; expected ${NUM_GPUS_PER_NODE}"

        if [[ -z "${reference_signature}" ]]; then
            reference_signature="${signature}"
        elif [[ "${signature}" != "${reference_signature}" ]]; then
            echo_error "CPU NUMA topology on ${node} differs from the coordinator host; heterogeneous CPU resource tuning is not supported"
        fi

        echo "CPU NUMA topology on ${node}: visible=${visible}, CPU+DRAM nodes=${cpu_ids} (CPUs=${cpu_counts}, DRAM_GB=${cpu_memory}), all memory nodes=${memory_ids}"
        echo "CPU NUMA worker mapping on ${node}: ${mapping}"
    done
}

# Inspect fixed coordinator, worker HTTP, and HTTP+3 exchange ports on one
# compute node. require-free waits until every port is reusable; require-bound
# waits until every selected port has been claimed. Exchange ports get an
# actual wildcard bind probe in addition to ss: UCX may own the sockaddr
# through a non-TCP listener, and a recently closed UCX listener can have no
# LISTEN entry while kernel socket state still makes a new bind fail with
# EADDRINUSE. The report includes Slurm cgroups, command lines, and matching
# TCP socket state where the kernel permits ownership inspection.
function run_port_inspection {
    [ $# -ne 5 ] && echo_error "$0 expected arguments 'node', 'specs', 'mode', 'output_file', and 'wait_seconds'"
    local node=$1 specs=$2 mode=$3 output_file=$4 wait_seconds=$5
    srun -N1 -w "${node}" --ntasks=1 --overlap \
        /bin/bash -s -- "${node}" "${specs}" "${mode}" "${wait_seconds}" \
        >"${output_file}" 2>&1 <<'EOS'
set -uo pipefail
node="$1"
specs="$2"
mode="$3"
wait_seconds="$4"

case "${mode}" in
    report|require-free|require-bound)
        ;;
    *)
        echo "Port inspection on ${node}: invalid mode ${mode}" >&2
        exit 2
        ;;
esac
if ! command -v ss >/dev/null 2>&1; then
    echo "Port inspection on ${node}: ss is not installed" >&2
    exit 2
fi
if ! command -v python3 >/dev/null 2>&1; then
    echo "Port inspection on ${node}: python3 is required for exchange-port bind probes" >&2
    exit 2
fi

probe_bind() {
    python3 - "$1" <<'PY'
import socket
import sys

port = int(sys.argv[1])
try:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("0.0.0.0", port))
except OSError as error:
    print(
        f"bind(0.0.0.0:{port}) failed: "
        f"[errno {error.errno}] {error.strerror}")
    raise SystemExit(1)
PY
}

inspect_once() {
    local mismatch=0 entry role port matches pids pid bind_error connections state show_details
    IFS=',' read -ra entries <<< "${specs}"
    for entry in "${entries[@]}"; do
        role="${entry%%:*}"
        port="${entry##*:}"
        state="free"
        bind_error=""
        connections=""
        show_details=0
        matches="$(ss -H -ltnp 2>/dev/null \
            | awk -v wanted="${port}" '{
                count = split($4, fields, ":")
                if (fields[count] == wanted) print
              }')"
        if [[ -n "${matches}" ]]; then
            state="listen"
        elif [[ "${role}" == *-exchange ]] && ! bind_error="$(probe_bind "${port}" 2>&1)"; then
            state="bind-blocked"
            connections="$(ss -H -tanp 2>/dev/null \
                | awk -v wanted="${port}" '{
                    count = split($4, fields, ":")
                    if (fields[count] == wanted) print
                  }')"
        fi

        case "${mode}" in
            report)
                case "${state}" in
                    listen)
                        echo "BUSY ${node} ${role} port ${port}"
                        show_details=1
                        ;;
                    bind-blocked)
                        echo "BIND-BLOCKED ${node} ${role} port ${port}"
                        [[ -n "${bind_error}" ]] && echo "${bind_error}"
                        [[ -n "${connections}" ]] && echo "${connections}"
                        ;;
                    free)
                        echo "FREE ${node} ${role} port ${port}"
                        ;;
                esac
                ;;
            require-free)
                if [[ "${state}" == "listen" ]]; then
                    mismatch=1
                    echo "BUSY ${node} ${role} port ${port}"
                    show_details=1
                elif [[ "${state}" == "bind-blocked" ]]; then
                    mismatch=1
                    echo "BIND-BLOCKED ${node} ${role} port ${port}"
                    [[ -n "${bind_error}" ]] && echo "${bind_error}"
                    [[ -n "${connections}" ]] && echo "${connections}"
                fi
                ;;
            require-bound)
                if [[ "${state}" == "free" ]]; then
                    mismatch=1
                    echo "MISSING ${node} ${role} port ${port}"
                else
                    echo "READY ${node} ${role} port ${port} (${state})"
                fi
                ;;
        esac

        if (( show_details == 1 )); then
            echo "${matches}"
            if command -v fuser >/dev/null 2>&1; then
                pids="$(fuser -n tcp "${port}" 2>/dev/null || true)"
                for pid in ${pids}; do
                    [[ "${pid}" =~ ^[0-9]+$ ]] || continue
                    ps -p "${pid}" -o pid=,ppid=,user=,stat=,lstart=,args= 2>/dev/null || true
                    if [[ -r "/proc/${pid}/cgroup" ]]; then
                        echo "  cgroup:"
                        sed 's/^/    /' "/proc/${pid}/cgroup"
                    fi
                    if [[ -r "/proc/${pid}/cmdline" ]]; then
                        echo -n "  cmdline: "
                        tr '\0' ' ' < "/proc/${pid}/cmdline"
                        echo
                    fi
                done
            fi
        fi
    done
    return "${mismatch}"
}

deadline=$((SECONDS + wait_seconds))
while true; do
    report="$(inspect_once)"
    status=$?
    if (( status == 0 )); then
        [[ -n "${report}" ]] && echo "${report}"
        exit 0
    fi
    if [[ "${mode}" == "report" ]] || (( SECONDS >= deadline )); then
        echo "${report}"
        exit "${status}"
    fi
    sleep 1
done
EOS
}

function preflight_cluster_ports {
    CLUSTER_PORT_SPECS_BY_NODE=()
    local preflight_timeout="${PRESTO_PORT_PREFLIGHT_TIMEOUT:-${PRESTO_PORT_RELEASE_TIMEOUT:-90}}"
    [[ "${preflight_timeout}" =~ ^[0-9]+$ ]] || \
        echo_error "Invalid PRESTO_PORT_PREFLIGHT_TIMEOUT=${preflight_timeout}"

    local worker_id=0 node local_worker_id config http_port exchange_port configured_exchange_port specs log_file
    for node in $(scontrol show hostnames "${SLURM_JOB_NODELIST}"); do
        specs=""
        if [[ "${node}" == "${COORD}" ]]; then
            specs="coordinator:${PORT}"
        fi
        for ((local_worker_id = 0; local_worker_id < NUM_GPUS_PER_NODE; ++local_worker_id)); do
            config="${CONFIGS}/etc_worker_${worker_id}/config_native.properties"
            http_port="$(awk -F= '/^http-server\.http\.port=/ {gsub(/[[:space:]]/, "", $2); print $2; exit}' "${config}")"
            [[ "${http_port}" =~ ^[1-9][0-9]*$ ]] || \
                echo_error "Worker ${worker_id}: invalid HTTP port in ${config}"
            (( http_port <= 65532 )) || \
                echo_error "Worker ${worker_id}: HTTP port ${http_port} leaves no valid HTTP+3 exchange port"
            exchange_port=$((http_port + 3))
            configured_exchange_port="$(awk -F= '/^cudf\.exchange\.server\.port=/ {gsub(/[[:space:]]/, "", $2); print $2; exit}' "${config}")"
            if [[ -n "${configured_exchange_port}" && "${configured_exchange_port}" != "${exchange_port}" ]]; then
                echo_error "Worker ${worker_id}: configured exchange port ${configured_exchange_port} does not match HTTP+3 (${exchange_port})"
            fi
            specs+="${specs:+,}worker${worker_id}-http:${http_port},worker${worker_id}-exchange:${exchange_port}"
            worker_id=$((worker_id + 1))
        done
        CLUSTER_PORT_SPECS_BY_NODE["${node}"]="${specs}"
        log_file="${LOGS}/port_preflight_${node}.log"
        if ! run_port_inspection "${node}" "${specs}" require-free "${log_file}" "${preflight_timeout}"; then
            cat "${log_file}" >&2
            echo_error "Fixed Presto/UCX ports are not bindable on ${node}; see ${log_file}"
        fi
        echo "Port preflight passed on ${node}: ${specs}"
    done
}

function collect_startup_diagnostics {
    local reason="${1:-startup failure}" node specs log_file index
    echo "Collecting cluster startup diagnostics: ${reason}" >&2
    echo "---- coord.log (last 120 lines) ----" >&2
    tail -n 120 "${LOGS}/coord.log" 2>/dev/null >&2 || true
    for ((index = 0; index < ${#WORKER_SRUN_IDS[@]}; ++index)); do
        echo "---- worker_${WORKER_SRUN_IDS[${index}]}.log on ${WORKER_SRUN_NODES[${index}]} (last 120 lines) ----" >&2
        tail -n 120 "${LOGS}/worker_${WORKER_SRUN_IDS[${index}]}.log" 2>/dev/null >&2 || true
    done
    for node in $(scontrol show hostnames "${SLURM_JOB_NODELIST}"); do
        specs="${CLUSTER_PORT_SPECS_BY_NODE["${node}"]:-}"
        [[ -n "${specs}" ]] || continue
        log_file="${LOGS}/startup_ports_${node}.log"
        run_port_inspection "${node}" "${specs}" report "${log_file}" 0 || true
        echo "---- listener ownership on ${node} ----" >&2
        cat "${log_file}" >&2 || true
    done
}

# Stop exactly the coordinator and worker srun clients started above, wait for
# their steps to disappear, and verify that their fixed ports have been
# released before the batch allocation exits.
function stop_cluster {
    [[ "${CLUSTER_STOPPED}" == "0" ]] || return 0
    CLUSTER_STOPPED=1

    local stop_timeout="${PRESTO_CLUSTER_STOP_TIMEOUT:-30}"
    local port_timeout="${PRESTO_PORT_RELEASE_TIMEOUT:-90}"
    [[ "${stop_timeout}" =~ ^[0-9]+$ ]] || {
        echo "Invalid PRESTO_CLUSTER_STOP_TIMEOUT=${stop_timeout}" >&2
        return 1
    }
    [[ "${port_timeout}" =~ ^[0-9]+$ ]] || {
        echo "Invalid PRESTO_PORT_RELEASE_TIMEOUT=${port_timeout}" >&2
        return 1
    }

    local -a tracked_pids=()
    local pid index deadline any_running=0 cleanup_rc=0 node specs log_file
    for ((index = ${#WORKER_SRUN_PIDS[@]} - 1; index >= 0; --index)); do
        pid="${WORKER_SRUN_PIDS[${index}]}"
        [[ -n "${pid}" ]] || continue
        tracked_pids+=("${pid}")
        if tracked_process_is_running "${pid}"; then
            echo "Stopping worker ${WORKER_SRUN_IDS[${index}]} srun PID ${pid}..."
            kill -TERM "${pid}" 2>/dev/null || true
        fi
    done
    if [[ -n "${COORD_SRUN_PID}" ]]; then
        tracked_pids+=("${COORD_SRUN_PID}")
        if tracked_process_is_running "${COORD_SRUN_PID}"; then
            echo "Stopping coordinator srun PID ${COORD_SRUN_PID}..."
            kill -TERM "${COORD_SRUN_PID}" 2>/dev/null || true
        fi
    fi

    deadline=$((SECONDS + stop_timeout))
    while true; do
        any_running=0
        for pid in "${tracked_pids[@]}"; do
            if tracked_process_is_running "${pid}"; then
                any_running=1
                break
            fi
        done
        (( any_running == 0 || SECONDS >= deadline )) && break
        sleep 1
    done

    if (( any_running != 0 )); then
        echo "Cluster steps did not stop within ${stop_timeout}s; sending KILL to the tracked srun clients." >&2
        for pid in "${tracked_pids[@]}"; do
            tracked_process_is_running "${pid}" && kill -KILL "${pid}" 2>/dev/null || true
        done
        cleanup_rc=1
    fi
    for pid in "${tracked_pids[@]}"; do
        wait "${pid}" 2>/dev/null || true
    done
    COORD_SRUN_PID=""
    WORKER_SRUN_PIDS=()

    for node in $(scontrol show hostnames "${SLURM_JOB_NODELIST}"); do
        specs="${CLUSTER_PORT_SPECS_BY_NODE["${node}"]:-}"
        [[ -n "${specs}" ]] || continue
        log_file="${LOGS}/port_cleanup_${node}.log"
        if ! run_port_inspection "${node}" "${specs}" require-free "${log_file}" "${port_timeout}"; then
            echo "Ports remained non-bindable after tracked-step cleanup on ${node}:" >&2
            cat "${log_file}" >&2 || true
            cleanup_rc=1
        else
            echo "Cluster ports released on ${node}."
        fi
    done

    return "${cleanup_rc}"
}

# When requested by launch-run.sh --verify-cpu-ucx, require every expected
# HTTP+3 UCX port to be claimed before query submission. Do not infer readiness
# from UCX log strings: their presence depends on log level and UCX version,
# which made the old verifier reject healthy, fully registered clusters.
function verify_cpu_ucx_worker_startup {
    [[ "${VERIFY_CPU_UCX:-0}" == "1" && "${VARIANT_TYPE:-gpu}" == "cpu" ]] || return 0

    local ready_timeout="${PRESTO_CPU_UCX_READY_TIMEOUT:-60}"
    [[ "${ready_timeout}" =~ ^[0-9]+$ ]] || \
        echo_error "Invalid PRESTO_CPU_UCX_READY_TIMEOUT=${ready_timeout}"

    local node specs exchange_specs entry role log_file
    local -a entries
    for node in $(scontrol show hostnames "${SLURM_JOB_NODELIST}"); do
        specs="${CLUSTER_PORT_SPECS_BY_NODE["${node}"]:-}"
        exchange_specs=""
        IFS=',' read -ra entries <<< "${specs}"
        for entry in "${entries[@]}"; do
            role="${entry%%:*}"
            [[ "${role}" == worker*-exchange ]] || continue
            exchange_specs+="${exchange_specs:+,}${entry}"
        done
        [[ -n "${exchange_specs}" ]] || \
            echo_error "CPU UCX verification found no expected exchange ports on ${node}"

        log_file="${LOGS}/cpu_ucx_readiness_${node}.log"
        if ! run_port_inspection "${node}" "${exchange_specs}" require-bound "${log_file}" "${ready_timeout}"; then
            cat "${log_file}" >&2 || true
            collect_startup_diagnostics "CPU UCX listener readiness failed on ${node}"
            echo_error "CPU UCX listener readiness verification failed on ${node}; see ${log_file}"
        fi
        cat "${log_file}"
    done
    echo "CPU UCX listener ports verified on all ${NUM_WORKERS} worker(s)."
}

# For multi-worker runs, require a retained coordinator query to report at
# least one CPU UCX replacement operator. Startup verification above proves
# every listener was ready before query submission; this check proves the
# selected query actually used the CPU-row UCX path. A one-worker plan cannot
# prove inter-worker transport and is therefore limited to listener readiness.
function verify_cpu_ucx_runtime {
    [[ "${VERIFY_CPU_UCX:-0}" == "1" && "${VARIANT_TYPE:-gpu}" == "cpu" ]] || return 0

    [[ "${NUM_WORKERS:-}" =~ ^[1-9][0-9]*$ ]] || \
        echo_error "CPU UCX verification requires a positive NUM_WORKERS value"
    if (( NUM_WORKERS == 1 )); then
        echo "CPU UCX runtime operator verification skipped for one worker; listener readiness was verified before query submission."
        return 0
    fi

    local query_details_dir="${RESULT_DIR:-${SCRIPT_DIR}/result_dir}/coordinator_queries/queries"
    if ! grep -R -E -q \
        '"operatorType"[[:space:]]*:[[:space:]]*"UcxCpuRow(Exchange|PartitionedOutput)"' \
        "${query_details_dir}" 2>/dev/null; then
        echo "CPU UCX verification found no UcxCpuRowExchange or UcxCpuRowPartitionedOutput operator." >&2
        echo "Run an exchange-heavy query (for example TPC-H Q18) with at least two workers." >&2
        echo_error "CPU UCX operator verification failed"
    fi
    echo "CPU UCX replacement operator verified in retained query details."
}

# Start the coordinator, wait for it to come up, fan out NUM_GPUS_PER_NODE
# local worker slots per node across $SLURM_JOB_NODELIST, then block until all
# $NUM_WORKERS register.  Used by both the benchmark and analyze flows.
function start_cluster {
    prepare_cpu_numa_layout
    preflight_cluster_ports

    echo "Starting Presto coordinator on ${COORD}..."
    run_coordinator
    wait_until_coordinator_is_running

    echo "Starting ${NUM_WORKERS} Presto workers across ${NUM_NODES} nodes..."
    local worker_id=0 node local_worker_id
    for node in $(scontrol show hostnames "$SLURM_JOB_NODELIST"); do
        for local_worker_id in $(seq 0 $((NUM_GPUS_PER_NODE - 1))); do
            if [[ "${VARIANT_TYPE}" == "gpu" ]]; then
                echo "  Starting worker ${worker_id} on node ${node} GPU ${local_worker_id}"
            else
                echo "  Starting worker ${worker_id} on node ${node} local slot ${local_worker_id}"
            fi
            run_worker "${local_worker_id}" "$WORKER_IMAGE" "${node}" "$worker_id"
            worker_id=$((worker_id + 1))
        done
    done

    echo "Waiting for ${NUM_WORKERS} workers to register with coordinator..."
    wait_for_workers_to_register "$NUM_WORKERS"
    verify_cpu_ucx_worker_startup
    start_perf_sampler
}

# ----------------------------------------------------------------------------
# Shared Hive metastore: publish/populate
# ----------------------------------------------------------------------------
# When HIVE_METASTORE_VERSION is set, analyze runs publish their post-ANALYZE
# tpchsf<SF> tree under $HIVE_METASTORE_SHARED_ROOT/<version>/tpchsf<SF>/, and
# subsequent benchmark runs populate from that snapshot instead of re-analyzing.
# Paths inside a .prestoSchema file are container-relative
# (file:/var/lib/presto/data/hive/data/user_data/tpch-rs-<SF>/<table>), so a
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
    local result_dir="${RESULT_DIR:-${SCRIPT_DIR}/result_dir}"
    local container_result_dir="${result_dir/${VT_ROOT}//workspace}"
    [[ "${container_result_dir}" == /workspace/* ]] || \
        echo_error "RESULT_DIR must be below VT_ROOT (${VT_ROOT}) so it is mounted in the CLI container: ${result_dir}"

    source "${SCRIPT_DIR}/defaults.env"

    local extra_args=()
    [[ "${ENABLE_METRICS}" == "1" ]] && extra_args+=("-m")
    if [[ "${ENABLE_NSYS}" == "1" ]]; then
        extra_args+=("-p" "--profile-script-path" "${container_script_dir}/profiler_functions.sh")
    elif [[ "${ENABLE_PERF:-0}" == "1" ]]; then
        extra_args+=("-p" "--profile-script-path" "${container_script_dir}/perf_profiler_functions.sh")
    fi
    [[ -n "${QUERIES:-}" ]] && extra_args+=("-q" "${QUERIES}")
    if [[ -n "${QUERIES_FILE:-}" ]]; then
        local container_queries_file="${QUERIES_FILE}"
        if [[ "${container_queries_file}" == "${VT_ROOT}/"* ]]; then
            container_queries_file="/workspace/${container_queries_file#"${VT_ROOT}/"}"
        fi
        [[ "${container_queries_file}" == /workspace/* ]] || \
            echo_error "QUERIES_FILE must be below VT_ROOT (${VT_ROOT}) so it is mounted in the CLI container: ${QUERIES_FILE}"
        extra_args+=("--queries-file" "${container_queries_file}")
    fi

    local benchmark_args=(
        ./run_benchmark.sh
        -b tpch
        -s "tpchsf${scale_factor}"
        -i "${num_iterations}"
        "${extra_args[@]}"
        --hostname "${COORD}"
        --port "${PORT}"
        -o "${container_result_dir}"
        --skip-drop-cache
    )
    local benchmark_cmd
    printf -v benchmark_cmd '%q ' "${benchmark_args[@]}"

    # Result validation is intentionally not wired here yet: that belongs
    # to PR #275 (upstream validate_results.py) and will be hooked up
    # after the PR merges and this branch is rebased.
    # Cache-drop is skipped because it requires docker (not available on
    # the cluster).
    run_coord_image "export PORT=$PORT; \
    export HOSTNAME=$COORD; \
    export PRESTO_DATA_DIR=/var/lib/presto/data/hive/data/user_data; \
    export MINIFORGE_HOME=/workspace/miniforge3; \
    export HOME=/workspace; \
    cd /workspace/presto/scripts; \
    ${benchmark_cmd}" "cli"
}

# Check if the coordinator is running via curl.  Fail after 10 retries.
function wait_until_coordinator_is_running {
    echo "waiting for coordinator to be accessible"
    validate_environment_preconditions COORD LOGS
    local state="INACTIVE" coord_rc=0
    for i in {1..10}; do
        state=$(curl -s http://${COORD}:${PORT}/v1/info/state || true)
        if [[ "$state" == "\"ACTIVE\"" ]]; then
            echo "coord started.  state: $state"
	    return 0
        fi
        if [[ -n "${COORD_SRUN_PID}" ]] && ! tracked_process_is_running "${COORD_SRUN_PID}"; then
            wait "${COORD_SRUN_PID}" || coord_rc=$?
            COORD_SRUN_PID=""
            collect_startup_diagnostics "coordinator srun exited with status ${coord_rc}"
            echo_error "coord did not start; its srun exited with status ${coord_rc}"
        fi
        sleep 5
    done
    collect_startup_diagnostics "coordinator did not become ACTIVE"
    echo_error "coord did not start.  state: $state"
}

# Check N nodes are registered with the coordinator.  Fail after 60 retries (5 minutes).
function wait_for_workers_to_register {
    validate_environment_preconditions LOGS COORD
    [ $# -ne 1 ] && echo_error "$0 expected one argument for 'expected number of workers'"
    echo "waiting for $1 workers to register"
    local expected_num_workers=$1
    local num_workers=0 index pid worker_rc
    for i in {1..60}; do
        num_workers=$(curl -fsS "http://${COORD}:${PORT}/v1/node" 2>/dev/null \
            | python3 -c 'import json, sys; print(len(json.load(sys.stdin)))' 2>/dev/null \
            || echo 0)
        if [[ "${num_workers}" =~ ^[0-9]+$ ]] && (( num_workers == expected_num_workers )); then
            echo "workers registered. num_nodes: $num_workers"
	    return 0
        fi
        for ((index = 0; index < ${#WORKER_SRUN_PIDS[@]}; ++index)); do
            pid="${WORKER_SRUN_PIDS[${index}]}"
            [[ -n "${pid}" ]] || continue
            if ! tracked_process_is_running "${pid}"; then
                worker_rc=0
                wait "${pid}" || worker_rc=$?
                WORKER_SRUN_PIDS[${index}]=""
                collect_startup_diagnostics "worker ${WORKER_SRUN_IDS[${index}]} on ${WORKER_SRUN_NODES[${index}]} exited with status ${worker_rc}; ${num_workers}/${expected_num_workers} workers registered"
                echo_error "worker ${WORKER_SRUN_IDS[${index}]} exited before cluster registration completed"
            fi
        done
        sleep 5
    done
    collect_startup_diagnostics "worker registration timed out at ${num_workers}/${expected_num_workers}"
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
    local result_dir="${RESULT_DIR:-${SCRIPT_DIR}/result_dir}"

    echo "Copying configs to ${result_dir}/configs/..."
    mkdir -p "${result_dir}/configs"
    cp "${CONFIGS}/etc_coordinator/config_native.properties" "${result_dir}/configs/coordinator.config"
    cp "${CONFIGS}/etc_worker_0/config_native.properties"    "${result_dir}/configs/worker.config"

    echo "Copying logs to ${result_dir}/..."
    cp "${LOGS}"/*.log "${result_dir}/"
    cp "${LOGS}"/*.out "${LOGS}"/*.err "${result_dir}/" 2>/dev/null || true
}

function inject_benchmark_metadata {
    local result_file="${RESULT_DIR:-${SCRIPT_DIR}/result_dir}/benchmark_result.json"
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

    local worker_image_path
    worker_image_path=$(resolve_image_path "${WORKER_IMAGE}")
    local image_digest
    echo "Computing SHA256 of ${worker_image_path}..."
    image_digest=$(sha256sum "${worker_image_path}" | awk '{print $1}') || true
    image_digest="${image_digest:-unknown}"
    echo "Image digest: ${image_digest}"

    python3 - "${result_file}" \
        "${kind}" "${timestamp}" "${NUM_WORKERS}" "${NUM_NODES}" \
        "${SCALE_FACTOR}" "${gpu_count}" "${gpu_name}" "${num_drivers}" \
        "${WORKER_IMAGE}" "${image_digest}" "${engine}" <<'PY'
import json
import os
import pathlib
import sys

(
    result_path,
    kind,
    timestamp,
    n_workers,
    node_count,
    scale_factor,
    gpu_count,
    gpu_name,
    num_drivers,
    worker_image,
    image_digest,
    engine,
) = sys.argv[1:]

path = pathlib.Path(result_path)
with path.open() as source:
    result = json.load(source)

context = result.get("context")
if not isinstance(context, dict):
    context = {}
    result["context"] = context
context.update(
    {
        "kind": kind,
        "timestamp": timestamp,
        "n_workers": int(n_workers),
        "node_count": int(node_count),
        "scale_factor": int(scale_factor),
        "gpu_count": int(gpu_count),
        "gpu_name": gpu_name,
        "num_drivers": int(num_drivers),
        "worker_image": worker_image,
        "image_digest": image_digest,
        "engine": engine,
    }
)

temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
with temporary.open("w") as output:
    json.dump(result, output, indent=2)
    output.write("\n")
os.replace(temporary, path)
PY
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

        declare -A prev_sizes
        local stable_count=0
        for i in {1..120}; do
            local all_stable=true
            for qnum in "${qlist[@]}"; do
                report="${LOGS}/nsys_worker_w${NSYS_WORKER_ID}_Q${qnum}.nsys-rep"
                fallback="${LOGS}/nsys_worker_w${NSYS_WORKER_ID}_Q${qnum}.qdstrm"
                if [[ -f "$report" ]]; then
                    target="$report"
                elif [[ -f "$fallback" ]]; then
                    target="$fallback"
                else
                    echo "    w${NSYS_WORKER_ID} Q${qnum}: no file yet"
                    all_stable=false
                    continue
                fi
                cur_size=$(stat -c%s "$target" 2>/dev/null || echo 0)
                prev=${prev_sizes["$target"]:-0}
                echo "    w${NSYS_WORKER_ID} Q${qnum}: cur=${cur_size} prev=${prev}"
                if (( cur_size == 0 || cur_size != prev )); then
                    all_stable=false
                fi
                prev_sizes["$target"]=$cur_size
            done
            echo "  all_stable=${all_stable} stable_count=${stable_count}"
            if $all_stable; then
                stable_count=$((stable_count + 1))
                if (( stable_count >= 3 )); then
                    echo "All ${#qlist[@]} nsys reports stable."
                    break
                fi
            else
                stable_count=0
            fi
            sleep 5
        done

        echo "Copying nsys reports to ${RESULT_DIR:-${SCRIPT_DIR}/result_dir}/..."
        cp "${LOGS}"/*.nsys-rep "${RESULT_DIR:-${SCRIPT_DIR}/result_dir}/"
    fi
}
