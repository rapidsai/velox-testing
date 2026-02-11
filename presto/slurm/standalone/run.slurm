#!/bin/bash
#SBATCH --job-name=presto-tpch
#SBATCH --time=01:00:00
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=144
#SBATCH --mem=0
#SBATCH --gres=gpu:4
#SBATCH --exclusive

# ==============================================================================
# Presto TPC-H Benchmark - Self-Contained Slurm Script
# ==============================================================================
# All configuration, functions, and execution logic in one file
# ==============================================================================

set -e
set -x

# ==============================================================================
# Environment Configuration
# ==============================================================================
export PORT=9200
export NUM_GPUS_PER_NODE=4
export CUDF_LIB=/usr/lib64/presto-native-libs

# UCX Configuration
export UCX_TLS=^ib,ud:aux,sm
export UCX_MAX_RNDV_RAILS=1
export UCX_RNDV_PIPELINE_ERROR_HANDLING=y
export UCX_TCP_KEEPINTVL=1ms
export UCX_KEEPALIVE_INTERVAL=1ms

# Computed values
export COORD=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -n 1)
export NUM_NODES=$SLURM_JOB_NUM_NODES
export NUM_WORKERS=$((NUM_NODES * NUM_GPUS_PER_NODE))

# Config directory (will be generated)
export CONFIGS="${SCRIPT_DIR}/configs"

# Memory calculations (simplified, in GB)
export RAM_GB=900
export HEAP_SIZE_GB=810
export HEADROOM_GB=162
export QUERY_MAX_TOTAL_MEM_GB=648
export QUERY_MAX_MEM_GB=583
export SYSTEM_MEM_GB=850
export QUERY_MEM_GB=807
export SYSTEM_MEM_LIMIT_GB=845
export JOIN_MAX_BCAST_MB=9000
export MAX_DRIVERS=2

# ==============================================================================
# Helper Functions
# ==============================================================================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

# ==============================================================================
# Config Generation Functions
# ==============================================================================
generate_configs() {
    log_info "Generating configuration files..."

    rm -rf "$CONFIGS"
    mkdir -p "$CONFIGS"

    # Determine single-node execution mode
    if [[ $NUM_WORKERS -eq 1 ]]; then
        SINGLE_NODE_EXEC="true"
        CUDF_EXCHANGE="false"
        JOIN_DIST_TYPE="AUTOMATIC"
        OPTIMIZER_GPU_SETTINGS="optimizer.joins-not-null-inference-strategy=USE_FUNCTION_METADATA\noptimizer.default-filter-factor-enabled=true"
        CLUSTER_TAG="cluster-tag=native-gpu"
    else
        SINGLE_NODE_EXEC="false"
        CUDF_EXCHANGE="true"
        JOIN_DIST_TYPE="PARTITIONED"
        OPTIMIZER_GPU_SETTINGS="# Multi-node GPU settings"
        CLUSTER_TAG="# Multi-node cluster"
    fi

    # Generate coordinator config
    mkdir -p "$CONFIGS/coordinator/catalog"
    sed -e "s|__PORT__|$PORT|g" \
        -e "s|__COORD_HOST__|$COORD|g" \
        -e "s|__HEADROOM_GB__|$HEADROOM_GB|g" \
        -e "s|__JOIN_MAX_BCAST_MB__|$JOIN_MAX_BCAST_MB|g" \
        -e "s|__JOIN_DISTRIBUTION_TYPE__|$JOIN_DIST_TYPE|g" \
        -e "s|__QUERY_MAX_TOTAL_MEM_GB__|$QUERY_MAX_TOTAL_MEM_GB|g" \
        -e "s|__QUERY_MAX_TOTAL_MEM_CLUSTER_GB__|$((QUERY_MAX_TOTAL_MEM_GB * NUM_WORKERS))|g" \
        -e "s|__QUERY_MAX_MEM_GB__|$QUERY_MAX_MEM_GB|g" \
        -e "s|__QUERY_MAX_MEM_CLUSTER_GB__|$((QUERY_MAX_MEM_GB * NUM_WORKERS))|g" \
        -e "s|__SINGLE_NODE_EXECUTION__|$SINGLE_NODE_EXEC|g" \
        -e "s|__OPTIMIZER_GPU_SETTINGS__|$OPTIMIZER_GPU_SETTINGS|g" \
        -e "s|__CLUSTER_TAG__|$CLUSTER_TAG|g" \
        "${SCRIPT_DIR}/config-templates/coordinator/config.properties" \
        > "$CONFIGS/coordinator/config.properties"

    cp "${SCRIPT_DIR}/config-templates/coordinator/node.properties" \
       "$CONFIGS/coordinator/node.properties"

    # Generate common configs
    sed -e "s|__HEAP_SIZE_GB__|$HEAP_SIZE_GB|g" \
        "${SCRIPT_DIR}/config-templates/common/jvm.config" \
        > "$CONFIGS/jvm.config"

    cp "${SCRIPT_DIR}/config-templates/common/log.properties" \
       "$CONFIGS/log.properties"

    # Copy catalog configs for coordinator
    cp "${SCRIPT_DIR}/config-templates/common/hive.properties" \
       "$CONFIGS/coordinator/catalog/hive.properties"
    cp "${SCRIPT_DIR}/config-templates/common/tpch.properties" \
       "$CONFIGS/coordinator/catalog/tpch.properties"
    cp "${SCRIPT_DIR}/config-templates/common/tpcds.properties" \
       "$CONFIGS/coordinator/catalog/tpcds.properties"
    cp "${SCRIPT_DIR}/config-templates/common/jmx.properties" \
       "$CONFIGS/coordinator/catalog/jmx.properties"
    cp "${SCRIPT_DIR}/config-templates/common/memory.properties" \
       "$CONFIGS/coordinator/catalog/memory.properties"

    # Generate worker configs for each worker
    local worker_id=0
    for node in $(scontrol show hostnames "$SLURM_JOB_NODELIST"); do
        for gpu_id in $(seq 0 $((NUM_GPUS_PER_NODE - 1))); do
            local http_port="10$(printf "%02d" "$worker_id")0"
            local exchange_port="10$(printf "%02d" "$worker_id")3"

            mkdir -p "$CONFIGS/worker_${worker_id}/catalog"

            sed -e "s|__WORKER_ID__|$worker_id|g" \
                "${SCRIPT_DIR}/config-templates/worker/node.properties" \
                > "$CONFIGS/worker_${worker_id}/node.properties"

            sed -e "s|__HTTP_PORT__|$http_port|g" \
                -e "s|__COORD_HOST__|$COORD|g" \
                -e "s|__PORT__|$PORT|g" \
                -e "s|__SYSTEM_MEM_GB__|$SYSTEM_MEM_GB|g" \
                -e "s|__QUERY_MEM_GB__|$QUERY_MEM_GB|g" \
                -e "s|__SYSTEM_MEM_LIMIT_GB__|$SYSTEM_MEM_LIMIT_GB|g" \
                -e "s|__MAX_DRIVERS__|$MAX_DRIVERS|g" \
                -e "s|__SINGLE_NODE_EXECUTION__|$SINGLE_NODE_EXEC|g" \
                -e "s|__CUDF_EXCHANGE__|$CUDF_EXCHANGE|g" \
                -e "s|__EXCHANGE_PORT__|$exchange_port|g" \
                "${SCRIPT_DIR}/config-templates/worker/config.properties" \
                > "$CONFIGS/worker_${worker_id}/config.properties"

            # Copy catalog configs for worker (only native-supported catalogs)
            cp "${SCRIPT_DIR}/config-templates/common/hive.properties" \
               "$CONFIGS/worker_${worker_id}/catalog/hive.properties"
            cp "${SCRIPT_DIR}/config-templates/common/tpch.properties" \
               "$CONFIGS/worker_${worker_id}/catalog/tpch.properties"
            cp "${SCRIPT_DIR}/config-templates/common/tpcds.properties" \
               "$CONFIGS/worker_${worker_id}/catalog/tpcds.properties"
            # Note: JMX and Memory connectors are Java-only, not supported by native workers

            worker_id=$((worker_id + 1))
        done
    done

    log_info "Configuration files generated successfully"
}

# ==============================================================================
# Coordinator Functions
# ==============================================================================
start_coordinator() {
    log_info "Starting coordinator on ${COORD}:${PORT}..."

    mkdir -p "${VT_ROOT}/.hive_metastore"

    srun -w "$COORD" --ntasks=1 --overlap \
        --container-image="${IMAGE_DIR}/${COORD_IMAGE}.sqsh" \
        --export=ALL,JAVA_HOME=/usr/lib/jvm/jre-17-openjdk \
        --container-env=JAVA_HOME=/usr/lib/jvm/jre-17-openjdk \
        --container-env=PATH=/usr/lib/jvm/jre-17-openjdk/bin:\$PATH \
        --container-mounts="${VT_ROOT}:/workspace,\
${CONFIGS}/jvm.config:/opt/presto-server/etc/jvm.config,\
${CONFIGS}/log.properties:/opt/presto-server/etc/log.properties,\
${CONFIGS}/coordinator/node.properties:/opt/presto-server/etc/node.properties,\
${CONFIGS}/coordinator/config.properties:/opt/presto-server/etc/config.properties,\
${CONFIGS}/coordinator/catalog/hive.properties:/opt/presto-server/etc/catalog/hive.properties,\
${CONFIGS}/coordinator/catalog/tpch.properties:/opt/presto-server/etc/catalog/tpch.properties,\
${CONFIGS}/coordinator/catalog/tpcds.properties:/opt/presto-server/etc/catalog/tpcds.properties,\
${CONFIGS}/coordinator/catalog/jmx.properties:/opt/presto-server/etc/catalog/jmx.properties,\
${CONFIGS}/coordinator/catalog/memory.properties:/opt/presto-server/etc/catalog/memory.properties,\
${DATA_DIR}:/var/lib/presto/data/hive/data/user_data,\
${VT_ROOT}/.hive_metastore:/var/lib/presto/data/hive/metastore" \
        -- bash -c "unset CONFIG NODE_CONFIG PRESTO_ETC JAVA_TOOL_OPTIONS JDK_JAVA_OPTIONS _JAVA_OPTIONS; \
            export JAVA_HOME=/usr/lib/jvm/jre-17-openjdk; \
            export PATH=/usr/lib/jvm/jre-17-openjdk/bin:\$PATH; \
            /opt/presto-server/bin/launcher run" \
        > "${SCRIPT_DIR}/logs/coordinator.log" 2>&1 &
}

wait_for_coordinator() {
    log_info "Waiting for coordinator to start..."

    for i in {1..60}; do
        state=$(curl -s "http://${COORD}:${PORT}/v1/info/state" 2>/dev/null || echo "")
        if [[ "$state" == '"ACTIVE"' ]]; then
            log_info "Coordinator is active"
            return 0
        fi
        sleep 5
    done

    log_error "Coordinator failed to start (timeout)"
}

# ==============================================================================
# Worker Functions
# ==============================================================================
start_worker() {
    local worker_id=$1
    local gpu_id=$2
    local node=$3

    log_info "Starting worker ${worker_id} on ${node} (GPU ${gpu_id})..."

    local worker_data="${SCRIPT_DIR}/worker_data_${worker_id}"
    mkdir -p "${worker_data}/hive/data/user_data"
    mkdir -p "${VT_ROOT}/.hive_metastore"

    srun -N1 -w "$node" --ntasks=1 --overlap \
        --container-image="${IMAGE_DIR}/${WORKER_IMAGE}.sqsh" \
        --export=ALL \
        --container-env=LD_LIBRARY_PATH="/usr/lib64/presto-native-libs:/usr/local/lib:/usr/lib64:${CUDF_LIB}" \
        --container-env=GLOG_vmodule=IntraNodeTransferRegistry=3,ExchangeOperator=3 \
        --container-env=GLOG_logtostderr=1 \
        --container-mounts="${VT_ROOT}:/workspace,\
${CONFIGS}/jvm.config:/opt/presto-server/etc/jvm.config,\
${CONFIGS}/log.properties:/opt/presto-server/etc/log.properties,\
${CONFIGS}/worker_${worker_id}/node.properties:/opt/presto-server/etc/node.properties,\
${CONFIGS}/worker_${worker_id}/config.properties:/opt/presto-server/etc/config.properties,\
${CONFIGS}/worker_${worker_id}/catalog/hive.properties:/opt/presto-server/etc/catalog/hive.properties,\
${CONFIGS}/worker_${worker_id}/catalog/tpch.properties:/opt/presto-server/etc/catalog/tpch.properties,\
${CONFIGS}/worker_${worker_id}/catalog/tpcds.properties:/opt/presto-server/etc/catalog/tpcds.properties,\
${worker_data}:/var/lib/presto/data,\
${DATA_DIR}:/var/lib/presto/data/hive/data/user_data,\
${VT_ROOT}/.hive_metastore:/var/lib/presto/data/hive/metastore" \
        -- bash -c "export CUDA_VISIBLE_DEVICES=${gpu_id}; \
            echo \"CUDA_VISIBLE_DEVICES=\$CUDA_VISIBLE_DEVICES\"; \
            nvidia-smi -L; \
            /usr/bin/presto_server --etc-dir=/opt/presto-server/etc" \
        > "${SCRIPT_DIR}/logs/worker_${worker_id}.log" 2>&1 &
}

start_all_workers() {
    log_info "Starting ${NUM_WORKERS} workers..."

    local worker_id=0
    for node in $(scontrol show hostnames "$SLURM_JOB_NODELIST"); do
        for gpu_id in $(seq 0 $((NUM_GPUS_PER_NODE - 1))); do
            start_worker "$worker_id" "$gpu_id" "$node"
            worker_id=$((worker_id + 1))
        done
    done
}

wait_for_workers() {
    log_info "Waiting for ${NUM_WORKERS} workers to register..."

    for i in {1..60}; do
        num_registered=$(curl -s "http://${COORD}:${PORT}/v1/node" 2>/dev/null | jq length 2>/dev/null || echo "0")
        if [[ "$num_registered" -eq "$NUM_WORKERS" ]]; then
            log_info "All ${NUM_WORKERS} workers registered"
            return 0
        fi
        log_info "Workers registered: ${num_registered}/${NUM_WORKERS}"
        sleep 5
    done

    log_error "Workers failed to register (timeout)"
}

# ==============================================================================
# Benchmark Functions
# ==============================================================================
setup_benchmark() {
    log_info "Setting up TPC-H schema (scale factor: ${SCALE_FACTOR})..."

    srun -w "$COORD" --ntasks=1 --overlap \
        --container-image="${IMAGE_DIR}/${COORD_IMAGE}.sqsh" \
        --export=ALL,JAVA_HOME=/usr/lib/jvm/jre-17-openjdk \
        --container-env=JAVA_HOME=/usr/lib/jvm/jre-17-openjdk \
        --container-env=PATH=/usr/lib/jvm/jre-17-openjdk/bin:\$PATH \
        --container-mounts="${VT_ROOT}:/workspace,\
${CONFIGS}/jvm.config:/opt/presto-server/etc/jvm.config,\
${CONFIGS}/log.properties:/opt/presto-server/etc/log.properties,\
${CONFIGS}/coordinator/node.properties:/opt/presto-server/etc/node.properties,\
${CONFIGS}/coordinator/config.properties:/opt/presto-server/etc/config.properties,\
${CONFIGS}/coordinator/catalog/hive.properties:/opt/presto-server/etc/catalog/hive.properties,\
${CONFIGS}/coordinator/catalog/tpch.properties:/opt/presto-server/etc/catalog/tpch.properties,\
${CONFIGS}/coordinator/catalog/tpcds.properties:/opt/presto-server/etc/catalog/tpcds.properties,\
${CONFIGS}/coordinator/catalog/jmx.properties:/opt/presto-server/etc/catalog/jmx.properties,\
${CONFIGS}/coordinator/catalog/memory.properties:/opt/presto-server/etc/catalog/memory.properties,\
${DATA_DIR}:/var/lib/presto/data/hive/data/user_data,\
${VT_ROOT}/.hive_metastore:/var/lib/presto/data/hive/metastore" \
        -- bash -c "yum install -y python3.12 jq > /dev/null 2>&1; \
            export PORT=${PORT}; \
            export HOSTNAME=${COORD}; \
            export PRESTO_DATA_DIR=/var/lib/presto/data/hive/data/user_data; \
            cd /workspace/presto/scripts; \
            ./setup_benchmark_tables.sh -b tpch -d date-scale-${SCALE_FACTOR} -s tpchsf${SCALE_FACTOR} --skip-analyze-tables --no-docker" \
        > "${SCRIPT_DIR}/logs/setup.log" 2>&1
}

run_queries() {
    log_info "Running TPC-H queries (${NUM_ITERATIONS} iterations)..."

    srun -w "$COORD" --ntasks=1 --overlap \
        --container-image="${IMAGE_DIR}/${COORD_IMAGE}.sqsh" \
        --export=ALL,JAVA_HOME=/usr/lib/jvm/jre-17-openjdk \
        --container-env=JAVA_HOME=/usr/lib/jvm/jre-17-openjdk \
        --container-env=PATH=/usr/lib/jvm/jre-17-openjdk/bin:\$PATH \
        --container-mounts="${VT_ROOT}:/workspace,\
${CONFIGS}/jvm.config:/opt/presto-server/etc/jvm.config,\
${CONFIGS}/log.properties:/opt/presto-server/etc/log.properties,\
${CONFIGS}/coordinator/node.properties:/opt/presto-server/etc/node.properties,\
${CONFIGS}/coordinator/config.properties:/opt/presto-server/etc/config.properties,\
${CONFIGS}/coordinator/catalog/hive.properties:/opt/presto-server/etc/catalog/hive.properties,\
${CONFIGS}/coordinator/catalog/tpch.properties:/opt/presto-server/etc/catalog/tpch.properties,\
${CONFIGS}/coordinator/catalog/tpcds.properties:/opt/presto-server/etc/catalog/tpcds.properties,\
${CONFIGS}/coordinator/catalog/jmx.properties:/opt/presto-server/etc/catalog/jmx.properties,\
${CONFIGS}/coordinator/catalog/memory.properties:/opt/presto-server/etc/catalog/memory.properties,\
${DATA_DIR}:/var/lib/presto/data/hive/data/user_data,\
${VT_ROOT}/.hive_metastore:/var/lib/presto/data/hive/metastore" \
        -- bash -c "yum install -y python3.12 jq > /dev/null 2>&1; \
            export PORT=${PORT}; \
            export HOSTNAME=${COORD}; \
            export PRESTO_DATA_DIR=/var/lib/presto/data/hive/data/user_data; \
            cd /workspace/presto/scripts; \
            ./run_benchmark.sh -b tpch -s tpchsf${SCALE_FACTOR} -i ${NUM_ITERATIONS} --hostname ${COORD} --port ${PORT} -o /workspace/presto/slurm/standalone/result_dir" \
        > "${SCRIPT_DIR}/logs/queries.log" 2>&1

    # Copy summary to result_dir
    if [[ -f "${SCRIPT_DIR}/logs/queries.log" ]]; then
        cp "${SCRIPT_DIR}/logs/queries.log" "${SCRIPT_DIR}/result_dir/summary.txt"
    fi
}

# ==============================================================================
# Main Execution
# ==============================================================================
log_info "=========================================="
log_info "Presto TPC-H Benchmark"
log_info "=========================================="
log_info "Job ID: $SLURM_JOB_ID"
log_info "Nodes: $NUM_NODES ($SLURM_JOB_NODELIST)"
log_info "Coordinator: $COORD"
log_info "Workers: $NUM_WORKERS"
log_info "Scale Factor: $SCALE_FACTOR"
log_info "Iterations: $NUM_ITERATIONS"
log_info "=========================================="

mkdir -p "${SCRIPT_DIR}/logs"

# Generate configs
generate_configs

# Start coordinator
start_coordinator
wait_for_coordinator

# Start workers
start_all_workers
wait_for_workers

# Setup and run benchmark
setup_benchmark
run_queries

log_info "=========================================="
log_info "Benchmark completed!"
log_info "Results: ${SCRIPT_DIR}/result_dir/"
log_info "Logs: ${SCRIPT_DIR}/logs/"
log_info "=========================================="
