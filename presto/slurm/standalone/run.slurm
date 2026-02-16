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

# Dynamic memory calculations based on system RAM, matching the formulas
# in presto/docker/config/params.json used by the pbench config generator.
RAM_GB=$(lsmem -b 2>/dev/null | grep "Total online memory" | awk '{print int($4 / (1024*1024*1024)); }')
if [[ -z "$RAM_GB" || "$RAM_GB" -eq 0 ]]; then
    log_error "Failed to detect system memory via lsmem"
fi

# Coordinator memory (Java)
export RAM_GB
export HEAP_SIZE_GB=$(echo "$RAM_GB" | awk '{printf "%.0f", $1 * 0.9}')
export HEADROOM_GB=$(echo "$HEAP_SIZE_GB" | awk '{printf "%.0f", $1 * 0.2}')
export QUERY_MAX_TOTAL_MEM_GB=$(echo "$HEAP_SIZE_GB" | awk '{printf "%.0f", $1 * 0.8}')
export QUERY_MAX_MEM_GB=$(echo "$QUERY_MAX_TOTAL_MEM_GB" | awk '{printf "%.0f", $1 * 0.9}')
export JOIN_MAX_BCAST_MB=$(echo "$RAM_GB" | awk '{printf "%.0f", $1 * 0.01 * 1024}')

# Worker memory (Native/C++) - reserved memory computed from params.json formulas:
#   sys_reserved  = min(RAM * 0.05, 2)     -- system reserved memory
#   proxygen_mem  = min(0.125, 2)           -- proxygen per-worker overhead
#   native_buffer = min(RAM * 0.05, 32)    -- native buffer memory
#   total_reserved = sys_reserved + proxygen_mem + native_buffer
SYS_RESERVED_GB=$(echo "$RAM_GB" | awk '{
    sys   = ($1 * 0.05 < 2)  ? $1 * 0.05 : 2;
    prox  = 0.125;
    buf   = ($1 * 0.05 < 32) ? $1 * 0.05 : 32;
    printf "%.0f", sys + prox + buf
}')
export SYSTEM_MEM_GB=$(echo "$RAM_GB $SYS_RESERVED_GB" | awk '{printf "%.0f", $1 - $2}')
export QUERY_MEM_GB=$(echo "$SYSTEM_MEM_GB" | awk '{printf "%.0f", $1 * 0.95}')
export SYSTEM_MEM_LIMIT_GB=$(echo "$RAM_GB" | awk '{printf "%.0f", $1 - 5}')

# Worker settings
export MAX_DRIVERS=2

log_info "Memory configuration: RAM=${RAM_GB}GB, Heap=${HEAP_SIZE_GB}GB, Worker System=${SYSTEM_MEM_GB}GB, Worker Query=${QUERY_MEM_GB}GB"

# ==============================================================================
# Config Generation Functions
# ==============================================================================
generate_configs() {
    log_info "Generating configuration files..."

    rm -rf "$CONFIGS"
    mkdir -p "$CONFIGS"

    # Determine single-node execution mode
    # GPU optimizer settings are always enabled for GPU variant
    OPTIMIZER_GPU_SETTINGS="optimizer.joins-not-null-inference-strategy=USE_FUNCTION_METADATA\noptimizer.default-filter-factor-enabled=true"

    if [[ $NUM_WORKERS -eq 1 ]]; then
        SINGLE_NODE_EXEC="true"
        CUDF_EXCHANGE="false"
        JOIN_DIST_TYPE="AUTOMATIC"
    else
        SINGLE_NODE_EXEC="false"
        CUDF_EXCHANGE="true"
        JOIN_DIST_TYPE="PARTITIONED"
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

    # Copy catalog configs for coordinator (only hive + tpch, matching original)
    cp "${SCRIPT_DIR}/config-templates/common/hive.properties" \
       "$CONFIGS/coordinator/catalog/hive.properties"

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

            # Copy catalog configs for worker (only hive + tpch, matching original)
            cp "${SCRIPT_DIR}/config-templates/common/hive.properties" \
               "$CONFIGS/worker_${worker_id}/catalog/hive.properties"

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
${CONFIGS}/coordinator/catalog:/opt/presto-server/etc/catalog,\
${DATA_DIR}:/var/lib/presto/data/hive/data/user_data,\
${VT_ROOT}/.hive_metastore:/var/lib/presto/data/hive/metastore" \
        -- bash -c "unset CONFIG NODE_CONFIG PRESTO_ETC JAVA_TOOL_OPTIONS JDK_JAVA_OPTIONS _JAVA_OPTIONS; \
            export JAVA_HOME=/usr/lib/jvm/jre-17-openjdk; \
            export PATH=/usr/lib/jvm/jre-17-openjdk/bin:\$PATH; \
            /opt/presto-server/bin/launcher run" \
        > "${SCRIPT_DIR}/logs/coordinator.log" 2>&1 &
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
        --container-env=LD_LIBRARY_PATH="${CUDF_LIB}:\$LD_LIBRARY_PATH" \
        --container-env=GLOG_vmodule=IntraNodeTransferRegistry=3,ExchangeOperator=3 \
        --container-env=GLOG_logtostderr=1 \
        --container-mounts="${VT_ROOT}:/workspace,\
${CONFIGS}/jvm.config:/opt/presto-server/etc/jvm.config,\
${CONFIGS}/log.properties:/opt/presto-server/etc/log.properties,\
${CONFIGS}/worker_${worker_id}/node.properties:/opt/presto-server/etc/node.properties,\
${CONFIGS}/worker_${worker_id}/config.properties:/opt/presto-server/etc/config.properties,\
${CONFIGS}/worker_${worker_id}/catalog:/opt/presto-server/etc/catalog,\
${worker_data}:/var/lib/presto/data,\
${DATA_DIR}:/var/lib/presto/data/hive/data/user_data,\
${VT_ROOT}/.hive_metastore:/var/lib/presto/data/hive/metastore" \
        -- bash -c "export CUDA_VISIBLE_DEVICES=${gpu_id}; \
            echo \"Worker ${worker_id}: waiting for coordinator at ${COORD}:${PORT}...\"; \
            for _i in \$(seq 1 120); do \
                if curl -sf http://${COORD}:${PORT}/v1/info/state >/dev/null 2>&1; then \
                    echo \"Worker ${worker_id}: coordinator is ready\"; \
                    break; \
                fi; \
                sleep 2; \
            done; \
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

# ==============================================================================
# Benchmark Functions
# ==============================================================================
setup_and_run_benchmark() {
    log_info "Setting up and running TPC-H benchmark (scale factor: ${SCALE_FACTOR}, iterations: ${NUM_ITERATIONS})..."

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
${CONFIGS}/coordinator/catalog:/opt/presto-server/etc/catalog,\
${DATA_DIR}:/var/lib/presto/data/hive/data/user_data,\
${VT_ROOT}/.hive_metastore:/var/lib/presto/data/hive/metastore" \
        -- bash -c "yum install -y python3.12 jq > /dev/null 2>&1; \
            export PORT=${PORT}; \
            export HOSTNAME=${COORD}; \
            export PRESTO_DATA_DIR=/var/lib/presto/data/hive/data/user_data; \
            echo 'Waiting for coordinator at ${COORD}:${PORT}...'; \
            for _i in \$(seq 1 120); do \
                state=\$(curl -s http://${COORD}:${PORT}/v1/info/state 2>/dev/null || echo ''); \
                if [ \"\$state\" = '\"ACTIVE\"' ]; then \
                    echo 'Coordinator is active'; \
                    break; \
                fi; \
                sleep 2; \
            done; \
            echo 'Waiting for ${NUM_WORKERS} workers to register...'; \
            for _i in \$(seq 1 120); do \
                n=\$(curl -s http://${COORD}:${PORT}/v1/node 2>/dev/null | jq length 2>/dev/null || echo 0); \
                if [ \"\$n\" -eq ${NUM_WORKERS} ] 2>/dev/null; then \
                    echo 'All ${NUM_WORKERS} workers registered'; \
                    break; \
                fi; \
                echo \"Workers registered: \$n/${NUM_WORKERS}\"; \
                sleep 5; \
            done; \
            cd /workspace/presto/scripts; \
            echo '=== Setting up benchmark tables ==='; \
            ./setup_benchmark_tables.sh -b tpch -d date-scale-${SCALE_FACTOR} -s tpchsf${SCALE_FACTOR} --skip-analyze-tables --no-docker; \
            echo '=== Copying pre-analyzed hive metastore ==='; \
            ANALYZED=/workspace/presto/slurm/presto-nvl72/ANALYZED_HIVE_METASTORE; \
            METASTORE=/var/lib/presto/data/hive/metastore; \
            if [ -d \"\$ANALYZED\" ]; then \
                for dataset in \$(ls \"\$ANALYZED\"); do \
                    if [ -d \"\$METASTORE/\$dataset\" ]; then \
                        echo \"Replacing dataset metadata: \$dataset\"; \
                        cp -r \"\$ANALYZED/\$dataset\" \"\$METASTORE/\"; \
                        for table in \$(ls \"\$METASTORE/\$dataset\"); do \
                            if [ -f \"\$METASTORE/\$dataset/\$table/..prestoSchema.crc\" ]; then \
                                rm \"\$METASTORE/\$dataset/\$table/..prestoSchema.crc\"; \
                            fi; \
                        done; \
                    fi; \
                done; \
            else \
                echo 'ANALYZED_HIVE_METASTORE not found, skipping metastore copy'; \
            fi; \
            echo '=== Running benchmark queries ==='; \
            ./run_benchmark.sh -b tpch -s tpchsf${SCALE_FACTOR} -i ${NUM_ITERATIONS} --hostname ${COORD} --port ${PORT} -o /workspace/presto/slurm/standalone/result_dir" \
        > "${SCRIPT_DIR}/logs/benchmark.log" 2>&1

    # Copy summary to result_dir
    if [[ -f "${SCRIPT_DIR}/logs/benchmark.log" ]]; then
        cp "${SCRIPT_DIR}/logs/benchmark.log" "${SCRIPT_DIR}/result_dir/summary.txt"
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

# Launch all containers simultaneously to parallelize image loading.
# In-container waits handle dependency ordering:
#   - Workers wait inside their container for the coordinator
#   - setup_benchmark waits inside its container for coordinator + workers
start_coordinator
start_all_workers
setup_and_run_benchmark

log_info "=========================================="
log_info "Benchmark completed!"
log_info "Results: ${SCRIPT_DIR}/result_dir/"
log_info "Logs: ${SCRIPT_DIR}/logs/"
log_info "=========================================="
