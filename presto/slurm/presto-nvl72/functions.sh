#!/bin/bash

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
    [ ! -d "$REPO_ROOT" ] && echo "REPO_ROOT must be a valid directory" && exit 1
    [ ! -d "$DATA" ] && echo "DATA must be a valid directory" && exit 1
    [ ! -d ${CONFIGS} ] && generate_configs

    validate_config_directory
}

function generate_configs {
    echo "GENERATING NEW CONFIGS"
    mkdir -p ${CONFIGS}
    pushd ${REPO_ROOT}/presto/scripts
    OVERWRITE_CONFIG=true ./generate_presto_config.sh
    popd
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
    validate_environment_preconditions LOGS CONFIGS REPO_ROOT COORD DATA
    local script=$1
    local type=$2
    [ "$type" != "coord" ] && [ "$type" != "cli" ] && echo_error "coord type must be coord/cli"
    local log_file="${type}.log"

    local coord_image="${IMAGE_DIR}/trino-coordinator.sqsh"
    if [ ! -f "${coord_image}" ]; then
        if [ -f "${IMAGE_DIR}/trino.sqsh" ]; then
            coord_image="${IMAGE_DIR}/trino.sqsh"
        else
            echo_error "coord image does not exist at ${IMAGE_DIR}/trino-coordinator.sqsh or ${IMAGE_DIR}/trino.sqsh"
        fi
    fi

    # Coordinator runs as a background process, whereas we want to wait for cli
    # so that the job will finish when the cli is done (terminating background
    # processes like the coordinator and workers).
    if [ "${type}" == "coord" ]; then
        srun -w $COORD --ntasks=1 --overlap \
--container-image=${coord_image} \
--container-mounts=${REPO_ROOT}:/workspace,\
${CONFIGS}/etc_common:/etc/trino,\
${CONFIGS}/etc_coordinator/node.properties:/etc/trino/node.properties,\
${CONFIGS}/etc_coordinator/config_native.properties:/etc/trino/config.properties,\
${CONFIGS}/etc_coordinator/catalog/hive.properties:/etc/trino/catalog/hive.properties,\
${DATA}:/var/lib/presto/data/hive/data/user_data,\
${REPO_ROOT}/.hive_metastore:/var/lib/presto/data/hive/metastore \
-- bash -lc "/usr/lib/trino/bin/launcher run --etc-dir /etc/trino" >> ${LOGS}/${log_file} 2>&1 &
    else
        srun -w $COORD --ntasks=1 --overlap \
--container-image=${coord_image} \
--container-mounts=${REPO_ROOT}:/workspace,\
${CONFIGS}/etc_common:/etc/trino,\
${CONFIGS}/etc_coordinator/node.properties:/etc/trino/node.properties,\
${CONFIGS}/etc_coordinator/config_native.properties:/etc/trino/config.properties,\
${CONFIGS}/etc_coordinator/catalog/hive.properties:/etc/trino/catalog/hive.properties,\
${DATA}:/var/lib/presto/data/hive/data/user_data,\
${REPO_ROOT}/.hive_metastore:/var/lib/presto/data/hive/metastore \
-- bash -lc "${script}" >> ${LOGS}/${log_file} 2>&1
    fi
}

# Runs a coordinator on a specific node with default configurations.
# Overrides the config files with the coord node and other needed updates.
function run_coordinator {
    validate_environment_preconditions CONFIGS SINGLE_NODE_EXECUTION
    local coord_config="${CONFIGS}/etc_coordinator/config_native.properties"
    local coord_node="${CONFIGS}/etc_coordinator/node.properties"
    # Replace placeholder in configs
    sed -i "s+discovery\.uri.*+discovery\.uri=http://${COORD}:${PORT}+g" ${coord_config}
    sed -i "s+http-server\.http\.port=.*+http-server\.http\.port=${PORT}+g" ${coord_config}

    # Normalize Trino memory settings to valid DataSize suffixes (e.g., 8G -> 8GB)
    sed -i -E 's/^(query\.max-memory-per-node\s*=\s*)([0-9]+)\s*[Gg]\s*$/\1\2GB/g' ${coord_config} 2>/dev/null || true
    sed -i -E 's/^(query\.max-memory-per-node\s*=\s*)([0-9]+)\s*[Mm]\s*$/\1\2MB/g' ${coord_config} 2>/dev/null || true
    sed -i -E 's/^(query\.max-total-memory-per-node\s*=\s*)([0-9]+)\s*[Gg]\s*$/\1\2GB/g' ${coord_config} 2>/dev/null || true
    sed -i -E 's/^(query\.max-total-memory-per-node\s*=\s*)([0-9]+)\s*[Mm]\s*$/\1\2MB/g' ${coord_config} 2>/dev/null || true
    sed -i -E 's/^(memory\.heap-headroom-per-node\s*=\s*)([0-9]+)\s*[Gg]\s*$/\1\2GB/g' ${coord_config} 2>/dev/null || true
    sed -i -E 's/^(memory\.heap-headroom-per-node\s*=\s*)([0-9]+)\s*[Mm]\s*$/\1\2MB/g' ${coord_config} 2>/dev/null || true
    # Remove Presto-only flags if present
    sed -i '/^single-node-execution-enabled\s*=/d' ${coord_config} 2>/dev/null || true
    # Remove defunct logging properties incompatible with Trino
    if [ -f "${CONFIGS}/etc_common/log.properties" ]; then
        sed -i '/^log\.max-history\s*=/d' "${CONFIGS}/etc_common/log.properties"
    fi
    # Remove CUDF/Velox and other Presto-native-only properties
    sed -i '/^cudf\./d' ${coord_config} 2>/dev/null || true
    sed -i '/^memory-arbitrator-kind\s*=/d' ${coord_config} 2>/dev/null || true
    sed -i '/^runtime-metrics-collection-enabled\s*=/d' ${coord_config} 2>/dev/null || true
    sed -i '/^system-.*\s*=/d' ${coord_config} 2>/dev/null || true
    sed -i '/^query-memory-gb\s*=/d' ${coord_config} 2>/dev/null || true
    # Remove/translate defunct or renamed Trino properties
    sed -i '/^experimental\.spiller-max-used-space-threshold\s*=/d' ${coord_config} 2>/dev/null || true
    sed -i '/^experimental\.spiller-spill-path\s*=/d' ${coord_config} 2>/dev/null || true
    sed -i '/^experimental\.enable-dynamic-filtering\s*=/d' ${coord_config} 2>/dev/null || true
    sed -i '/^experimental\.reserved-pool-enabled\s*=/d' ${coord_config} 2>/dev/null || true
    sed -i '/^optimizer\.optimize-hash-generation\s*=/d' ${coord_config} 2>/dev/null || true
    sed -i '/^cluster-tag\s*=/d' ${coord_config} 2>/dev/null || true
    sed -i '/^discovery-server\.enabled\s*=/d' ${coord_config} 2>/dev/null || true
    sed -i '/^native-execution-enabled\s*=/d' ${coord_config} 2>/dev/null || true
    sed -i '/^presto\.version\s*=/d' ${coord_config} 2>/dev/null || true
    sed -i '/^use-alternative-function-signatures\s*=/d' ${coord_config} 2>/dev/null || true
    # Remove defunct memory property (Trino suggests other keys)
    sed -i '/^query\.max-total-memory-per-node\s*=/d' ${coord_config} 2>/dev/null || true
    # Remove "not used" optimizer/experimental properties reported by Trino
    sed -i '/^experimental\.max-revocable-memory-per-node\s*=/d' ${coord_config} 2>/dev/null || true
    sed -i '/^experimental\.optimized-repartitioning\s*=/d' ${coord_config} 2>/dev/null || true
    sed -i '/^experimental\.pushdown-dereference-enabled\s*=/d' ${coord_config} 2>/dev/null || true
    sed -i '/^experimental\.pushdown-subfields-enabled\s*=/d' ${coord_config} 2>/dev/null || true
    sed -i '/^optimizer\.default-join-selectivity-coefficient\s*=/d' ${coord_config} 2>/dev/null || true
    sed -i '/^optimizer\.exploit-constraints\s*=/d' ${coord_config} 2>/dev/null || true
    sed -i '/^optimizer\.generate-domain-filters\s*=/d' ${coord_config} 2>/dev/null || true
    sed -i '/^optimizer\.handle-complex-equi-joins\s*=/d' ${coord_config} 2>/dev/null || true
    sed -i '/^optimizer\.in-predicates-as-inner-joins-enabled\s*=/d' ${coord_config} 2>/dev/null || true
    sed -i '/^optimizer\.infer-inequality-predicates\s*=/d' ${coord_config} 2>/dev/null || true
    sed -i '/^optimizer\.joins-not-null-inference-strategy\s*=/d' ${coord_config} 2>/dev/null || true
    sed -i '/^optimizer\.partial-aggregation-strategy\s*=/d' ${coord_config} 2>/dev/null || true
    # Translate renamed properties if present
    if grep -q '^experimental\.max-spill-per-node=' ${coord_config} 2>/dev/null; then
        val=$(grep '^experimental\.max-spill-per-node=' ${coord_config} | tail -1 | cut -d'=' -f2-)
        sed -i '/^experimental\.max-spill-per-node\s*=/d' ${coord_config}
        echo "max-spill-per-node=${val}" >> ${coord_config}
    fi
    if grep -q '^experimental\.query-max-spill-per-node=' ${coord_config} 2>/dev/null; then
        val=$(grep '^experimental\.query-max-spill-per-node=' ${coord_config} | tail -1 | cut -d'=' -f2-)
        sed -i '/^experimental\.query-max-spill-per-node\s*=/d' ${coord_config}
        echo "query-max-spill-per-node=${val}" >> ${coord_config}
    fi
    if grep -q '^node-scheduler\.max-pending-splits-per-task=' ${coord_config} 2>/dev/null; then
        val=$(grep '^node-scheduler\.max-pending-splits-per-task=' ${coord_config} | tail -1 | cut -d'=' -f2-)
        sed -i '/^node-scheduler\.max-pending-splits-per-task\s*=/d' ${coord_config}
        echo "node-scheduler.min-pending-splits-per-task=${val}" >> ${coord_config}
    fi
    if grep -q '^regex-library=' ${coord_config} 2>/dev/null; then
        val=$(grep '^regex-library=' ${coord_config} | tail -1 | cut -d'=' -f2-)
        sed -i '/^regex-library\s*=/d' ${coord_config}
        echo "deprecated.regex-library=${val}" >> ${coord_config}
    fi
    # Validate query.max-memory-per-node format; if invalid, drop to use defaults
    # Remove problematic memory limit; let Trino defaults apply
    sed -i '/^query\.max-memory-per-node\s*=/d' ${coord_config} 2>/dev/null || true

    # Ensure data dir path for coordinator (keep existing /var/lib paths)
    if grep -q "^node\.data-dir=" "${coord_node}"; then
        sed -i "s+^node\.data-dir=.*+node\.data-dir=/var/lib/presto/data+g" ${coord_node}
    else
        echo "node.data-dir=/var/lib/presto/data" >> ${coord_node}
    fi
    # Ensure Trino hive connector name (replace Presto's hive_hadoop2/hive-hadoop2)
    local coord_hive="${CONFIGS}/etc_coordinator/catalog/hive.properties"
    if [ -f "${coord_hive}" ]; then
        sed -i -E 's/^(connector\.name\s*=\s*).*/\1hive/' "${coord_hive}"
        # Remove/translate Presto-native or invalid Trino Hive properties
        sed -i '/^cudf\.hive\.use-buffered-input\s*=/d' "${coord_hive}" 2>/dev/null || true
        sed -i '/^hive\.allow-drop-table\s*=/d' "${coord_hive}" 2>/dev/null || true
        sed -i '/^hive\.file-splittable\s*=/d' "${coord_hive}" 2>/dev/null || true
        sed -i '/^parquet\.reader\.chunk-read-limit\s*=/d' "${coord_hive}" 2>/dev/null || true
        sed -i '/^parquet\.reader\.pass-read-limit\s*=/d' "${coord_hive}" 2>/dev/null || true
        # Configure file metastore with a local path (no URI scheme)
        if grep -q '^hive\.metastore=' "${coord_hive}"; then
            sed -i 's/^hive\.metastore\s*=.*/hive.metastore=file/' "${coord_hive}"
        else
            echo "hive.metastore=file" >> "${coord_hive}"
        fi
        if grep -q '^hive\.metastore\.catalog\.dir=' "${coord_hive}"; then
            sed -i 's|^hive\.metastore\.catalog\.dir\s*=.*|hive.metastore.catalog.dir=/var/lib/presto/data/hive/metastore|' "${coord_hive}"
        else
            echo "hive.metastore.catalog.dir=/var/lib/presto/data/hive/metastore" >> "${coord_hive}"
        fi
        # Remove thrift/glue URIs if present to avoid conflicts
        sed -i '/^hive\.metastore\.uri\s*=/d' "${coord_hive}" 2>/dev/null || true
        sed -i '/^hive\.metastore\.uris\s*=/d' "${coord_hivme}" 2>/dev/null || true
    fi

    mkdir -p ${REPO_ROOT}/.hive_metastore

run_coord_image ":" "coord"
}

# Runs a worker on a given node with custom configuration files which are generated as necessary.
function run_worker {
    [ $# -ne 4 ] && echo_error "$0 expected arguments 'gpu_id', 'image', 'node_id', and 'worker_id'"
    validate_environment_preconditions LOGS CONFIGS REPO_ROOT COORD SINGLE_NODE_EXECUTION DATA

    local gpu_id=$1
    local image=$2
    local node=$3
    local worker_id=$4
    local worker_two_digit=$(printf "%02d\n" "$worker_id")
    echo "running worker ${worker_id} with image ${image} on node ${node}"

    local worker_image="${IMAGE_DIR}/${image}.sqsh"
    [ ! -f "${worker_image}" ] && echo_error "worker image does not exist at ${worker_image}"

    # Make a copy of the worker config that can be given a unique id for this worker.
    rm -rf "${CONFIGS}/etc_worker_${worker_id}"
    cp -r "${CONFIGS}/etc_worker" "${CONFIGS}/etc_worker_${worker_id}"
    local worker_config="${CONFIGS}/etc_worker_${worker_id}/config_native.properties"
    local worker_node="${CONFIGS}/etc_worker_${worker_id}/node.properties"
    local worker_hive="${CONFIGS}/etc_worker_${worker_id}/catalog/hive.properties"
    local worker_data="${SCRIPT_DIR}/worker_data_${worker_id}"

    # Create unique configuration/data files for each worker:
    # Give each worker a unique port.
    sed -i "s+http-server\.http\.port.*+http-server\.http\.port=10${worker_two_digit}0+g" ${worker_config}
    # Update discovery based on which node the coordinator is running on.
    sed -i "s+discovery\.uri.*+discovery\.uri=http://${COORD}:${PORT}+g" ${worker_config}
    # Normalize Trino memory settings to valid DataSize suffixes (e.g., 8G -> 8GB)
    sed -i -E 's/^(query\.max-memory-per-node\s*=\s*)([0-9]+)\s*[Gg]\s*$/\1\2GB/g' ${worker_config} 2>/dev/null || true
    sed -i -E 's/^(query\.max-memory-per-node\s*=\s*)([0-9]+)\s*[Mm]\s*$/\1\2MB/g' ${worker_config} 2>/dev/null || true
    sed -i -E 's/^(query\.max-total-memory-per-node\s*=\s*)([0-9]+)\s*[Gg]\s*$/\1\2GB/g' ${worker_config} 2>/dev/null || true
    sed -i -E 's/^(query\.max-total-memory-per-node\s*=\s*)([0-9]+)\s*[Mm]\s*$/\1\2MB/g' ${worker_config} 2>/dev/null || true
    sed -i -E 's/^(memory\.heap-headroom-per-node\s*=\s*)([0-9]+)\s*[Gg]\s*$/\1\2GB/g' ${worker_config} 2>/dev/null || true
    sed -i -E 's/^(memory\.heap-headroom-per-node\s*=\s*)([0-9]+)\s*[Mm]\s*$/\1\2MB/g' ${worker_config} 2>/dev/null || true
    # Remove Presto-only flags if present
    sed -i '/^single-node-execution-enabled\s*=/d' ${worker_config} 2>/dev/null || true
    # Remove defunct logging properties incompatible with Trino
    if [ -f "${CONFIGS}/etc_common/log.properties" ]; then
        sed -i '/^log\.max-history\s*=/d' "${CONFIGS}/etc_common/log.properties"
    fi
    # Remove CUDF/Velox and other Presto-native-only properties
    sed -i '/^cudf\./d' ${worker_config} 2>/dev/null || true
    sed -i '/^memory-arbitrator-kind\s*=/d' ${worker_config} 2>/dev/null || true
    sed -i '/^runtime-metrics-collection-enabled\s*=/d' ${worker_config} 2>/dev/null || true
    sed -i '/^system-.*\s*=/d' ${worker_config} 2>/dev/null || true
    sed -i '/^query-memory-gb\s*=/d' ${worker_config} 2>/dev/null || true
    # Remove/translate defunct or renamed Trino properties (worker)
    sed -i '/^experimental\.spiller-max-used-space-threshold\s*=/d' ${worker_config} 2>/dev/null || true
    sed -i '/^experimental\.spiller-spill-path\s*=/d' ${worker_config} 2>/dev/null || true
    sed -i '/^experimental\.enable-dynamic-filtering\s*=/d' ${worker_config} 2>/dev/null || true
    sed -i '/^experimental\.reserved-pool-enabled\s*=/d' ${worker_config} 2>/dev/null || true
    sed -i '/^optimizer\.optimize-hash-generation\s*=/d' ${worker_config} 2>/dev/null || true
    sed -i '/^cluster-tag\s*=/d' ${worker_config} 2>/dev/null || true
    sed -i '/^discovery-server\.enabled\s*=/d' ${worker_config} 2>/dev/null || true
    sed -i '/^native-execution-enabled\s*=/d' ${worker_config} 2>/dev/null || true
    sed -i '/^presto\.version\s*=/d' ${worker_config} 2>/dev/null || true
    sed -i '/^use-alternative-function-signatures\s*=/d' ${worker_config} 2>/dev/null || true
    # Remove defunct memory property
    sed -i '/^query\.max-total-memory-per-node\s*=/d' ${worker_config} 2>/dev/null || true
    # Remove "not used" optimizer/experimental properties reported by Trino
    sed -i '/^experimental\.max-revocable-memory-per-node\s*=/d' ${worker_config} 2>/dev/null || true
    sed -i '/^experimental\.optimized-repartitioning\s*=/d' ${worker_config} 2>/dev/null || true
    sed -i '/^experimental\.pushdown-dereference-enabled\s*=/d' ${worker_config} 2>/dev/null || true
    sed -i '/^experimental\.pushdown-subfields-enabled\s*=/d' ${worker_config} 2>/dev/null || true
    sed -i '/^optimizer\.default-join-selectivity-coefficient\s*=/d' ${worker_config} 2>/dev/null || true
    sed -i '/^optimizer\.exploit-constraints\s*=/d' ${worker_config} 2>/dev/null || true
    sed -i '/^optimizer\.generate-domain-filters\s*=/d' ${worker_config} 2>/dev/null || true
    sed -i '/^optimizer\.handle-complex-equi-joins\s*=/d' ${worker_config} 2>/dev/null || true
    sed -i '/^optimizer\.in-predicates-as-inner-joins-enabled\s*=/d' ${worker_config} 2>/dev/null || true
    sed -i '/^optimizer\.infer-inequality-predicates\s*=/d' ${worker_config} 2>/dev/null || true
    sed -i '/^optimizer\.joins-not-null-inference-strategy\s*=/d' ${worker_config} 2>/dev/null || true
    sed -i '/^optimizer\.partial-aggregation-strategy\s*=/d' ${worker_config} 2>/dev/null || true
    # Translate renamed properties if present
    if grep -q '^experimental\.max-spill-per-node=' ${worker_config} 2>/dev/null; then
        val=$(grep '^experimental\.max-spill-per-node=' ${worker_config} | tail -1 | cut -d'=' -f2-)
        sed -i '/^experimental\.max-spill-per-node\s*=/d' ${worker_config}
        echo "max-spill-per-node=${val}" >> ${worker_config}
    fi
    if grep -q '^experimental\.query-max-spill-per-node=' ${worker_config} 2>/dev/null; then
        val=$(grep '^experimental\.query-max-spill-per-node=' ${worker_config} | tail -1 | cut -d'=' -f2-)
        sed -i '/^experimental\.query-max-spill-per-node\s*=/d' ${worker_config}
        echo "query-max-spill-per-node=${val}" >> ${worker_config}
    fi
    if grep -q '^node-scheduler\.max-pending-splits-per-task=' ${worker_config} 2>/dev/null; then
        val=$(grep '^node-scheduler\.max-pending-splits-per-task=' ${worker_config} | tail -1 | cut -d'=' -f2-)
        sed -i '/^node-scheduler\.max-pending-splits-per-task\s*=/d' ${worker_config}
        echo "node-scheduler.min-pending-splits-per-task=${val}" >> ${worker_config}
    fi
    if grep -q '^regex-library=' ${worker_config} 2>/dev/null; then
        val=$(grep '^regex-library=' ${worker_config} | tail -1 | cut -d'=' -f2-)
        sed -i '/^regex-library\s*=/d' ${worker_config}
        echo "deprecated.regex-library=${val}" >> ${worker_config}
    fi
    # Validate query.max-memory-per-node format; if invalid, drop to use defaults
    # Remove problematic memory limit; let Trino defaults apply
    sed -i '/^query\.max-memory-per-node\s*=/d' ${worker_config} 2>/dev/null || true
    # Give each worker a unique id.
    sed -i "s+node\.id.*+node\.id=worker_${worker_id}+g" ${worker_node}
    # Ensure data dir path (keep existing /var/lib paths)
    if grep -q "^node\.data-dir=" "${worker_node}"; then
        sed -i "s+^node\.data-dir=.*+node\.data-dir=/var/lib/presto/data+g" ${worker_node}
    else
        echo "node.data-dir=/var/lib/presto/data" >> ${worker_node}
    fi
    # Ensure Trino hive connector name (replace Presto's hive_hadoop2/hive-hadoop2)
    if [ -f "${worker_hive}" ]; then
        sed -i -E 's/^(connector\.name\s*=\s*).*/\1hive/' "${worker_hive}"
        # Remove/translate Presto-native or invalid Trino Hive properties
        sed -i '/^cudf\.hive\.use-buffered-input\s*=/d' "${worker_hive}" 2>/dev/null || true
        sed -i '/^hive\.allow-drop-table\s*=/d' "${worker_hive}" 2>/dev/null || true
        sed -i '/^hive\.file-splittable\s*=/d' "${worker_hive}" 2>/dev/null || true
        sed -i '/^parquet\.reader\.chunk-read-limit\s*=/d' "${worker_hive}" 2>/dev/null || true
        sed -i '/^parquet\.reader\.pass-read-limit\s*=/d' "${worker_hive}" 2>/dev/null || true
        # Configure file metastore with a local path (no URI scheme)
        if grep -q '^hive\.metastore=' "${worker_hive}"; then
            sed -i 's/^hive\.metastore\s*=.*/hive.metastore=file/' "${worker_hive}"
        else
            echo "hive.metastore=file" >> "${worker_hive}"
        fi
        if grep -q '^hive\.metastore\.catalog\.dir=' "${worker_hive}"; then
            sed -i 's|^hive\.metastore\.catalog\.dir\s*=.*|hive.metastore.catalog.dir=/var/lib/presto/data/hive/metastore|' "${worker_hive}"
        else
            echo "hive.metastore.catalog.dir=/var/lib/presto/data/hive/metastore" >> "${worker_hive}"
        fi
        # Remove thrift/glue URIs if present to avoid conflicts
        sed -i '/^hive\.metastore\.uri\s*=/d' "${worker_hive}" 2>/dev/null || true
        sed -i '/^hive\.metastore\.uris\s*=/d' "${worker_hive}" 2>/dev/null || true
    fi

    # Create unique data dir per worker.
    mkdir -p ${worker_data}
    mkdir -p ${worker_data}/hive/data/user_data
    mkdir -p ${REPO_ROOT}/.hive_metastore

    # Need to fix this to run with cpu nodes as well.
    # Run the worker with the new configs.
    # Use --overlap to allow multiple srun commands from same job
    # Don't use --gres=gpu:1 here since the job already allocated GPUs
    srun -N1 -w $node --ntasks=1 --overlap \
--container-image=${worker_image} \
--export=ALL \
--container-mounts=${REPO_ROOT}:/workspace,\
${CONFIGS}/etc_common:/etc/trino,\
${worker_node}:/etc/trino/node.properties,\
${worker_config}:/etc/trino/config.properties,\
${worker_hive}:/etc/trino/catalog/hive.properties,\
${worker_data}:/var/lib/presto/data,\
${DATA}:/var/lib/presto/data/hive/data/user_data,\
${REPO_ROOT}/.hive_metastore:/var/lib/presto/data/hive/metastore \
-- /bin/bash -c "/usr/lib/trino/bin/run-trino" > ${LOGS}/worker_${worker_id}.log 2>&1 &
}

#./analyze_tables.sh --port $PORT --hostname $HOSTNAME -s tpchsf${scale_factor}
function setup_benchmark {
    echo "setting up benchmark"
    [ $# -ne 1 ] && echo_error "$0 expected one argument for 'scale factor'"
    local scale_factor=$1
    local data_path="/data/date-scale-${scale_factor}"
    run_coord_image "export PORT=$PORT; export HOSTNAME=$COORD; export PRESTO_DATA_DIR=/var/lib/presto/data/hive/data/user_data; export MINIFORGE_HOME=/workspace/.miniforge3; (command -v apt-get >/dev/null 2>&1 && apt-get update && apt-get install -y jq) || (command -v yum >/dev/null 2>&1 && yum install -y jq) || true; cd /workspace/presto/scripts; ./setup_benchmark_tables.sh -b tpch -d date-scale-${scale_factor} -s tpchsf${scale_factor}; analyze_tables.sh -s tpchsf${scale_factor}" "cli"

    # Copy the hive metastore from the source of truth to the container.  This means we don't have to create
    # or analyze the tables.
    #for dataset in $(ls ${SCRIPT_DIR}/ANALYZED_HIVE_METASTORE); do
	#if [[ -d ${REPO_ROOT}/.hive_metastore/${dataset} ]]; then
	#    echo "replacing dataset metadata: $dataset"
	#    cp -r ${SCRIPT_DIR}/ANALYZED_HIVE_METASTORE/${dataset} ${REPO_ROOT}/.hive_metastore/
	#    for table in $(ls ${REPO_ROOT}/.hive_metastore/${dataset}); do
		# Need to remove checksum file (it will be recreated).
	#	if [ -f ${REPO_ROOT}/.hive_metastore/${dataset}/${table}/..prestoSchema.crc ]; then
	#	    rm ${REPO_ROOT}/.hive_metastore/${dataset}/${table}/..prestoSchema.crc
	#	fi
	#    done
        #fi
    #done
}

# Run a cli node that will connect to the coordinator and run queries from queries.sql
# Results are stored in cli.log.
function run_queries {
    echo "running queries"
    [ $# -ne 2 ] && echo_error "$0 expected two arguments for '<iterations>' and '<scale_factor>'"
    local num_iterations=$1
    local scale_factor=$2
    run_coord_image "export PORT=$PORT; export HOSTNAME=$COORD; export PRESTO_DATA_DIR=/var/lib/presto/data/hive/data/user_data; export MINIFORGE_HOME=/workspace/.miniforge3; (command -v apt-get >/dev/null 2>&1 && apt-get update && apt-get install -y jq) || (command -v yum >/dev/null 2>&1 && yum install -y jq) || true; cd /workspace/presto/scripts; ./run_benchmark.sh -b tpch -s tpchsf${scale_factor} -i ${num_iterations} --hostname ${COORD} --port $PORT -o /workspace/presto/slurm/presto-nvl72/result_dir" "cli"
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
    for i in {1..20}; do
        local new_num=0
        # Try multiple Trino endpoints: active first, then legacy
        for ep in "/v1/node/active" "/v1/node"; do
            resp="$(curl -fsS --compressed -H 'Accept: application/json' http://${COORD}:${PORT}${ep} 2>/dev/null || true)"
            # Prefer jq if available
            if command -v jq >/dev/null 2>&1 && [ -n "$resp" ] && echo "$resp" | jq -e . >/dev/null 2>&1; then
                new_num="$(echo "$resp" | jq -r '
                  if type=="array" then length
                  elif type=="object" then
                    if has("activeNodes") then (.activeNodes|length)
                    elif has("nodes") then (.nodes|length)
                    else 0 end
                  else 0 end
                ' 2>/dev/null || echo "")"
            # Fallback to python if present
            elif command -v python3 >/dev/null 2>&1 && [ -n "$resp" ]; then
                new_num="$(python3 - <<'PY' 2>/dev/null || true
import sys, json
try:
    data=json.load(sys.stdin)
    if isinstance(data, list):
        print(len(data))
    elif isinstance(data, dict):
        if 'activeNodes' in data and isinstance(data['activeNodes'], list):
            print(len(data['activeNodes']))
        elif 'nodes' in data and isinstance(data['nodes'], list):
            print(len(data['nodes']))
        else:
            print(0)
    else:
        print(0)
except Exception:
    pass
PY
                <<< "$resp")"
            # Last resort: crude count by matching common field tokens
            elif [ -n "$resp" ]; then
                new_num="$(printf "%s" "$resp" | grep -o '"httpUri"\|"uri"\|"nodeId"' | wc -l | awk '{print $1}')"
            fi
            [[ "$new_num" =~ ^[0-9]+$ ]] || new_num=0
            # If we got any positive count, stop trying endpoints
            if (( new_num > 0 )); then
                break
            fi
        done
        # If HTTP node endpoints failed, fall back to SQL via Trino statements API
        if (( new_num == 0 )); then
            if command -v jq >/dev/null 2>&1; then
                sql_payload='select count(*) from system.runtime.nodes where coordinator=false'
                resp="$(curl -fsS -X POST http://${COORD}:${PORT}/v1/statement \
                  -H 'X-Trino-User: health' \
                  -H 'X-Trino-Source: wait_for_workers' \
                  -H 'Accept: application/json' \
                  --data-binary "$sql_payload" 2>/dev/null || true)"
                # Follow nextUri chain up to 10 steps to get data
                next_uri="$(echo "$resp" | jq -r '.nextUri // empty')"
                data_val="$(echo "$resp" | jq -r '.data[0][0] // empty')"
                steps=0
                while [ -z "$data_val" ] && [ -n "$next_uri" ] && [ $steps -lt 10 ]; do
                    resp="$(curl -fsS "$next_uri" 2>/dev/null || true)"
                    data_val="$(echo "$resp" | jq -r '.data[0][0] // empty')"
                    next_uri="$(echo "$resp" | jq -r '.nextUri // empty')"
                    steps=$((steps+1))
                done
                if [[ "$data_val" =~ ^[0-9]+$ ]]; then
                    new_num="$data_val"
                fi
            fi
        fi
        num_workers=$new_num
        if (( num_workers >= expected_num_workers )); then
            echo "workers registered. num_nodes: $num_workers"
	    return 0
        fi
        echo "wait_for_workers_to_register: observed $num_workers/$expected_num_workers workers (COORD=${COORD}:${PORT})"
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

    # Copy slurm output files from the job directory
    if [ -n "${SLURM_JOB_ID}" ]; then
        cp ${SCRIPT_DIR}/trino-tpch-run_${SLURM_JOB_ID}.out ${run_dir}/ 2>/dev/null || true
        cp ${SCRIPT_DIR}/trino-tpch-run_${SLURM_JOB_ID}.err ${run_dir}/ 2>/dev/null || true
    fi

    # Copy configs
    mkdir -p ${run_dir}/configs
    cp ${CONFIGS}/etc_coordinator/config_native.properties ${run_dir}/configs/coordinator.config 2>/dev/null || true
    cp ${CONFIGS}/etc_worker_0/config_native.properties ${run_dir}/configs/worker.config 2>/dev/null || true

    echo "Results saved to: ${run_dir}"
}
