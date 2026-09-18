#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

required=(
  RUN_ID ROLE WORKER_COUNT WORKERS_PER_INSTANCE COORDINATOR_ADDRESS AWS_REGION VCPU_PER_WORKER
  ENGINE_VARIANT GPU_DEVICE_ID GPU_BATCH_SIZE_MIN_THRESHOLD
  GPU_PARTITIONED_OUTPUT_BATCH_ROWS
  GPU_UCXX_BLOCKING_POLLING GPU_UCX_TLS
  GPU_UCX_NET_DEVICES GPU_UCX_TCP_TX_SEG_SIZE GPU_UCX_TCP_RX_SEG_SIZE
  GPU_UCX_RNDV_FRAG_SIZE
  GPU_UCX_CUDA_COPY_MAX_REG_RATIO
  GPU_UCX_TCP_MAX_BW
  GPU_UCX_TCP_MAX_POLL GPU_UCX_CUDA_COPY_MAX_POLL
  GPU_USE_BUFFERED_INPUT GPU_USE_KVIKIO
  KVIKIO_REMOTE_IO_BACKEND KVIKIO_NTHREADS
  KVIKIO_TASK_SIZE KVIKIO_BOUNCE_BUFFER_SIZE
  KVIKIO_REMOTE_IO_NUM_REACTORS KVIKIO_REMOTE_IO_MAX_CONCURRENT_REQUESTS
  LIBCUDF_NUM_HOST_WORKERS CUDA_MODULE_LOADING
  TASK_MAX_DRIVERS_PER_TASK HIVE_MAX_SPLIT_SIZE
  HIVE_SPLIT_LOADER_CONCURRENCY DYNAMIC_FILTERING_ENABLED
  CPU_EXCHANGE_TUNING_ENABLED
  COORDINATOR_HEAP_GIB COORDINATOR_HEADROOM_GIB
  COORDINATOR_QUERY_TOTAL_MEMORY_PER_NODE_GIB
  COORDINATOR_QUERY_MEMORY_PER_NODE_GIB WORKER_SYSTEM_MEMORY_GIB
  WORKER_QUERY_MEMORY_GIB WORKER_MEMORY_LIMIT_GIB WORKER_MEMORY_SHRINK_GIB
  ASYNC_DATA_CACHE_ENABLED ASYNC_CACHE_SSD_GIB ASYNC_CACHE_NUM_SHARDS
  SPILL_ENABLED SPILL_PATH MAX_SPILL_GIB QUERY_MAX_SPILL_GIB
  COORDINATOR_IMAGE WORKER_IMAGE
)
for name in "${required[@]}"; do
  if [[ -z ${!name:-} ]]; then
    echo "missing required environment variable: ${name}" >&2
    exit 2
  fi
done

if [[ ${ROLE} != coordinator && ${ROLE} != worker ]]; then
  echo "ROLE must be coordinator or worker" >&2
  exit 2
fi
if [[ ${ENGINE_VARIANT} != cpu && ${ENGINE_VARIANT} != gpu ]]; then
  echo "ENGINE_VARIANT must be cpu or gpu" >&2
  exit 2
fi
if [[ ${ROLE} == worker ]]; then
  if [[ -z ${WORKER_INDEX:-} || -z ${LOCAL_WORKER_INDEX:-} ]]; then
    echo "WORKER_INDEX and LOCAL_WORKER_INDEX are required for worker role" >&2
    exit 2
  fi
fi

repo=/opt/velox-testing
runtime_root=/opt/presto-aws
generated="${repo}/presto/docker/config/generated/${ENGINE_VARIANT}"
role_key=${ROLE}
container_name="presto-${ROLE}"
http_port=8080
exchange_port=8083
if [[ ${ROLE} == worker ]]; then
  if ((WORKERS_PER_INSTANCE > 1)); then
    role_key="worker_${WORKER_INDEX}"
    container_name="presto-native-worker-${ENGINE_VARIANT}"
    http_port=$((8080 + LOCAL_WORKER_INDEX * 10))
    exchange_port=$((http_port + 3))
  else
    container_name="presto-native-worker-${ENGINE_VARIANT}"
  fi
fi
base="${runtime_root}/base_config/${role_key}"
final="${runtime_root}/final_config/${role_key}"
assembled="${runtime_root}/runtime_etc/${role_key}"
data_dir="${runtime_root}/data/${role_key}"
logs_dir="${runtime_root}/logs/${role_key}"

set_property() {
  local path=$1 key=$2 value=$3
  python3 - "${path}" "${key}" "${value}" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
key = sys.argv[2]
value = sys.argv[3]
prefix = f"{key}="
lines = path.read_text().splitlines()
matches = [index for index, line in enumerate(lines) if line.startswith(prefix)]
if len(matches) != 1:
    raise SystemExit(f"expected exactly one {key} in {path}; found {len(matches)}")
lines[matches[0]] = f"{key}={value}"
path.write_text("\n".join(lines) + "\n")
PY
}

upsert_property() {
  local path=$1 key=$2 value=$3
  python3 - "${path}" "${key}" "${value}" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
key = sys.argv[2]
value = sys.argv[3]
prefix = f"{key}="
lines = path.read_text().splitlines()
matches = [index for index, line in enumerate(lines) if line.startswith(prefix)]
if len(matches) > 1:
    raise SystemExit(f"expected at most one {key} in {path}; found {len(matches)}")
if matches:
    lines[matches[0]] = f"{key}={value}"
else:
    lines.append(f"{key}={value}")
path.write_text("\n".join(lines) + "\n")
PY
}

delete_property() {
  local path=$1 key=$2
  python3 - "${path}" "${key}" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
prefix = f"{sys.argv[2]}="
lines = [line for line in path.read_text().splitlines() if not line.startswith(prefix)]
path.write_text("\n".join(lines) + "\n")
PY
}

cd "${repo}/presto/scripts"
OVERWRITE_CONFIG=true \
NUM_WORKERS="${WORKER_COUNT}" \
VARIANT_TYPE="${ENGINE_VARIANT}" \
VCPU_PER_WORKER="${VCPU_PER_WORKER}" \
  ./generate_presto_config.sh

rm -rf "${base}" "${final}" "${assembled}"
mkdir -p "${base}" "${final}" "${assembled}" "${data_dir}" "${logs_dir}"

if [[ ${ROLE} == coordinator ]]; then
  cp -a "${generated}/etc_common/." "${base}/"
  cp "${generated}/etc_coordinator/config_native.properties" "${base}/config.properties"
  cp "${generated}/etc_coordinator/node.properties" "${base}/node.properties"
  mkdir -p "${base}/catalog"
  cp "${generated}/etc_coordinator/catalog/hive.properties" "${base}/catalog/hive.properties"
else
  worker_source="${generated}/etc_worker_${WORKER_INDEX}"
  cp -a "${generated}/etc_common/." "${base}/"
  cp "${worker_source}/config_native.properties" "${base}/config.properties"
  cp "${worker_source}/node.properties" "${base}/node.properties"
  mkdir -p "${base}/catalog"
  cp "${worker_source}/catalog/hive.properties" "${base}/catalog/hive.properties"
fi
cp -a "${base}/." "${final}/"
if [[ ${ENGINE_VARIANT} == cpu ]]; then
  delete_property "${final}/catalog/hive.properties" hive.max-split-size
  printf 'hive.max-split-size=%s\n' "${HIVE_MAX_SPLIT_SIZE}" \
    >>"${final}/catalog/hive.properties"
  delete_property "${final}/catalog/hive.properties" hive.split-loader-concurrency
  printf 'hive.split-loader-concurrency=%s\n' "${HIVE_SPLIT_LOADER_CONCURRENCY}" \
    >>"${final}/catalog/hive.properties"
else
  delete_property "${final}/catalog/hive.properties" hive.file-splittable
  printf 'hive.file-splittable=false\n' >>"${final}/catalog/hive.properties"
  if [[ ${ROLE} == worker ]]; then
    delete_property "${final}/catalog/hive.properties" cudf.hive.use-buffered-input
    printf 'cudf.hive.use-buffered-input=%s\n' "${GPU_USE_BUFFERED_INPUT}" \
      >>"${final}/catalog/hive.properties"
  fi
fi

if [[ ${ROLE} == coordinator ]]; then
  set_property "${final}/config.properties" discovery.uri \
    "http://${COORDINATOR_ADDRESS}:8080"
  set_property "${final}/config.properties" query-manager.required-workers \
    "${WORKER_COUNT}"
  set_property "${final}/config.properties" \
    query-manager.required-workers-max-wait 10m
  set_property "${final}/config.properties" query.max-total-memory-per-node \
    "${COORDINATOR_QUERY_TOTAL_MEMORY_PER_NODE_GIB}GB"
  set_property "${final}/config.properties" query.max-total-memory \
    "$((COORDINATOR_QUERY_TOTAL_MEMORY_PER_NODE_GIB * WORKER_COUNT))GB"
  set_property "${final}/config.properties" query.max-memory-per-node \
    "${COORDINATOR_QUERY_MEMORY_PER_NODE_GIB}GB"
  set_property "${final}/config.properties" query.max-memory \
    "$((COORDINATOR_QUERY_MEMORY_PER_NODE_GIB * WORKER_COUNT))GB"
  set_property "${final}/config.properties" memory.heap-headroom-per-node \
    "${COORDINATOR_HEADROOM_GIB}GB"
  if ((WORKER_COUNT == 1)); then
    set_property "${final}/config.properties" single-node-execution-enabled true
  else
    set_property "${final}/config.properties" single-node-execution-enabled false
  fi
  set_property "${final}/config.properties" experimental.enable-dynamic-filtering \
    "${DYNAMIC_FILTERING_ENABLED}"
  upsert_property "${final}/config.properties" experimental.spill-enabled \
    "${SPILL_ENABLED}"
  if [[ ${SPILL_ENABLED} == true ]]; then
    upsert_property "${final}/config.properties" experimental.max-spill-per-node \
      "${MAX_SPILL_GIB}GB"
    upsert_property "${final}/config.properties" experimental.query-max-spill-per-node \
      "${QUERY_MAX_SPILL_GIB}GB"
  fi
  python3 - "${final}/jvm.config" "${COORDINATOR_HEAP_GIB}" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
heap = sys.argv[2]
lines = path.read_text().splitlines()
lines = [
    f"-Xmx{heap}G" if line.startswith("-Xmx") else
    f"-Xms{heap}G" if line.startswith("-Xms") else line
    for line in lines
]
path.write_text("\n".join(lines) + "\n")
PY
else
  set_property "${final}/config.properties" http-server.http.port "${http_port}"
  set_property "${final}/config.properties" discovery.uri \
    "http://${COORDINATOR_ADDRESS}:8080"
  if [[ ${ENGINE_VARIANT} == gpu ]]; then
    # Keep the cuDF exchange listener three ports above each worker's HTTP
    # endpoint while spacing local workers far enough apart to avoid collisions.
    set_property "${final}/config.properties" cudf.exchange.server.port \
      "${exchange_port}"
  fi
  if ((WORKER_COUNT == 1)); then
    set_property "${final}/config.properties" single-node-execution-enabled true
  else
    set_property "${final}/config.properties" single-node-execution-enabled false
  fi
  set_property "${final}/config.properties" task.max-drivers-per-task \
    "${TASK_MAX_DRIVERS_PER_TASK}"
  set_property "${final}/config.properties" system-memory-gb \
    "${WORKER_SYSTEM_MEMORY_GIB}"
  set_property "${final}/config.properties" query-memory-gb \
    "${WORKER_QUERY_MEMORY_GIB}"
  set_property "${final}/config.properties" query.max-memory-per-node \
    "${WORKER_QUERY_MEMORY_GIB}GB"
  set_property "${final}/config.properties" system-mem-limit-gb \
    "${WORKER_MEMORY_LIMIT_GIB}"
  set_property "${final}/config.properties" system-mem-shrink-gb \
    "${WORKER_MEMORY_SHRINK_GIB}"
  set_property "${final}/config.properties" async-data-cache-enabled \
    "${ASYNC_DATA_CACHE_ENABLED}"
  upsert_property "${final}/config.properties" async-cache-ssd-gb \
    "${ASYNC_CACHE_SSD_GIB}"
  upsert_property "${final}/config.properties" async-cache-num-shards \
    "${ASYNC_CACHE_NUM_SHARDS}"
  # Prestissimo uses its native spill gate on workers. The
  # experimental.spill-enabled property is the Java coordinator setting and
  # does not enable Velox operator spilling.
  upsert_property "${final}/config.properties" spill-enabled \
    "${SPILL_ENABLED}"
  if [[ ${SPILL_ENABLED} == true ]]; then
    upsert_property "${final}/config.properties" max-spill-bytes \
      "$((MAX_SPILL_GIB * 1024 * 1024 * 1024))"
    upsert_property "${final}/config.properties" experimental.spiller-spill-path \
      "${SPILL_PATH}"
    upsert_property "${final}/config.properties" experimental.max-spill-per-node \
      "${MAX_SPILL_GIB}GB"
    upsert_property "${final}/config.properties" experimental.query-max-spill-per-node \
      "${QUERY_MAX_SPILL_GIB}GB"
    upsert_property "${final}/config.properties" \
      experimental.spiller-max-used-space-threshold 0.9
  fi
  upsert_property "${final}/config.properties" runtime-metrics-collection-enabled \
    true
  if [[ ${ENGINE_VARIANT} == gpu ]]; then
    set_property "${final}/config.properties" \
      cudf.partitioned_output_batch_rows \
      "${GPU_PARTITIONED_OUTPUT_BATCH_ROWS}"
    upsert_property "${final}/config.properties" \
      ucxx.blocking_polling \
      "${GPU_UCXX_BLOCKING_POLLING}"
    delete_property "${final}/config.properties" cudf.batch_size_min_threshold
    printf 'cudf.batch_size_min_threshold=%s\n' \
      "${GPU_BATCH_SIZE_MIN_THRESHOLD}" \
      >>"${final}/config.properties"
    delete_property "${final}/config.properties" cudf.s3.use_kvikio
    printf 'cudf.s3.use_kvikio=%s\n' "${GPU_USE_KVIKIO}" \
      >>"${final}/config.properties"
  elif [[ ${CPU_EXCHANGE_TUNING_ENABLED} == false ]]; then
    for key in \
      exchange.http-client.enable-connection-pool \
      exchange.max-buffer-size \
      sink.max-buffer-size \
      exchange.max-response-size \
      exchange.client-threads \
      exchange.http-client.max-connections \
      exchange.http-client.max-connections-per-server \
      exchange.http-client.max-requests-queued-per-destination \
      exchange.http-client.request-timeout; do
      delete_property "${final}/config.properties" "${key}"
    done
  fi
  if ((ASYNC_CACHE_SSD_GIB > 0)); then
    if [[ -z ${ASYNC_CACHE_SSD_PATH:-} ]]; then
      echo "ASYNC_CACHE_SSD_PATH is required for SSD cache" >&2
      exit 2
    fi
    cache_path=${ASYNC_CACHE_SSD_PATH}
    if ((WORKERS_PER_INSTANCE > 1)); then
      cache_path="${ASYNC_CACHE_SSD_PATH}/worker-${WORKER_INDEX}"
      mkdir -p "${cache_path}"
    fi
    upsert_property "${final}/config.properties" async-cache-ssd-path "${cache_path}"
  fi
  set_property "${final}/node.properties" node.id \
    "aws-${RUN_ID}-worker-${WORKER_INDEX}"
  upsert_property "${final}/node.properties" node.internal-address \
    "$(hostname -I | awk '{print $1}')"
fi

cp -a "${final}/." "${assembled}/"
find "${base}" -type f -print0 | sort -z | xargs -0 sha256sum \
  >"${runtime_root}/base_config_${role_key}.sha256"
find "${final}" -type f -print0 | sort -z | xargs -0 sha256sum \
  >"${runtime_root}/final_config_${role_key}.sha256"
diff -ru "${base}" "${final}" >"${runtime_root}/config_${role_key}.diff" || true
tuning_id=$(
  printf '%s\n' \
    "engine=${ENGINE_VARIANT}" \
    "drivers=${TASK_MAX_DRIVERS_PER_TASK}" \
    "split=${HIVE_MAX_SPLIT_SIZE}" \
    "loader=${HIVE_SPLIT_LOADER_CONCURRENCY}" \
    "dynamic_filtering=${DYNAMIC_FILTERING_ENABLED}" \
    "exchange_tuning=${CPU_EXCHANGE_TUNING_ENABLED}" \
    "spill_enabled=${SPILL_ENABLED}" \
    "spill_path=${SPILL_PATH}" \
    "max_spill_gib=${MAX_SPILL_GIB}" \
    "query_max_spill_gib=${QUERY_MAX_SPILL_GIB}" |
    sha256sum | cut -c1-12
)
history="${runtime_root}/config_history/${tuning_id}/${role_key}"
rm -rf "${history}"
mkdir -p "${history}"
cp -a "${final}/." "${history}/"
cat >"${history}/tuning.json" <<EOF
{
  "engine_variant": "${ENGINE_VARIANT}",
  "task_max_drivers_per_task": ${TASK_MAX_DRIVERS_PER_TASK},
  "hive_max_split_size": "${HIVE_MAX_SPLIT_SIZE}",
  "hive_split_loader_concurrency": ${HIVE_SPLIT_LOADER_CONCURRENCY},
  "dynamic_filtering_enabled": ${DYNAMIC_FILTERING_ENABLED},
  "cpu_exchange_tuning_enabled": ${CPU_EXCHANGE_TUNING_ENABLED},
  "spill_enabled": ${SPILL_ENABLED},
  "spill_path": "${SPILL_PATH}",
  "max_spill_gib": ${MAX_SPILL_GIB},
  "query_max_spill_gib": ${QUERY_MAX_SPILL_GIB}
}
EOF

if [[ ${ROLE} == coordinator ]]; then
  docker rm -f presto-coordinator 2>/dev/null || true
elif ((WORKERS_PER_INSTANCE > 1 && LOCAL_WORKER_INDEX == 0)); then
  stale_workers=$(docker ps -aq --filter "name=^presto-native-worker-${ENGINE_VARIANT}-")
  if [[ -n ${stale_workers} ]]; then
    docker rm -f ${stale_workers} 2>/dev/null || true
  fi
else
  docker rm -f "${container_name}" 2>/dev/null || true
fi
if [[ ${ROLE} == worker ]] &&
  ((WORKERS_PER_INSTANCE > 1 && LOCAL_WORKER_INDEX + 1 < WORKERS_PER_INSTANCE)); then
  # aws_cluster invokes this script once per local GPU. Earlier invocations
  # only materialize configs; the final invocation starts one container with
  # all GPUs so launch_presto_servers.sh can apply GPU-local NUMA affinity.
  exit 0
fi
timestamp=$(date -u +%Y%m%dT%H%M%SZ)

if [[ ${ROLE} == coordinator ]]; then
  docker run -d \
    --name presto-coordinator \
    --network host \
    --restart no \
    -e "AWS_DEFAULT_REGION=${AWS_REGION}" \
    -e "AWS_REGION=${AWS_REGION}" \
    -e "SERVER_START_TIMESTAMP=${timestamp}" \
    -v "${assembled}:/opt/presto-server/etc:ro" \
    -v "${runtime_root}/metastore:/var/lib/presto/data/hive/metastore" \
    -v "${data_dir}:/var/lib/presto/data/local" \
    -v "${logs_dir}:/opt/presto-server/logs" \
    -v "${repo}/presto/docker/launch_coordinator.sh:/opt/launch_coordinator.sh:ro" \
    --entrypoint bash \
    "${COORDINATOR_IMAGE}" \
    /opt/launch_coordinator.sh
else
  cache_mount_args=()
  gpu_args=()
  gpu_env=()
  config_mount_args=(-v "${assembled}:/opt/presto-server/etc:ro")
  worker_launch_args=()
  worker_logs_dir=${logs_dir}
  worker_data_dir=${data_dir}
  if ((ASYNC_CACHE_SSD_GIB > 0)) || [[ ${SPILL_ENABLED} == true ]]; then
    cache_mount_args=(-v /mnt/nvme:/mnt/nvme)
  fi
  if [[ ${ENGINE_VARIANT} == gpu ]]; then
    # KvikIO performs S3 reads inside the worker container. Export the
    # instance-profile credentials because containerized processes cannot
    # reliably reach IMDS when the instance hop limit is one.
    eval "$(aws configure export-credentials --format env)"
    gpu_args=(
      --cap-add IPC_LOCK
      --cap-add NET_RAW
      --ulimit memlock=-1:-1
      --shm-size 1g
      -v /sys/devices/system/node:/sys/devices/system/node:ro
    )
    for efa_device in /dev/infiniband/*; do
      if [[ -c ${efa_device} ]]; then
        gpu_args+=(--device "${efa_device}")
      fi
    done
    if ((WORKERS_PER_INSTANCE > 1)); then
      gpu_args=(--gpus all "${gpu_args[@]}")
      config_mount_args=()
      worker_launch_args=()
      worker_index_base=$((WORKER_INDEX - LOCAL_WORKER_INDEX))
      for ((local_index = 0; local_index < WORKERS_PER_INSTANCE; local_index++)); do
        global_index=$((worker_index_base + local_index))
        config_mount_args+=(
          -v "${runtime_root}/runtime_etc/worker_${global_index}:/opt/presto-server/etc${local_index}:ro"
        )
        worker_launch_args+=("${local_index}")
      done
      worker_logs_dir="${runtime_root}/logs/worker_host_${worker_index_base}"
      worker_data_dir="${runtime_root}/data/worker_host_${worker_index_base}"
      mkdir -p "${worker_logs_dir}" "${worker_data_dir}"
    else
      gpu_args=(--gpus "device=${GPU_DEVICE_ID}" "${gpu_args[@]}")
    fi
    gpu_env=(
      -e "KVIKIO_REMOTE_IO_BACKEND=${KVIKIO_REMOTE_IO_BACKEND}"
      -e "KVIKIO_NTHREADS=${KVIKIO_NTHREADS}"
      -e "KVIKIO_TASK_SIZE=${KVIKIO_TASK_SIZE}"
      -e "KVIKIO_BOUNCE_BUFFER_SIZE=${KVIKIO_BOUNCE_BUFFER_SIZE}"
      -e "KVIKIO_REMOTE_IO_NUM_REACTORS=${KVIKIO_REMOTE_IO_NUM_REACTORS}"
      -e "KVIKIO_REMOTE_IO_MAX_CONCURRENT_REQUESTS=${KVIKIO_REMOTE_IO_MAX_CONCURRENT_REQUESTS}"
      -e KVIKIO_REMOTE_IO_REACTOR_DISPATCH=PER_CHUNK
      -e "LIBCUDF_NUM_HOST_WORKERS=${LIBCUDF_NUM_HOST_WORKERS}"
      -e "CUDA_MODULE_LOADING=${CUDA_MODULE_LOADING}"
      -e AWS_ACCESS_KEY_ID
      -e AWS_SECRET_ACCESS_KEY
      -e AWS_SESSION_TOKEN
      -e "UCX_TLS=${GPU_UCX_TLS}"
      -e "UCX_NET_DEVICES=${GPU_UCX_NET_DEVICES}"
      -e "UCX_TCP_TX_SEG_SIZE=${GPU_UCX_TCP_TX_SEG_SIZE}"
      -e "UCX_TCP_RX_SEG_SIZE=${GPU_UCX_TCP_RX_SEG_SIZE}"
      -e "UCX_RNDV_FRAG_SIZE=${GPU_UCX_RNDV_FRAG_SIZE}"
      -e "UCX_CUDA_COPY_MAX_REG_RATIO=${GPU_UCX_CUDA_COPY_MAX_REG_RATIO}"
      -e "UCX_TCP_MAX_BW=${GPU_UCX_TCP_MAX_BW}"
      -e "UCX_TCP_MAX_POLL=${GPU_UCX_TCP_MAX_POLL}"
      -e "UCX_CUDA_COPY_MAX_POLL=${GPU_UCX_CUDA_COPY_MAX_POLL}"
      -e UCX_TCP_CM_REUSEADDR=y
      -e UCX_LOG_LEVEL=info
      -e UCX_PROTO_INFO=y
      -e UCX_RNDV_PIPELINE_ERROR_HANDLING=y
      -e UCX_TCP_KEEPINTVL=1ms
      -e UCX_KEEPALIVE_INTERVAL=1ms
    )
    if ((WORKERS_PER_INSTANCE > 1)); then
      gpu_env+=(-e NVIDIA_VISIBLE_DEVICES=all)
    else
      gpu_env+=(
        -e "CUDA_VISIBLE_DEVICES=${GPU_DEVICE_ID}"
        -e "NVIDIA_VISIBLE_DEVICES=${GPU_DEVICE_ID}"
      )
    fi
  fi
  docker run -d \
    --name "${container_name}" \
    --network host \
    --ipc host \
    --restart no \
    --cap-add SYS_NICE \
    "${gpu_args[@]}" \
    -e "AWS_DEFAULT_REGION=${AWS_REGION}" \
    -e "AWS_REGION=${AWS_REGION}" \
    -e "SERVER_START_TIMESTAMP=${timestamp}" \
    "${gpu_env[@]}" \
    "${config_mount_args[@]}" \
    -v "${runtime_root}/metastore:/var/lib/presto/data/hive/metastore" \
    -v "${worker_data_dir}:/var/lib/presto/data/local" \
    -v "${worker_logs_dir}:/opt/presto-server/logs" \
    "${cache_mount_args[@]}" \
    -v "${repo}/presto/docker/launch_presto_servers.sh:/opt/launch_presto_servers.sh:ro" \
    --entrypoint bash \
    "${WORKER_IMAGE}" \
    /opt/launch_presto_servers.sh "${worker_launch_args[@]}"
fi

if [[ ${ROLE} == coordinator || ${LOCAL_WORKER_INDEX:-0} == 0 ]]; then
  mkdir -p "${runtime_root}/telemetry"
  if [[ -f ${runtime_root}/telemetry/${ROLE}.sampler.pid ]]; then
    kill "$(cat "${runtime_root}/telemetry/${ROLE}.sampler.pid")" 2>/dev/null || true
  fi
  nohup bash "${repo}/presto/aws/ec2/remote/sample_host.sh" \
    "${ROLE}" "${runtime_root}/telemetry/${ROLE}.jsonl" \
    >"${runtime_root}/telemetry/${ROLE}.sampler.log" 2>&1 &
  echo "$!" >"${runtime_root}/telemetry/${ROLE}.sampler.pid"
fi
