#!/usr/bin/env bash
set -uo

cd ..

export AWS_DEFAULT_REGION=us-east-2

export KVIKIO_TASK_SIZE=$((128 * 1024 * 1024))
export KVIKIO_BOUNCE_BUFFER_SIZE=$((128 * 1024 * 1024))
export KVIKIO_REMOTE_IO_BACKEND=EASY_THREADPOOL
# export KVIKIO_REMOTE_IO_BACKEND=MULTI_POLL
export KVIKIO_REMOTE_IO_NUM_REACTORS=16
export KVIKIO_REMOTE_IO_MAX_CONCURRENT_REQUESTS=256

#-b worker
./start_native_gpu_presto.sh --overwrite-config --kvikio-threads 160 --num-drivers 6 \
--logs-dir /opt/dlami/nvme/presto_logs

./register_external_tables.sh -b tpch -s tpch_sf3k_v2_float_s3 -l s3://rapids-tpch/presto-gpu/sf3k_v2_float

./start_native_cpu_presto.sh --overwrite-config --kvikio-threads 160

until curl -s http://localhost:8080/v1/info | grep -q '"starting":false'; do echo "waiting for coordinator..."; sleep 5; done && echo "Coordinator ready!"
until [[ $(curl -s http://localhost:8080/v1/node | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null) -ge 1 ]]; do echo "waiting for worker..."; sleep 5; done && echo "Worker ready!"

./analyze_tables.sh -s tpch_sf3k_v2_float_s3

cd -
