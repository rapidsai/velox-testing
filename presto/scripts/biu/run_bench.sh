#!/usr/bin/env bash
set -euo pipefail

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

./run_benchmark.sh -b tpch -s tpch_sf1k_v2_float_s3 --reference-results-dir ~/rapids/reference_data/sf1k_v2_float

cd -
