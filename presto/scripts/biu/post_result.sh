#!/usr/bin/env bash
set -euo pipefail

cd ..
rm -rf benchmark_output presto_logs
scp -r ubuntu@my-g7e-8:/home/ubuntu/rapids/velox-testing/presto/scripts/benchmark_output .
scp -r ubuntu@my-g7e-8:/opt/dlami/nvme/presto_logs .

source ${HOME}/.secret/benchmark.env
# IMG_HASH=$(docker inspect --format='{{.Id}}' presto-native-worker-gpu:"${USER}")
cd ../..
./scripts/run_py_script.sh -p benchmark_reporting_tools/post_results.py \
presto/scripts/benchmark_output \
--sku-name g7e.8xlarge \
--storage-configuration-name s3-us-east-2-presto-tpch-scale-1000 \
--benchmark-name tpch-rs-1000 \
--cache-state cold \
--logs-dir presto/scripts/presto_logs \
--identifier-hash "sha256:18ad23e49567c9e40814d91827317cba4c2dec57eaaea5fcf9fb545b6fdcb201"
