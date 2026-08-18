#!/usr/bin/env bash
set -uo pipefail

mkdir -p /opt/dlami/nvme/presto_logs /opt/dlami/nvme/presto-gpu-nvme-cache
sudo rm -rf /opt/dlami/nvme/presto_logs/* /opt/dlami/nvme/presto-gpu-nvme-cache/*

cd ..
rm -rf benchmark_output presto_logs

