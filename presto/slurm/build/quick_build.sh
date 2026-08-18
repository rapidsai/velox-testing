#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Quick Start Commands for Presto Build with Slurm/Enroot
# Copy and paste these commands to your terminal
#
# Adjust paths as needed:
#   - Source: $PRESTO_SLURM_BUILD_DIR
#   - Images: /mnt/data/$USER/images/presto

set -e

IMAGES_DIR=${IMAGES_DIR:-/mnt/data/$USER/images/presto}
mkdir -p "$IMAGES_DIR"
PRESTO_SLURM_BUILD_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ==============================================================================
# STAGE 1: Build Dependencies Image (Run once, ~30-60 minutes)
# ==============================================================================
srun --export=ALL,PMIX_MCA_gds=^ds12 \
  --nodes=1 --mem=0 --ntasks-per-node=1 \
  --cpus-per-task=144 --gpus-per-task=4 --gres=gpu:4 \
  --mpi=pmix_v4 --container-remap-root \
  --container-image=quay.io/centos/centos:stream9 \
  --container-mounts=$PRESTO_SLURM_BUILD_DIR:/presto-build \
  --container-save=$IMAGES_DIR/prestissimo-dependency-centos9.sqsh \
  /presto-build/build-deps-in-container.sh

# ==============================================================================
# STAGE 2: Build Native Worker Image (~20-40 minutes)
# ==============================================================================

# GPU Worker (default)
srun --export=ALL,PMIX_MCA_gds=^ds12,NUM_THREADS=144,CUDA_ARCHITECTURES=100,PRESTO_DIR=/presto-build/presto/presto-native-execution \
  --nodes=1 --mem=0 --ntasks-per-node=1 \
  --cpus-per-task=144 --gpus-per-task=4 --gres=gpu:4 \
  --mpi=pmix_v4 --container-remap-root \
  --container-image=$IMAGES_DIR/prestissimo-dependency-centos9.sqsh \
  --container-mounts=$PRESTO_SLURM_BUILD_DIR:/presto-build \
  --container-save=$IMAGES_DIR/presto-native-worker-gpu.sqsh \
  /presto-build/build-presto.sh

# CPU Worker (optional)
# srun --export=ALL,PMIX_MCA_gds=^ds12,NUM_THREADS=144,GPU=OFF,PRESTO_DIR=/presto-build/presto/presto-native-execution,EXTRA_CMAKE_FLAGS="-DPRESTO_ENABLE_TESTING=OFF -DPRESTO_ENABLE_PARQUET=ON -DPRESTO_ENABLE_CUDF=OFF -DVELOX_BUILD_TESTING=OFF" \
#   --nodes=1 --mem=0 --ntasks-per-node=1 \
#   --cpus-per-task=144 \
#   --mpi=pmix_v4 --container-remap-root \
#   --container-image=$IMAGES_DIR/prestissimo-dependency-centos9.sqsh \
#   --container-mounts=$PRESTO_SLURM_BUILD_DIR:/presto-build \
#   --container-save=$IMAGES_DIR/presto-native-worker-cpu.sqsh \
#   /presto-build/build-presto.sh

# ==============================================================================
# STAGE 3: Build Coordinator Image (Run once, ~30-60 minutes)
# ==============================================================================
srun --export=ALL,PMIX_MCA_gds=^ds12,PRESTO_VERSION=testing,PRESTO_SOURCE_DIR=/presto-build/presto \
  --nodes=1 --mem=0 --ntasks-per-node=1 \
  --cpus-per-task=144 \
  --mpi=pmix_v4 --container-remap-root \
  --container-image=$IMAGES_DIR/prestissimo-dependency-centos9.sqsh \
  --container-mounts=$PRESTO_SLURM_BUILD_DIR:/presto-build \
  --container-save=$IMAGES_DIR/presto-coordinator.sqsh \
  /presto-build/setup-coordinator.sh

# ==============================================================================
# INCREMENTAL BUILDS (after code changes)
# ==============================================================================

# Rebuild worker only (dependencies already built)
# srun --export=ALL,PMIX_MCA_gds=^ds12,NUM_THREADS=144,CUDA_ARCHITECTURES=100,PRESTO_DIR=/presto-build/presto/presto-native-execution \
#   --nodes=1 --mem=0 --ntasks-per-node=1 \
#   --cpus-per-task=144 --gpus-per-task=4 --gres=gpu:4 \
#   --mpi=pmix_v4 --container-remap-root \
#   --container-image=$IMAGES_DIR/presto-native-worker-gpu.sqsh \
#   --container-mounts=$PRESTO_SLURM_BUILD_DIR:/presto-build \
#   --container-save=$IMAGES_DIR/presto-native-worker-gpu-REBUILD.sqsh \
#   /presto-build/build-presto.sh

# ==============================================================================
# VERIFICATION (inside container)
# ==============================================================================

# After building worker, verify in interactive shell:
# which presto_server
# ldd /usr/bin/presto_server | grep -v "libcuda\|libnvidia"
# ls -lh /usr/lib64/presto-native-libs/

# After building coordinator, verify:
# /opt/presto-cli --version
# /opt/presto-server/bin/launcher --help

# ==============================================================================
# ENVIRONMENT VARIABLE REFERENCE
# ==============================================================================

# Dependencies build:
#   ARM_BUILD_TARGET=local     # For ARM architecture tuning

# Worker build:
#   NUM_THREADS=144            # Build parallelism
#   CUDA_ARCHITECTURES=100     # Target GPU (100 = compute 10.0 / GB200)
#   GPU=ON                     # ON for GPU, OFF for CPU
#   BUILD_TYPE=release         # release or debug
#   PRESTO_DIR=/path           # Path to presto-native-execution
#   INSTALL_NSIGHT=true        # Install nsight-systems for profiling

# Coordinator build:
#   PRESTO_VERSION=testing     # Version string
#   PRESTO_SOURCE_DIR=/path    # Path to Presto Java source

# ==============================================================================
# COMMON CUDA ARCHITECTURES
# ==============================================================================
# 75  = Turing (T4, RTX 2000)
# 80  = Ampere (A100, A30, RTX 3000)
# 86  = Ampere (RTX 3000, A10)
# 90  = Hopper (H100, H200)
# 100 = Blackwell (GB200, B100)
# 120 = Next-gen

# For multiple: CUDA_ARCHITECTURES="75;80;86;90;100;120"
# For single:   CUDA_ARCHITECTURES=100
