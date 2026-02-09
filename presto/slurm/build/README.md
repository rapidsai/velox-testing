# Presto Build Guide for Slurm with Enroot

This guide provides complete instructions for building Presto components using Slurm and enroot containers. This workflow is designed for HPC clusters where Docker is not available.

## Overview

The build process creates container images in stages:

1. **Base Image** → **Dependencies Image** (with all C++ libraries, CUDA, UCX)
2. **Dependencies Image** → **Worker Image** (with Presto native execution binary)
3. **Base Image** or **Dependencies Image** → **Coordinator Image** (with Java Presto)

Each stage uses `srun` with `--container-save` to create a new `.sqsh` image file.

## Prerequisites

- Slurm cluster with enroot support
- Source code mounted/available at: `/mnt/data/$USER/src/velox-testing/presto/slurm/build`
- Write access to image storage: `/mnt/data/$USER/images/presto/`
- Sufficient resources: 144 CPUs, 4 GPUs recommended

## Quick Reference

Before starting, ensure the Presto/Velox source code is available at: `/mnt/data/$USER/src/velox-testing/presto/slurm/build`. A convenience script is provided for that:

```bash
./clone.sh
```

Then run the builds:

```bash
# Stage 1: Build dependencies (15-20 minutes)
srun --export=ALL,PMIX_MCA_gds=^ds12 \
  --nodes=1 --mem=0 --ntasks-per-node=1 \
  --cpus-per-task=144 --gpus-per-task=4 --gres=gpu:4 \
  --mpi=pmix_v4 --container-remap-root \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --container-image=quay.io/centos/centos:stream9 \
  --container-save=/mnt/data/$USER/images/presto/prestissimo-dependency-centos9.sqsh \
  /presto-build/build-deps-in-container.sh

# Stage 2: Build worker (20-25 minutes)
srun --export=ALL,PMIX_MCA_gds=^ds12,NUM_THREADS=144,CUDA_ARCHITECTURES=100,PRESTO_DIR=/presto-build/presto/presto-native-execution \
  --nodes=1 --mem=0 --ntasks-per-node=1 \
  --cpus-per-task=144 --gpus-per-task=4 --gres=gpu:4 \
  --mpi=pmix_v4 --container-remap-root \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --container-image=/mnt/data/$USER/images/presto/prestissimo-dependency-centos9.sqsh \
  --container-save=/mnt/data/$USER/images/presto/presto-native-worker-gpu.sqsh \
  /presto-build/build-presto.sh

# Stage 3: Build coordinator (15-20 minutes)
srun --export=ALL,PMIX_MCA_gds=^ds12,PRESTO_VERSION=testing,PRESTO_SOURCE_DIR=/presto-build/presto \
  --nodes=1 --mem=0 --ntasks-per-node=1 \
  --cpus-per-task=144 \
  --mpi=pmix_v4 --container-remap-root \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --container-image=/mnt/data/$USER/images/presto/prestissimo-dependency-centos9.sqsh \
  --container-save=/mnt/data/$USER/images/presto/presto-coordinator.sqsh \
  /presto-build/setup-coordinator.sh
```

## Detailed Instructions

### Stage 0: Retrieve Presto and Velox source

Before starting, ensure the Presto/Velox source code is available at: `/mnt/data/$USER/src/velox-testing/presto/slurm/build`. A convenience script is provided for that:

```bash
./clone.sh
```

### Stage 1: Build Dependencies Image

This creates an image with all Presto/Velox dependencies, CUDA, and UCX.

**Command:**
```bash
srun --export=ALL,PMIX_MCA_gds=^ds12 \
  --nodes=1 \
  --mem=0 \
  --ntasks-per-node=1 \
  --cpus-per-task=144 \
  --gpus-per-task=4 \
  --gres=gpu:4 \
  --mpi=pmix_v4 \
  --container-remap-root \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --container-image=quay.io/centos/centos:stream9 \
  --container-save=/mnt/data/$USER/images/presto/prestissimo-dependency-centos9.sqsh \
  /presto-build/build-deps-in-container.sh
```

**What it does:**
- Installs build tools (gcc-toolset-12, gcc-toolset-14, cmake, clang15)
- Builds Velox dependencies (folly, boost, arrow, etc.)
- Installs CUDA 13.0
- Builds UCX 1.19.0 with CUDA support
- Installs storage adapters (S3, GCS, Azure, HDFS)

**Output:**
- Image: `/mnt/data/$USER/images/presto/prestissimo-dependency-centos9.sqsh`
- Time: 15-20 minutes
- Size: ~8 GB

**Environment variables (optional):**
```bash
--export=ALL,PMIX_MCA_gds=^ds12,ARM_BUILD_TARGET=local
```

Note: This image generally needs to be built once and then never again. Cases for rebuild are either changing CUDA or UCX build versions, or a change in Presto's dependencies.

### Stage 2: Build Native Worker Image

This builds Presto C++ native execution on top of the dependencies image.

**Command:**
```bash
srun --export=ALL,PMIX_MCA_gds=^ds12,NUM_THREADS=144,CUDA_ARCHITECTURES=100,PRESTO_DIR=/presto-build/presto/presto-native-execution \
  --nodes=1 \
  --mem=0 \
  --ntasks-per-node=1 \
  --cpus-per-task=144 \
  --gpus-per-task=4 \
  --gres=gpu:4 \
  --mpi=pmix_v4 \
  --container-remap-root \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --container-image=/mnt/data/$USER/images/presto/prestissimo-dependency-centos9.sqsh \
  --container-save=/mnt/data/$USER/images/presto/presto-native-worker-gpu.sqsh \
  /presto-build/build-presto.sh
```

**What it does:**
- Builds Presto native execution with GPU support
- Collects runtime libraries
- Installs to `/usr/bin/presto_server`
- Sets up library paths in `/usr/lib64/presto-native-libs/`
- Configures `ld.so.conf`

**Output:**
- Image: `/mnt/data/$USER/images/presto/presto-native-worker-gpu.sqsh`
- Time: 20-25 minutes
- Size: ~11 GB

**Environment variables:**
| Variable | Default | Description |
|----------|---------|-------------|
| `NUM_THREADS` | 12 | Build parallelism |
| `CUDA_ARCHITECTURES` | 75;80;86;90;100;120 | Target GPU architectures |
| `PRESTO_DIR` | (required) | Path to presto-native-execution |
| `BUILD_TYPE` | release | release or debug |
| `GPU` | ON | ON or OFF |
| `INSTALL_NSIGHT` | false | Install nsight-systems |

**For CPU-only worker:**
```bash
srun --export=ALL,PMIX_MCA_gds=^ds12,NUM_THREADS=144,GPU=OFF,PRESTO_DIR=/presto-build/presto/presto-native-execution \
  --export=EXTRA_CMAKE_FLAGS="-DPRESTO_ENABLE_TESTING=OFF -DPRESTO_ENABLE_PARQUET=ON -DPRESTO_ENABLE_CUDF=OFF -DVELOX_BUILD_TESTING=OFF" \
  --nodes=1 \
  --mem=0 \
  --ntasks-per-node=1 \
  --cpus-per-task=144 \
  --mpi=pmix_v4 \
  --container-remap-root \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --container-image=/mnt/data/$USER/images/presto/prestissimo-dependency-centos9.sqsh \
  --container-save=/mnt/data/$USER/images/presto/presto-native-worker-cpu.sqsh \
  /presto-build/build-presto.sh
```

### Stage 3: Build Coordinator Image

This builds the Java-based Presto coordinator.

**Command:**
```bash
srun --export=ALL,PMIX_MCA_gds=^ds12,PRESTO_VERSION=testing,PRESTO_SOURCE_DIR=/presto-build/presto \
  --nodes=1 \
  --mem=0 \
  --ntasks-per-node=1 \
  --cpus-per-task=144 \
  --mpi=pmix_v4 \
  --container-remap-root \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --container-image=/mnt/data/$USER/images/presto/prestissimo-dependency-centos9.sqsh \
  --container-save=/mnt/data/$USER/images/presto/presto-coordinator.sqsh \
  /presto-build/setup-coordinator.sh
```

**Alternative: Build on base image (smaller, but needs Java):**
```bash
srun --export=ALL,PMIX_MCA_gds=^ds12,PRESTO_VERSION=testing,PRESTO_SOURCE_DIR=/presto-build/presto \
  --nodes=1 \
  --mem=0 \
  --ntasks-per-node=1 \
  --cpus-per-task=144 \
  --mpi=pmix_v4 \
  --container-remap-root \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --container-image=quay.io/centos/centos:stream9 \
  --container-save=/mnt/data/$USER/images/presto/presto-coordinator.sqsh \
  /presto-build/setup-coordinator.sh
```

**What it does:**
- Installs Java 17 and Maven
- Builds Presto Java package
- Extracts Presto server to `/opt/presto-server`
- Installs Presto CLI to `/opt/presto-cli`
- Sets up configuration files

**Output:**
- Image: `/mnt/data/$USER/images/presto/presto-coordinator.sqsh`
- Time: 15-20 minutes
- Size: ~2-3 GB (on base), ~6-8 GB (on deps)

**Environment variables:**
| Variable | Default | Description |
|----------|---------|-------------|
| `PRESTO_VERSION` | testing | Version string for build |
| `PRESTO_SOURCE_DIR` | /presto-build/presto | Presto source location |
| `PRESTO_HOME` | /opt/presto-server | Installation directory |
| `PRESTO_BUILD_DIR` | /presto_build | Build artifacts location |

## Incremental Builds

### Rebuilding Worker After Code Changes

If you only changed Presto/Velox C++ code (not dependencies):

```bash
# Rebuild worker without --container-save to test
srun --export=ALL,PMIX_MCA_gds=^ds12,NUM_THREADS=144,CUDA_ARCHITECTURES=100,PRESTO_DIR=/presto-build/presto/presto-native-execution \
  --nodes=1 \
  --mem=0 \
  --ntasks-per-node=1 \
  --cpus-per-task=144 \
  --gpus-per-task=4 \
  --gres=gpu:4 \
  --mpi=pmix_v4 \
  --container-remap-root \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --container-image=/mnt/data/$USER/images/presto/presto-native-worker-gpu.sqsh \
  --container-save=/mnt/data/$USER/images/presto/presto-native-worker-gpu-REBUILD.sqsh \
  /presto-build/build-presto.sh
```

## Troubleshooting

### Build Failures

**Check build logs:**
```bash
# Launch interactive container from failed stage
srun --export=ALL,PMIX_MCA_gds=^ds12 \
  --nodes=1 --mem=0 --ntasks-per-node=1 \
  --cpus-per-task=144 --gpus-per-task=4 --gres=gpu:4 \
  --mpi=pmix_v4 --container-remap-root \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --container-image=/mnt/data/$USER/images/presto/prestissimo-dependency-centos9.sqsh \
  --pty /bin/bash

# Inside container, try the build script
bash -x /presto-build/build-presto.sh
```

**Common issues:**

1. **Out of memory**: Reduce `NUM_THREADS`
2. **Build artifacts conflict**: Remove `/presto_native_*_build` directories

### Runtime Issues

**Check PATH and LD_LIBRARY_PATH:**
```bash
which presto_server
ldconfig -p | grep presto
cat /etc/ld.so.conf.d/presto_native.conf
cat /etc/ld.so.conf.d/libcuda.conf
```

## Build Times on NVL4 node with 144 threads
- Dependencies: 15-20 minutes
- Worker: 20-25 minutes
- Coordinator: 15-20 minutes

## Directory Structure

### Source Code (Host)
```
/mnt/data/$USER/src/velox-testing/presto/slurm/build/
├── presto/                       # Presto source code
│   ├── ...
│   ├── presto-native-execution/
│   │   └── velox/                # Velox source code
│   └── ...
└── ...
```

### Container Images
```
/mnt/data/$USER/images/presto/
├── prestissimo-dependency-centos9.sqsh    # Dependencies image
├── presto-native-worker-gpu.sqsh          # GPU worker image
├── presto-native-worker-cpu.sqsh          # CPU worker image (optional)
└── presto-coordinator.sqsh                # Coordinator image
```

### Inside Containers
```
/usr/bin/presto_server                     # Worker binary
/usr/lib64/presto-native-libs/             # Runtime libraries
/opt/presto-server/                        # Coordinator installation
/opt/presto-cli                            # Presto CLI
/presto-build/                             # Mounted scripts and source code
```

## Complete Example Workflow

```bash
# Step 1: Build dependencies image (once)
srun --export=ALL,PMIX_MCA_gds=^ds12 \
  --nodes=1 --mem=0 --ntasks-per-node=1 --cpus-per-task=144 \
  --gpus-per-task=4 --gres=gpu:4 --mpi=pmix_v4 --container-remap-root \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --container-image=quay.io/centos/centos:stream9 \
  --container-save=/mnt/data/$USER/images/presto/prestissimo-dependency-centos9.sqsh \
  /presto-build/build-deps-in-container.sh

# Step 2: Build worker image (after code changes)
srun --export=ALL,PMIX_MCA_gds=^ds12,NUM_THREADS=144,CUDA_ARCHITECTURES=100,PRESTO_DIR=/presto-build/presto/presto-native-execution \
  --nodes=1 --mem=0 --ntasks-per-node=1 --cpus-per-task=144 \
  --gpus-per-task=4 --gres=gpu:4 --mpi=pmix_v4 --container-remap-root \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --container-image=/mnt/data/$USER/images/presto/prestissimo-dependency-centos9.sqsh \
  --container-save=/mnt/data/$USER/images/presto/presto-native-worker-gpu.sqsh \
  /presto-build/build-presto.sh

# Step 3: Build coordinator image (once)
srun --export=ALL,PMIX_MCA_gds=^ds12,PRESTO_VERSION=testing,PRESTO_SOURCE_DIR=/presto-build/presto \
  --nodes=1 --mem=0 --ntasks-per-node=1 --cpus-per-task=144 \
  --mpi=pmix_v4 --container-remap-root \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --container-image=/mnt/data/$USER/images/presto/prestissimo-dependency-centos9.sqsh \
  --container-save=/mnt/data/$USER/images/presto/presto-coordinator.sqsh \
  /presto-build/setup-coordinator.sh
```

## Tips and Best Practices

1. **Use `--pty /bin/bash` for debugging**: Test builds interactively before using `--container-save`.

2. **Separate build and runtime images**: Keep worker image without source code for production.

3. **Use specific CUDA architectures**: Set `CUDA_ARCHITECTURES` to match your hardware for faster builds and smaller binaries.
