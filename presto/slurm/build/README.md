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
  --container-image=quay.io/centos/centos:stream9 \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --container-save=/mnt/data/$USER/images/presto/prestissimo-dependency-centos9.sqsh \
  /presto-build/scripts/build-deps-in-container.sh

# Stage 2: Build worker (20-40 minutes)
srun --export=ALL,PMIX_MCA_gds=^ds12,NUM_THREADS=144,CUDA_ARCHITECTURES=100,PRESTO_DIR=/presto-build/presto/presto-native-execution \
  --nodes=1 --mem=0 --ntasks-per-node=1 \
  --cpus-per-task=144 --gpus-per-task=4 --gres=gpu:4 \
  --mpi=pmix_v4 --container-remap-root \
  --container-image=/mnt/data/$USER/images/presto/prestissimo-dependency-centos9.sqsh \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --container-save=/mnt/data/$USER/images/presto/presto-native-worker-gpu.sqsh \
  /presto-build/scripts/build-presto.sh

# Stage 3: Build coordinator (15-20 minutes)
srun --export=ALL,PMIX_MCA_gds=^ds12,PRESTO_VERSION=testing,PRESTO_SOURCE_DIR=/presto-build/presto \
  --nodes=1 --mem=0 --ntasks-per-node=1 \
  --cpus-per-task=144 \
  --mpi=pmix_v4 --container-remap-root \
  --container-image=/mnt/data/$USER/images/presto/prestissimo-dependency-centos9.sqsh \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --container-save=/mnt/data/$USER/images/presto/presto-coordinator.sqsh \
  /presto-build/scripts/setup-coordinator.sh
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
  --container-image=quay.io/centos/centos:stream9 \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --container-save=/mnt/data/$USER/images/presto/prestissimo-dependency-centos9.sqsh \
  /presto-build/scripts/build-deps-in-container.sh
```

**What it does:**
- Installs build tools (gcc-toolset-12, gcc-toolset-14, cmake, clang15)
- Builds Velox dependencies (folly, boost, arrow, etc.)
- Installs CUDA 12.8
- Builds UCX 1.19.0 with CUDA support
- Installs storage adapters (S3, GCS, Azure, HDFS)

**Output:**
- Image: `/mnt/data/$USER/images/presto/prestissimo-dependency-centos9.sqsh`
- Time: ~30-60 minutes
- Size: ~5-8 GB

**Environment variables (optional):**
```bash
--export=ALL,PMIX_MCA_gds=^ds12,ARM_BUILD_TARGET=local
```

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
  --container-image=/mnt/data/$USER/images/presto/prestissimo-dependency-centos9.sqsh \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --container-save=/mnt/data/$USER/images/presto/presto-native-worker-gpu.sqsh \
  /presto-build/scripts/build-presto.sh
```

**What it does:**
- Builds Presto native execution with GPU support
- Collects runtime libraries
- Installs to `/usr/bin/presto_server`
- Sets up library paths in `/usr/lib64/presto-native-libs/`
- Configures `ld.so.conf`

**Output:**
- Image: `/mnt/data/$USER/images/presto/presto-native-worker-gpu.sqsh`
- Time: ~20-40 minutes
- Size: ~6-9 GB

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
  --container-image=/mnt/data/$USER/images/presto/prestissimo-dependency-centos9.sqsh \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --container-save=/mnt/data/$USER/images/presto/presto-native-worker-cpu.sqsh \
  /presto-build/scripts/build-presto.sh
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
  --container-image=/mnt/data/$USER/images/presto/prestissimo-dependency-centos9.sqsh \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --container-save=/mnt/data/$USER/images/presto/presto-coordinator.sqsh \
  /presto-build/scripts/setup-coordinator.sh
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
  --container-image=quay.io/centos/centos:stream9 \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --container-save=/mnt/data/$USER/images/presto/presto-coordinator.sqsh \
  /presto-build/scripts/setup-coordinator.sh
```

**What it does:**
- Installs Java 17 and Maven
- Builds Presto Java package
- Extracts Presto server to `/opt/presto-server`
- Installs Presto CLI to `/opt/presto-cli`
- Sets up configuration files

**Output:**
- Image: `/mnt/data/$USER/images/presto/presto-coordinator.sqsh`
- Time: ~30-60 minutes (first build), ~5 minutes (if package pre-built)
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

If you only changed Presto C++ code (not dependencies):

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
  --container-image=/mnt/data/$USER/images/presto/prestissimo-dependency-centos9.sqsh \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --pty /bin/bash

# Inside container, build:
bash /presto-build/scripts/build-presto.sh

# If successful, save the image from outside:
# (Use --container-save in the srun command above)
```

### Rebuilding Only UCX

If you need to add UCX to an existing dependencies image:

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
  --container-image=/mnt/data/$USER/images/presto/prestissimo-dependency-centos9.sqsh \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --container-save=/mnt/data/$USER/images/presto/prestissimo-dependency-centos9-with-ucx.sqsh \
  /presto-build/install-ucx.sh
```

## Running the Images

### Interactive Shell

```bash
# Worker image
srun --export=ALL,PMIX_MCA_gds=^ds12 \
  --nodes=1 \
  --mem=0 \
  --ntasks-per-node=1 \
  --cpus-per-task=144 \
  --gpus-per-task=4 \
  --gres=gpu:4 \
  --mpi=pmix_v4 \
  --container-remap-root \
  --container-image=/mnt/data/$USER/images/presto/presto-native-worker-gpu.sqsh \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --pty /bin/bash

# Coordinator image
srun --export=ALL \
  --nodes=1 \
  --ntasks-per-node=1 \
  --cpus-per-task=16 \
  --mpi=pmix_v4 \
  --container-remap-root \
  --container-image=/mnt/data/$USER/images/presto/presto-coordinator.sqsh \
  --pty /bin/bash
```

### Running Presto Services

**Start Coordinator:**
```bash
srun --export=ALL \
  --nodes=1 \
  --ntasks-per-node=1 \
  --cpus-per-task=16 \
  --mpi=pmix_v4 \
  --container-remap-root \
  --container-image=/mnt/data/$USER/images/presto/presto-coordinator.sqsh \
  --container-mounts=/mnt/data/$USER/presto-config:/config,/mnt/data/$USER/presto-data:/var/lib/presto/data \
  /opt/presto-server/bin/launcher run
```

**Start Worker:**
```bash
srun --export=ALL,GLOG_logtostderr=1 \
  --nodes=1 \
  --ntasks-per-node=1 \
  --cpus-per-task=144 \
  --gpus-per-task=4 \
  --gres=gpu:4 \
  --mpi=pmix_v4 \
  --container-remap-root \
  --container-image=/mnt/data/$USER/images/presto/presto-native-worker-gpu.sqsh \
  --container-mounts=/mnt/data/$USER/worker-config:/config \
  /usr/bin/presto_server --etc-dir=/config
```

## Configuration

### Worker Configuration

Create a config directory (e.g., `/mnt/data/$USER/worker-config/`) with:

**config.properties:**
```properties
coordinator=false
http-server.http.port=8080
discovery.uri=http://presto-coordinator:8080
```

**node.properties:**
```properties
node.environment=production
node.id=worker-1
node.data-dir=/var/lib/presto/data
```

### Coordinator Configuration

Create a config directory (e.g., `/mnt/data/$USER/presto-config/`) with files from `/opt/presto-server/etc/` in the coordinator image, or copy from `velox-testing/presto/docker/etc/`.

## Troubleshooting

### Build Failures

**Check build logs:**
```bash
# Launch interactive container from failed stage
srun --export=ALL,PMIX_MCA_gds=^ds12 \
  --nodes=1 --mem=0 --ntasks-per-node=1 \
  --cpus-per-task=144 --gpus-per-task=4 --gres=gpu:4 \
  --mpi=pmix_v4 --container-remap-root \
  --container-image=/mnt/data/$USER/images/presto/prestissimo-dependency-centos9.sqsh \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --pty /bin/bash

# Inside container, try the build script
bash -x /presto-build/scripts/build-presto.sh
```

**Common issues:**

1. **cmake not found**: Fixed in latest scripts (PATH issue)
2. **UCX not found**: Latest scripts install UCX after CUDA
3. **Out of memory**: Reduce `NUM_THREADS` or increase `--cpus-per-task`
4. **Build artifacts conflict**: Remove `/presto_native_*_build` directories

### Runtime Issues

**Check library dependencies:**
```bash
ldd /usr/bin/presto_server | grep "not found"
# Note: libcuda.so and libnvidia* are expected to be "not found" (provided by driver)
```

**Check PATH and LD_LIBRARY_PATH:**
```bash
which presto_server
ldconfig -p | grep presto
cat /etc/ld.so.conf.d/presto_native.conf
```

## Resource Recommendations

### Minimum Resources
- **Dependencies**: 64 CPUs, 128GB RAM, 4 GPUs (for CUDA install)
- **Worker**: 64 CPUs, 128GB RAM, 1 GPU
- **Coordinator**: 16 CPUs, 32GB RAM, no GPU

### Recommended Resources (for faster builds)
- **Dependencies**: 144 CPUs, 256GB RAM, 4 GPUs
- **Worker**: 144 CPUs, 256GB RAM, 4 GPUs
- **Coordinator**: 64 CPUs, 128GB RAM

### Build Times (with recommended resources)
- Dependencies: 30-45 minutes
- Worker: 15-25 minutes
- Coordinator: 30-45 minutes (first build), 5-10 minutes (incremental)

## Directory Structure

### Source Code (Host)
```
/mnt/data/$USER/src/velox-testing/presto/slurm/build/
├── ./                            # Presto source code
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
/veloxtesting/                             # Mounted source code
```

## Complete Example Workflow

```bash
# Step 1: Build dependencies image (once)
srun --export=ALL,PMIX_MCA_gds=^ds12 \
  --nodes=1 --mem=0 --ntasks-per-node=1 --cpus-per-task=144 \
  --gpus-per-task=4 --gres=gpu:4 --mpi=pmix_v4 --container-remap-root \
  --container-image=quay.io/centos/centos:stream9 \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --container-save=/mnt/data/$USER/images/presto/prestissimo-dependency-centos9.sqsh \
  /presto-build/scripts/build-deps-in-container.sh

# Step 2: Build worker image (after code changes)
srun --export=ALL,PMIX_MCA_gds=^ds12,NUM_THREADS=144,CUDA_ARCHITECTURES=100,PRESTO_DIR=/presto-build/presto/presto-native-execution \
  --nodes=1 --mem=0 --ntasks-per-node=1 --cpus-per-task=144 \
  --gpus-per-task=4 --gres=gpu:4 --mpi=pmix_v4 --container-remap-root \
  --container-image=/mnt/data/$USER/images/presto/prestissimo-dependency-centos9.sqsh \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --container-save=/mnt/data/$USER/images/presto/presto-native-worker-gpu.sqsh \
  /presto-build/scripts/build-presto.sh

# Step 3: Build coordinator image (once)
srun --export=ALL,PMIX_MCA_gds=^ds12,PRESTO_VERSION=testing,PRESTO_SOURCE_DIR=/presto-build/presto \
  --nodes=1 --mem=0 --ntasks-per-node=1 --cpus-per-task=144 \
  --mpi=pmix_v4 --container-remap-root \
  --container-image=/mnt/data/$USER/images/presto/prestissimo-dependency-centos9.sqsh \
  --container-mounts=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build \
  --container-save=/mnt/data/$USER/images/presto/presto-coordinator.sqsh \
  /presto-build/scripts/setup-coordinator.sh

# Step 4: Run coordinator
srun --export=ALL \
  --nodes=1 --ntasks-per-node=1 --cpus-per-task=16 \
  --mpi=pmix_v4 --container-remap-root \
  --container-image=/mnt/data/$USER/images/presto/presto-coordinator.sqsh \
  --container-mounts=/mnt/data/$USER/configs:/configs \
  /opt/presto-server/bin/launcher run &

# Step 5: Run worker(s)
srun --export=ALL,GLOG_logtostderr=1 \
  --nodes=1 --ntasks-per-node=1 --cpus-per-task=144 \
  --gpus-per-task=4 --gres=gpu:4 \
  --mpi=pmix_v4 --container-remap-root \
  --container-image=/mnt/data/$USER/images/presto/presto-native-worker-gpu.sqsh \
  --container-mounts=/mnt/data/$USER/configs:/configs \
  /usr/bin/presto_server --etc-dir=/configs/worker &
```

## Tips and Best Practices

1. **Use `--pty /bin/bash` for debugging**: Test builds interactively before using `--container-save`

2. **Mount source code read-only in production**: Add `:ro` to container-mounts for safety

3. **Separate build and runtime images**: Keep worker image without source code for production

4. **Version your images**: Include commit hash or date in image names

5. **Cache Maven artifacts**: Mount `~/.m2` for faster coordinator rebuilds

6. **Use specific CUDA architectures**: Set `CUDA_ARCHITECTURES` to match your hardware for faster builds and smaller binaries

7. **Monitor build resources**: Use `sstat` or `sacct` to check resource usage
