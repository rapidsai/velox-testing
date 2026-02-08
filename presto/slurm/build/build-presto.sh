#!/bin/bash
# Build script for Presto Native Worker
# Replicates all steps from native_build.dockerfile
# Run this inside a container with dependencies already built

set -e

# Configuration (adjust as needed)
BUILD_TYPE=${BUILD_TYPE:-release}
GPU=${GPU:-ON}
BUILD_BASE_DIR=${BUILD_BASE_DIR:-/presto_native_${BUILD_TYPE}_gpu_${GPU}_build}
BUILD_DIR=${BUILD_DIR:-""}
NUM_THREADS=${NUM_THREADS:-12}
CUDA_ARCHITECTURES=${CUDA_ARCHITECTURES:-"75;80;86;90;100;120"}
EXTRA_CMAKE_FLAGS=${EXTRA_CMAKE_FLAGS:-"-DPRESTO_ENABLE_TESTING=OFF -DPRESTO_ENABLE_PARQUET=ON -DPRESTO_ENABLE_CUDF=${GPU} -DVELOX_BUILD_TESTING=OFF"}
INSTALL_NSIGHT=${INSTALL_NSIGHT:-false}

# Work directory
PRESTO_DIR=${PRESTO_DIR:-/presto_native_staging/presto/presto-native-execution}

# Override /root/.local/share/uv/tools to workaround --container-remap-root
export UV_TOOL_DIR=/uv/tools
export UV_NO_CACHE=1
export UV_NO_CONFIG=1

export CUDACXX=/usr/local/cuda/bin/nvcc

echo "============================================"
echo "Building Presto Native Worker"
echo "============================================"
echo "PRESTO_DIR: $PRESTO_DIR"
echo "BUILD_TYPE: $BUILD_TYPE"
echo "GPU: $GPU"
echo "BUILD_BASE_DIR: $BUILD_BASE_DIR"
echo "NUM_THREADS: $NUM_THREADS"
echo "CUDA_ARCHITECTURES: $CUDA_ARCHITECTURES"
echo "EXTRA_CMAKE_FLAGS: $EXTRA_CMAKE_FLAGS"
echo "============================================"

# Verify presto directory exists
if [[ ! -f "$PRESTO_DIR/Makefile" ]]; then
    echo "ERROR: Presto Makefile not found at $PRESTO_DIR/Makefile"
    echo "Make sure you've copied or mounted the presto-native-execution sources"
    exit 1
fi

# Step 1: Install nsight-systems (optional, for profiling)
if [[ "$INSTALL_NSIGHT" == "true" ]]; then
    echo ""
    echo "============================================"
    echo "Installing nsight-systems..."
    echo "============================================"
    rpm --import https://developer.download.nvidia.com/compute/cuda/repos/ubuntu1804/x86_64/7fa2af80.pub || true
    source /etc/os-release
    dnf config-manager --add-repo "https://developer.download.nvidia.com/devtools/repos/rhel${VERSION_ID%%.*}/$(rpm --eval '%{_arch}' | sed s/aarch/arm/)/" 2>/dev/null || true
    dnf install -y nsight-systems-cli-2025.5.1 2>/dev/null || echo "Warning: nsight-systems installation failed, continuing..."
fi

# Step 2: Set up compiler environment
# For GPU builds, use gcc-toolset-14
if [[ "${GPU}" == "ON" ]]; then
    echo "GPU build detected, using gcc-toolset-14"
    export CC=/opt/rh/gcc-toolset-14/root/bin/gcc
    export CXX=/opt/rh/gcc-toolset-14/root/bin/g++
    source /opt/rh/gcc-toolset-14/enable
else
    echo "CPU build detected, using gcc-toolset-12"
    export CC=/opt/rh/gcc-toolset-12/root/bin/gcc
    export CXX=/opt/rh/gcc-toolset-12/root/bin/g++
    source /opt/rh/gcc-toolset-12/enable
fi

# Export build variables
export BUILD_TYPE
export BUILD_BASE_DIR
export BUILD_DIR
export NUM_THREADS
export CUDA_ARCHITECTURES
export EXTRA_CMAKE_FLAGS

# Add common library paths
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/lib64

# Step 3: Build Presto
echo ""
echo "============================================"
echo "Building Presto Native Execution..."
echo "============================================"
cd "$PRESTO_DIR"
make cmake-and-build BUILD_TYPE=${BUILD_TYPE} BUILD_DIR="${BUILD_DIR}" BUILD_BASE_DIR=${BUILD_BASE_DIR}

echo ""
echo "Build complete!"
echo "Binary location: ${BUILD_BASE_DIR}/${BUILD_DIR}/presto_cpp/main/presto_server"

# Step 4: Verify no missing libraries (fail if any found, except CUDA libs)
echo ""
echo "============================================"
echo "Verifying library dependencies..."
echo "============================================"
PRESTO_BINARY="${BUILD_BASE_DIR}/${BUILD_DIR}/presto_cpp/main/presto_server"

if ! LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib ldd "$PRESTO_BINARY" | grep "not found" | grep -v -E "libcuda\.so|libnvidia"; then
    echo "✓ All required libraries found!"
else
    echo "ERROR: Missing libraries detected!"
    echo "The following libraries are missing:"
    LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib ldd "$PRESTO_BINARY" | grep "not found" | grep -v -E "libcuda\.so|libnvidia"
    exit 1
fi

# Step 5: Collect runtime libraries
echo ""
echo "============================================"
echo "Collecting runtime libraries..."
echo "============================================"
mkdir -p /runtime-libraries
LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib ldd "$PRESTO_BINARY" | \
    awk 'NF == 4 && $3 != "not" && $1 !~ /libcuda\.so|libnvidia/ {
        cmd = "cp " $3 " /runtime-libraries/";
        print "Copying: " $3;
        system(cmd)
    }'

echo "Runtime libraries collected in /runtime-libraries"
ls -lh /runtime-libraries/ | tail -20

# Step 6: Install binary to /usr/bin
echo ""
echo "============================================"
echo "Installing presto_server..."
echo "============================================"
cp "$PRESTO_BINARY" /usr/bin/presto_server
chmod +x /usr/bin/presto_server
echo "✓ Installed to /usr/bin/presto_server"

# Step 7: Set up runtime library path
echo ""
echo "============================================"
echo "Setting up runtime library paths..."
echo "============================================"
mkdir -p /usr/lib64/presto-native-libs
cp /runtime-libraries/* /usr/lib64/presto-native-libs/

# Add to ld.so.conf if not already there
if ! grep -q "/usr/lib64/presto-native-libs" /etc/ld.so.conf.d/presto_native.conf 2>/dev/null; then
    echo "/usr/lib64/presto-native-libs" > /etc/ld.so.conf.d/presto_native.conf
    echo "✓ Added /usr/lib64/presto-native-libs to ld.so.conf"
fi

# Update library cache
ldconfig
echo "✓ Updated library cache"

# Step 8: Final verification
echo ""
echo "============================================"
echo "Final verification..."
echo "============================================"
if command -v presto_server &> /dev/null; then
    echo "✓ presto_server is in PATH"
    echo "  Location: $(which presto_server)"
else
    echo "ERROR: presto_server not found in PATH"
    exit 1
fi

# Verify it can load
if ldd /usr/bin/presto_server | grep "not found" | grep -v -E "libcuda\.so|libnvidia"; then
    echo "ERROR: presto_server has missing library dependencies!"
    exit 1
else
    echo "✓ All library dependencies satisfied"
fi

echo ""
echo "============================================"
echo "Build Complete!"
echo "============================================"
echo "  Binary: /usr/bin/presto_server"
echo "  Libraries: /usr/lib64/presto-native-libs/"
echo "  Build artifacts: ${BUILD_BASE_DIR}"
echo "============================================"
