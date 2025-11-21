# Dockerfile for Velox ASV Benchmarks with Cython Python Bindings
# This dockerfile builds Cython-based Python bindings for TPC-H benchmarks and runs ASV
#
# Build from the velox-adapters-build image which already has Velox compiled
# with CUDF support

FROM velox-adapters-build:latest

# Install Python development dependencies for Cython bindings
RUN dnf install -y \
    python3-devel \
    python3-pip \
    && dnf clean all

# Upgrade pip and install Python packages
RUN python3 -m pip install --upgrade pip setuptools wheel

# Install Cython, ASV, and other Python dependencies
RUN python3 -m pip install \
    'Cython>=0.29' \
    numpy \
    asv \
    && python3 -m pip cache purge

# Get build configuration from environment
ARG BUILD_BASE_DIR=/opt/velox-build
ARG BUILD_TYPE=release
ARG TPCH_DATA_PATH=/workspace/velox/velox-benchmark-data

# Set environment variables for runtime
ENV VELOX_ROOT=/workspace/velox \
    VELOX_BUILD_DIR=${BUILD_BASE_DIR}/${BUILD_TYPE} \
    BUILD_BASE_DIR=${BUILD_BASE_DIR} \
    BUILD_TYPE=${BUILD_TYPE} \
    TPCH_DATA_PATH=${TPCH_DATA_PATH} \
    PYTHONUNBUFFERED=1

# Update LD_LIBRARY_PATH to include Python bridge library location
ENV LD_LIBRARY_PATH=${BUILD_BASE_DIR}/${BUILD_TYPE}/velox/experimental/cudf/benchmarks/python/cpp/tpch:${BUILD_BASE_DIR}/${BUILD_TYPE}/lib:${BUILD_BASE_DIR}/${BUILD_TYPE}/_deps/cudf-build:${BUILD_BASE_DIR}/${BUILD_TYPE}/_deps/rmm-build:${BUILD_BASE_DIR}/${BUILD_TYPE}/_deps/rapids_logger-build:${BUILD_BASE_DIR}/${BUILD_TYPE}/_deps/kvikio-build:${BUILD_BASE_DIR}/${BUILD_TYPE}/_deps/nvcomp_proprietary_binary-src/lib64:${LD_LIBRARY_PATH}

# Set working directory
WORKDIR /workspace/velox

# Configure git to trust the repository (for ASV git operations)
RUN git config --global --add safe.directory /workspace/velox

# Build the Python bindings at image build time
# This runs the CMake build for the C++ bridge and then builds the Cython extensions
RUN --mount=type=bind,source=velox,target=/workspace/velox,ro \
    <<EOF
set -euxo pipefail

# Copy Python bindings source to a writable location
# We need this because build.sh builds "in-place" and the mounted source is read-only
echo "Copying Python bindings source to writable location..."
mkdir -p /tmp/python-bindings
cp -r /workspace/velox/velox/experimental/cudf/benchmarks/python/* /tmp/python-bindings/

# Build both C++ bridge and Cython bindings using the official build.sh script
echo "Building C++ bridge and Cython bindings using build.sh..."
cd /tmp/python-bindings

# Set environment variables for build.sh
export VELOX_ROOT=/workspace/velox
export VELOX_BUILD_DIR=${VELOX_BUILD_DIR}

# Run the official build script
bash build.sh

# Verify the C++ wrapper library was built
WRAPPER_LIB="${VELOX_BUILD_DIR}/velox/experimental/cudf/benchmarks/python/cpp/tpch/libpython_benchmark_bridge_tpch.so"
if [ ! -f "${WRAPPER_LIB}" ]; then
    echo "ERROR: Failed to build C++ wrapper library: ${WRAPPER_LIB}"
    exit 1
fi

# Verify .so files were created
if ! ls cudf_tpch_benchmark*.so 1> /dev/null 2>&1; then
    echo "ERROR: Failed to build Cython bindings - .so file not found"
    exit 1
fi

echo "✓ C++ bridge and Cython bindings built successfully"

# Install the Python package using pip
# Use --no-build-isolation because extensions are already built by build.sh
echo "Installing Python bindings package..."
pip3 install --no-build-isolation --no-deps .

# Verify installation
python3 -c "import cudf_tpch_benchmark; print('✓ Package installed successfully')" || {
    echo "ERROR: Failed to import installed package"
    exit 1
}

echo "✓ Python bindings installed to system Python"

EOF

# Copy and setup entrypoint script
COPY velox-testing/velox/asv_benchmarks/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Expose port for viewing results
EXPOSE 8080

# Set entrypoint
ENTRYPOINT ["/entrypoint.sh"]
