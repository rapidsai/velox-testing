#!/bin/bash
# Single script to build CentOS dependencies
# Run this INSIDE the container after mounting veloxtesting
#
# Usage (from host):
#   docker run -it --name deps-builder \
#     -v /raid/pentschev/src/veloxtesting:/veloxtesting:ro \
#     quay.io/centos/centos:stream9 bash
#
# Then inside container:
#   /veloxtesting/build-deps-in-container.sh

set -e

VELOXTESTING_DIR=${VELOXTESTING_DIR:-/veloxtesting}

echo "============================================"
echo "Building CentOS Dependencies"
echo "============================================"
echo "Veloxtesting mount: ${VELOXTESTING_DIR}"
echo "============================================"
echo ""

# Verify veloxtesting directory is mounted
if [[ ! -d "${VELOXTESTING_DIR}" ]]; then
    echo "ERROR: Veloxtesting directory not found at ${VELOXTESTING_DIR}"
    echo ""
    echo "Please mount it when starting the container:"
    echo "  docker run -it --name deps-builder \\"
    echo "    -v /raid/pentschev/src/veloxtesting:/veloxtesting:ro \\"
    echo "    quay.io/centos/centos:stream9 bash"
    echo ""
    echo "Or set VELOXTESTING_DIR to the correct mount point."
    exit 1
fi

# Verify required source directories exist
if [[ ! -d "${VELOXTESTING_DIR}/presto/presto-native-execution/scripts" ]]; then
    echo "ERROR: Presto scripts not found at ${VELOXTESTING_DIR}/presto/presto-native-execution/scripts"
    exit 1
fi

if [[ ! -d "${VELOXTESTING_DIR}/velox/scripts" ]]; then
    echo "ERROR: Velox scripts not found at ${VELOXTESTING_DIR}/velox/scripts"
    exit 1
fi

if [[ ! -f "${VELOXTESTING_DIR}/velox/CMake/resolve_dependency_modules/arrow/cmake-compatibility.patch" ]]; then
    echo "ERROR: CMake patch not found at ${VELOXTESTING_DIR}/velox/CMake/resolve_dependency_modules/arrow/cmake-compatibility.patch"
    exit 1
fi

echo "Step 1/4: Copying Presto scripts to /scripts..."
cp -r "${VELOXTESTING_DIR}/presto/presto-native-execution/scripts" /scripts

echo "Step 2/4: Copying Velox scripts to /velox/scripts..."
mkdir -p /velox
cp -r "${VELOXTESTING_DIR}/velox/scripts" /velox/scripts

echo "Step 3/4: Copying Velox CMake patch to /velox/cmake-compatibility.patch..."
cp "${VELOXTESTING_DIR}/velox/CMake/resolve_dependency_modules/arrow/cmake-compatibility.patch" \
   /velox/cmake-compatibility.patch

echo "Step 4/4: Running dependency build..."
echo "============================================"
echo "This will take 30-60 minutes..."
echo "============================================"
echo ""

# Run the build script directly
"${VELOXTESTING_DIR}/scripts/build-centos-deps.sh"

echo ""
echo "============================================"
echo "Build Complete!"
echo "============================================"
echo ""
echo "To commit this container to an image, run from the host:"
echo "  docker commit <container-id> presto/prestissimo-dependency:centos9"
echo ""
echo "To verify the image:"
echo "  docker images | grep prestissimo-dependency"
echo "============================================"
