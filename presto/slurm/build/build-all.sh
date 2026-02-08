#!/bin/bash
# Master build script to set up complete Presto environment
# Run this inside a CentOS Stream 9 container
#
# This replicates the full Docker-based build but runs entirely in bash

set -e

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")

# Configuration
BUILD_DEPS=${BUILD_DEPS:-true}
BUILD_WORKER=${BUILD_WORKER:-true}
BUILD_COORDINATOR=${BUILD_COORDINATOR:-true}
SKIP_UCX=${SKIP_UCX:-false}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-deps)
            BUILD_DEPS=false
            shift
            ;;
        --skip-worker)
            BUILD_WORKER=false
            shift
            ;;
        --skip-coordinator)
            BUILD_COORDINATOR=false
            shift
            ;;
        --skip-ucx)
            SKIP_UCX=true
            shift
            ;;
        --deps-only)
            BUILD_DEPS=true
            BUILD_WORKER=false
            BUILD_COORDINATOR=false
            shift
            ;;
        --worker-only)
            BUILD_DEPS=false
            BUILD_WORKER=true
            BUILD_COORDINATOR=false
            shift
            ;;
        --coordinator-only)
            BUILD_DEPS=false
            BUILD_WORKER=false
            BUILD_COORDINATOR=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --skip-deps          Skip building dependencies"
            echo "  --skip-worker        Skip building native worker"
            echo "  --skip-coordinator   Skip building coordinator"
            echo "  --skip-ucx           Skip UCX installation"
            echo "  --deps-only          Only build dependencies"
            echo "  --worker-only        Only build worker"
            echo "  --coordinator-only   Only build coordinator"
            echo "  -h, --help           Show this help message"
            echo ""
            echo "Environment variables:"
            echo "  NUM_THREADS          Number of build threads (default: 144)"
            echo "  CUDA_ARCHITECTURES   CUDA architectures to build for (default: 75;80;86;90;100;120)"
            echo "  PRESTO_VERSION       Presto version for coordinator (default: testing)"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
    esac
done

echo "============================================"
echo "Presto Complete Build Script"
echo "============================================"
echo "Build Configuration:"
echo "  Dependencies: $BUILD_DEPS"
echo "  Native Worker: $BUILD_WORKER"
echo "  Coordinator: $BUILD_COORDINATOR"
echo "  Skip UCX: $SKIP_UCX"
echo "============================================"

# Step 1: Build dependencies
if [[ "$BUILD_DEPS" == "true" ]]; then
    echo ""
    echo "============================================"
    echo "STAGE 1: Building Dependencies"
    echo "============================================"

    if [[ -f "${SCRIPT_DIR}/build-deps-in-container.sh" ]]; then
        bash "${SCRIPT_DIR}/build-deps-in-container.sh"
    else
        echo "ERROR: build-deps-in-container.sh not found"
        echo "Expected at: ${SCRIPT_DIR}/build-deps-in-container.sh"
        exit 1
    fi

    echo ""
    echo "✓ Dependencies build complete"
fi

# Step 1.5: Install UCX if needed (for cuDF/UCXX)
if [[ "$BUILD_WORKER" == "true" && "$SKIP_UCX" == "false" ]]; then
    # Check if UCX is already installed
    if ! ldconfig -p | grep -q libucx; then
        echo ""
        echo "============================================"
        echo "STAGE 1.5: Installing UCX"
        echo "============================================"

        if [[ -f "${SCRIPT_DIR}/../install-ucx.sh" ]]; then
            bash "${SCRIPT_DIR}/../install-ucx.sh"
        elif [[ -f "/veloxtesting/install-ucx.sh" ]]; then
            bash /veloxtesting/install-ucx.sh
        else
            echo "WARNING: UCX install script not found, skipping..."
            echo "You may need to install UCX manually if the worker build fails"
        fi

        echo ""
        echo "✓ UCX installation complete"
    else
        echo "✓ UCX already installed, skipping"
    fi
fi

# Step 2: Build native worker
if [[ "$BUILD_WORKER" == "true" ]]; then
    echo ""
    echo "============================================"
    echo "STAGE 2: Building Native Worker"
    echo "============================================"

    if [[ -f "${SCRIPT_DIR}/build-presto.sh" ]]; then
        bash "${SCRIPT_DIR}/build-presto.sh"
    else
        echo "ERROR: build-presto.sh not found"
        echo "Expected at: ${SCRIPT_DIR}/build-presto.sh"
        exit 1
    fi

    echo ""
    echo "✓ Native worker build complete"
fi

# Step 3: Build/setup coordinator
if [[ "$BUILD_COORDINATOR" == "true" ]]; then
    echo ""
    echo "============================================"
    echo "STAGE 3: Setting Up Coordinator"
    echo "============================================"

    if [[ -f "${SCRIPT_DIR}/setup-coordinator.sh" ]]; then
        bash "${SCRIPT_DIR}/setup-coordinator.sh"
    else
        echo "ERROR: setup-coordinator.sh not found"
        echo "Expected at: ${SCRIPT_DIR}/setup-coordinator.sh"
        exit 1
    fi

    echo ""
    echo "✓ Coordinator setup complete"
fi

# Summary
echo ""
echo "============================================"
echo "BUILD COMPLETE!"
echo "============================================"
echo ""

if [[ "$BUILD_WORKER" == "true" ]]; then
    echo "Native Worker:"
    echo "  Binary: /usr/bin/presto_server"
    echo "  Libraries: /usr/lib64/presto-native-libs/"
    if command -v presto_server &> /dev/null; then
        echo "  Status: ✓ Installed"
    else
        echo "  Status: ✗ Not found in PATH"
    fi
    echo ""
fi

if [[ "$BUILD_COORDINATOR" == "true" ]]; then
    echo "Coordinator:"
    echo "  Home: ${PRESTO_HOME:-/opt/presto-server}"
    echo "  CLI: /opt/presto-cli"
    if [[ -f "/opt/presto-cli" ]]; then
        echo "  Status: ✓ Installed"
    else
        echo "  Status: ✗ Not found"
    fi
    echo ""
fi

echo "Next Steps:"
echo "  1. Configure Presto (edit configs in /opt/presto-server/etc/)"
echo "  2. Start coordinator: /opt/presto-server/bin/launcher start"
echo "  3. Start worker: presto_server --etc-dir=/path/to/worker/config"
echo ""
echo "============================================"
