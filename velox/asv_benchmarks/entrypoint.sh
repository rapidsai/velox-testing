#!/bin/bash
set -euo pipefail

echo "=============================================="
echo "   Velox ASV Benchmark Container"
echo "=============================================="
echo "Velox Root:      ${VELOX_ROOT}"
echo "Build directory: ${VELOX_BUILD_DIR}"
echo "TPC-H data path: ${TPCH_DATA_PATH}"
echo "=============================================="

# Configure Git to trust the mounted repository directories
# This is needed when the repository is owned by a different user (host vs container)
git config --global --add safe.directory /workspace/velox/.git
git config --global --add safe.directory /workspace/velox-testing/.git
echo "✓ Git safe.directory configured for mounted repositories"

# Publish existing benchmark results if available, and start preview server if requested

RESULTS_DIR="/asv_results"
HTML_DIR="${RESULTS_DIR}/html"

# Only run if results exist
if [ "${ASV_PUBLISH_PREVIEW_EXISTING:-false}" = "true" ]; then
        echo ""
        echo "=============================================="
        echo "   Publishing Existing Benchmark Results"
        echo "=============================================="
        asv publish || {
            echo "WARNING: asv publish failed"
            exit 1
        }
        if [ -d "${HTML_DIR}" ] && [ "$(ls -A ${HTML_DIR})" ]; then
            echo "✓ HTML reports generated at: ${HTML_DIR}"
        else
            echo "WARNING: No HTML reports found in ${HTML_DIR}"
            exit 1
        fi

        echo ""
        echo "=============================================="
        echo "   Starting ASV Preview Server"
        echo "=============================================="
        echo "Access benchmark results at: http://localhost:${ASV_PORT:-8080}"
        echo "Results directory: ${HTML_DIR}"
        echo "Press Ctrl+C to stop the server"
        echo ""
        exec asv preview --port "${ASV_PORT:-8080}"
        # No further execution beyond here if ASV_PUBLISH_PREVIEW_EXISTING is true
        exit 0
fi

if [ "${ASV_PREVIEW_EXISTING:-false}" = "true" ]; then
    echo ""
    echo "=============================================="
    echo "   Starting ASV Preview Server"
    echo "=============================================="
    echo "Access benchmark results at: http://localhost:${ASV_PORT:-8080}"
    echo "Results directory: ${HTML_DIR}"
    echo "Press Ctrl+C to stop the server"
    echo ""
    exec asv preview --port "${ASV_PORT:-8080}"
    exit 0
fi

# Verify Python bindings are installed (skip for virtualenv - ASV will handle it)
if [ "${ASV_ENV_TYPE:-existing}" = "existing" ]; then
    echo ""
    echo "=== Verifying Python bindings ==="
    python3 -c "import cudf_tpch_benchmark; print('✓ cudf_tpch_benchmark module loaded successfully')" || {
        echo "ERROR: Failed to import cudf_tpch_benchmark module"
        echo ""
        echo "The package should have been installed during image build."
        echo "Try rebuilding the image: ./build_asv_image.sh --rebuild"
        echo ""
        echo "Debugging information:"
        echo "LD_LIBRARY_PATH: ${LD_LIBRARY_PATH:-<not set>}"
        python3 -c "import sys; print('Python path:', sys.path)"
        exit 1
    }
else
    echo ""
    echo "=== Virtualenv Mode Detected ==="
    echo "Python bindings will be built and installed by ASV in each virtualenv"
    echo "Skipping host-level verification..."
fi

# Run smoke test if data is available (can be disabled with ASV_SKIP_SMOKE_TEST=true)
if [ "${ASV_SKIP_SMOKE_TEST:-false}" = "false" ] && [ -d "${TPCH_DATA_PATH}" ] && [ "$(ls -A ${TPCH_DATA_PATH} 2>/dev/null)" ]; then
    echo ""
    echo "=== Running smoke test ==="
    echo "Testing with Query 6 (fast query)..."
    echo "(Set ASV_SKIP_SMOKE_TEST=true to skip this)"
    
    # Save current directory
    CURRENT_DIR=$(pwd)
    
    # Run example script with timeout
    cd /workspace/velox/velox/experimental/cudf/benchmarks/python/examples
    if timeout 30 python3 example_usage.py --data-path "${TPCH_DATA_PATH}" --query 6 > /tmp/smoke_test.log 2>&1; then
        echo "✓ Smoke test passed - Query execution successful"
        echo "  $(grep 'Execution Time:' /tmp/smoke_test.log || echo 'Query completed')"
    else
        EXIT_CODE=$?
        echo "⚠ Smoke test failed (exit code: $EXIT_CODE)"
        echo ""
        echo "This might indicate:"
        echo "  - Data format issues"
        echo "  - Missing TPC-H tables"
        echo "  - GPU/CUDA problems"
        echo ""
        echo "Smoke test output (last 20 lines):"
        tail -20 /tmp/smoke_test.log
        exit 1
    fi
    
    # Return to original directory
    cd "$CURRENT_DIR"
    rm -f /tmp/smoke_test.log
fi

# Check if we should skip auto-run (for interactive mode)
if [ "${ASV_SKIP_AUTORUN:-false}" = "true" ]; then
    echo ""
    echo "=============================================="
    echo "   Interactive Mode - Skipping Auto-Run"
    echo "=============================================="
    echo ""
    echo "Container is ready! You can:"
    echo "  - Run all benchmarks: asv run --show-stderr"
    echo "  - Run specific: asv run --show-stderr --bench tpch_benchmarks.TimeQuery06"
    echo "  - View results: asv publish && asv preview --port ${ASV_PORT:-8080}"
    echo "  - Check data: ls ${TPCH_DATA_PATH}"
    echo "  - Clear results: rm -rf /asv_results/*"
    echo ""
    echo "Results are saved to: /asv_results (mounted from host)"
    echo ""
    exit 0
fi

# Check if TPC-H data exists
if [ ! -d "${TPCH_DATA_PATH}" ]; then
    echo ""
    echo "=============================================="
    echo "WARNING: TPC-H data path does not exist: ${TPCH_DATA_PATH}"
    echo "=============================================="
    echo ""
    echo "Please mount TPC-H data at ${TPCH_DATA_PATH}"
    echo ""
    echo "Example:"
    echo "  docker run -v /path/to/tpch/data:${TPCH_DATA_PATH}:ro ..."
    echo ""
    echo "Container is ready but waiting for data..."
    echo "Starting interactive shell. You can:"
    echo "  - Mount data and restart the container"
    echo "  - Run ASV manually: cd /workspace/velox-testing/velox/asv_benchmarks && asv run"
    echo "  - Run examples: cd ${VELOX_ROOT}/velox/experimental/cudf/benchmarks/python/examples"
    echo ""
    # Keep container running for manual interaction
    exec /bin/bash
fi

# Navigate to ASV benchmarks directory
cd /workspace/velox-testing/velox/asv_benchmarks

# Verify we're in the right place
if [ ! -f "asv.conf.json" ]; then
    echo "ERROR: asv.conf.json not found in $(pwd)"
    exit 1
fi

# Set machine name for ASV (prevents interactive prompts)
# If ASV_AUTO_MACHINE is true, generate unique name with timestamp
if [ "${ASV_AUTO_MACHINE:-false}" = "true" ]; then
    TIMESTAMP=$(date +%s)
    MACHINE_NAME="docker-run-${TIMESTAMP}"
    echo "Auto-generated machine name: ${MACHINE_NAME}"
else
    MACHINE_NAME="${ASV_MACHINE:-docker-container}"
fi

# Setup ASV machine configuration
echo "Setting up ASV machine configuration..."

if [ -f "machine.json" ]; then
    echo "Configuring ASV machine from machine.json..."
    
    # Update machine name in machine.json if auto-generated
    if [ "${ASV_AUTO_MACHINE:-false}" = "true" ]; then
        echo "Updating machine.json with new machine name: ${MACHINE_NAME}"
        # Create a temporary file with updated machine name
        python3 << PYEOF
import json
with open('machine.json', 'r') as f:
    data = json.load(f)
# Update the machine field with the new name
data['machine'] = '${MACHINE_NAME}'
with open('/tmp/machine.json', 'w') as f:
    json.dump(data, f, indent=4)
PYEOF
        cp /tmp/machine.json ~/.asv-machine.json
    else
        # Use original machine.json
        cp machine.json ~/.asv-machine.json
    fi
    
    # Create results directory if it doesn't exist
    mkdir -p /asv_results
    
    # Create machine-specific results directory and copy machine info
    # This is where ASV looks for machine configuration
    mkdir -p "/asv_results/${MACHINE_NAME}"
    cp ~/.asv-machine.json "/asv_results/${MACHINE_NAME}/machine.json"
    
    # Also copy to root results directory (needed for asv publish)
    cp ~/.asv-machine.json /asv_results/machine.json
    
    # Register machine with ASV using command-line arguments
    echo "Registering machine with ASV..."
    asv machine --yes \
        --machine "${MACHINE_NAME}" \
        --os "Linux-Generic-64bit" \
        --arch "x86_64" \
        --cpu "Intel Xeon/AWS" \
        --ram "32GB" 2>/dev/null || {
        echo "Note: Machine may already be registered"
    }
    
    echo "✓ Machine configuration ready (machine: ${MACHINE_NAME})"
    echo "  Machine info written to: /asv_results/${MACHINE_NAME}/machine.json"
else
    echo "WARNING: machine.json not found, ASV may prompt for machine info"
fi

# Run ASV benchmarks
echo ""
echo "=============================================="
echo "   Running ASV Benchmarks"
echo "=============================================="
echo "Working directory: $(pwd)"
echo "Machine name: ${MACHINE_NAME}"
echo "LD_LIBRARY_PATH: ${LD_LIBRARY_PATH:-<not set>}"
echo ""

# Create results directory if it doesn't exist
mkdir -p /asv_results

# Discover benchmarks first (creates benchmarks.json)
if [ ! -f "/asv_results/benchmarks.json" ]; then
    echo "Discovering benchmarks..."
    asv run --show-stderr --machine "${MACHINE_NAME}" --bench just-discover || {
        echo "Warning: Benchmark discovery may have failed"
    }
fi

# Fix ownership to match host user (if USER_ID/GROUP_ID are set)
if [ -n "${USER_ID:-}" ] && [ -n "${GROUP_ID:-}" ]; then
    echo "Setting ownership of /asv_results to ${USER_ID}:${GROUP_ID}..."
    chown -R "${USER_ID}:${GROUP_ID}" /asv_results || {
        echo "Warning: Could not change ownership, continuing anyway..."
    }
fi

# Clear previous results if requested (for fresh run)
if [ "${ASV_CLEAR_RESULTS:-false}" = "true" ]; then
    echo "Clearing previous benchmark results..."
    # Preserve machine.json when clearing
    if [ -f "/asv_results/machine.json" ]; then
        cp /asv_results/machine.json /tmp/machine.json.backup
    fi
    rm -rf /asv_results/*
    if [ -f "/tmp/machine.json.backup" ]; then
        cp /tmp/machine.json.backup /asv_results/machine.json
        rm /tmp/machine.json.backup
    fi
    echo "✓ Results cleared (machine.json preserved)"
    echo ""
else
    echo "Keeping previous results (set ASV_CLEAR_RESULTS=true to clear)"
    echo ""
fi

# Run all benchmarks (or specific ones if provided via env var)
# Note: With environment_type: "existing", we can only benchmark the current state
# Set ASV_SKIP_EXISTING=false to re-run benchmarks (useful for variability testing)
SKIP_EXISTING_FLAG=""
if [ "${ASV_SKIP_EXISTING:-true}" = "true" ]; then
    SKIP_EXISTING_FLAG="--skip-existing"
    echo "Using --skip-existing (set ASV_SKIP_EXISTING=false to force re-run)"
else
    echo "Force re-running benchmarks (will show run-to-run variability)"
fi

# Determine commit range to benchmark
# Default behavior depends on environment type:
# - virtualenv: can benchmark commit ranges (HEAD, HEAD~5..HEAD, v1.0..v2.0, etc.)
# - existing: only benchmarks current state (no commit checkout)
COMMIT_RANGE="${ASV_COMMIT_RANGE:-}"
if [ -z "$COMMIT_RANGE" ]; then
    if [ "${ASV_ENV_TYPE:-existing}" = "existing" ]; then
        # For existing environment, we can't checkout commits, so we benchmark current state
        # Use HEAD^! to specify single commit (current state)
        COMMIT_RANGE="HEAD^!"
        echo "Environment type: existing (benchmarking current state only)"
    else
        # For virtualenv, default to HEAD (single most recent commit)
        COMMIT_RANGE="${ASV_COMMIT_RANGE:-HEAD^!}"
        echo "Environment type: virtualenv (can benchmark commit ranges)"
    fi
fi

echo "Commit range: ${COMMIT_RANGE}"

ASV_RUN_CMD="asv run ${SKIP_EXISTING_FLAG} --show-stderr --machine ${MACHINE_NAME} ${COMMIT_RANGE}"


# Add --record-samples or --append-samples flag if requested
if [ "${ASV_APPEND_SAMPLES:-false}" = "true" ]; then
    ASV_RUN_CMD="${ASV_RUN_CMD} --append-samples"
    echo "Appending samples (combining with previous measurements)"
elif [ "${ASV_RECORD_SAMPLES:-false}" = "true" ]; then
    ASV_RUN_CMD="${ASV_RUN_CMD} --record-samples"
    echo "Recording samples (captures variance for statistical analysis)"
fi

if [ -n "${ASV_BENCH:-}" ]; then
    echo "Running specific benchmark: ${ASV_BENCH}"
    ${ASV_RUN_CMD} --bench "${ASV_BENCH}" || {
        echo "WARNING: Benchmark ${ASV_BENCH} may have failed"
    }
else
    echo "Running all benchmarks..."
    ${ASV_RUN_CMD} || {
        echo "WARNING: Some benchmarks may have failed"
    }
fi

# Generate HTML reports
echo ""
echo "=============================================="
echo "   Generating HTML Reports"
echo "=============================================="
if [ "${ASV_PUBLISH:-true}" = "true" ]; then
    asv publish
else
    echo "Skipping HTML reports generation (set ASV_PUBLISH=true to generate)"
    exit 0
fi

# Check if HTML was generated
if [ -d "/asv_results/html" ] && [ "$(ls -A /asv_results/html)" ]; then
    echo ""
    echo "✓ HTML reports generated successfully"
else
    echo ""
    echo "WARNING: HTML reports may not have been generated properly"
fi

# Fix final ownership of all results
if [ -n "${USER_ID:-}" ] && [ -n "${GROUP_ID:-}" ]; then
    echo ""
    echo "Fixing ownership of results..."
    chown -R "${USER_ID}:${GROUP_ID}" /asv_results 2>/dev/null || true
    echo "✓ Ownership set to ${USER_ID}:${GROUP_ID}"
fi

# Start ASV preview server to view results (if enabled)
if [ "${ASV_PREVIEW:-true}" = "true" ]; then
    echo ""
    echo "=============================================="
    echo "   Starting ASV Preview Server"
    echo "=============================================="
    echo ""
    echo "Access benchmark results at: http://localhost:${ASV_PORT:-8080}"
    echo "Results directory: /asv_results (mounted from host)"
    echo ""
    echo "Press Ctrl+C to stop the server"
    echo ""
    
    exec asv preview --port ${ASV_PORT:-8080}
else
    echo ""
    echo "=============================================="
    echo "   Benchmarks Complete"
    echo "=============================================="
    echo ""
    echo "HTML reports generated at: /asv_results/html"
    echo "Results directory: /asv_results (mounted from host)"
    echo ""
    echo "To view results locally:"
    echo "  cd ${ASV_RESULTS_HOST_PATH:-./asv_results}/html"
    echo "  python3 -m http.server ${ASV_PORT:-8080} --directory ."
    echo ""
    echo "Or run with ASV_PREVIEW=true to start the preview server automatically"
    echo ""
fi
