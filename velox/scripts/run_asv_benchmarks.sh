#!/bin/bash
# Script for running Velox ASV (Airspeed Velocity) benchmarks in Docker
#
# This script runs TPC-H benchmarks using ASV with Python bindings in a Docker container.
#
# Usage:
#   ./run_asv_benchmarks.sh --data-path /path/to/tpch/data [options]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
DATA_PATH="../../presto/testing/integration_tests/data/tpch/"
RESULTS_PATH="../asv_benchmarks/results/"
PORT=8081
BENCH=""
COMMIT_RANGE=""
INTERACTIVE=false
NO_PREVIEW=false
NO_PUBLISH=false

# Print usage
usage() {
    cat << EOF
${BLUE}Velox ASV Benchmarks - Docker Runner${NC}

Usage: $0 --data-path PATH [options]

Required:
  --data-path PATH          Path to TPC-H data directory on host

Options:
  --results-path PATH       Path to save ASV results (default: ./asv_results)
  --port PORT               HTTP server port (default: 8081)
  --bench PATTERN           Run specific benchmark pattern (e.g., "TimeQuery06")
  --commits RANGE           Commit range to benchmark (e.g., "HEAD~5..HEAD", "v1.0..v2.0")
                            Default: HEAD^! (single current commit)
  --no-preview              Run benchmarks and generate HTML without starting preview server
  --interactive, -i         Run in interactive mode (bash shell)
  --no-publish              Run benchmarks and skip HTML reports generation
  --help, -h                Show this help message

Description:
  This script runs ASV benchmarks for TPC-H queries using the
  velox-asv-benchmark Docker image. Results are saved to the specified
  results directory and served via HTTP on the specified port.
  
  The results directory is volume-mounted so you can easily delete it
  to start fresh without affecting the source code.

Prerequisites:
  - velox-asv-benchmark image must be built (run ./build_asv_image.sh)
  - TPC-H data in Hive-style Parquet format
  - Docker runtime with GPU support (nvidia-docker)

Examples:
  # Run all benchmarks
  $0 --data-path /data/tpch

  # Run specific query
  $0 --data-path /data/tpch --bench "tpch_benchmarks.TimeQuery06"

  # Run multiple queries (regex pattern)
  $0 --data-path /data/tpch --bench "tpch_benchmarks.TimeQuery0[1-5]"

  # Benchmark last 5 commits
  $0 --data-path /data/tpch --commits "HEAD~5..HEAD"

  # Benchmark commits between two tags/versions
  $0 --data-path /data/tpch --commits "v1.0..v2.0"

  # Interactive mode for debugging
  $0 --data-path /data/tpch --interactive

  # Custom port and results location
  $0 --data-path /data/tpch --port 9090 --results-path /results/asv

  # Skip smoke test for faster startup
  ASV_SKIP_SMOKE_TEST=true $0 --data-path /data/tpch

  # Run benchmarks without starting preview server (for CI/CD)
  $0 --data-path /data/tpch --no-preview

Viewing Results:
  After running, access the web interface at http://localhost:<PORT>
  Results are served by 'asv preview' and saved to the --results-path directory.

Generating Graphs:
  ASV graphs require multiple data points. Use one of these approaches:
  
  1. Automated multi-run (recommended):
     ./run_asv_multi_benchmarks.sh --data-path /data/tpch --count 3
  
  2. Manual runs with unique machine names:
     ASV_MACHINE="run1" ASV_RECORD_SAMPLES=true ./run_asv_benchmarks.sh --data-path /data/tpch --no-preview
     ASV_MACHINE="run2" ASV_RECORD_SAMPLES=true ./run_asv_benchmarks.sh --data-path /data/tpch --no-preview
     ASV_MACHINE="run3" ASV_RECORD_SAMPLES=true ./run_asv_benchmarks.sh --data-path /data/tpch
  
  3. Auto-generated unique names:
     ASV_AUTO_MACHINE=true ASV_RECORD_SAMPLES=true ./run_asv_benchmarks.sh --data-path /data/tpch
  
  See GRAPH_GENERATION.md for detailed guide.

Smoke Test:
  On startup, a quick smoke test runs Query 6 to verify:
  - Data is accessible and in correct format
  - Python bindings work end-to-end
  - GPU/CUDA is functioning
  Set ASV_SKIP_SMOKE_TEST=true to skip this test.

EOF
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --data-path)
            DATA_PATH="$2"
            shift 2
            ;;
        --results-path)
            RESULTS_PATH="$2"
            shift 2
            ;;
        --port)
            PORT="$2"
            shift 2
            ;;
        --bench)
            BENCH="$2"
            shift 2
            ;;
        --commits|--range)
            COMMIT_RANGE="$2"
            shift 2
            ;;
        --no-preview)
            NO_PREVIEW=true
            shift
            ;;
        --interactive|-i)
            INTERACTIVE=true
            shift
            ;;
        --no-publish)
            NO_PUBLISH=true
            shift
            ;;
        --help|-h)
            usage
            ;;
        *)
            echo -e "${RED}Error: Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done


# Check if data path exists
if [ ! -d "$DATA_PATH" ]; then
    echo -e "${RED}Error: Data path does not exist: $DATA_PATH${NC}"
    exit 1
fi

# Get absolute paths
DATA_PATH=$(realpath "$DATA_PATH")
RESULTS_PATH=$(realpath "$RESULTS_PATH" 2>/dev/null || echo "$RESULTS_PATH")

# Get script directory and navigate to docker directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DOCKER_DIR="$SCRIPT_DIR/../docker"

cd "$DOCKER_DIR"

# Check if ASV benchmark image exists
if ! docker images velox-asv-benchmark:latest --format "{{.Repository}}" | grep -q "velox-asv-benchmark"; then
    echo -e "${RED}Error: Docker image 'velox-asv-benchmark:latest' not found${NC}"
    echo ""
    echo "You need to build the ASV benchmark image first:"
    echo "  cd $SCRIPT_DIR"
    echo "  ./build_asv_image.sh"
    echo ""
    exit 1
fi

echo -e "${BLUE}=============================================${NC}"
echo -e "${BLUE}  Velox ASV Benchmarks - Docker Runner${NC}"
echo -e "${BLUE}=============================================${NC}"
echo ""
echo -e "${YELLOW}Configuration:${NC}"
echo "  Data path:    $DATA_PATH"
echo "  Results path: $RESULTS_PATH"
echo "  Port:         $PORT"
echo "  Benchmark:    ${BENCH:-all}"
echo "  Commits:      ${COMMIT_RANGE:-HEAD^! (single current commit)}"
echo "  Interactive:  $INTERACTIVE"
echo "  Preview:      $([ "$NO_PREVIEW" = true ] && echo "disabled" || echo "enabled")"
echo "  Publish:      $([ "$NO_PUBLISH" = true ] && echo "disabled" || echo "enabled")"
echo ""

# Export environment variables
export BENCHMARK_DATA_HOST_PATH="$DATA_PATH"
export ASV_RESULTS_HOST_PATH="$RESULTS_PATH"
export ASV_PORT="$PORT"
export ASV_BENCH="$BENCH"
export ASV_COMMIT_RANGE="$COMMIT_RANGE"
# Export user/group IDs for proper file ownership
export USER_ID=$(id -u)
export GROUP_ID=$(id -g)

# Set preview mode based on flag
if [ "$NO_PREVIEW" = true ]; then
    export ASV_PREVIEW=false
else
    export ASV_PREVIEW=true
fi

if [ "$NO_PUBLISH" = true ]; then
    export ASV_PUBLISH=false
else
    export ASV_PUBLISH=true
fi

# Create results directory if it doesn't exist
mkdir -p "$RESULTS_PATH"

# Run container
if [ "$INTERACTIVE" = true ]; then
    echo -e "${YELLOW}Starting container in interactive mode...${NC}"
    echo ""
    echo "Benchmark data path inside the container: /workspace/velox/velox-benchmark-data"
    docker compose -f docker-compose.adapters.benchmark.yml run --rm --remove-orphans --entrypoint /bin/bash velox-asv-benchmark
else
    echo -e "${YELLOW}Starting benchmark container...${NC}"
    echo ""
    if [ -n "$BENCH" ]; then
        echo -e "${GREEN}Running benchmark: $BENCH${NC}"
    else
        echo -e "${GREEN}Running all benchmarks...${NC}"
    fi
    echo ""
    
    # Trap Ctrl+C to clean up
    trap "echo ''; echo 'Stopping container...'; docker compose -f docker-compose.adapters.benchmark.yml down --remove-orphans; exit 0" INT
    
    docker compose -f docker-compose.adapters.benchmark.yml up --remove-orphans velox-asv-benchmark
fi

echo ""
echo -e "${GREEN}Done!${NC}"
