#!/bin/bash
# Helper script to run multiple ASV benchmarks with unique machine names
# This creates multiple data points on the graph for the same commit
#
# Usage:
#   ./run_asv_multi_benchmarks.sh --data-path PATH --count N [options]

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Default values
COUNT=3
DATA_PATH="../../presto/testing/integration_tests/data/tpch/"
RESULTS_PATH="../asv_benchmarks/results/"
PORT=8001
COMMIT_RANGE=""
SKIP_PREVIEW=false
CLEAR_RESULTS=true
INTERLEAVE_ROUNDS=false

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
        --count)
            COUNT="$2"
            shift 2
            ;;
        --commits|--range)
            COMMIT_RANGE="$2"
            shift 2
            ;;
        --interleave-rounds)
            INTERLEAVE_ROUNDS=true
            shift
            ;;
        --skip-preview)
            SKIP_PREVIEW=true
            shift
            ;;
        --help|-h)
            cat << EOF
${BLUE}Run Multiple ASV Benchmarks - Create Graph Data Points${NC}

Usage: $0 --data-path PATH [options]

Required:
  --data-path PATH          Path to TPC-H data directory

Options:
  --results-path PATH       Path to save results (default: ./asv_results)
  --port PORT               HTTP server port (default: 8080)
  --count N                 Number of benchmark runs (default: 3)
  --commits RANGE           Commit range to benchmark (e.g., "HEAD~5..HEAD")
                            Note: For multi-run, typically use single commit (default)
  --interleave-rounds       Run benchmarks with interleaved rounds (requires rounds > 1)
  --skip-preview            Don't start preview server after last run
  --clear-results           Clear existing results before starting (fresh run)
  --help, -h                Show this help

Description:
  Runs ASV benchmarks multiple times, each with a unique machine name.
  This creates multiple data points on the graph at the same commit,
  allowing you to see performance variability and trends.

Examples:
  # Run 3 times and view results
  $0 --data-path /data/tpch

  # Run 5 times
  $0 --data-path /data/tpch --count 5

  # Run 2 times without preview (for CI/CD)
  $0 --data-path /data/tpch --count 2 --skip-preview

EOF
            exit 0
            ;;
        *)
            echo -e "${RED}Error: Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

echo -e "${BLUE}=============================================${NC}"
echo -e "${BLUE}  Running Multiple ASV Benchmarks${NC}"
echo -e "${BLUE}=============================================${NC}"
echo ""
echo -e "${YELLOW}Configuration:${NC}"
echo "  Data path:    $DATA_PATH"
echo "  Results path: $RESULTS_PATH"
echo "  Port:         $PORT"
echo "  Run count:    $COUNT"
echo "  Commits:      ${COMMIT_RANGE:-HEAD^! (single current commit)}"
echo "  Clear results: $CLEAR_RESULTS"
echo ""

# Clear results if requested
if [ "$CLEAR_RESULTS" = true ]; then
    echo -e "${YELLOW}Clearing existing results...${NC}"
    # Use the dedicated clearing script to handle permissions and thorough cleanup
    "$SCRIPT_DIR/clear_asv_results.sh" "$RESULTS_PATH"
    echo ""
fi

export ASV_AUTO_MACHINE=true

# Run benchmarks multiple times
for i in $(seq 1 $COUNT); do
    echo ""
    echo -e "${GREEN}=============================================${NC}"
    echo -e "${GREEN}  Run $i of $COUNT${NC}"
    echo -e "${GREEN}=============================================${NC}"
    echo ""
    
    # All runs use --record-samples since each has a unique machine name
    # (--append-samples is only for re-running on the SAME machine)
    echo "Run $i: Recording samples for machine $(date +%s)"
    
    # Build command with optional commit range and interleave-rounds
    CMD="./run_asv_benchmarks.sh --data-path \"$DATA_PATH\" --results-path \"$RESULTS_PATH\" --port \"$PORT\" --no-publish --no-preview"
    if [ -n "$COMMIT_RANGE" ]; then
        CMD="$CMD --commits \"$COMMIT_RANGE\""
    fi
    if [ "$INTERLEAVE_ROUNDS" = true ]; then
        CMD="$CMD --interleave-rounds"
    fi
    
    ASV_RECORD_SAMPLES=true \
    ASV_SKIP_EXISTING=false \
    eval $CMD
    
    echo ""
    echo -e "${GREEN}✓ Run $i completed${NC}"
    
    # Wait a bit between runs to ensure unique timestamps
    if [ $i -lt $COUNT ]; then
        echo "Waiting 2 seconds before next run..."
        sleep 2
    fi
done

echo ""
echo -e "${GREEN}=============================================${NC}"
echo -e "${GREEN}  All $COUNT Runs Complete!${NC}"
echo -e "${GREEN}=============================================${NC}"
echo ""

# Run final asv publish to collate all run data and generate graphs
echo -e "${YELLOW}Running final asv publish to generate combined graphs...${NC}"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DOCKER_DIR="$SCRIPT_DIR/../docker"

# Export necessary environment variables for the final publish
export BENCHMARK_DATA_HOST_PATH="$DATA_PATH"
export ASV_RESULTS_HOST_PATH="$RESULTS_PATH"
export ASV_PORT="$PORT"
export ASV_BENCH=""
export ASV_COMMIT_RANGE="$COMMIT_RANGE"
export USER_ID=$(id -u)
export GROUP_ID=$(id -g)
export ASV_SKIP_AUTORUN=true
export ASV_SKIP_SMOKE_TEST=true
export ASV_PUBLISH_PREVIEW_EXISTING=true

cd "$DOCKER_DIR"

echo ""
echo -e "${GREEN}Starting preview server at http://localhost:${PORT}${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop the server${NC}"
echo ""

# Trap Ctrl+C to clean up
trap "echo ''; echo 'Stopping container...'; docker compose -f docker-compose.adapters.benchmark.yml down --remove-orphans; exit 0" INT

# Run container with preview enabled and port mapping
docker compose -f docker-compose.adapters.benchmark.yml run --rm --service-ports --remove-orphans velox-asv-benchmark

echo ""
echo -e "${GREEN}Preview server stopped${NC}"
echo ""

