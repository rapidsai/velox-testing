SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DOCKER_DIR="$SCRIPT_DIR/../docker"

# Export necessary environment variables for the final publish
export BENCHMARK_DATA_HOST_PATH="$DATA_PATH"
export ASV_RESULTS_HOST_PATH="../asv_benchmarks/results"
export ASV_PORT="8001"
export ASV_BENCH=""
export USER_ID=$(id -u)
export GROUP_ID=$(id -g)
export ASV_SKIP_AUTORUN=true
export ASV_SKIP_SMOKE_TEST=true
export ASV_PREVIEW_EXISTING=true

cd "$DOCKER_DIR"

echo ""
echo -e "${GREEN}Starting preview server at http://localhost:${ASV_PORT}${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop the server${NC}"
echo ""

# Trap Ctrl+C to clean up
trap "echo ''; echo 'Stopping container...'; docker compose -f docker-compose.adapters.benchmark.yml down; exit 0" INT

# Run container with preview enabled
docker compose -f docker-compose.adapters.benchmark.yml run --rm --service-ports velox-asv-benchmark

echo ""
echo -e "${GREEN}Preview server stopped${NC}"
echo ""