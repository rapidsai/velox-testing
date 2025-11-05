#!/bin/bash
# Script for building Velox ASV (Airspeed Velocity) benchmark Docker image
#
# This script builds the Docker image that contains Python bindings for
# TPC-H benchmarks and ASV for performance tracking.
#
# Usage:
#   ./build_asv_image.sh [options]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
REBUILD=false

# Print usage
usage() {
    cat << EOF
${BLUE}Velox ASV Benchmark Image Builder${NC}

Usage: $0 [options]

Options:
  --rebuild                 Rebuild Docker image from scratch (--no-cache)
  --help, -h                Show this help message

Description:
  This script builds the velox-asv-benchmark Docker image which includes:
  - Cython-based Python bindings for TPC-H benchmarks
  - ASV (Airspeed Velocity) for performance tracking
  - All necessary dependencies and runtime environment

  The image is built on top of velox-adapters-build with benchmarks enabled.
  If the base image doesn't exist, this script can automatically build it
  using build_velox.sh with benchmarks enabled.

Prerequisites:
  - Docker and docker compose installed
  - Sufficient disk space for image layers
  - (Optional) velox-adapters-build image with benchmarks enabled

Examples:
  # Build the image (incremental)
  # Will prompt to build base image if it doesn't exist
  $0

  # Rebuild from scratch
  $0 --rebuild

Notes:
  - If velox-adapters-build image doesn't exist, the script will offer to
    build it using ./build_velox.sh with benchmarks enabled
  - Benchmarks are required for the Python bindings and ASV to work properly

After building, run benchmarks with:
  ./run_asv_benchmarks.sh --data-path /path/to/tpch/data

EOF
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --rebuild)
            REBUILD=true
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

# Get script directory and navigate to docker directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DOCKER_DIR="$SCRIPT_DIR/../docker"

echo -e "${BLUE}=============================================${NC}"
echo -e "${BLUE}  Velox ASV Benchmark Image Builder${NC}"
echo -e "${BLUE}=============================================${NC}"
echo ""

# Check if base image exists
if ! docker images velox-adapters-build:latest --format "{{.Repository}}" | grep -q "velox-adapters-build"; then
    echo -e "${YELLOW}Warning: Base image 'velox-adapters-build:latest' not found${NC}"
    echo ""
    echo -e "${YELLOW}Building base image (velox-adapters-build) with benchmarks enabled...${NC}"
    echo ""
    
    # Use build_velox.sh script to build with benchmarks enabled
    cd "$SCRIPT_DIR"
    ./build_velox.sh --benchmarks true
    
    echo ""
    echo -e "${GREEN}✓ Base image built${NC}"
    echo ""
fi

# Docker-compose file has defaults for all variables, so no need to export them
# But we can set them if desired for consistency
export USER_ID=${USER_ID:-$(id -u)}
export GROUP_ID=${GROUP_ID:-$(id -g)}

# Build or rebuild ASV benchmark image
cd "$DOCKER_DIR"
if [ "$REBUILD" = true ]; then
    echo -e "${YELLOW}Rebuilding ASV benchmark image (--no-cache)...${NC}"
    echo "This may take a while..."
    echo ""
    docker compose -f docker-compose.adapters.benchmark.yml build --no-cache velox-asv-benchmark
else
    echo -e "${YELLOW}Building ASV benchmark image...${NC}"
    echo "This may take a while on first build..."
    echo ""
    docker compose -f docker-compose.adapters.benchmark.yml build velox-asv-benchmark
fi

echo ""
echo -e "${GREEN}=============================================${NC}"
echo -e "${GREEN}✓ ASV benchmark image built successfully!${NC}"
echo -e "${GREEN}=============================================${NC}"
echo ""
echo "Image: velox-asv-benchmark:latest"
echo ""
echo "Next steps:"
echo "  1. Prepare TPC-H data in Hive-style Parquet format"
echo "  2. Run benchmarks:"
echo "     ./run_asv_benchmarks.sh --data-path ../../presto/testing/integration_tests/data/tpch/"
echo ""
echo "For more information:"
echo "  ./run_asv_benchmarks.sh --help"
echo ""

