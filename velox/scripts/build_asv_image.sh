#!/bin/bash
# Script for building Velox ASV (Airspeed Velocity) benchmark Docker image
#
# This script:
# 1. Applies Velox patches for TPC-H Python bindings
# 2. Builds the Docker image that contains Python bindings and ASV
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
  This script builds the velox-asv-benchmark Docker image with:
  
  Step 1: Check base image (velox-adapters-build) exists
  Step 2: Apply Velox patches for TPC-H Python bindings (idempotent)
  Step 3: Build ASV image with:
    - Cython-based Python bindings for TPC-H benchmarks
    - ASV (Airspeed Velocity) for performance tracking
    - All necessary dependencies and runtime environment

  The image is built on top of velox-adapters-build with benchmarks enabled.
  The base image MUST exist before running this script.

Prerequisites:
  - Docker and docker compose installed
  - Sufficient disk space for image layers
  - velox-adapters-build image with benchmarks enabled (REQUIRED)

Examples:
  # Build base image first (if not already built)
  ./build_velox.sh --benchmarks true --sccache
  
  # Then build the ASV image (incremental)
  $0

  # Rebuild ASV image from scratch
  $0 --rebuild

Notes:
  - The velox-adapters-build image MUST be built with --benchmarks enabled
  - If the base image doesn't exist, this script will exit with an error
  - Benchmarks are required for the Python bindings and ASV to work properly

After building, run benchmarks with:
  ./run_asv_benchmarks.sh --data-path /path/to/tpch/data

EOF
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --no-cache)
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


# Step 1: Check if base image exists
echo -e "${YELLOW}Step 1: Checking for base image (velox-adapters-build)...${NC}"
echo ""
if ! docker images velox-adapters-build:latest --format "{{.Repository}}" | grep -q "velox-adapters-build"; then
    echo -e "${RED}Error: Base image 'velox-adapters-build:latest' not found${NC}"
    echo ""
    echo "The ASV benchmark image requires velox-adapters-build as a base image."
    echo ""
    echo "Please build the base image first with benchmarks enabled:"
    echo ""
    echo "  cd $SCRIPT_DIR"
    echo "  ./build_velox.sh --benchmarks true --sccache"
    echo ""
    echo "Then run this script again to build the ASV benchmark image."
    echo ""
    exit 1
else
    echo -e "${GREEN}✓ Base image 'velox-adapters-build:latest' found${NC}"
    echo ""
fi

# Docker-compose file has defaults for all variables, so no need to export them
# But we can set them if desired for consistency
export USER_ID=${USER_ID:-$(id -u)}
export GROUP_ID=${GROUP_ID:-$(id -g)}

# Step 3: Build or rebuild ASV benchmark image
cd "$DOCKER_DIR"
if [ "$REBUILD" = true ]; then
    echo -e "${YELLOW}Step 3: Rebuilding ASV benchmark image (--no-cache)...${NC}"
    echo "This may take a while..."
    echo ""
    docker compose -f docker-compose.adapters.benchmark.yml build --no-cache velox-asv-benchmark
else
    echo -e "${YELLOW}Step 3: Building ASV benchmark image...${NC}"
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
echo "  1. Run benchmarks:"
echo "     ./run_asv_benchmarks.sh"
echo "2. Run benchmarks for a specific commit range:"
echo "     ./run_asv_commit_range.sh --commits HEAD~5..HEAD"
echo ""
echo "For more information:"
echo "  ./run_asv_benchmarks.sh --help"
echo "  ./run_asv_commit_range.sh --help"
echo ""
