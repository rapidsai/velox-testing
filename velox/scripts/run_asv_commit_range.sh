#!/bin/bash

# Script to benchmark a range of Velox commits with ASV
# This script:
# 1. Iterates over each commit in a range
# 2. Rebuilds Velox-adapters-build image with sccache
# 3. Rebuilds ASV benchmark image
# 4. Runs ASV benchmarks (without publish/preview)
# 5. Tags the docker image with commit hash
# 6. Finally generates HTML reports and starts preview server
#
# Usage Examples:
#
#   # Basic: Benchmark last 5 commits (main line only, excludes merge history)
#   ./run_asv_commit_range.sh --commits HEAD~5..HEAD
#
#   # Benchmark last 10 commits
#   ./run_asv_commit_range.sh --commits HEAD~10..HEAD
#
#   # Benchmark between two releases
#   ./run_asv_commit_range.sh --commits v1.0..v2.0
#
#   # Benchmark between specific commits
#   ./run_asv_commit_range.sh --commits abc123..def456
#
#   # Custom configuration
#   ./run_asv_commit_range.sh \
#       --commits HEAD~5..HEAD \
#       --data-path /custom/tpch/data \
#       --results-path /custom/results \
#       --port 9090
#
# Note: Uses --first-parent flag, so HEAD~2..HEAD benchmarks 2 commits, not all merge history
# For full help: ./run_asv_commit_range.sh --help

set -euo pipefail

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get script directory for relative paths
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Default values (relative to script location)
# Script is at: velox-testing/velox/scripts/
#
# Important: There are TWO "velox" directories:
#   1. VELOX_REPO:          Sibling velox git repo (projects/velox) - the code being benchmarked
#   2. VELOX_TESTING_ROOT:  This repo (projects/velox-testing) - contains docker/build scripts
#
VELOX_REPO="${SCRIPT_DIR}/../../../velox"                                    # Sibling velox repo (git repo to benchmark)
VELOX_TESTING_ROOT="${SCRIPT_DIR}/../.."                                     # velox-testing root (contains docker scripts)
DATA_PATH="${VELOX_TESTING_ROOT}/presto/testing/integration_tests/data/tpch/"  # ../../presto/...
RESULTS_PATH="${SCRIPT_DIR}/../asv_benchmarks/results"                       # ../asv_benchmarks/results
SCCACHE_AUTH_DIR="${SCRIPT_DIR}/../../.sccache-auth"                         # ../../.sccache-auth
PORT=8081
COMMIT_RANGE="HEAD~5..HEAD" # last 2 commits
MODE="range"  # range (all commits) or endpoints (first & last only)

# Build configuration
BUILD_VELOX_CMD="./build_velox.sh --build-type release --no-cache --sccache --sccache-version 0.12.0-rapids.16"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --velox-repo)
            VELOX_REPO="$2"
            shift 2
            ;;
        --data-path)
            DATA_PATH="$2"
            shift 2
            ;;
        --results-path)
            RESULTS_PATH="$2"
            shift 2
            ;;
        --sccache-auth-dir)
            SCCACHE_AUTH_DIR="$2"
            shift 2
            ;;
        --port)
            PORT="$2"
            shift 2
            ;;
        --commits|--range)
            COMMIT_RANGE="$2"
            shift 2
            ;;
        --mode)
            MODE="$2"
            if [[ "$MODE" != "range" && "$MODE" != "endpoints" ]]; then
                echo -e "${RED}Error: --mode must be 'range' or 'endpoints'${NC}"
                exit 1
            fi
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Benchmark a range of Velox commits with ASV."
            echo ""
            echo "Options:"
            echo "  --velox-repo PATH          Path to Velox repository (default: ../../../velox from script location)"
            echo "  --data-path PATH           Path to TPC-H data directory (default: ../../presto/.../data/tpch/)"
            echo "  --results-path PATH        Path to store ASV results (default: ../asv_benchmarks/results)"
            echo "  --sccache-auth-dir PATH    Path to sccache auth directory (default: ../../.sccache-auth)"
            echo "  --port PORT                HTTP server port for preview (default: 8081)"
            echo "  --commits RANGE            Git commit range to benchmark (e.g., HEAD~5..HEAD, v1.0..v2.0, abc123..def456)"
            echo "  --mode MODE                Benchmark mode: 'range' (all commits) or 'endpoints' (first & last only)"
            echo "  -h, --help                 Show this help message"
            echo ""
            echo "Modes:"
            echo "  range      - Benchmark all commits in range (default, comprehensive)"
            echo "                Time: N commits = N builds"
            echo "  endpoints  - Benchmark only first and last commit (fast regression check)"
            echo "                Time: N commits = 2 builds (10x-50x faster)"
            echo ""
            echo "Note: Results are always cleared at the start for fresh benchmarking"
            echo ""
            echo "Examples:"
            echo ""
            echo "  # Basic: Benchmark last 5 commits with default paths"
            echo "  $0 --commits HEAD~5..HEAD"
            echo ""
            echo "  # Benchmark last 10 commits"
            echo "  $0 --commits HEAD~10..HEAD"
            echo ""
            echo "  # Benchmark between two tags/releases"
            echo "  $0 --commits v1.0..v2.0"
            echo ""
            echo "  # Benchmark between specific commits"
            echo "  $0 --commits abc123..def456"
            echo ""
            echo "  # Quick regression check (endpoints mode - only first & last)"
            echo "  $0 --commits HEAD~20..HEAD --mode endpoints"
            echo ""
            echo "  # Full range (all commits - comprehensive)"
            echo "  $0 --commits HEAD~5..HEAD --mode range"
            echo ""
            echo "  # Custom paths and port"
            echo "  $0 --commits HEAD~5..HEAD \\"
            echo "     --data-path /custom/tpch/data \\"
            echo "     --results-path /custom/results \\"
            echo "     --port 9090"
            echo ""
            echo "  # Full custom configuration"
            echo "  $0 --velox-repo /path/to/velox \\"
            echo "     --data-path /path/to/tpch_data \\"
            echo "     --results-path /path/to/results \\"
            echo "     --sccache-auth-dir /path/to/sccache-auth \\"
            echo "     --commits HEAD~3..HEAD \\"
            echo "     --port 8888"
            echo ""
            echo "Commit Range Syntax:"
            echo "  HEAD~N..HEAD          Last N commits (on main line, excludes merge history)"
            echo "  v1.0..v2.0            Between two tags"
            echo "  abc123..def456        Between two commits"
            echo "  branch1..branch2      Between two branches"
            echo ""
            echo "  Note: Uses --first-parent to follow main line only."
            echo "        This means HEAD~2..HEAD benchmarks 2 commits, not all merge history."
            echo ""
            echo "What It Does:"
            echo "  For each commit in the range:"
            echo "    1. Checkout the commit"
            echo "    2. Rebuild Velox with sccache (--no-cache for fresh build)"
            echo "    3. Rebuild ASV benchmark image (--no-cache)"
            echo "    4. Run all TPC-H benchmarks with unique machine name"
            echo "    5. Tag Docker image as velox-adapters-build:<commit-hash>"
            echo ""
            echo "  After all commits:"
            echo "    6. Generate HTML reports from all results"
            echo "    7. Start preview server on specified port"
            echo "    8. Tag most recent commit as velox-adapters-build:latest"
            echo "    9. Clean up intermediate tagged images"
            echo ""
            echo "Notes:"
            echo "  - Each commit takes ~30-60 minutes to build and benchmark"
            echo "  - sccache provides 2-10x speedup for subsequent builds"
            echo "  - Results are saved with unique machine names per commit"
            echo "  - Press Ctrl+C on preview server to trigger cleanup"
            echo "  - Git state is always restored to original branch/commit"
            echo ""
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Validate required arguments
if [ -z "$COMMIT_RANGE" ]; then
    echo -e "${RED}Error: --commits/--range is required${NC}"
    echo "Use --help for usage information"
    exit 1
fi

# Validate paths
if [ ! -d "$VELOX_REPO" ]; then
    echo -e "${RED}Error: Velox repository not found at: $VELOX_REPO${NC}"
    exit 1
fi

if [ ! -d "$DATA_PATH" ]; then
    echo -e "${RED}Error: TPC-H data directory not found at: $DATA_PATH${NC}"
    exit 1
fi

if [ ! -d "$SCCACHE_AUTH_DIR" ]; then
    echo -e "${RED}Error: sccache auth directory not found at: $SCCACHE_AUTH_DIR${NC}"
    exit 1
fi

# Detect current Velox branch before we start checking out commits
echo -e "${BLUE}Detecting current Velox branch...${NC}"
cd "$VELOX_REPO"
VELOX_DETECTED_BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "HEAD")
echo -e "${GREEN}Current Velox branch: $VELOX_DETECTED_BRANCH${NC}"
echo ""

# Display configuration
echo ""
echo -e "${BLUE}=============================================${NC}"
echo -e "${BLUE}  ASV Commit Range Benchmark Configuration${NC}"
echo -e "${BLUE}=============================================${NC}"
echo ""
echo "Mode:                 $MODE"
echo "Velox Repository:     $VELOX_REPO"
echo "Velox Branch:         $VELOX_DETECTED_BRANCH"
echo "TPC-H Data Path:      $DATA_PATH"
echo "Results Path:         $RESULTS_PATH"
echo "sccache Auth Dir:     $SCCACHE_AUTH_DIR"
echo "Preview Port:         $PORT"
echo "Commit Range:         $COMMIT_RANGE"
echo ""
if [ "$MODE" = "range" ]; then
    echo "Note: 'range' mode - benchmarking ALL commits in range"
    echo "      Results are cleared at the start for fresh benchmarking"
    echo "      Images will be rebuilt with --no-cache for each commit"
    echo "      The most recent commit will be tagged as 'latest' at the end"
else
    echo "Note: 'endpoints' mode - benchmarking ONLY first & last commit (fast)"
    echo "      Results are cleared at the start for fresh benchmarking"
    echo "      Time savings: 2 builds instead of N builds"
fi
echo ""

# Get list of commits to benchmark
echo -e "${BLUE}Getting list of commits...${NC}"
cd "$VELOX_REPO"

# Get commit list (oldest first for chronological benchmarking)
# Using --first-parent to avoid including all commits from merged branches
# This makes HEAD~2..HEAD return 2 commits instead of all merge history
COMMITS=$(git rev-list --reverse --first-parent "$COMMIT_RANGE" 2>/dev/null || {
    echo -e "${RED}Error: Invalid commit range: $COMMIT_RANGE${NC}"
    echo "Examples: HEAD~5..HEAD, v1.0..v2.0, abc123..def456"
    echo "Note: Using --first-parent to follow main line only (excludes merge history)"
    exit 1
})

# Count commits
COMMIT_COUNT=$(echo "$COMMITS" | wc -l)
echo -e "${GREEN}Found $COMMIT_COUNT commits to benchmark${NC}"
echo ""

# Display commits
echo "Commits to benchmark:"
COMMIT_NUM=1
while IFS= read -r commit; do
    COMMIT_SHORT=$(git rev-parse --short "$commit")
    COMMIT_MSG=$(git log -1 --pretty=format:"%s" "$commit")
    echo "  $COMMIT_NUM. $COMMIT_SHORT - $COMMIT_MSG"
    COMMIT_NUM=$((COMMIT_NUM + 1))
done <<< "$COMMITS"
echo ""

# Determine which commits to benchmark based on mode
if [ "$MODE" = "endpoints" ]; then
    # Endpoints mode: only first and last commit
    FIRST_COMMIT=$(echo "$COMMITS" | tail -1)  # oldest
    LAST_COMMIT=$(echo "$COMMITS" | head -1)   # newest
    COMMITS_TO_BENCHMARK="$FIRST_COMMIT"$'\n'"$LAST_COMMIT"
    BENCHMARK_COUNT=2
    
    echo -e "${BLUE}Endpoints mode: Benchmarking 2 commits (first & last) from total $COMMIT_COUNT${NC}"
    echo "  First: $(git rev-parse --short "$FIRST_COMMIT") - $(git log -1 --pretty=format:'%s' "$FIRST_COMMIT")"
    echo "  Last:  $(git rev-parse --short "$LAST_COMMIT") - $(git log -1 --pretty=format:'%s' "$LAST_COMMIT")"
    echo ""
    
    read -p "Proceed with benchmarking these 2 commits? (y/N) " -n 1 -r
else
    # Range mode: all commits
    COMMITS_TO_BENCHMARK="$COMMITS"
    BENCHMARK_COUNT=$COMMIT_COUNT
    
    echo -e "${BLUE}Range mode: Benchmarking all $COMMIT_COUNT commits${NC}"
    echo ""
    
    read -p "Proceed with benchmarking these $COMMIT_COUNT commits? (y/N) " -n 1 -r
fi

echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi

# Always clear results for fresh benchmarking
echo -e "${YELLOW}Clearing previous results...${NC}"
if [ -d "$RESULTS_PATH" ]; then
    # Use the clear script if it exists
    if [ -f "${VELOX_TESTING_ROOT}/velox/scripts/clear_asv_results.sh" ]; then
        bash "${VELOX_TESTING_ROOT}/velox/scripts/clear_asv_results.sh" "$RESULTS_PATH"
    else
        rm -rf "$RESULTS_PATH"/* 2>/dev/null || {
            echo -e "${YELLOW}Warning: Could not clear results with rm, trying docker...${NC}"
            docker run --rm -v "$RESULTS_PATH:/data" alpine rm -rf /data/*
        }
    fi
    echo -e "${GREEN}✓ Results cleared${NC}"
fi
echo ""

# Create results directory
mkdir -p "$RESULTS_PATH"

# Save original branch/commit to restore later
ORIGINAL_BRANCH=$(git rev-parse --abbrev-ref HEAD)
ORIGINAL_COMMIT=$(git rev-parse HEAD)
HEAD_COMMIT_SHORT=$(git rev-parse --short HEAD)
echo "Original branch: $ORIGINAL_BRANCH ($ORIGINAL_COMMIT)"
echo ""

# Array to track tagged images
TAGGED_IMAGES=()
LATEST_COMMIT_SHORT=""

# Function to cleanup and restore original state
cleanup_and_restore() {
    local exit_code=$?
    
    echo ""
    echo -e "${BLUE}=============================================${NC}"
    echo -e "${BLUE}  Cleanup and Restore${NC}"
    echo -e "${BLUE}=============================================${NC}"
    echo ""
    
    # Tag the most recent/HEAD commit as latest
    if [ -n "$LATEST_COMMIT_SHORT" ]; then
        echo -e "${BLUE}Tagging velox-adapters-build:$LATEST_COMMIT_SHORT as latest...${NC}"
        docker tag "velox-adapters-build:$LATEST_COMMIT_SHORT" velox-adapters-build:latest || {
            echo -e "${YELLOW}Warning: Could not tag latest image${NC}"
        }
        echo -e "${GREEN}✓ Tagged as velox-adapters-build:latest${NC}"
    fi
    
    # Remove all other tagged images
    if [ ${#TAGGED_IMAGES[@]} -gt 0 ]; then
        echo ""
        echo -e "${BLUE}Cleaning up intermediate tagged images...${NC}"
        for image in "${TAGGED_IMAGES[@]}"; do
            # Skip the latest commit image
            if [ "$image" != "velox-adapters-build:$LATEST_COMMIT_SHORT" ]; then
                echo "  Removing $image"
                docker rmi "$image" 2>/dev/null || echo "    (already removed or in use)"
            fi
        done
        echo -e "${GREEN}✓ Cleanup complete${NC}"
    fi
    
    # Restore Git state
    echo ""
    echo -e "${BLUE}Restoring original Git state...${NC}"
    cd "$VELOX_REPO"
    # if [ "$ORIGINAL_BRANCH" = "HEAD" ]; then
    #     # We were in detached HEAD state
    #     git checkout "$ORIGINAL_COMMIT" 2>&1 | head -n 3 || true
    # else
    #     git checkout "$ORIGINAL_BRANCH" 2>&1 | head -n 3 || true
    # fi
    git switch - --discard-changes > /dev/null 2>&1 || true
    echo -e "${GREEN}✓ Restored to original state${NC}"
    
    if [ $exit_code -ne 0 ]; then
        echo ""
        echo -e "${RED}Script exited with error code: $exit_code${NC}"
    fi
}

# Trap to ensure we cleanup and restore state on exit (including errors)
trap cleanup_and_restore EXIT

# Function to benchmark a single commit
benchmark_commit() {
    local commit=$1
    local commit_num=$2
    local total_commits=$3
    
    # Always start in VELOX_REPO (the git repository being benchmarked)
    cd "$VELOX_REPO"
    
    local commit_short=$(git rev-parse --short "$commit")
    local commit_msg=$(git log -1 --pretty=format:"%s" "$commit")
    
    echo ""
    echo -e "${GREEN}=============================================${NC}"
    echo -e "${GREEN}  Commit $commit_num/$total_commits: $commit_short${NC}"
    echo -e "${GREEN}  $commit_msg${NC}"
    echo -e "${GREEN}=============================================${NC}"
    echo ""
    
    # Checkout the commit
    echo -e "${BLUE}Checking out commit $commit_short...${NC}"
    git checkout "$commit" 2>&1 | head -n 5 || true
    echo -e "${GREEN}✓ Checked out $commit_short${NC}"
    echo ""
    
    # Update the latest commit short hash (this will be the most recent one processed)
    LATEST_COMMIT_SHORT="$commit_short"
    
    # Move to scripts directory for all subsequent operations
    cd "${VELOX_TESTING_ROOT}/velox/scripts"
    
    # Step 1: Apply Velox patches
    echo -e "${BLUE}Step 1: Applying Velox patches...${NC}"
    echo ""
    
    ./apply_velox_patches.sh || {
        echo -e "${RED}Error: Failed to apply patches for commit $commit_short${NC}"
        return 1
    }
    
    echo -e "${GREEN}✓ Patches applied successfully${NC}"
    echo ""

    # Step 2: Build Velox-adapters-build image (with automatic retry on failure)
    echo -e "${BLUE}Step 2: Building Velox-adapters-build:latest (--no-cache)...${NC}"
    
    # Export sccache auth directory for build script
    export SCCACHE_AUTH_DIR="$SCCACHE_AUTH_DIR"
    
    # Try to build Velox
    if ! $BUILD_VELOX_CMD; then
        echo -e "${YELLOW}Warning: Velox build failed. Attempting recovery...${NC}"
        echo ""
        
        # Step 2a: Rebuild CentOS dependencies image
        echo -e "${BLUE}Step 2a: Rebuilding CentOS dependencies image...${NC}"
        if ! ./build_centos_deps_image.sh; then
            echo -e "${RED}Error: Failed to build CentOS dependencies image${NC}"
            return 1
        fi
        echo -e "${GREEN}✓ CentOS dependencies image built successfully${NC}"
        echo ""
        
        # Step 2b: Retry Velox build
        echo -e "${BLUE}Step 2b: Retrying Velox build...${NC}"
        if ! $BUILD_VELOX_CMD; then
            echo -e "${RED}Error: Velox build failed again after rebuilding deps${NC}"
            return 1
        fi
    fi
    
    echo -e "${GREEN}✓ Velox image built successfully${NC}"
    echo ""

    # Step 3: Build ASV benchmark image
    echo -e "${BLUE}Step 3: Building ASV benchmark image...${NC}"
    ./build_asv_image.sh --no-cache
    echo -e "${GREEN}✓ ASV benchmark image built successfully${NC}"
    echo ""

    # Step 4: Run ASV benchmarks (without publish/preview)
    echo -e "${BLUE}Step 4: Running ASV benchmarks for commit $commit_short (No Publish/Preview)...${NC}"

    VELOX_BRANCH="$VELOX_DETECTED_BRANCH" \
    ASV_SKIP_EXISTING=false \
    ASV_RECORD_SAMPLES=true \
    ASV_PUBLISH=false \
    ASV_PREVIEW=false \
    ASV_COMMIT_RANGE="HEAD^!" \
    ./run_asv_benchmarks.sh \
        --data-path "$DATA_PATH" \
        --results-path "$RESULTS_PATH" \
        --interleave-rounds \
        --no-publish \
        --no-preview || {
        echo -e "${RED}Error: Benchmarks failed for commit $commit_short${NC}"
        echo -e "${YELLOW}Continuing...${NC}"
    }
    
    echo -e "${GREEN}✓ Benchmarks completed for commit $commit_short${NC}"
    echo ""
    
    # Step 4: Tag the Velox image with commit hash
    echo -e "${BLUE}Step 4: Tagging Docker image...${NC}"
    docker tag velox-adapters-build:latest "velox-adapters-build:$commit_short" || {
        echo -e "${YELLOW}Warning: Failed to tag image${NC}"
    }
    TAGGED_IMAGES+=("velox-adapters-build:$commit_short")
    echo -e "${GREEN}✓ Tagged as velox-adapters-build:$commit_short${NC}"
    echo ""
    
    # Step 5: Clean up any modified files in the Velox repository
    echo -e "${BLUE}Step 5: Cleaning up modified files in Velox repository...${NC}"
    cd "$VELOX_REPO"
    git reset --hard HEAD^ > /dev/null 2>&1 || {
        echo -e "${YELLOW}Warning: Failed to reset repository${NC}"
    }
    echo -e "${GREEN}✓ Cleaned up modified files${NC}"
    echo ""
    
    echo -e "${GREEN}✓ Completed commit $commit_num/$total_commits: $commit_short${NC}"
    echo ""
    
    return 0
}

# Iterate over each commit
CURRENT_NUM=1

echo -e "${BLUE}Starting benchmark loop for $BENCHMARK_COUNT commits...${NC}"
echo "Commits to process:"
echo "$COMMITS_TO_BENCHMARK" | while IFS= read -r c; do
    echo "  - $(git -C "$VELOX_REPO" rev-parse --short "$c" 2>/dev/null || echo "$c")"
done
echo ""

while IFS= read -r commit; do
    [ -z "$commit" ] && continue  # Skip empty lines
    benchmark_commit "$commit" "$CURRENT_NUM" "$BENCHMARK_COUNT" || {
        echo -e "${RED}Error: Failed to benchmark commit${NC}"
        exit 1
    }
    CURRENT_NUM=$((CURRENT_NUM + 1))
done <<< "$COMMITS_TO_BENCHMARK"

# All commits benchmarked, now generate reports and start preview
echo ""
echo -e "${GREEN}=============================================${NC}"
echo -e "${GREEN}  All Commits Benchmarked Successfully!${NC}"
echo -e "${GREEN}=============================================${NC}"
echo ""

# Switch to the previously detected Velox branch if it is set
if [ -n "$VELOX_DETECTED_BRANCH" ]; then
    echo -e "${BLUE}Switching Velox repository to previously detected branch: $VELOX_DETECTED_BRANCH...${NC}"
    cd "$VELOX_REPO"
    git switch "$VELOX_DETECTED_BRANCH" || {
        echo -e "${YELLOW}Warning: Failed to switch Velox repository to branch $VELOX_DETECTED_BRANCH${NC}"
    }
fi


# Final Step: Publish and preview existing benchmark results
echo -e "${BLUE}Final Step: Publishing HTML reports and starting preview server...${NC}"


# Move to velox-testing/velox/scripts directory and run benchmark script
cd "${VELOX_TESTING_ROOT}/velox/scripts"
./run_asv_benchmarks.sh \
    --data-path "$DATA_PATH" \
    --results-path "$RESULTS_PATH" \
    --port "$PORT" \
    --publish-existing || {
    echo -e "${RED}Error: Failed to publish/preview results${NC}"
    exit 1
}

echo ""
echo -e "${GREEN}=============================================${NC}"
echo -e "${GREEN}  Benchmark Complete!${NC}"
echo -e "${GREEN}=============================================${NC}"
echo ""
echo "Results saved to: $RESULTS_PATH"
echo "Commits benchmarked: $COMMIT_COUNT"
echo "Latest image: velox-adapters-build:latest (tagged from $LATEST_COMMIT_SHORT)"
echo ""
echo -e "${YELLOW}Note: Cleanup will run when the preview server is stopped (Ctrl+C)${NC}"
echo ""

echo ""
echo -e "${GREEN}=============================================${NC}"
echo -e "${GREEN}  ASV Preview Server${NC}"
echo -e "${GREEN}=============================================${NC}"
echo ""
echo -e "Access benchmark results at: ${BLUE}http://localhost:${PORT}${NC}"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""