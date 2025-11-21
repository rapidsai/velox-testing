#!/bin/bash
# Script to apply Velox patches before building ASV benchmarks
#
# This script applies patches to the Velox repository to add TPC-H Python bindings
# and other necessary modifications for ASV benchmarking.
#
# Usage:
#   ./apply_velox_patches.sh [--velox-repo PATH] [--patches-dir PATH]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Default values
VELOX_REPO="${SCRIPT_DIR}/../../../velox"  # Sibling velox repo
PATCHES_DIR="${SCRIPT_DIR}/../patches"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --velox-repo)
            VELOX_REPO="$2"
            shift 2
            ;;
        --patches-dir)
            PATCHES_DIR="$2"
            shift 2
            ;;
        --help|-h)
            cat << EOF
${BLUE}Velox Patch Applicator${NC}

Usage: $0 [options]

Options:
  --velox-repo PATH      Path to Velox repository (default: ../../../velox)
  --patches-dir PATH     Path to patches directory (default: ../patches)
  --help, -h             Show this help message

Description:
  Applies patches to the Velox repository for TPC-H Python bindings.
  Patches are applied in numerical order (0001, 0002, 0003, etc.).

Examples:
  # Apply patches to default location
  $0

  # Apply patches to custom Velox repo
  $0 --velox-repo /path/to/velox

Notes:
  - This script checks if patches are already applied before applying them
  - If a patch is already applied, it will be skipped
  - All patches must apply cleanly or the script will fail

EOF
            exit 0
            ;;
        *)
            echo -e "${RED}Error: Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Convert to absolute paths
VELOX_REPO="$(cd "$VELOX_REPO" && pwd)"
PATCHES_DIR="$(cd "$PATCHES_DIR" && pwd)"

echo -e "${BLUE}=============================================${NC}"
echo -e "${BLUE}  Velox Patch Applicator${NC}"
echo -e "${BLUE}=============================================${NC}"
echo ""
echo "Velox Repository: $VELOX_REPO"
echo "Patches Directory: $PATCHES_DIR"
echo ""

# Check if velox repo exists
if [ ! -d "$VELOX_REPO/.git" ]; then
    echo -e "${RED}Error: Velox repository not found at: $VELOX_REPO${NC}"
    echo "Please ensure the Velox repository exists and is a valid Git repository"
    exit 1
fi

# Check if patches directory exists
if [ ! -d "$PATCHES_DIR" ]; then
    echo -e "${RED}Error: Patches directory not found at: $PATCHES_DIR${NC}"
    exit 1
fi

# Change to velox repo
cd "$VELOX_REPO"

# Get list of patch files sorted by name
PATCHES=($(find "$PATCHES_DIR" -maxdepth 1 -name "*.patch" -type f | sort))

if [ ${#PATCHES[@]} -eq 0 ]; then
    echo -e "${YELLOW}No patches found in $PATCHES_DIR${NC}"
    exit 0
fi

echo "Found ${#PATCHES[@]} patch(es) to apply:"
for patch in "${PATCHES[@]}"; do
    echo "  - $(basename "$patch")"
done
echo ""

# Track applied and skipped patches
APPLIED_COUNT=0
SKIPPED_COUNT=0
FAILED_COUNT=0

# Apply each patch
for patch in "${PATCHES[@]}"; do
    PATCH_NAME=$(basename "$patch")
    echo -e "${BLUE}Checking patch: $PATCH_NAME${NC}"
    
    # Check if patch is already applied
    if git apply --check "$patch" > /dev/null 2>&1; then
        echo -e "  ${YELLOW}→ Applying patch...${NC}"
        if git apply "$patch"; then
            echo -e "  ${GREEN}✓ Patch applied successfully${NC}"
            APPLIED_COUNT=$((APPLIED_COUNT + 1))
        else
            echo -e "  ${RED}✗ Failed to apply patch${NC}"
            FAILED_COUNT=$((FAILED_COUNT + 1))
        fi
    else
        # Check if patch is already applied by trying reverse
        if git apply --reverse --check "$patch" > /dev/null 2>&1; then
            echo -e "  ${YELLOW}⊙ Patch already applied (skipping)${NC}"
            SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
        else
            echo -e "  ${RED}✗ Patch cannot be applied (conflicts or errors)${NC}"
            echo ""
            echo -e "${RED}Patch conflicts detected!${NC}"
            echo "You may need to:"
            echo "  1. Manually resolve conflicts"
            echo "  2. Update the patch for current Velox version"
            echo "  3. Check if the patch is partially applied"
            FAILED_COUNT=$((FAILED_COUNT + 1))
        fi
    fi
    echo ""
done

# Summary
echo -e "${BLUE}=============================================${NC}"
echo -e "${BLUE}  Patch Application Summary${NC}"
echo -e "${BLUE}=============================================${NC}"
echo ""
echo "Total patches: ${#PATCHES[@]}"
echo -e "${GREEN}Applied: $APPLIED_COUNT${NC}"
echo -e "${YELLOW}Skipped (already applied): $SKIPPED_COUNT${NC}"
if [ $FAILED_COUNT -gt 0 ]; then
    echo -e "${RED}Failed: $FAILED_COUNT${NC}"
    echo ""
    echo -e "${RED}Error: Some patches failed to apply!${NC}"
    exit 1
else
    echo -e "${GREEN}Failed: 0${NC}"
fi
echo ""

if [ $APPLIED_COUNT -gt 0 ]; then
    echo -e "${GREEN}✓ Successfully applied $APPLIED_COUNT patch(es)${NC}"
elif [ $SKIPPED_COUNT -gt 0 ]; then
    echo -e "${YELLOW}⊙ All patches already applied${NC}"
else
    echo -e "${YELLOW}⊙ No changes needed${NC}"
fi
echo ""

