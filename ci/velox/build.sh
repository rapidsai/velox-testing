#!/bin/bash

# Velox Build and Test Script
# This script builds Velox with cuDF support and optionally runs tests

set -e  # Exit on any error

# Function to print colored output
print_info() {
    echo -e "\033[1;34m[INFO]\033[0m $1"
}

print_error() {
    echo -e "\033[1;31m[ERROR]\033[0m $1"
}

print_success() {
    echo -e "\033[1;32m[SUCCESS]\033[0m $1"
}

# Parse command line arguments
BUILD_TYPE="Release"
CUDA_ARCH="native"
BUILD_DIR="build"

while [[ $# -gt 0 ]]; do
    case $1 in
        --build-type)
            BUILD_TYPE="$2"
            shift 2
            ;;
        --cuda-arch)
            CUDA_ARCH="$2"
            shift 2
            ;;
        --build-dir)
            BUILD_DIR="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --build-type     CMAKE_BUILD_TYPE (default: Release)"
            echo "  --cuda-arch      CUDA architecture (default: native)"
            echo "  --build-dir      Build directory name (default: build)"
            echo "  --help, -h       Show this help message"
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

print_info "Starting Velox build with cuDF support..."
print_info "Build directory: $BUILD_DIR"
print_info "Build type: $BUILD_TYPE"
print_info "CUDA architecture: $CUDA_ARCH"

# Fix Git permissions for container environments
print_info "Configuring Git safe directory..."
git config --global --add safe.directory .

# Compiler cache statistics are handled by the GitHub Action workflow

# Store source directory before changing directories
SOURCE_DIR="$(pwd)"
print_info "Source directory: $SOURCE_DIR"

# Create and enter build directory
print_info "Creating build directory: $BUILD_DIR"
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

# Configure with CMake
print_info "Configuring with CMake..."
print_info "Build directory: $(pwd)"

cmake -G Ninja \
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
    -DCMAKE_CUDA_ARCHITECTURES="$CUDA_ARCH" \
    -DVELOX_ENABLE_CUDF=ON \
    -DVELOX_ENABLE_PARQUET=ON \
    -DVELOX_ENABLE_S3=ON \
    -DVELOX_BUILD_TESTING=ON \
    "$SOURCE_DIR"

# Build with Ninja
print_info "Building Velox..."
if [ -n "$NUM_THREADS" ]; then
    print_info "Using $NUM_THREADS parallel jobs"
    ninja -j "$NUM_THREADS"
else
    ninja
fi

print_success "Velox build completed successfully!"
