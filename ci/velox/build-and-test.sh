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
RUN_TESTS=false
BUILD_TYPE="Release"
CUDA_ARCH="native"
BUILD_TESTING="OFF"

while [[ $# -gt 0 ]]; do
    case $1 in
        --run-tests)
            RUN_TESTS=true
            BUILD_TESTING="ON"
            shift
            ;;
        --build-type)
            BUILD_TYPE="$2"
            shift 2
            ;;
        --cuda-arch)
            CUDA_ARCH="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --run-tests      Build and run tests (default: false)"
            echo "  --build-type     CMAKE_BUILD_TYPE (default: Release)"
            echo "  --cuda-arch      CUDA architecture (default: native)"
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
print_info "Build type: $BUILD_TYPE"
print_info "CUDA architecture: $CUDA_ARCH"
print_info "Build testing: $BUILD_TESTING"

# Configure git safe directory
print_info "Configuring git safe directory..."
git config --global --add safe.directory .

# Create and enter build directory
print_info "Creating build directory..."
mkdir -p build
cd build

# Configure with CMake
print_info "Configuring with CMake..."
cmake -G Ninja \
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
    -DCMAKE_CUDA_ARCHITECTURES="$CUDA_ARCH" \
    -DVELOX_ENABLE_CUDF=ON \
    -DVELOX_ENABLE_PARQUET=ON \
    -DVELOX_ENABLE_S3=ON \
    -DVELOX_BUILD_TESTING="$BUILD_TESTING" \
    ..

# Build with Ninja
print_info "Building Velox..."
ninja

print_success "Velox build completed successfully!"

# Run tests if requested
if [ "$RUN_TESTS" = true ]; then
    print_info "Running Velox test suite..."
    ctest -R cudf -V --output-on-failure
    print_success "All tests completed!"
fi

print_success "Script completed successfully!"