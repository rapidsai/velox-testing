#!/bin/bash

# Presto Build and Test Script
# This script builds Presto with Velox connector and optionally runs integration tests

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

print_warning() {
    echo -e "\033[1;33m[WARNING]\033[0m $1"
}

# Parse command line arguments
RUN_TESTS=false
SKIP_TESTS_FLAG="-DskipTests"
PRESTO_PROFILE="-Pvelox"
BUILD_COORDINATOR=true
BUILD_PRESTISSIMO=true
PRESTISSIMO_DIR=""
VELOX_BUILD_DIR=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --run-tests)
            RUN_TESTS=true
            SKIP_TESTS_FLAG=""
            shift
            ;;
        --profile)
            PRESTO_PROFILE="-P$2"
            shift 2
            ;;
        --coordinator-only)
            BUILD_COORDINATOR=true
            BUILD_PRESTISSIMO=false
            shift
            ;;
        --prestissimo-only)
            BUILD_COORDINATOR=false
            BUILD_PRESTISSIMO=true
            shift
            ;;
        --prestissimo-dir)
            PRESTISSIMO_DIR="$2"
            shift 2
            ;;
        --velox-build-dir)
            VELOX_BUILD_DIR="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --run-tests         Run integration tests after build (default: false)"
            echo "  --profile           Maven profile to use (default: velox)"
            echo "  --coordinator-only  Build only Presto coordinator (Java)"
            echo "  --prestissimo-only  Build only Prestissimo C++ worker"
            echo "  --prestissimo-dir   Directory containing Prestissimo source (default: presto-native-execution)"
            echo "  --velox-build-dir   Path to existing Velox build directory (for Prestissimo)"
            echo "  --help, -h          Show this help message"
            echo ""
            echo "Environment:"
            echo "  This script expects to be run from the directory containing the 'presto' folder"
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

print_info "Starting Presto build with Velox connector..."
print_info "Profile: $PRESTO_PROFILE"
print_info "Skip tests during build: $SKIP_TESTS_FLAG"
print_info "Run integration tests: $RUN_TESTS"
print_info "Build coordinator: $BUILD_COORDINATOR"
print_info "Build Prestissimo: $BUILD_PRESTISSIMO"
if [ -n "$VELOX_BUILD_DIR" ]; then
    print_info "Using external Velox build: $VELOX_BUILD_DIR"
fi

# Set default Prestissimo directory if not specified
if [ -z "$PRESTISSIMO_DIR" ]; then
    PRESTISSIMO_DIR="presto-native-execution"
fi

# Check if presto directory exists
if [ ! -d "presto" ]; then
    print_error "Presto directory not found! Please ensure you've checked out Presto to ./presto/"
    exit 1
fi

# Enter presto directory
cd presto

# Configure git safe directory
print_info "Configuring git safe directory..."
git config --global --add safe.directory .

# Build Presto Coordinator (Java)
if [ "$BUILD_COORDINATOR" = true ]; then
    print_info "Building Presto Coordinator (Java) with Velox connector..."
    print_warning "Note: This build configuration may need adjustments for your environment"
    
    # Run Maven build for coordinator
    ./mvnw clean package $PRESTO_PROFILE $SKIP_TESTS_FLAG
    
    print_success "Presto Coordinator build completed successfully!"
fi

# Build Prestissimo (C++ worker)
if [ "$BUILD_PRESTISSIMO" = true ]; then
    print_info "Building Prestissimo C++ worker..."
    
    # Check if Prestissimo directory exists
    if [ ! -d "$PRESTISSIMO_DIR" ]; then
        print_error "Prestissimo directory '$PRESTISSIMO_DIR' not found!"
        print_info "Please ensure presto-native-execution is available or specify --prestissimo-dir"
        exit 1
    fi
    
    # Validate external Velox build if specified
    if [ -n "$VELOX_BUILD_DIR" ]; then
        if [ ! -d "$VELOX_BUILD_DIR" ]; then
            print_error "Velox build directory '$VELOX_BUILD_DIR' not found!"
            exit 1
        fi
        
        # Check if Velox build contains necessary files
        if [ ! -f "$VELOX_BUILD_DIR/CMakeCache.txt" ]; then
            print_error "Invalid Velox build directory - CMakeCache.txt not found in '$VELOX_BUILD_DIR'"
            print_info "Please ensure Velox has been built successfully"
            exit 1
        fi
        
        # Get absolute path for CMake
        VELOX_BUILD_DIR=$(realpath "$VELOX_BUILD_DIR")
        print_info "Using Velox build at: $VELOX_BUILD_DIR"
    fi
    
    cd "$PRESTISSIMO_DIR"
    
    # Check if CMakeLists.txt exists
    if [ ! -f "CMakeLists.txt" ]; then
        print_error "CMakeLists.txt not found in $PRESTISSIMO_DIR"
        print_info "This doesn't appear to be a valid Prestissimo source directory"
        exit 1
    fi
    
    # Create build directory for Prestissimo
    print_info "Configuring Prestissimo build..."
    mkdir -p _build/release
    cd _build/release
    
    # Configure CMake arguments
    CMAKE_ARGS="-DCMAKE_BUILD_TYPE=Release -DPRESTO_ENABLE_TESTING=OFF"
    
    # Add Velox build directory if provided
    if [ -n "$VELOX_BUILD_DIR" ]; then
        print_info "Configuring Prestissimo to use external Velox build..."
        # Point CMake to the external Velox build
        CMAKE_ARGS="$CMAKE_ARGS -Dvelox_DIR=$VELOX_BUILD_DIR"
        CMAKE_ARGS="$CMAKE_ARGS -DCMAKE_PREFIX_PATH=$VELOX_BUILD_DIR"
        CMAKE_ARGS="$CMAKE_ARGS -DVELOX_BUILD_DIR=$VELOX_BUILD_DIR"
    else
        print_warning "No external Velox build specified - Prestissimo will build its own Velox"
        CMAKE_ARGS="$CMAKE_ARGS -DVELOX_ENABLE_PRESTISSIMO=ON"
    fi
    
    # Configure with CMake
    print_info "Running CMake configuration..."
    eval "cmake $CMAKE_ARGS ../.."
    
    # Build Prestissimo
    print_info "Compiling Prestissimo..."
    make -j$(nproc)
    
    cd ../../..
    
    print_success "Prestissimo C++ worker build completed successfully!"
fi

# Run integration tests if requested
if [ "$RUN_TESTS" = true ]; then
    print_info "Running Presto integration tests..."
    
    # Check if test runner exists
    if [ -f "./presto-server/bin/run-tests.sh" ]; then
        ./presto-server/bin/run-tests.sh
        print_success "Integration tests completed!"
    else
        print_error "Test runner not found at ./presto-server/bin/run-tests.sh"
        print_info "Available files in presto-server/bin/:"
        ls -la ./presto-server/bin/ || print_warning "presto-server/bin/ directory not found"
        exit 1
    fi
fi

print_success "Script completed successfully!"