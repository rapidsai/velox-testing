#!/bin/bash

set -e

# Default values
BUILD_TARGET="native-gpu"
RUN_TESTS="true"
CCACHE_DIR=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --build-target)
      BUILD_TARGET="$2"
      shift 2
      ;;
    --run-tests)
      RUN_TESTS="$2"
      shift 2
      ;;
    --ccache-dir)
      CCACHE_DIR="$2"
      shift 2
      ;;
    --help)
      echo "Usage: $0 [options]"
      echo ""
      echo "Options:"
      echo "  --build-target TARGET        Presto deployment variant (native-gpu, native-cpu, java-only)"
      echo "  --run-tests BOOLEAN          Run integration tests with pytest (true/false)"
      echo "  --ccache-dir PATH            Path to ccache directory for native builds"
      echo "  --help                       Show this help message"
      echo ""
      echo "Examples:"
      echo "  $0 --build-target native-gpu --run-tests true --ccache-dir /path/to/ccache"
      echo "  $0 --build-target java-only --run-tests false"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

echo "Presto Build and Test Script"
echo "============================"
echo "Build target: $BUILD_TARGET"
echo "Run tests: $RUN_TESTS"
echo "Ccache dir: ${CCACHE_DIR:-"Not specified"}"
echo ""

echo "Building and deploying Presto using Docker Compose infrastructure..."

# Navigate to scripts directory as per README instructions
cd scripts

# Select and execute the appropriate start script based on build target
case "$BUILD_TARGET" in
  "java-only")
    echo "Starting Presto Java Coordinator + Java Workers..."
    ./start_java_presto.sh
    ;;
  "native-cpu")
    echo "Starting Presto Java Coordinator + Native CPU Workers..."
    if [ -n "$CCACHE_DIR" ]; then
      ./start_native_cpu_presto.sh --ccache-dir "$CCACHE_DIR"
    else
      ./start_native_cpu_presto.sh
    fi
    ;;
  "native-gpu"|*)
    echo "Starting Presto Java Coordinator + Native GPU Workers..."
    if [ -n "$CCACHE_DIR" ]; then
      ./start_native_gpu_presto.sh --ccache-dir "$CCACHE_DIR"
    else
      ./start_native_gpu_presto.sh
    fi
    ;;
esac
cd ..

# Wait for services to be ready
echo "Waiting for Presto server to be ready..."
sleep 30

# Check if Presto server is accessible
echo "Checking Presto server accessibility..."
if curl -f http://localhost:8080/v1/info; then
  echo "✅ Presto server is accessible"
else
  echo "⚠️  Warning: Presto server not accessible"
fi

# Run tests if enabled
if [ "$RUN_TESTS" = "true" ]; then
  echo ""
  echo "Running Presto integration tests with pytest..."
  
  # Check if integration tests directory exists
  if [ -d "testing" ]; then
    echo "Setting up Python virtual environment for tests..."
    
    # Create virtual environment
    python -m venv test_venv
    
    # Activate virtual environment
    source test_venv/bin/activate
    
    echo "Installing Python test dependencies in virtual environment..."
    # Install pytest and other requirements
    pip install -r testing/requirements.txt
    
    echo "Running integration tests..."
    # Run pytest with verbose output (discovers all test files automatically)
    pytest testing -v
    
    # Store test result
    test_result=$?
    
    # Deactivate virtual environment
    deactivate
    
    # Clean up virtual environment
    rm -rf test_venv
    
    # Check test results
    if [ $test_result -eq 0 ]; then
      echo "✅ Integration tests completed successfully"
    else
      echo "❌ Integration tests failed"
      exit 1
    fi
  else
    echo "⚠️  Testing directory not found at testing/"
    echo "Skipping integration tests"
  fi
else
  echo "Skipping tests as requested"
fi

echo ""
echo "✅ Presto deployment and testing completed successfully!"