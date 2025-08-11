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
      ./start_native_cpu_presto.sh --ccache-dir "$CCACHE_DIR" --no-submodules
    else
      ./start_native_cpu_presto.sh --no-submodules
    fi
    ;;
  "native-gpu"|*)
    echo "Starting Presto Java Coordinator + Native GPU Workers..."
    if [ -n "$CCACHE_DIR" ]; then
      ./start_native_gpu_presto.sh --ccache-dir "$CCACHE_DIR" --no-submodules
    else
      ./start_native_gpu_presto.sh --no-submodules
    fi
    ;;
esac
cd ..

# Wait for Presto server to be ready with retry logic
echo "Waiting for Presto server to be ready..."
MAX_SERVER_ATTEMPTS=15
SERVER_SLEEP_INTERVAL=2
server_attempt=1
server_ready=0

while [ $server_attempt -le $MAX_SERVER_ATTEMPTS ]; do
  echo "Attempt $server_attempt/$MAX_SERVER_ATTEMPTS: Checking Presto server accessibility..."
  if curl -sf http://localhost:8080/v1/info > /dev/null 2>&1; then
    echo "✅ Presto server is accessible after $server_attempt attempt(s)"
    server_ready=1
    break
  else
    echo "Server not ready yet. Retrying in $SERVER_SLEEP_INTERVAL seconds..."
    sleep $SERVER_SLEEP_INTERVAL
  fi
  server_attempt=$((server_attempt + 1))
done

if [ $server_ready -ne 1 ]; then
  echo "❌ Presto server not accessible after $MAX_SERVER_ATTEMPTS attempts. Exiting."
  exit 1
fi
# Wait for at least one Presto worker to be active before proceeding
MAX_ATTEMPTS=10
SLEEP_BETWEEN_ATTEMPTS=10
attempt=1
worker_found=0

echo "Waiting for at least one Presto worker to be active..."

while [ $attempt -le $MAX_ATTEMPTS ]; do
  # Query the Presto coordinator for the list of active workers
  worker_count=$(curl -sf http://localhost:8080/v1/service | grep -c '"type":"worker"')
  if [ "$worker_count" -gt 0 ]; then
    echo "✅ Found $worker_count active Presto worker(s) after $attempt attempt(s)."
    worker_found=1
    break
  else
    echo "Attempt $attempt/$MAX_ATTEMPTS: No active Presto workers found yet. Retrying in $SLEEP_BETWEEN_ATTEMPTS seconds..."
    sleep $SLEEP_BETWEEN_ATTEMPTS
  fi
  attempt=$((attempt + 1))
done

if [ $worker_found -ne 1 ]; then
  echo "❌ No active Presto workers found after $MAX_ATTEMPTS attempts. Exiting."
  exit 1
fi

# Run tests if enabled
if [ "$RUN_TESTS" = "true" ]; then
  echo ""
  echo "Running Presto integration tests with pytest..."
  
  # Check if integration tests directory exists
  if [ -d "testing" ]; then
    echo "Setting up Python virtual environment for tests..."
    
    # Create virtual environment
    python3 -m venv test_venv
    
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