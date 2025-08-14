#!/bin/bash

set -e

# Default values
BUILD_TARGET="native-gpu"
CCACHE_DIR=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --build-target)
      BUILD_TARGET="$2"
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
      CCACHE_DIR=${CCACHE_DIR} NO_SUBMODULES=true ./start_native_cpu_presto.sh
    ;;
  "native-gpu"|*)
    echo "Starting Presto Java Coordinator + Native GPU Workers..."
      CCACHE_DIR=${CCACHE_DIR} NO_SUBMODULES=true ./start_native_gpu_presto.sh
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
    echo "Presto server is accessible after $server_attempt attempt(s)"
    server_ready=1
    break
  else
    echo "Server not ready yet. Retrying in $SERVER_SLEEP_INTERVAL seconds..."
    sleep $SERVER_SLEEP_INTERVAL
  fi
  server_attempt=$((server_attempt + 1))
done

if [ $server_ready -ne 1 ]; then
  echo "Presto server not accessible after $MAX_SERVER_ATTEMPTS attempts. Exiting."
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
    echo "Found $worker_count active Presto worker(s) after $attempt attempt(s)."
    worker_found=1
    break
  else
    echo "Attempt $attempt/$MAX_ATTEMPTS: No active Presto workers found yet. Retrying in $SLEEP_BETWEEN_ATTEMPTS seconds..."
    sleep $SLEEP_BETWEEN_ATTEMPTS
  fi
  attempt=$((attempt + 1))
done

if [ $worker_found -ne 1 ]; then
  echo "No active Presto workers found after $MAX_ATTEMPTS attempts. Exiting."
  exit 1
fi
echo "Presto deployment completed successfully!"
