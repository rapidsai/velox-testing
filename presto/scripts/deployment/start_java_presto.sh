#!/bin/bash
# Presto Java Deployment Script
# Purpose: Start Presto Java coordinator and worker services only
# For TPC-H data generation and benchmarking, use separate scripts in ../data/ and ../../benchmarks/

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

show_usage() {
    cat << 'EOF'
Usage: $0 [OPTIONS]

Purpose: Start Presto Java coordinator and worker services

Options:
  -h, --help              Show this help message
  --health-check         Wait for services to be healthy before exiting
  --timeout SECONDS      Health check timeout (default: 60)

Environment Variables:
  TPCH_PARQUET_DIR       Directory for TPC-H Parquet files (optional)
  HIVE_METASTORE_DIR     Directory for Hive metastore (optional)

Examples:
  $0                          # Start Presto services
  $0 --health-check           # Start and wait for health check
  $0 --health-check --timeout 120  # Custom health check timeout

Note: This script only starts Presto services. For TPC-H workflows:
  - Data generation: ../data/generate_tpch_data.sh
  - Table registration: ../data/register_tpch_tables.sh  
  - Benchmarking: ../../benchmarks/tpch/run_benchmark.py

EOF
}

# Parse command line arguments
HEALTH_CHECK=false
TIMEOUT=60

while [[ $# -gt 0 ]]; do
    case $1 in
        --health-check)
            HEALTH_CHECK=true
            shift
            ;;
        --timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

print_status "Starting Presto Java services..."

# Change to docker directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(cd "$SCRIPT_DIR/../../docker" && pwd)"
cd "$DOCKER_DIR"

# Stop any existing services
print_status "Stopping any existing Presto services..."
../scripts/stop_presto.sh 2>/dev/null || true

# Unset DOCKER_HOST to use local Docker daemon
unset DOCKER_HOST

# Start services using docker-compose
print_status "Starting Presto Java services with docker-compose..."
docker compose -f docker-compose.java.yml up -d

if [[ "$HEALTH_CHECK" == "true" ]]; then
    print_status "Waiting for Presto services to be healthy (timeout: ${TIMEOUT}s)..."
    
    coordinator_ready=false
    worker_ready=false
    elapsed=0
    
    while [[ $elapsed -lt $TIMEOUT ]]; do
        # Check coordinator health
        if curl -sSf "http://localhost:8080/v1/info" > /dev/null 2>&1; then
            if [[ "$coordinator_ready" == "false" ]]; then
                print_status "Coordinator is ready"
                coordinator_ready=true
            fi
        fi
        
        # Check worker health (look for "SERVER STARTED" in logs)
        if docker logs presto-java-worker 2>&1 | grep -q "SERVER STARTED"; then
            if [[ "$worker_ready" == "false" ]]; then
                print_status "Worker is ready"
                worker_ready=true
            fi
        fi
        
        # Both services ready
        if [[ "$coordinator_ready" == "true" && "$worker_ready" == "true" ]]; then
            print_success "All Presto Java services are healthy"
            print_status "Coordinator: http://localhost:8080"
            print_status "Worker: Internal communication only"
            exit 0
        fi
        
        sleep 2
        elapsed=$((elapsed + 2))
        echo -n "."
    done
    
    print_error "Health check timed out after ${TIMEOUT}s"
    print_status "Coordinator ready: $coordinator_ready"
    print_status "Worker ready: $worker_ready"
    exit 1
else
    print_success "Presto Java services started"
    print_status "Coordinator: http://localhost:8080"
    print_status "Use --health-check to wait for services to be ready"
    print_status ""
    print_status "Next steps:"
    print_status "  - Generate TPC-H data: ../data/generate_tpch_data.sh"
    print_status "  - Register tables: ../data/register_tpch_tables.sh"
    print_status "  - Run benchmarks: ../../benchmarks/tpch/run_benchmark.py"
fi
