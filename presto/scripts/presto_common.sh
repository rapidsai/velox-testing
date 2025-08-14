#!/bin/bash

# Common functions shared across Presto start scripts
# Source this file in other scripts: source "$(dirname "$0")/presto_common.sh"

# Function to parse common command line arguments for scale factor
parse_scale_factor_args() {
    SCALE_FACTOR="1"  # Default to SF1
    SCALE_FACTOR_SPECIFIED="false"  # Track if scale factor was explicitly specified
    LOAD_ALL_SCALE_FACTORS="false"  # Track if all scale factors should be loaded
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            -s|--scale-factor)
                SCALE_FACTOR="$2"
                SCALE_FACTOR_SPECIFIED="true"
                shift 2
                ;;
            --all-sf|--all-scale-factors)
                LOAD_ALL_SCALE_FACTORS="true"
                SCALE_FACTOR_SPECIFIED="true"
                shift
                ;;
            sf1)
                SCALE_FACTOR="1"
                SCALE_FACTOR_SPECIFIED="true"
                shift
                ;;
            sf10)
                SCALE_FACTOR="10"
                SCALE_FACTOR_SPECIFIED="true"
                shift
                ;;
            sf100)
                SCALE_FACTOR="100"
                SCALE_FACTOR_SPECIFIED="true"
                shift
                ;;
            -h|--help)
                show_usage
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                echo "Use -h or --help for usage information"
                exit 1
                ;;
        esac
    done
    
    # Validate scale factor
    if [[ ! "$SCALE_FACTOR" =~ ^(1|10|100)$ ]]; then
        echo "Error: Scale factor must be 1, 10, or 100"
        exit 1
    fi
}

# Function to show common usage information
show_usage() {
    cat <<EOF
Usage: $0 [OPTIONS] [SCALE_FACTOR]

Options:
  -s, --scale-factor SF    TPC-H scale factor (1, 10, 100)
  --all-sf, --all-scale-factors  Load all scale factors (1, 10, 100) simultaneously
  -h, --help              Show this help message

Scale Factor Shortcuts:
  sf1                     Use scale factor 1 (default)
  sf10                    Use scale factor 10
  sf100                   Use scale factor 100

Environment Variables:
  RUN_TPCH_BENCHMARK=true Run TPC-H benchmark after startup
  TPCH_PARQUET_DIR=path   Use existing TPC-H data directory

Note: Specifying a scale factor will automatically run the TPC-H benchmark

Examples:
  $0                      # Start with SF1 (default), no benchmark
  $0 sf10                 # Start with SF10 and run benchmark
  $0 -s 100               # Start with SF100 and run benchmark
  $0 sf1                  # Start with SF1 and run benchmark
  $0 --all-sf             # Load all scale factors and run benchmark
  RUN_TPCH_BENCHMARK=true $0  # Start with SF1 and run benchmark
EOF
}

# Function to wait for Presto coordinator and worker to be ready
wait_for_presto_ready() {
    local worker_container_name="${1:-presto-java-worker}"
    local timeout_minutes="${2:-5}"
    local max_iterations=$((timeout_minutes * 30))  # 2-second intervals
    
    echo "Waiting for Presto to be ready for TPC-H benchmark..."
    sleep 30
    
    # Wait for Presto coordinator and worker to be responsive
    for i in $(seq 1 $max_iterations); do
        coordinator_ready=false
        worker_ready=false
        
        # Check coordinator
        if curl -sSf "http://localhost:8080/v1/info" > /dev/null 2>&1; then
            coordinator_ready=true
        fi
        
        # Check worker (look for "SERVER STARTED" in logs)
        if docker logs "$worker_container_name" 2>&1 | grep -q "SERVER STARTED"; then
            worker_ready=true
        fi
        
        if [[ "$coordinator_ready" == "true" && "$worker_ready" == "true" ]]; then
            echo "Presto coordinator and worker are ready. Starting TPC-H benchmark..."
            return 0
        fi
        
        echo -n "."
        sleep 2
        if [[ $i -eq $max_iterations ]]; then
            echo "Presto coordinator or worker not responding after ${timeout_minutes} minutes. Skipping benchmark."
            echo "Coordinator ready: $coordinator_ready, Worker ready: $worker_ready"
            return 1
        fi
    done
}

# Function to generate TPC-H data if needed
setup_tpch_data() {
    # Auto-generate TPCH Parquet locally if TPCH_PARQUET_DIR not set or empty
    if [[ -z "${TPCH_PARQUET_DIR:-}" ]]; then
        if [[ "$LOAD_ALL_SCALE_FACTORS" == "true" ]]; then
            echo "TPCH_PARQUET_DIR not set; generating all TPC-H Parquet scale factors (1, 10, 100)..."
            bash "$(dirname "$0")/data/generate_tpch_data.sh" -s 1
            bash "$(dirname "$0")/data/generate_tpch_data.sh" -s 10
            bash "$(dirname "$0")/data/generate_tpch_data.sh" -s 100
            export TPCH_PARQUET_DIR="$(cd "$(dirname "$0")"/.. && pwd)/docker/data/tpch"
        else
            echo "TPCH_PARQUET_DIR not set; generating local TPCH Parquet (SF=${SCALE_FACTOR})..."
            bash "$(dirname "$0")/data/generate_tpch_data.sh" -s "${SCALE_FACTOR}"
            export TPCH_PARQUET_DIR="$(cd "$(dirname "$0")"/.. && pwd)/docker/data/tpch"
        fi
    fi
}

# Function to register TPC-H tables
register_tpch_tables() {
    # If TPCH_PARQUET_DIR is provided, auto-register external TPCH tables in Hive
    if [[ -n "${TPCH_PARQUET_DIR}" ]]; then
        if [[ "$LOAD_ALL_SCALE_FACTORS" == "true" ]]; then
            echo "Registering all TPC-H external Parquet tables (SF1, SF10, SF100) from ${TPCH_PARQUET_DIR}..."
            bash "$(dirname "$0")/data/register_tpch_tables.sh" -s 1
            bash "$(dirname "$0")/data/register_tpch_tables.sh" -s 10
            bash "$(dirname "$0")/data/register_tpch_tables.sh" -s 100
        else
            echo "Registering TPCH external Parquet tables from ${TPCH_PARQUET_DIR}..."
            bash "$(dirname "$0")/data/register_tpch_tables.sh"
        fi
    fi
}

# Function to run TPC-H benchmark if requested
run_tpch_benchmark_if_requested() {
    # Run benchmark if explicitly requested via environment variable OR if scale factor was specified via command line
    if [[ "${RUN_TPCH_BENCHMARK:-false}" == "true" ]] || [[ "$SCALE_FACTOR_SPECIFIED" == "true" ]]; then
        local worker_container_name="${1:-presto-java-worker}"
        
        if ! wait_for_presto_ready "$worker_container_name"; then
            return 1
        fi
        
        # Run the full TPC-H benchmark
        if [[ "$LOAD_ALL_SCALE_FACTORS" == "true" ]]; then
            echo "Running TPC-H benchmark for all scale factors..."
            python "$(dirname "$0")/../benchmarks/tpch/run_benchmark.py" --scale-factor 1 --output-format json
            python "$(dirname "$0")/../benchmarks/tpch/run_benchmark.py" --scale-factor 10 --output-format json
            python "$(dirname "$0")/../benchmarks/tpch/run_benchmark.py" --scale-factor 100 --output-format json
            echo "TPC-H benchmark completed for all scale factors. Results saved to tpch_benchmark_results_sf*_*.json"
        else
            echo "Running TPC-H benchmark..."
            python "$(dirname "$0")/../benchmarks/tpch/run_benchmark.py" --scale-factor ${SCALE_FACTOR} --output-format json
            echo "TPC-H benchmark completed. Results saved to tpch_benchmark_results_*.json"
        fi
    fi
}
