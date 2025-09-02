#!/bin/bash

# ============================================================================
# Presto Deployment Variants Benchmark Script
# 
# This script benchmarks TPC-H queries across three Presto deployment variants:
# 1. Java-based Presto (standard)
# 2. Native CPU Presto with Velox
# 3. Native GPU Presto with Velox + CUDF
#
# Features:
# - Extracts actual query execution times from Presto debug output
# - Comprehensive machine configuration logging
# - Nsys profiling integration for GPU variants
# - Hive connector support for local parquet data
# - Strict Hive connector usage with parquet data
# - Detailed timing and results output
# ============================================================================

set -euo pipefail

# Script configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="${SCRIPT_DIR}/../docker"
RESULTS_BASE_DIR="${SCRIPT_DIR}/../benchmark-results"
BENCHMARK_DATA_TOOLS_DIR="${SCRIPT_DIR}/../../benchmark_data_tools"
COMMON_DIR="${SCRIPT_DIR}/../../common"
TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
RESULTS_DIR="${RESULTS_BASE_DIR}/${TIMESTAMP}"

# Default configuration
VARIANTS_TO_RUN=("java" "native-cpu" "native-gpu")
QUERIES_TO_RUN=(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22)
NUM_RUNS=3
TIMEOUT_SECONDS=300
PRESTO_HOST="localhost"
PRESTO_PORT="8080"
PRESTO_USER="test_user"
SCHEMA_NAME="sf1"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DATA_DIR="${REPO_ROOT}/presto/testing/integration_tests/data/tpch_sf1"
ENABLE_PROFILING=false
AUTO_GENERATE_DATA=false
USE_TPCH_CONNECTOR=false

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print functions
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

# Display help information
show_help() {
    cat << EOF
Presto Deployment Variants Benchmark Script

Usage: $0 [OPTIONS]

OPTIONS:
    -v, --variants VARIANTS     Comma-separated list of variants to run
                               Options: java,native-cpu,native-gpu
                               Default: java,native-cpu,native-gpu
    
    -q, --queries QUERIES       Comma-separated list of TPC-H queries to run (1-22)
                               Default: 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
    
    -r, --runs NUM_RUNS         Number of runs per query per variant
                               Default: 3
    
    -t, --timeout SECONDS       Query timeout in seconds
                               Default: 300
    
    -s, --schema SCHEMA         TPC-H schema/scale factor to use
                               Default: sf1
    
    -d, --data-dir PATH         Path to TPC-H parquet data directory
                               Default: <repo_root>/presto/testing/integration_tests/data/tpch_sf1
    
    -p, --profile               Enable nsys profiling for GPU variants
                               Default: false
    
    -g, --generate-data         Auto-generate parquet data files if they don't exist using benchmark_data_tools
                               Default: false
    
    -h, --help                  Show this help message

EXAMPLES:
    # Run all variants with all queries
    $0
    
    # Run only GPU variant with query 1
    $0 -v native-gpu -q 1
    
    # Run Java and CPU variants with queries 1-5, 3 runs each
    $0 -v java,native-cpu -q 1,2,3,4,5 -r 3
    
    # Run with custom data directory and profiling enabled
    $0 -d /path/to/tpch/data -p
    
    # Auto-generate data if it doesn't exist
    $0 -g -d /path/to/new/tpch_sf10 -s sf10

VARIANT DESCRIPTIONS:
    java        - Standard Java-based Presto deployment
    native-cpu  - Native Presto with Velox (CPU execution)
    native-gpu  - Native Presto with Velox + CUDF (GPU acceleration)

EOF
}

# Parse command line arguments
parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -v|--variants)
                IFS=',' read -r -a VARIANTS_TO_RUN <<< "$2"
                shift 2
                ;;
            -q|--queries)
                IFS=',' read -r -a QUERIES_TO_RUN <<< "$2"
                shift 2
                ;;
            -r|--runs)
                NUM_RUNS="$2"
                shift 2
                ;;
            -t|--timeout)
                TIMEOUT_SECONDS="$2"
                shift 2
                ;;
            -s|--schema)
                SCHEMA_NAME="$2"
                shift 2
                ;;
            -d|--data-dir)
                DATA_DIR="$2"
                shift 2
                ;;
            -p|--profile)
                ENABLE_PROFILING=true
                shift
                ;;
            -g|--generate-data)
                AUTO_GENERATE_DATA=true
                shift
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
}

# Setup and validate dependencies
setup_dependencies() {
    # Check Python 3 availability
    if ! command -v python3 >/dev/null 2>&1; then
        print_error "Python 3 is required but not found"
        exit 1
    fi
    print_status "Python 3 found: $(python3 --version)"
    
    # Setup Python virtual environment (like run_integ_test.sh)
    local venv_dir="${SCRIPT_DIR}/.venv"
    if [[ ! -d "$venv_dir" ]]; then
        print_status "Setting up Python virtual environment..."
        python3 -m venv "$venv_dir" || {
            print_error "Failed to create virtual environment"
            exit 1
        }
    fi
    
    # Activate virtual environment
    source "$venv_dir/bin/activate" || {
        print_error "Failed to activate virtual environment"
        exit 1
    }
    
    # Install integration test requirements
    local test_utils_dir="${REPO_ROOT}/presto/testing/integration_tests"
    if [[ -f "$test_utils_dir/requirements.txt" ]]; then
        print_status "Installing integration test requirements..."
        pip install -q -r "$test_utils_dir/requirements.txt" >/dev/null 2>&1 || {
            print_warning "Failed to install integration test requirements - continuing anyway"
        }
    fi
    
    print_status "Python environment setup completed"
    
    # Source common TPC-H validation functions after print functions are defined
    if [[ -f "$COMMON_DIR/tpch_data_validation.sh" ]]; then
        source "$COMMON_DIR/tpch_data_validation.sh"
        print_status "Loaded common TPC-H validation functions"
    else
        print_warning "Common TPC-H validation functions not found, using basic validation"
    fi
}

# Validate arguments
validate_arguments() {
    # Validate variants
    for variant in "${VARIANTS_TO_RUN[@]}"; do
        if [[ ! "$variant" =~ ^(java|native-cpu|native-gpu)$ ]]; then
            print_error "Invalid variant: $variant"
            print_error "Valid options: java, native-cpu, native-gpu"
            exit 1
        fi
    done
    
    # Validate queries
    for query in "${QUERIES_TO_RUN[@]}"; do
        if [[ ! "$query" =~ ^([1-9]|1[0-9]|2[0-2])$ ]]; then
            print_error "Invalid query number: $query"
            print_error "Valid range: 1-22"
            exit 1
        fi
    done
    
    # Validate numeric parameters
    if [[ ! "$NUM_RUNS" =~ ^[1-9][0-9]*$ ]]; then
        print_error "Invalid number of runs: $NUM_RUNS"
        exit 1
    fi
    
    if [[ ! "$TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
        print_error "Invalid timeout: $TIMEOUT_SECONDS"
        exit 1
    fi
}

# Get machine configuration information
get_machine_config() {
    local config_file="$1"
    {
        echo "========================================"
        echo "Machine Configuration"
        echo "========================================"
        echo "Timestamp: $(date)"
        echo "Hostname: $(hostname)"
        echo "User: $(whoami)"
        echo ""
        
        echo "OS Information:"
        cat /etc/os-release 2>/dev/null || echo "OS release info not available"
        echo ""
        
        echo "Kernel Version:"
        uname -a
        echo ""
        
        echo "CPU Information:"
        lscpu | head -20
        echo ""
        
        echo "Memory Information:"
        free -h
        echo ""
        
        echo "Disk Space:"
        df -h | grep -E '^(/dev/|tmpfs)'
        echo ""
        
        echo "Network Interfaces:"
        ip addr show | grep -E '^[0-9]|inet '
        echo ""
        
        echo "Docker Version:"
        docker --version 2>/dev/null || echo "Docker not found"
        echo ""
        
        echo "Docker Compose Version:"
        get_docker_compose_cmd
        docker compose version 2>/dev/null || docker-compose --version 2>/dev/null || echo "Docker Compose not found"
        echo ""
        
        echo "GPU Information:"
        nvidia-smi 2>/dev/null || echo "NVIDIA GPU not available"
        echo ""
        
        echo "Environment Variables:"
        env | grep -E "(CUDA|NVIDIA|GPU)" | sort || echo "No GPU-related environment variables"
        echo ""
        
        echo "Benchmark Configuration:"
        echo "Variants: ${VARIANTS_TO_RUN[*]}"
        echo "Queries: ${QUERIES_TO_RUN[*]}"
        echo "Number of runs: $NUM_RUNS"
        echo "Timeout: $TIMEOUT_SECONDS seconds"
        echo "Schema: $SCHEMA_NAME"
        echo "Data directory: $DATA_DIR"
        echo "Profiling enabled: $ENABLE_PROFILING"
        echo "Auto-generate data: $AUTO_GENERATE_DATA"
        echo "Using Hive connector with parquet data"
        echo ""
        
        if [[ "$ENABLE_PROFILING" == true ]]; then
            echo "Nsys Information:"
            nsys --version 2>/dev/null || echo "nsys not available"
            echo ""
        fi
        
    } > "$config_file"
}

# Detect Docker Compose command
get_docker_compose_cmd() {
    if command -v "docker-compose" >/dev/null 2>&1; then
        echo "docker-compose"
    elif docker compose version >/dev/null 2>&1; then
        echo "docker compose"
    else
        print_error "Neither 'docker-compose' nor 'docker compose' command found"
        exit 1
    fi
}

# Check if nsys is available
check_nsys_available() {
    command -v nsys >/dev/null 2>&1
}

# Wait for Presto to be ready
wait_for_presto() {
    local max_wait_time=180
    local wait_interval=5
    local elapsed=0
    
    print_status "Waiting for Presto to be ready..."
    
    # First check if coordinator is responding
    while ! curl -s "http://${PRESTO_HOST}:${PRESTO_PORT}/v1/info" >/dev/null 2>&1; do
        if [[ $elapsed -ge $max_wait_time ]]; then
            print_error "Presto failed to start within ${max_wait_time} seconds"
            return 1
        fi
        echo -n "."
        sleep $wait_interval
        elapsed=$((elapsed + wait_interval))
    done
    
    print_status "Coordinator is up, waiting for workers to register..."
    
    # Wait for workers to register
    while true; do
        if [[ $elapsed -ge $max_wait_time ]]; then
            print_error "Workers failed to register within ${max_wait_time} seconds"
            return 1
        fi
        
        # Check worker count
        local worker_count
        worker_count=$(curl -s "http://${PRESTO_HOST}:${PRESTO_PORT}/v1/node" | jq -r 'length' 2>/dev/null || echo "0")
        
        if [[ "$worker_count" -gt 0 ]]; then
            print_status "Found $worker_count worker node(s)"
            break
        fi
        
        echo -n "w"
        sleep $wait_interval
        elapsed=$((elapsed + wait_interval))
    done
    
    # Test query execution using containerized CLI
    local docker_compose_cmd
    docker_compose_cmd=$(get_docker_compose_cmd)
    
    local catalog="hive"
    local schema="default"
    
    if [[ "$USE_TPCH_CONNECTOR" == true ]]; then
        catalog="tpch"
        schema="$SCHEMA_NAME"
    fi
    
    while true; do
        if [[ $elapsed -ge $max_wait_time ]]; then
            print_error "Presto test query failed after ${max_wait_time} seconds"
            return 1
        fi
        
        local test_output
        cd "$DOCKER_DIR"
        test_output=$(timeout 30 docker exec presto-cli presto-cli \
            --server presto-coordinator:${PRESTO_PORT} \
            --catalog "$catalog" \
            --schema "$schema" \
            --debug \
            --execute "SELECT 1" 2>&1 || true)
        cd "$SCRIPT_DIR"
        
        if echo "$test_output" | grep -qE '^"?1"?$'; then
            print_success "Presto is ready and responding to queries"
            return 0
        elif echo "$test_output" | grep -q "initializing"; then
            echo -n "i"
            sleep $wait_interval
            elapsed=$((elapsed + wait_interval))
            continue
        elif echo "$test_output" | grep -q "Insufficient active worker nodes"; then
            echo -n "w"
            sleep $wait_interval
            elapsed=$((elapsed + wait_interval))
            continue
        else
            print_error "Presto test query failed: $test_output"
            return 1
        fi
    done
}

# Set up TPC-H tables when using Hive connector
setup_hive_tables() {
    if [[ "$USE_TPCH_CONNECTOR" == false ]]; then
        print_status "Setting up TPC-H tables in Hive catalog..."
        print_status "Setting up tables using existing integration test utilities..."
        local test_utils_dir="${REPO_ROOT}/presto/testing/integration_tests"
        
        if [[ -f "$test_utils_dir/test_utils.py" ]]; then
            # Dependencies already installed in setup_dependencies()
            
            # Use the separate Python script for table creation
            cd "$BENCHMARK_DATA_TOOLS_DIR"
            python3 setup_hive_tables.py \
                --host "$PRESTO_HOST" \
                --port "$PRESTO_PORT" \
                --user "$PRESTO_USER" \
                --data-path "/opt/data" && \
                print_status "Hive tables setup completed using existing utilities"
            cd "$SCRIPT_DIR"
        else
            print_error "Integration test utilities not found at: $test_utils_dir"
            exit 1
        fi
    fi
}

# Start a Presto variant
start_presto_variant() {
    local variant="$1"
    local start_script=""
    
    case "$variant" in
        "java")
            start_script="start_java_presto.sh"
            ;;
        "native-cpu")
            start_script="start_native_cpu_presto.sh"
            ;;
        "native-gpu")
            start_script="start_native_gpu_presto.sh"
            ;;
        *)
            print_error "Unknown variant: $variant"
            return 1
            ;;
    esac
    
    print_status "Starting Presto variant: $(get_variant_name "$variant")"
    
    cd "$SCRIPT_DIR"
    DATA_DIR="$DATA_DIR" ./"$start_script"
    
    if wait_for_presto; then
        setup_hive_tables
        return 0
    else
        print_error "Failed to start Presto variant: $variant"
        return 1
    fi
}

# Stop Presto variant
stop_presto_variant() {
    local variant="$1"
    
    print_status "Stopping Presto variant: $(get_variant_name "$variant")"
    
    cd "$SCRIPT_DIR"
    ./stop_presto.sh
}

# Get variant display name
get_variant_name() {
    case "$1" in
        "java")
            echo "Java-based Presto"
            ;;
        "native-cpu")
            echo "Native CPU Presto with Velox"
            ;;
        "native-gpu")
            echo "Native GPU Presto with Velox"
            ;;
        *)
            echo "Unknown Variant"
            ;;
    esac
}

# Generate TPC-H query from existing queries.json
generate_tpch_query() {
    local query_num="$1"
    local query_file="$2"
    local queries_json="${REPO_ROOT}/presto/testing/integration_tests/queries/tpch/queries.json"
    
    # Check if queries.json exists
    if [[ ! -f "$queries_json" ]]; then
        if [[ "$AUTO_GENERATE_DATA" == true ]]; then
            print_status "queries.json not found, auto-generating..."
            local queries_dir
            queries_dir=$(dirname "$queries_json")
            generate_queries_json "$queries_dir"
        else
            print_error "TPC-H queries.json not found at: $queries_json"
            print_error "Use --generate-data to auto-generate queries.json"
            return 1
        fi
    fi
    
    # Extract query from JSON using python (more reliable than jq)
    local query_key="Q${query_num}"
    local query_sql
    
    query_sql=$(python3 -c "
import json
import sys
try:
    with open('$queries_json', 'r') as f:
        queries = json.load(f)
    if '$query_key' in queries:
        print(queries['$query_key'])
    else:
        print('Query $query_key not found', file=sys.stderr)
        sys.exit(1)
except Exception as e:
    print(f'Error reading queries.json: {e}', file=sys.stderr)
    sys.exit(1)
")
    
    if [[ $? -ne 0 ]]; then
        print_error "Failed to extract query $query_key from queries.json"
        return 1
    fi
    
    # Write query to file (ensure it ends with semicolon)
    if [[ "$query_sql" =~ \;$ ]]; then
        echo "$query_sql" > "$query_file"
    else
        echo "$query_sql;" > "$query_file"
    fi
}

# Run a single query
run_query() {
    local variant="$1"
    local query_num="$2"
    local run_number="$3"
    local output_dir="$4"
    
    local query_padded=$(printf "%02d" "$query_num")
    local query_file="${output_dir}/q${query_padded}.sql"
    local temp_output="${output_dir}/q${query_padded}_run${run_number}_raw.out"
    local timing_output="${output_dir}/q${query_padded}_run${run_number}_timing.out"
    
    # Generate query
    generate_tpch_query "$query_num" "$query_file"
    
    local docker_compose_cmd
    docker_compose_cmd=$(get_docker_compose_cmd)
    
    local catalog="hive"
    local schema="default"
    
    if [[ "$USE_TPCH_CONNECTOR" == true ]]; then
        catalog="tpch"
        schema="$SCHEMA_NAME"
    fi
    
    # Set up profiling command if enabled and variant is native-gpu
    local profile_cmd=""
    local nsys_output=""
    if [[ "$ENABLE_PROFILING" == true ]] && [[ "$variant" == "native-gpu" ]]; then
        if check_nsys_available; then
            nsys_output="$output_dir/q${query_padded}_run${run_number}_gpu.nsys-rep"
            profile_cmd="nsys profile \
                -t nvtx,cuda,osrt \
                -f true \
                --cuda-memory-usage=true \
                --cuda-um-cpu-page-faults=true \
                --cuda-um-gpu-page-faults=true \
                --output='$nsys_output'"
            print_status "Running Q${query_padded} run ${run_number} with nsys profiling"
        else
            print_status "WARNING: nsys not found. Profiling disabled for this run."
        fi
    fi
    
    # Execute query and measure time
    local start_time=$(date +%s.%N)
    
    local exit_code
    cd "$DOCKER_DIR"
    timeout $TIMEOUT_SECONDS bash -c "
        $profile_cmd docker exec -i presto-cli presto-cli \
            --server presto-coordinator:${PRESTO_PORT} \
            --catalog $catalog \
            --schema $schema \
            --file /dev/stdin \
            --output-format CSV \
            --debug \
            < '$query_file' \
            > '$temp_output' 2>&1
    " > "$timing_output" 2>&1
    exit_code=$?
    cd "$SCRIPT_DIR"
    
    local end_time=$(date +%s.%N)
    local total_time=$(echo "$end_time - $start_time" | bc -l)
    
    # Extract execution time from debug output
    if [[ $exit_code -eq 0 ]]; then
        local execution_time=""
        if grep -q "Query.*finished" "$temp_output"; then
            execution_time=$(grep -o "Query.*finished.*in [0-9.]*s" "$temp_output" | grep -o "[0-9.]*s" | sed 's/s$//' | head -1)
        fi
        if [[ -z "$execution_time" ]]; then
            execution_time=$(grep -o "CPU: [0-9.]*s" "$temp_output" | grep -o "[0-9.]*" | head -1)
        fi
        if [[ -z "$execution_time" ]]; then
            execution_time="$total_time"
        fi
        
        # If profiling was enabled and nsys output exists, note it
        if [[ -n "$nsys_output" ]] && [[ -f "$nsys_output" ]]; then
            echo "# nsys profile generated: $(basename "$nsys_output")" >> "$temp_output"
        fi
        
        echo "$execution_time"
        return 0
    else
        echo "FAILED"
        return 1
    fi
}

# Check if benchmark_data_tools is available
check_benchmark_data_tools() {
    if [[ ! -d "$BENCHMARK_DATA_TOOLS_DIR" ]]; then
        print_warning "benchmark_data_tools not found at: $BENCHMARK_DATA_TOOLS_DIR"
        return 1
    fi
    
    if [[ ! -f "$BENCHMARK_DATA_TOOLS_DIR/generate_data_files.py" ]]; then
        print_warning "generate_data_files.py not found in benchmark_data_tools"
        return 1
    fi
    
    return 0
}

# Extract scale factor from metadata.json file or fallback to path parsing
get_scale_factor_from_data_dir() {
    local data_dir="$1"
    local metadata_file="$data_dir/metadata.json"
    
    # Try to read from metadata.json first
    if [[ -f "$metadata_file" ]]; then
        local scale_factor
        scale_factor=$(python3 -c "
import json
import sys
try:
    with open('$metadata_file', 'r') as f:
        metadata = json.load(f)
    print(metadata.get('scale_factor', 1))
except Exception as e:
    print('1', file=sys.stderr)
    sys.exit(1)
" 2>/dev/null)
        if [[ $? -eq 0 ]] && [[ -n "$scale_factor" ]]; then
            echo "$scale_factor"
            return 0
        fi
    fi
    
    # Fallback to parsing directory path
    if [[ "$data_dir" =~ tpch_sf([0-9]+(\.[0-9]+)?) ]]; then
        echo "${BASH_REMATCH[1]}"
    else
        # Default to scale factor 1
        echo "1"
    fi
}

# Generate parquet data files using benchmark_data_tools
generate_parquet_data() {
    local data_dir="$1"
    local scale_factor
    scale_factor=$(get_scale_factor_from_data_dir "$data_dir")
    
    print_status "Auto-generating TPC-H parquet data (scale factor: $scale_factor)"
    print_status "Data directory: $data_dir"
    
    # Check if benchmark_data_tools is available
    if ! check_benchmark_data_tools; then
        print_error "Cannot auto-generate data - benchmark_data_tools not available"
        exit 1
    fi
    
    # Python 3 and requirements already checked in setup_dependencies()
    
    # Install benchmark data tools requirements if needed (not covered by integration test requirements)
    local requirements_file="$BENCHMARK_DATA_TOOLS_DIR/requirements.txt"
    if [[ -f "$requirements_file" ]]; then
        print_status "Installing benchmark data tools requirements..."
        pip install -q -r "$requirements_file" >/dev/null 2>&1 || {
            print_warning "Failed to install benchmark data tools requirements - continuing anyway"
        }
    fi
    
    # Generate data
    print_status "Generating parquet files (this may take several minutes)..."
    cd "$BENCHMARK_DATA_TOOLS_DIR"
    python3 generate_data_files.py \
        --benchmark-type tpch \
        --data-dir-path "$data_dir" \
        --scale-factor "$scale_factor" \
        --convert-decimals-to-floats || {
        print_error "Failed to generate parquet data"
        exit 1
    }
    
    cd "$SCRIPT_DIR"
    print_success "Successfully generated TPC-H parquet data"
}

# Generate queries.json using benchmark_data_tools
generate_queries_json() {
    local queries_dir="$1"
    
    print_status "Auto-generating TPC-H queries.json"
    
    # Check if benchmark_data_tools is available
    if ! check_benchmark_data_tools; then
        print_error "Cannot auto-generate queries - benchmark_data_tools not available"
        exit 1
    fi
    
    # Generate queries
    cd "$BENCHMARK_DATA_TOOLS_DIR"
    python3 generate_query_file.py \
        --benchmark-type tpch \
        --queries-dir-path "$queries_dir" || {
        print_error "Failed to generate queries.json"
        exit 1
    }
    
    cd "$SCRIPT_DIR"
    print_success "Successfully generated TPC-H queries.json"
}

# Check if data directory has parquet files
check_data_directory() {
    if [[ ! -d "$DATA_DIR" ]]; then
        if [[ "$AUTO_GENERATE_DATA" == true ]]; then
            print_status "Data directory not found, will auto-generate: $DATA_DIR"
            generate_parquet_data "$DATA_DIR"
        else
            print_error "TPC-H data directory not found: $DATA_DIR"
            print_error "Use --generate-data to auto-generate data files"
            exit 1
        fi
    fi
    
    # Use the shared validation function if available
    if declare -f validate_tpch_data_structure >/dev/null; then
        print_status "Validating TPC-H data structure using shared validation..."
        if validate_tpch_data_structure "$DATA_DIR"; then
            print_status "TPC-H data validation passed"
            USE_TPCH_CONNECTOR=false
            
            # Show data summary
            local data_size
            data_size=$(get_tpch_data_size "$DATA_DIR")
            print_status "Data directory size: $data_size"
            print_status "Table summary:"
            list_tpch_tables "$DATA_DIR"
        else
            if [[ "$AUTO_GENERATE_DATA" == true ]]; then
                print_status "Data validation failed, auto-generating data..."
                generate_parquet_data "$DATA_DIR"
                USE_TPCH_CONNECTOR=false
            else
                print_error "TPC-H data validation failed"
                print_error "Use --generate-data to auto-generate data files"
                exit 1
            fi
        fi
    else
        # Fallback to basic check if shared functions not available
        if find "$DATA_DIR" -name "*.parquet" -type f | head -1 | grep -q .; then
            print_status "Found parquet files in $DATA_DIR"
            USE_TPCH_CONNECTOR=false
        else
            if [[ "$AUTO_GENERATE_DATA" == true ]]; then
                print_status "No parquet files found, auto-generating data..."
                generate_parquet_data "$DATA_DIR"
                USE_TPCH_CONNECTOR=false
            else
                print_error "No parquet files found in $DATA_DIR - benchmark requires parquet data"
                print_error "Use --generate-data to auto-generate data files"
                exit 1
            fi
        fi
    fi
}

# Benchmark a variant
benchmark_variant() {
    local variant="$1"
    local variant_dir="${RESULTS_DIR}/${variant}"
    mkdir -p "$variant_dir"
    
    echo ""
    echo "============================================"
    echo "Benchmarking $(get_variant_name "$variant")"
    echo "============================================"
    
    # Native variants use TPCH connector to avoid Java coordinator + C++ worker communication issues
    local original_tpch_connector="$USE_TPCH_CONNECTOR"
    if [[ "$variant" == "native-cpu" || "$variant" == "native-gpu" ]]; then
        print_status "Using built-in TPCH connector for native variant"
        print_status "Note: Mixed Java coordinator + C++ worker deployments have architectural limitations"
        USE_TPCH_CONNECTOR=true
    fi
    
    if ! start_presto_variant "$variant"; then
        print_error "Failed to start variant $variant, skipping..."
        # Hive connector configuration preserved
        return 1
    fi
    
    local results_file="${variant_dir}/results.json"
    local timings_file="${variant_dir}/timings.csv"
    local summary_file="${variant_dir}/summary.txt"
    
    # Initialize results files
    echo "{" > "$results_file"
    echo "query,run,execution_time" > "$timings_file"
    
    local total_queries=0
    local successful_queries=0
    local failed_queries=0
    local total_time=0
    
    for query in "${QUERIES_TO_RUN[@]}"; do
        local query_padded=$(printf "%02d" "$query")
        print_status "Running TPC-H Query $query_padded"
        
        for ((run=1; run<=NUM_RUNS; run++)); do
            local execution_time
            execution_time=$(run_query "$variant" "$query" "$run" "$variant_dir")
            total_queries=$((total_queries + 1))
            
            if [[ "$execution_time" != "FAILED" ]]; then
                successful_queries=$((successful_queries + 1))
                total_time=$(echo "$total_time + $execution_time" | bc -l)
                echo "q${query_padded},${run},${execution_time}" >> "$timings_file"
                echo "  \"q${query_padded}_run${run}\": ${execution_time}," >> "$results_file"
                print_success "Q${query_padded} run $run: ${execution_time}s"
            else
                failed_queries=$((failed_queries + 1))
                echo "q${query_padded},${run},FAILED" >> "$timings_file"
                echo "  \"q${query_padded}_run${run}\": \"FAILED\"," >> "$results_file"
                print_error "Q${query_padded} run $run: FAILED"
            fi
        done
    done
    
    # Finalize results file
    sed -i '$ s/,$//' "$results_file"  # Remove trailing comma
    echo "}" >> "$results_file"
    
    # Generate summary
    {
        echo "========================================"
        echo "$(get_variant_name "$variant") Summary"
        echo "========================================"
        echo "Total queries executed: $total_queries"
        echo "Successful queries: $successful_queries"
        echo "Failed queries: $failed_queries"
        
        if [[ $successful_queries -gt 0 ]]; then
            local avg_time=$(echo "scale=4; $total_time / $successful_queries" | bc -l | tr -d '\n\r')
            local min_time=$(grep -v "FAILED" "$timings_file" | tail -n +2 | cut -d',' -f3 | sort -n | head -1 | tr -d '\n\r')
            local max_time=$(grep -v "FAILED" "$timings_file" | tail -n +2 | cut -d',' -f3 | sort -n | tail -1 | tr -d '\n\r')
            
            echo "Total execution time: ${total_time}s"
            echo "Average execution time: ${avg_time}s"
            echo "Min execution time: ${min_time}s"
            echo "Max execution time: ${max_time}s"
        fi
        
        echo ""
        echo "Data source: $(if [[ "$USE_TPCH_CONNECTOR" == true ]]; then echo "Built-in TPCH connector ($SCHEMA_NAME)"; else echo "Hive connector ($DATA_DIR)"; fi)"
        
        if [[ "$ENABLE_PROFILING" == true ]] && [[ "$variant" == "native-gpu" ]]; then
            echo "GPU profiling: Enabled"
            local nsys_files
            nsys_files=$(find "$variant_dir" -name "*.nsys-rep" | wc -l)
            echo "Nsys profile files generated: $nsys_files"
        fi
        
    } > "$summary_file"
    
    print_success "$(get_variant_name "$variant") benchmark completed"
    cat "$summary_file"
    
    stop_presto_variant "$variant"
    
    # Restore original connector setting
    USE_TPCH_CONNECTOR="$original_tpch_connector"
    
    if [[ ${failed_queries:-0} -gt 0 ]] && [[ ${failed_queries:-0} -eq $total_queries ]]; then
        print_error "Failed benchmark for $(get_variant_name "$variant")"
        return 1
    fi
    
    return 0
}

# Generate overall summary
generate_overall_summary() {
    local summary_file="${RESULTS_DIR}/benchmark_summary.txt"
    
    {
        echo "========================================"
        echo "Presto Deployment Variants Benchmark"
        echo "Summary Report"
        echo "========================================"
        echo "Timestamp: $(date)"
        echo "Results directory: $RESULTS_DIR"
        echo ""
        echo "Configuration:"
        echo "- Variants tested: ${VARIANTS_TO_RUN[*]}"
        echo "- Queries: ${QUERIES_TO_RUN[*]}"
        echo "- Runs per query: $NUM_RUNS"
        echo "- Timeout: $TIMEOUT_SECONDS seconds"
        echo "- Schema: $SCHEMA_NAME"
        echo "- Data directory: $DATA_DIR"
        echo "- Profiling enabled: $ENABLE_PROFILING"
        echo ""
        
        for variant in "${VARIANTS_TO_RUN[@]}"; do
            local variant_summary="${RESULTS_DIR}/${variant}/summary.txt"
            if [[ -f "$variant_summary" ]]; then
                echo "✅ $(get_variant_name "$variant"):"
                grep -E "(Total queries|Successful|Failed|Average|Data source)" "$variant_summary" | sed 's/^/   /'
            else
                echo "❌ $(get_variant_name "$variant"): No results found"
            fi
            echo ""
        done
        
        echo "Detailed results available in:"
        for variant in "${VARIANTS_TO_RUN[@]}"; do
            if [[ -d "${RESULTS_DIR}/${variant}" ]]; then
                echo "- ${variant}: ${RESULTS_DIR}/${variant}/"
            fi
        done
        
    } > "$summary_file"
    
    print_status "Overall summary saved to: $summary_file"
}

# This function is now replaced by setup_hive_tables.py script
# which uses the test_utils create_tables() functionality

# Main execution
main() {
    echo "============================================"
    echo "Presto Deployment Variants Benchmark"
    echo "============================================"
    
    # Create results directory
    mkdir -p "$RESULTS_DIR"
    
    print_status "Results will be saved to: $RESULTS_DIR"
    
    # Detect Docker Compose command
    local docker_compose_cmd
    docker_compose_cmd=$(get_docker_compose_cmd)
    print_status "Using Docker Compose command: $docker_compose_cmd"
    
    # Get machine configuration
    get_machine_config "${RESULTS_DIR}/machine_config.txt"
    print_status "Machine configuration logged"
    
    # Check data directory and determine connector
    check_data_directory
    
    # Benchmark each variant
    for variant in "${VARIANTS_TO_RUN[@]}"; do
        if ! benchmark_variant "$variant"; then
            print_warning "Variant $variant failed, continuing with others..."
        fi
    done
    
    print_status "Generating overall benchmark summary..."
    generate_overall_summary
    
    echo ""
    echo "============================================"
    echo "Benchmark Completed Successfully!"
    echo "============================================"
    print_status "Results available in: $RESULTS_DIR"
    print_status "Overall summary: ${RESULTS_DIR}/benchmark_summary.txt"
}

# Parse arguments and run
parse_arguments "$@"
validate_arguments
setup_dependencies
main
