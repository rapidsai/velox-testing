#!/bin/bash
# TPC-H Benchmark Library for Velox
# This file contains all TPC-H specific logic and functions

# TPC-H specific constants
TPCH_REQUIRED_TABLES=("customer" "lineitem" "nation" "orders" "part" "partsupp" "region" "supplier")

get_tpch_help() {
  cat <<EOF
TPC-H Options:
  --fix-metadata  Automatically generate/fix TPC-H metadata files with correct container paths

TPC-H Examples:
  \$(basename "\$0")                          # Run all TPC-H queries on CPU and GPU
  \$(basename "\$0") tpch 6 cpu               # Run TPC-H Q6 on CPU only
  \$(basename "\$0") tpch "1 6" "cpu gpu"    # Run TPC-H Q1 and Q6 on both CPU and GPU
  \$(basename "\$0") tpch 6 gpu true         # Run TPC-H Q6 on GPU with profiling
  \$(basename "\$0") tpch 6 gpu false --fix-metadata  # Auto-fix TPC-H metadata files
  \$(basename "\$0") tpch 6 gpu true --benchmark-results-output /tmp/results  # Custom output dir

TPC-H Data Requirements:
  The TPC-H data directory must contain:
  - *.parquet files: The actual data tables (customer.parquet, lineitem.parquet, etc.)
  - Metadata files: Small text files with same names as tables (customer, lineitem, etc.)
    that contain the full path to the corresponding .parquet file within the container.
    
  Example metadata file content for 'customer':
    /workspace/velox/velox-tpch-data/customer.parquet
    
  Note: Use --fix-metadata to automatically generate/fix these files.

TPC-H Build Requirements:
  - Velox must be built with benchmarks enabled: ./build_velox.sh --benchmarks
  - For profiling support, nsys is automatically installed when benchmarks are enabled
EOF
}

check_tpch_data() {
  local data_dir="$1"
  local fix_metadata="$2"
  
  TPCH_DATA_PATH="$data_dir/tpch"
  
  if [[ ! -d "$TPCH_DATA_PATH" ]]; then
    echo "ERROR: TPC-H data directory not found at $TPCH_DATA_PATH" >&2
    echo "Please ensure TPC-H data exists in a directory named 'tpch'" >&2
    exit 1
  fi
  
  echo "Found TPC-H data directory: $TPCH_DATA_PATH"
  
  # Check for required TPC-H tables
  local missing_tables=0
  
  for table in "${TPCH_REQUIRED_TABLES[@]}"; do
    # Check for .parquet file
    if [[ ! -f "$TPCH_DATA_PATH/${table}.parquet" ]]; then
      echo "ERROR: Required TPC-H table '$TPCH_DATA_PATH/${table}.parquet' not found." >&2
      missing_tables=1
    fi
  done
  
  if [[ "$missing_tables" -ne 0 ]]; then
    echo "ERROR: Missing TPC-H data files in $TPCH_DATA_PATH" >&2
    echo "Please ensure all TPC-H tables are available as .parquet files" >&2
    exit 1
  fi
  
  # Fix metadata files if requested, before validation
  if [[ "$fix_metadata" == "true" ]]; then
    echo "Fixing TPC-H metadata files..."
    if ! fix_tpch_metadata_files "$TPCH_DATA_PATH"; then
      echo "ERROR: Failed to fix TPC-H metadata files. Exiting." >&2
      exit 1
    fi
    echo ""
  fi
  
  # Validate metadata files
  validate_tpch_metadata_files "$TPCH_DATA_PATH"
  
  echo "TPC-H benchmark data verification passed"
  echo "Data directory: $TPCH_DATA_PATH"
}

validate_tpch_metadata_files() {
  local data_path="$1"
  local container_data_path="/workspace/velox/velox-tpch-data"
  
  echo "Validating TPC-H metadata files..."
  
  local errors=0
  
  for table in "${TPCH_REQUIRED_TABLES[@]}"; do
    local metadata_file="$data_path/${table}"
    local expected_content="${container_data_path}/${table}.parquet"
    
    if [[ ! -f "$metadata_file" ]]; then
      echo "ERROR: Missing TPC-H metadata file '$metadata_file'" >&2
      errors=1
    else
      local current_content
      current_content=$(cat "$metadata_file" 2>/dev/null | tr -d '\n\r ')
      
      if [[ "$current_content" != "$expected_content" ]]; then
        echo "ERROR: TPC-H metadata file '$metadata_file' has incorrect path:" >&2
        echo "  Current: '$current_content'" >&2
        echo "  Expected: '$expected_content'" >&2
        errors=1
      else
        echo "  $table metadata file is correct"
      fi
    fi
  done
  
  if [[ "$errors" -ne 0 ]]; then
    echo "" >&2
    echo "ERROR: TPC-H metadata file validation failed." >&2
    echo "Use --fix-metadata to automatically generate/fix TPC-H metadata files." >&2
    exit 1
  fi
}

fix_tpch_metadata_files() {
  local data_path="$1"
  local container_data_path="/workspace/velox/velox-tpch-data"
  
  local fixed_count=0
  
  for table in "${TPCH_REQUIRED_TABLES[@]}"; do
    local metadata_file="$data_path/${table}"
    local expected_content="${container_data_path}/${table}.parquet"
    
    if [[ ! -f "$metadata_file" ]]; then
      echo "  Generating $table metadata file"
      if echo "$expected_content" > "$metadata_file" 2>/dev/null; then
        ((fixed_count++))
      else
        echo "ERROR: Failed to write $metadata_file" >&2
        return 1
      fi
    else
      local current_content
      current_content=$(cat "$metadata_file" 2>/dev/null | tr -d '\n\r ')
      
      if [[ "$current_content" != "$expected_content" ]]; then
        echo "  Fixing $table metadata file (was: '$current_content')"
        if echo "$expected_content" > "$metadata_file" 2>/dev/null; then
          ((fixed_count++))
        else
          echo "ERROR: Failed to update $metadata_file" >&2
          return 1
        fi
      else
        echo "  $table metadata file is already correct"
      fi
    fi
  done
  
  echo "Fixed/generated $fixed_count TPC-H metadata files"
  return 0
}

run_tpch_single_benchmark() {
  local query_number="$1"
  local device="$2"
  local profile="$3"
  local data_path="$4"
  local run_in_container_func="$5"
  
  printf -v query_number_padded '%02d' "$query_number"
  
  # Set device-specific parameters  
  case "$device" in
    "cpu")
      num_drivers=${NUM_DRIVERS:-32}
      BENCHMARK_EXECUTABLE="/opt/velox-build/release/velox/benchmarks/tpch/velox_tpch_benchmark"
      CUDF_FLAGS=""
      VELOX_CUDF_ENABLED=false
      ;;
    "gpu")
      num_drivers=${NUM_DRIVERS:-4}
      cudf_chunk_read_limit=$((1024 * 1024 * 1024 * 1))
      cudf_pass_read_limit=0
      BENCHMARK_EXECUTABLE="/opt/velox-build/release/velox/experimental/cudf/benchmarks/velox_cudf_tpch_benchmark"
      CUDF_FLAGS="--cudf_chunk_read_limit=${cudf_chunk_read_limit} --cudf_pass_read_limit=${cudf_pass_read_limit}"
      VELOX_CUDF_ENABLED=true
      ;;
  esac
  
  # Common benchmark settings
  output_batch_rows=${BATCH_SIZE_ROWS:-100000}
  VELOX_CUDF_MEMORY_RESOURCE="async"
  
  echo "Running query ${query_number_padded} on ${device} with ${num_drivers} drivers."
  
  # Set up profiling if requested
  PROFILE_CMD=""
  if [[ "$profile" == "true" ]]; then
    # Check if nsys is available before setting up profiling
    if $run_in_container_func "which nsys" &>/dev/null; then
      PROFILE_CMD="nsys profile -t nvtx,cuda,osrt -f true --cuda-memory-usage=true --cuda-um-cpu-page-faults=true --cuda-um-gpu-page-faults=true --output=benchmark_results/q${query_number_padded}_${device}_${num_drivers}_drivers.nsys-rep"
    else
      echo "WARNING: nsys not found in container. Profiling disabled." >&2
      echo "         To enable profiling, rebuild with: ./build_velox.sh --benchmarks" >&2
    fi
  fi
  
  # Volume mount for data
  VOLUME_MOUNT="-v $data_path:/workspace/velox/velox-tpch-data:ro"
  
  $run_in_container_func 'bash -c "
      export LD_LIBRARY_PATH=/opt/velox-build/release/lib:/opt/velox-build/release/_deps/cudf-build:/opt/velox-build/release/_deps/rapids_logger-build:/opt/velox-build/release/_deps/kvikio-build:/opt/velox-build/release/_deps/nvcomp_proprietary_binary-src/lib64
      cd /workspace/velox
      set +e +u -x
      '"${PROFILE_CMD}"' \
        '"${BENCHMARK_EXECUTABLE}"' \
        --data_path=velox-tpch-data \
        --data_format=parquet \
        --run_query_verbose='"${query_number_padded}"' \
        --num_repeats=1 \
        --velox_cudf_enabled='"${VELOX_CUDF_ENABLED}"' \
        --velox_cudf_memory_resource='"${VELOX_CUDF_MEMORY_RESOURCE}"' \
        --num_drivers='"${num_drivers}"' \
        --preferred_output_batch_rows='"${output_batch_rows}"' \
        --max_output_batch_rows='"${output_batch_rows}"' \
        '"${CUDF_FLAGS}"' 2>&1 | \
        tee benchmark_results/q'"${query_number_padded}"'_'"${device}"'_'"${num_drivers}"'_drivers
      { set -e +x; } &>/dev/null
    "' "$VOLUME_MOUNT"
}

run_tpch_benchmark() {
  local queries="$1"
  local devices="$2" 
  local profile="$3"
  local data_dir="$4"
  local run_in_container_func="$5"
  
  echo "Running TPC-H benchmark..."
  echo "Queries: $queries"
  echo "Devices: $devices"
  echo "Profile: $profile"
  
  TPCH_DATA_PATH="$data_dir/tpch"
  
  # Run benchmarks for each query and device combination
  for query_number in $queries; do
    for device in $devices; do
      run_tpch_single_benchmark "$query_number" "$device" "$profile" "$TPCH_DATA_PATH" "$run_in_container_func"
    done
  done
}

get_tpch_default_queries() {
  echo "$(seq 1 22)"
}

check_tpch_benchmark_executable_with_path() {
  local benchmark_executable="$1"
  local run_in_container_func="$2"
  
  if ! $run_in_container_func "test -f ${benchmark_executable}" 2>/dev/null; then
    echo "ERROR: TPC-H benchmark executable not found at ${benchmark_executable}" >&2
    echo "Please rebuild Velox with benchmarks enabled by running: ./build_velox.sh --benchmarks" >&2
    exit 1
  fi
} 


check_tpch_benchmark_executable() {
    check_tpch_benchmark_executable_with_path "/opt/velox-build/release/velox/experimental/cudf/benchmarks/velox_cudf_tpch_benchmark" "$1"
    check_tpch_benchmark_executable_with_path "/opt/velox-build/release/velox/benchmarks/tpch/velox_tpch_benchmark" "$1"
}


setup_tpch_container_environment() {
  echo "Setting up TPC-H container environment..."
  
  # Create benchmark results directory and clear any existing results
  run_in_container 'rm -rf /workspace/velox/benchmark_results && mkdir -p /workspace/velox/benchmark_results'
  
  echo "TPC-H container environment ready"
}
