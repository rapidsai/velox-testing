#!/bin/bash
# TPC-H Benchmark Library for Velox
# This file contains TPC-H specific benchmark utilities used by benchmark_velox.sh

# TPC-H specific constants
TPCH_REQUIRED_TABLES=("customer" "lineitem" "nation" "orders" "part" "partsupp" "region" "supplier")

get_tpch_help() {
  cat <<EOF
TPC-H Examples:
  \$(basename "\$0")                                       # Run all TPC-H queries on CPU and GPU (defaults)
  \$(basename "\$0") --queries 6 --device-type cpu        # Run TPC-H Q6 on CPU only
  \$(basename "\$0") --queries "1 6" --device-type "cpu gpu"  # Run Q1 and Q6 on both CPU and GPU
  \$(basename "\$0") --queries 6 --device-type gpu --profile true  # Run Q6 on GPU with profiling
  \$(basename "\$0") --queries 6 --device-type gpu -o /tmp/results  # Custom output directory
  \$(basename "\$0") --queries 6 --device-type cpu --data-dir /path/to/data  # Custom data directory

TPC-H Data Requirements:
    
  The data must use the Hive-style directory structure with at least one parquet file per table.

TPC-H Build Requirements:
  - Velox must be built with benchmarks enabled: ./build_velox.sh --benchmarks true
  - For profiling support, nsys is automatically installed when benchmarks are enabled
EOF
}

check_tpch_data() {
  local data_dir="$1"
  
  TPCH_DATA_PATH="$data_dir/tpch"
  
  if [[ ! -d "$TPCH_DATA_PATH" ]]; then
    echo "ERROR: TPC-H data directory not found at $TPCH_DATA_PATH" >&2
    echo "Please ensure TPC-H data exists in a directory named 'tpch'" >&2
    exit 1
  fi
  
  echo "Found TPC-H data directory: $TPCH_DATA_PATH"
  
  # Validate Hive directory structure
  echo "Validating TPC-H Hive directory structure..."
  
  local missing_tables=0
  
  for table in "${TPCH_REQUIRED_TABLES[@]}"; do
    # Check for Hive-style directory structure: table/*.parquet
    if [[ ! -d "$TPCH_DATA_PATH/${table}" ]]; then
      echo "ERROR: Required TPC-H table directory '$TPCH_DATA_PATH/${table}/' not found." >&2
      missing_tables=1
    else
      # Check for parquet files in the directory and subdirectories (for partitioned data)
      local parquet_files=()
      while IFS= read -r -d '' file; do
        parquet_files+=("$file")
      done < <(find "$TPCH_DATA_PATH/${table}" -name "*.parquet" -type f -print0 2>/dev/null)
      
      if [[ ${#parquet_files[@]} -eq 0 ]]; then
        echo "ERROR: No parquet files found in '$TPCH_DATA_PATH/${table}/' directory (including subdirectories)." >&2
        missing_tables=1
      else
        local parquet_count=${#parquet_files[@]}
        # Check if partitioned (has subdirectories with parquet files)
        local direct_files=("$TPCH_DATA_PATH/${table}"/*.parquet)
        if [[ -f "${direct_files[0]}" ]]; then
          echo "  $table table directory contains $parquet_count parquet file(s)"
        else
          echo "  $table table directory contains $parquet_count parquet file(s) in partitioned subdirectories"
        fi
      fi
    fi
  done
  
  if [[ "$missing_tables" -ne 0 ]]; then
    echo "ERROR: TPC-H Hive directory validation failed." >&2
    echo "Expected Hive directory structure with at least one parquet file per table:" >&2
    echo "  $TPCH_DATA_PATH/customer/*.parquet (or in subdirectories)" >&2
    echo "  $TPCH_DATA_PATH/lineitem/*.parquet (or in subdirectories)" >&2
    echo "  $TPCH_DATA_PATH/nation/*.parquet (or in subdirectories)" >&2
    echo "  $TPCH_DATA_PATH/orders/*.parquet (or in subdirectories)" >&2
    echo "  $TPCH_DATA_PATH/part/*.parquet (or in subdirectories)" >&2
    echo "  $TPCH_DATA_PATH/partsupp/*.parquet (or in subdirectories)" >&2
    echo "  $TPCH_DATA_PATH/region/*.parquet (or in subdirectories)" >&2
    echo "  $TPCH_DATA_PATH/supplier/*.parquet (or in subdirectories)" >&2
    echo "" >&2
    echo "Each table directory can contain one or multiple parquet files in the directory itself or in partitioned subdirectories." >&2
    echo "Examples of supported patterns:" >&2
    echo "  - Single file: customer/customer.parquet" >&2
    echo "  - Multiple files: lineitem/part-00000.parquet, lineitem/part-00001.parquet, ..." >&2
    echo "  - Partitioned: orders/year=1992/part-00000.parquet, orders/year=1993/part-00000.parquet, ..." >&2
    echo "  - Multi-partition: customer/region=AMERICA/part-00000.parquet, customer/region=EUROPE/part-00001.parquet, ..." >&2
    exit 1
  fi
  
  echo "TPC-H benchmark data verification passed"
  echo "Data directory: $TPCH_DATA_PATH"
}

get_tpch_benchmark_executable_path() {
  local device_type="$1"
  case "$device_type" in
    "cpu")
      echo "/opt/velox-build/release/velox/benchmarks/tpch/velox_tpch_benchmark"
      ;;
    "gpu")
      echo "/opt/velox-build/release/velox/experimental/cudf/benchmarks/velox_cudf_tpch_benchmark"
      ;;
  esac
}

run_tpch_single_benchmark() {
  local query_number="$1"
  local device_type="$2"
  local profile="$3"
  local data_path="$4"
  local run_in_container_func="$5"
  
  printf -v query_number_padded '%02d' "$query_number"
  
  # Set device-specific parameters  
  case "$device_type" in
    "cpu")
      num_drivers=${NUM_DRIVERS:-32}
      BENCHMARK_EXECUTABLE="$(get_tpch_benchmark_executable_path "$device_type")"
      CUDF_FLAGS=""
      VELOX_CUDF_ENABLED=false
      ;;
    "gpu")
      num_drivers=${NUM_DRIVERS:-4}
      cudf_chunk_read_limit=$((1024 * 1024 * 1024 * 1))
      cudf_pass_read_limit=0
      BENCHMARK_EXECUTABLE="$(get_tpch_benchmark_executable_path "$device_type")"
      CUDF_FLAGS="--cudf_chunk_read_limit=${cudf_chunk_read_limit} --cudf_pass_read_limit=${cudf_pass_read_limit}"
      VELOX_CUDF_ENABLED=true
      ;;
  esac
  
  # Common benchmark settings
  output_batch_rows=${BATCH_SIZE_ROWS:-100000}
  VELOX_CUDF_MEMORY_RESOURCE="async"
  
  echo "Running query ${query_number_padded} on ${device_type} with ${num_drivers} drivers."
  
  # Set up profiling if requested
  PROFILE_CMD=""
  if [[ "$profile" == "true" ]]; then
    # Check if nsys is available before setting up profiling
    if $run_in_container_func "which nsys" &>/dev/null; then
      PROFILE_CMD="nsys profile \
        -t nvtx,cuda,osrt \
        -f true \
        --cuda-memory-usage=true \
        --cuda-um-cpu-page-faults=true \
        --cuda-um-gpu-page-faults=true \
        --output=benchmark_results/q${query_number_padded}_${device_type}_${num_drivers}_drivers.nsys-rep"
    else
      echo "WARNING: nsys not found in container. Profiling disabled." >&2
      echo "         To enable profiling, rebuild with: ./build_velox.sh --benchmarks" >&2
    fi
  fi
  
  # Execute benchmark using velox-benchmark service (volumes and environment pre-configured)
  $run_in_container_func 'bash -c "
      '"${PROFILE_CMD}"' \
        '"${BENCHMARK_EXECUTABLE}"' \
        --data_path=/workspace/velox/velox-tpch-data \
        --data_format=parquet \
        --run_query_verbose='"${query_number_padded}"' \
        --num_repeats=1 \
        --velox_cudf_enabled='"${VELOX_CUDF_ENABLED}"' \
        --velox_cudf_memory_resource='"${VELOX_CUDF_MEMORY_RESOURCE}"' \
        --num_drivers='"${num_drivers}"' \
        --preferred_output_batch_rows='"${output_batch_rows}"' \
        --max_output_batch_rows='"${output_batch_rows}"' \
        '"${CUDF_FLAGS}"' 2>&1 | \
        tee benchmark_results/q'"${query_number_padded}"'_'"${device_type}"'_'"${num_drivers}"'_drivers
    "'
}

get_tpch_default_queries() {
  echo "$(seq 1 22)"
}

check_tpch_benchmark_executable_with_path() {
  local benchmark_executable="$1"
  local run_in_container_func="$2"
  local error_msg_hint="$3"
  
  if ! $run_in_container_func "test -f ${benchmark_executable}" 2>/dev/null; then
    echo "ERROR: TPC-H benchmark executable not found at ${benchmark_executable}" >&2
    echo "$error_msg_hint" >&2
    exit 1
  fi
} 


check_tpch_benchmark_executable() {
    local run_in_container_func="$1"
    local device_type="${2:-cpu gpu}"  # Default to both if not specified
    
    # Always check the CPU benchmark executable
    check_tpch_benchmark_executable_with_path \
        "$(get_tpch_benchmark_executable_path "cpu")" \
        "$run_in_container_func" \
        "Please rebuild Velox with benchmarks enabled by running: ./build_velox.sh --benchmarks true" 
    
    # Only check CUDF executable if GPU is requested
    if [[ "$device_type" == *"gpu"* ]]; then
        check_tpch_benchmark_executable_with_path "$(get_tpch_benchmark_executable_path "gpu")" \
         "$run_in_container_func" \
         "Please rebuild Velox with GPU support and benchmarks enabled by running: ./build_velox.sh --gpu --benchmarks true"
    fi
}

