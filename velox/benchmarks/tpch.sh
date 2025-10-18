#!/bin/bash
# TPC-H Benchmark Module for Velox
# This file contains TPC-H specific benchmark utilities used by benchmark_velox.sh

# Copyright (c) 2025, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# TPC-H specific constants
TPCH_REQUIRED_TABLES=("customer" "lineitem" "nation" "orders" "part" "partsupp" "region" "supplier")

get_tpch_help() {
  cat <<EOF

TPC-H Data Requirements:
    
  The data must use the Hive-style directory structure with at least one parquet file per table.

TPC-H Build Requirements:
  - Velox must be built with benchmarks enabled: ./build_velox.sh --benchmarks true
  - For profiling support, nsys is automatically installed when benchmarks are enabled
EOF
}


# Check for Hive-style data layout with directory structure containing parquet files
check_tpch_hive_data_layout() {
  local data_dir=$1
      echo "Validating TPC-H Hive-style data layout..."
  
  local missing_tables=0
  
  for table in "${TPCH_REQUIRED_TABLES[@]}"; do
    # Check for Hive-style directory structure: table/*.parquet
    if [[ ! -d "$data_dir/${table}" ]]; then
      echo "ERROR: Required TPC-H table directory '$data_dir/${table}/' not found." >&2
      missing_tables=1
    else
      # Check for parquet files in the directory and subdirectories (for partitioned data)
      local parquet_files=()
      while IFS= read -r -d '' file; do
        parquet_files+=("$file")
      done < <(find "$data_dir/${table}" -name "*.parquet" -type f -print0 2>/dev/null)
      
      if [[ ${#parquet_files[@]} -eq 0 ]]; then
        echo "ERROR: No parquet files found in '$data_dir/${table}/' directory (including subdirectories)." >&2
        missing_tables=1
      else
        local parquet_count=${#parquet_files[@]}
        # Check if partitioned (has subdirectories with parquet files)
        local direct_files=("$data_dir/${table}"/*.parquet)
        if [[ -f "${direct_files[0]}" ]]; then
          echo "  $table table directory contains $parquet_count parquet file(s)"
        else
          echo "  $table table directory contains $parquet_count parquet file(s) in partitioned subdirectories"
        fi
      fi
    fi
  done
  
  if [[ "$missing_tables" -ne 0 ]]; then
    echo "ERROR: TPC-H Hive-style data layout validation failed." >&2
    echo "Expected Hive-style directory structure with at least one parquet file per table:" >&2
    echo "  $data_dir/customer/*.parquet (or in subdirectories)" >&2
    echo "  $data_dir/lineitem/*.parquet (or in subdirectories)" >&2
    echo "  $data_dir/nation/*.parquet (or in subdirectories)" >&2
    echo "  $data_dir/orders/*.parquet (or in subdirectories)" >&2
    echo "  $data_dir/part/*.parquet (or in subdirectories)" >&2
    echo "  $data_dir/partsupp/*.parquet (or in subdirectories)" >&2
    echo "  $data_dir/region/*.parquet (or in subdirectories)" >&2
    echo "  $data_dir/supplier/*.parquet (or in subdirectories)" >&2
    echo "" >&2
    echo "Each table directory can contain one or multiple parquet files in the directory itself or in partitioned subdirectories." >&2
    echo "Examples of supported patterns:" >&2
    echo "  - Single file: customer/customer.parquet" >&2
    echo "  - Multiple files: lineitem/part-00000.parquet, lineitem/part-00001.parquet, ..." >&2
    echo "  - Partitioned: orders/year=1992/part-00000.parquet, orders/year=1993/part-00000.parquet, ..." >&2
    echo "  - Multi-partition: customer/region=AMERICA/part-00000.parquet, customer/region=EUROPE/part-00001.parquet, ..." >&2
    return 1
  fi
  
  return 0
}

# Check if table definition files exist (indicating unsupported file-based table definitions)
check_for_table_definition_files() {
  local data_dir=$1
  local definition_files_found=0
  
  for table in "${TPCH_REQUIRED_TABLES[@]}"; do
    if [[ -f "$data_dir/${table}" ]]; then
      definition_files_found=1
      break
    fi
  done
  
  if [[ $definition_files_found -eq 1 ]]; then
    echo "ERROR: Table definition files detected but not supported." >&2
    echo "" >&2
    echo "Found table definition files (e.g., '$data_dir/${table}') instead of directory structure." >&2
    echo "File-based table definitions are not supported due to Docker container path complexity." >&2
    echo "" >&2
    echo "Please use Hive-style directory structure instead:" >&2
    echo "  $data_dir/customer/*.parquet (directory with parquet files)" >&2
    echo "  $data_dir/lineitem/*.parquet (directory with parquet files)" >&2
    echo "  $data_dir/nation/*.parquet (directory with parquet files)" >&2
    echo "  $data_dir/orders/*.parquet (directory with parquet files)" >&2
    echo "  $data_dir/part/*.parquet (directory with parquet files)" >&2
    echo "  $data_dir/partsupp/*.parquet (directory with parquet files)" >&2
    echo "  $data_dir/region/*.parquet (directory with parquet files)" >&2
    echo "  $data_dir/supplier/*.parquet (directory with parquet files)" >&2
    return 1
  fi
  
  return 0
}

check_tpch_data() {
  local data_dir=$1
  
  if [[ ! -d "$data_dir" ]]; then
    echo "ERROR: TPC-H data directory not found at $data_dir" >&2
    exit 1
  fi
  
  echo "Found TPC-H data directory: $data_dir"
  
  # Check for unsupported table definition files first
  if ! check_for_table_definition_files "$data_dir"; then
    exit 1
  fi
  
  # Validate Hive-style data layout
  if check_tpch_hive_data_layout "$data_dir"; then
    echo "TPC-H Hive-style data layout validation passed"
  else
    exit 1
  fi

}

get_tpch_benchmark_executable_path() {
  local device_type="$1"
  case "$device_type" in
    "cpu")
      echo "/opt/velox-build/${BUILD_TYPE}/velox/benchmarks/tpch/velox_tpch_benchmark"
      ;;
    "gpu")
      echo "/opt/velox-build/${BUILD_TYPE}/velox/experimental/cudf/benchmarks/velox_cudf_tpch_benchmark"
      ;;
  esac
}

# Enables GPU metrics collection in nsys profiling if supported
# Requires GPU compute capability > 7  and nvidia-smi availability
# Modifies the profile command variable passed by reference to include --gpu-metrics-devices
setup_gpu_metrics_profiling_if_supported() {
  local run_in_container_func="$1"
  local -n profile_cmd_ref=$2  
  
  # Check GPU compute capability (>7 required for metrics)
  if $run_in_container_func "nvidia-smi --query-gpu=compute_cap --format=csv,noheader -i 0 2>/dev/null | cut -d '.' -f 1" | awk '{if ($1 > 7) exit 0; else exit 1}'; then
    local device_id=${CUDA_VISIBLE_DEVICES:-"all"}
    profile_cmd_ref="${profile_cmd_ref} --gpu-metrics-devices=${device_id}"
    echo "GPU metrics enabled for device ${device_id}"
    return 0
  else
    return 1
  fi
}

run_tpch_single_benchmark() {
  local query_number="$1"
  local device_type="$2"
  local profile="$3"
  local run_in_container_func="$4"
  local num_repeats="$5"
  local verbose_logging="${6:-false}"
  local call_site_collection="${7:-false}"
  local sync_call_sites_file="${8:-}"
  local bisection_midpoint="${9:-}"
  local bisection_total_rows="${10:-}"
  
  printf -v query_number_padded '%02d' "$query_number"
  
  # Set device-specific parameters  
  case "$device_type" in
    "cpu")
      num_drivers=${NUM_DRIVERS:-32}
      BENCHMARK_EXECUTABLE="$(get_tpch_benchmark_executable_path "$device_type")"
      CUDF_FLAGS=""
      VELOX_CUDF_FLAGS=""
      ;;
    "gpu")
      num_drivers=${NUM_DRIVERS:-4}
      cudf_chunk_read_limit=$((1024 * 1024 * 1024 * 1))
      cudf_pass_read_limit=0
      BENCHMARK_EXECUTABLE="$(get_tpch_benchmark_executable_path "$device_type")"
      CUDF_FLAGS="--cudf_chunk_read_limit=${cudf_chunk_read_limit} --cudf_pass_read_limit=${cudf_pass_read_limit}"
      VELOX_CUDF_FLAGS="--velox_cudf_enabled=true --velox_cudf_memory_resource=${VELOX_CUDF_MEMORY_RESOURCE:-async}"
      ;;
  esac
  
  # Common benchmark settings
  output_batch_rows=${BATCH_SIZE_ROWS:-100000}
  
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

      # Configure GPU metrics collection if supported
      setup_gpu_metrics_profiling_if_supported "$run_in_container_func" PROFILE_CMD

    else
      echo "WARNING: nsys not found in container. Profiling disabled." >&2
      echo "         To enable profiling, rebuild with: ./build_velox.sh --benchmarks" >&2
    fi
  fi
  
  # Execute benchmark using velox-benchmark service (volumes and environment pre-configured)
  set +e
  
  # Set up verbose logging environment variables if requested
  # Set up verbose logging and bisection search environment variables
  VERBOSE_ENV_PREFIX=""
  
  # Enable verbose logging if requested OR if using bisection modes
  if [[ "$verbose_logging" == "true" || "$call_site_collection" == "true" || -n "$sync_call_sites_file" || (-n "$bisection_midpoint" && -n "$bisection_total_rows") ]]; then
    VERBOSE_ENV_PREFIX="RMM_LOG_FILE=benchmark_results/q${query_number_padded}_${device_type}_${num_drivers}_drivers_rmm.csv RMM_DEBUG_LOG_FILE=benchmark_results/q${query_number_padded}_${device_type}_${num_drivers}_drivers_debug.log RMM_STACK_TRACE_FILE=benchmark_results/q${query_number_padded}_${device_type}_${num_drivers}_drivers_stacktrace.csv"
    
    # Add bisection search environment variables
    if [[ "$call_site_collection" == "true" ]]; then
      echo "Call site collection mode: Syncing ALL deallocation call sites"
      echo "  - Call site IDs will be logged to: benchmark_results/q${query_number_padded}_${device_type}_${num_drivers}_drivers_stacktrace.csv"
    elif [[ -n "$sync_call_sites_file" ]]; then
      # Copy sync file to container accessible location
      sync_file_basename=$(basename "$sync_call_sites_file")
      VERBOSE_ENV_PREFIX="$VERBOSE_ENV_PREFIX RMM_SYNC_CALL_SITES_FILE=/workspace/velox/$sync_file_basename"
      echo "Bisection mode: Syncing specific call sites from: $sync_call_sites_file"
      echo "  - Only call sites listed in file will be synchronized"
      echo "  - To enable debug output, set: export RMM_SYNC_DEBUG=1"
      echo "  - To control stack trace depth, set: export RMM_STACK_TRACE_DEPTH=N (default: 8, max: 32)"
      echo "  - To disable all sync, set: export RMM_SYNC_DISABLE=1"
    fi
    
    # Pass through debug and disable environment variables if set
    if [[ "${RMM_SYNC_DEBUG:-}" == "1" ]]; then
      VERBOSE_ENV_PREFIX="$VERBOSE_ENV_PREFIX RMM_SYNC_DEBUG=1"
      echo "  - DEBUG MODE ENABLED: Will show all sync matches and events"
    fi
    if [[ "${RMM_SYNC_DISABLE:-}" == "1" ]]; then
      VERBOSE_ENV_PREFIX="$VERBOSE_ENV_PREFIX RMM_SYNC_DISABLE=1"
      echo "  - SYNC DISABLED: No synchronization will occur"
    fi
    
    # Pass through stack trace depth if set
    if [[ -n "${RMM_STACK_TRACE_DEPTH:-}" ]]; then
      VERBOSE_ENV_PREFIX="$VERBOSE_ENV_PREFIX RMM_STACK_TRACE_DEPTH=${RMM_STACK_TRACE_DEPTH}"
      echo "  - STACK TRACE DEPTH: Capturing ${RMM_STACK_TRACE_DEPTH} levels"
    else
      echo "  - STACK TRACE DEPTH: Capturing 8 levels (default)"
    fi
    
    # Pass through row-based bisection parameters if set
    if [[ -n "$bisection_midpoint" && -n "$bisection_total_rows" ]]; then
      VERBOSE_ENV_PREFIX="$VERBOSE_ENV_PREFIX RMM_BISECTION_MIDPOINT=${bisection_midpoint} RMM_BISECTION_TOTAL_ROWS=${bisection_total_rows}"
      echo "  - ROW BISECTION: midpoint=${bisection_midpoint}, total_rows=${bisection_total_rows}"
    fi
    
    echo "Verbose logging enabled:"
    echo "  - RMM memory event logging (CSV): benchmark_results/q${query_number_padded}_${device_type}_${num_drivers}_drivers_rmm.csv"
    echo "  - RMM debug logging: benchmark_results/q${query_number_padded}_${device_type}_${num_drivers}_drivers_debug.log"
    echo "  - RMM call site logging: benchmark_results/q${query_number_padded}_${device_type}_${num_drivers}_drivers_stacktrace.csv"
  fi
  
  # Copy sync call sites file to container if needed
  if [[ -n "$sync_call_sites_file" ]]; then
    sync_file_basename=$(basename "$sync_call_sites_file")
    echo "Copying sync call sites file to container: $sync_call_sites_file -> /workspace/velox/$sync_file_basename"
    echo "Local file contents:"
    cat "$sync_call_sites_file"
    echo "---"
    
    # Use here-document approach to copy file contents
    sync_file_content=$(cat "$sync_call_sites_file")
    $run_in_container_func "cat > /workspace/velox/$sync_file_basename << 'EOF'
$sync_file_content
EOF"
    
    # Verify the file was created successfully
    if $run_in_container_func "test -f /workspace/velox/$sync_file_basename"; then
      echo "Successfully copied sync call sites file to container"
      echo "Container file contents:"
      $run_in_container_func "cat /workspace/velox/$sync_file_basename"
      echo "Container file permissions:"
      $run_in_container_func "ls -la /workspace/velox/$sync_file_basename"
    else
      echo "ERROR: Failed to copy sync call sites file to container" >&2
      return 1
    fi
  fi

  $run_in_container_func 'bash -c "
      set -exuo pipefail
      BASE_FILENAME=\"benchmark_results/q'"${query_number_padded}"'_'"${device_type}"'_'"${num_drivers}"'_drivers\"
      echo \"Starting benchmark with environment: '"${VERBOSE_ENV_PREFIX}"'\"
      echo \"Benchmark executable: '"${BENCHMARK_EXECUTABLE}"'\"
      '"${VERBOSE_ENV_PREFIX}"' '"${PROFILE_CMD}"' \
        '"${BENCHMARK_EXECUTABLE}"' \
        --data_path=/workspace/velox/velox-benchmark-data \
        --data_format=parquet \
        --run_query_verbose='"${query_number_padded}"' \
        --num_repeats='"${num_repeats}"' \
        --num_drivers='"${num_drivers}"' \
        --preferred_output_batch_rows='"${output_batch_rows}"' \
        --max_output_batch_rows='"${output_batch_rows}"' \
        '"${VELOX_CUDF_FLAGS}"' \
        '"${CUDF_FLAGS}"' 2>&1 | \
        tee \"\$BASE_FILENAME\"
      chown \"${USER_ID}:${GROUP_ID}\" \"\$BASE_FILENAME\"
      NSYS_REP_FILE=\"\${BASE_FILENAME}.nsys-rep\"
      if [ -f \"\$NSYS_REP_FILE\" ]; then
        chown \"${USER_ID}:${GROUP_ID}\" \"\$NSYS_REP_FILE\"
      fi
      RMM_LOG_FILE=\"\${BASE_FILENAME}_rmm.csv\"
      if [ -f \"\$RMM_LOG_FILE\" ]; then
        chown \"${USER_ID}:${GROUP_ID}\" \"\$RMM_LOG_FILE\"
        echo \"RMM memory event log saved to: \$RMM_LOG_FILE\"
      fi
      RMM_DEBUG_LOG_FILE=\"\${BASE_FILENAME}_debug.log\"
      if [ -f \"\$RMM_DEBUG_LOG_FILE\" ]; then
        chown \"${USER_ID}:${GROUP_ID}\" \"\$RMM_DEBUG_LOG_FILE\"
        echo \"RMM debug log saved to: \$RMM_DEBUG_LOG_FILE\"
      fi
      RMM_STACK_TRACE_FILE=\"\${BASE_FILENAME}_stacktrace.csv\"
      if [ -f \"\$RMM_STACK_TRACE_FILE\" ]; then
        chown \"${USER_ID}:${GROUP_ID}\" \"\$RMM_STACK_TRACE_FILE\"
        echo \"RMM stack trace log saved to: \$RMM_STACK_TRACE_FILE\"
      fi
    "'

  EXIT_CODE=$?
  if [[ $EXIT_CODE -ne 0 ]]; then
    return $EXIT_CODE
  fi

  set -e 
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
    
    # Check CPU benchmark executable only if CPU is requested
    if [[ "$device_type" == *"cpu"* ]]; then
        check_tpch_benchmark_executable_with_path \
            "$(get_tpch_benchmark_executable_path "cpu")" \
            "$run_in_container_func" \
            "Please rebuild Velox with benchmarks enabled by running: ./build_velox.sh --benchmarks true" 
    fi
    
    # Check CUDF executable only if GPU is requested
    if [[ "$device_type" == *"gpu"* ]]; then
        check_tpch_benchmark_executable_with_path "$(get_tpch_benchmark_executable_path "gpu")" \
         "$run_in_container_func" \
         "Please rebuild Velox with GPU support and benchmarks enabled by running: ./build_velox.sh --gpu --benchmarks true"
    fi
}

