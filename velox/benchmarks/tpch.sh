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
  local stream_debug="${6:-false}"
  
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
      VELOX_CUDF_FLAGS="--velox_cudf_enabled=true --velox_cudf_memory_resource=async"
      ;;
  esac
  
  # Common benchmark settings
  output_batch_rows=${BATCH_SIZE_ROWS:-100000}
  
  echo "Running query ${query_number_padded} on ${device_type} with ${num_drivers} drivers."
  
  # Set up profiling if requested
  PROFILE_CMD=""
  if [[ "$profile" == "true" ]] || [[ "$stream_debug" == "true" ]]; then
    # Check if nsys is available before setting up profiling
    if $run_in_container_func "which nsys" &>/dev/null; then
      
      # Base nsys configuration
      local nsys_traces="-t nvtx,cuda,osrt"
      local nsys_options="--cuda-memory-usage=true --cuda-um-cpu-page-faults=true --cuda-um-gpu-page-faults=true"
      
      # Enhanced stream debugging configuration
      if [[ "$stream_debug" == "true" ]]; then
        echo "Enabling enhanced stream debugging..."
        # Add detailed CUDA API tracing for stream analysis
        nsys_traces="-t nvtx,cuda,osrt,cudnn,cublas"
        nsys_options="$nsys_options --cuda-graph-trace=node --capture-range=cudaProfilerApi --capture-range-end=stop"
        # Increase sample rate for better stream timing resolution
        nsys_options="$nsys_options --sample=cpu --cpuctxsw=process-tree --backtrace=dwarf"
        # Export additional data for analysis
        nsys_options="$nsys_options --export=sqlite"
      fi
      
      PROFILE_CMD="nsys profile \
        $nsys_traces \
        -f true \
        $nsys_options \
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
  $run_in_container_func 'bash -c "
      set -exuo pipefail
      BASE_FILENAME=\"benchmark_results/q'"${query_number_padded}"'_'"${device_type}"'_'"${num_drivers}"'_drivers\"
      '"${PROFILE_CMD}"' \
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
    "'

  EXIT_CODE=$?
  if [[ $EXIT_CODE -ne 0 ]]; then
    return $EXIT_CODE
  fi

  # Post-process nsys data if stream debugging is enabled
  if [[ "$stream_debug" == "true" ]]; then
    echo "Post-processing stream debug data..."
    echo "Analysis files will be saved to host directory: $(realpath ${BENCHMARK_RESULTS_OUTPUT:-./benchmark-results})"
    
    # Generate stream analysis using nsys stats
    $run_in_container_func 'bash -c "
      USER_ID=${USER_ID}
      GROUP_ID=${GROUP_ID}
      PROFILE_FILE=\"benchmark_results/q'"${query_number_padded}"'_'"${device_type}"'_'"${num_drivers}"'_drivers.nsys-rep\"
      ANALYSIS_FILE=\"benchmark_results/q'"${query_number_padded}"'_'"${device_type}"'_'"${num_drivers}"'_drivers_stream_analysis.txt\"
      
      if [ -f \"\$PROFILE_FILE\" ]; then
        echo \"Generating stream analysis for \$PROFILE_FILE...\"
        
        # Create comprehensive stream analysis
        {
          echo \"CUDA Stream Analysis Report\"
          echo \"===========================\"
          echo \"Profile: \$PROFILE_FILE\"
          echo \"Query: Q'"${query_number_padded}"'\"
          echo \"Device: '"${device_type}"'\"
          echo \"Drivers: '"${num_drivers}"'\"
          echo \"Generated: \$(date)\"
          echo \"\"
          
          echo \"CUDA API Summary:\"
          echo \"=================\"
          nsys stats --report cuda_api_sum \"\$PROFILE_FILE\" 2>/dev/null || echo \"CUDA API stats not available\"
          echo \"\"
          
          echo \"CUDA Stream Operations:\"
          echo \"======================\"
          nsys stats --report cuda_api_trace --format csv \"\$PROFILE_FILE\" 2>/dev/null | \
            grep -E \"(Stream|Event|Synchronize)\" | head -50 || echo \"Stream operations not available\"
          echo \"\"
          
          echo \"CUDA Kernel Execution:\"
          echo \"=====================\"
          nsys stats --report cuda_gpu_kern_sum \"\$PROFILE_FILE\" 2>/dev/null || echo \"Kernel stats not available\"
          echo \"\"
          
          echo \"Memory Operations:\"
          echo \"=================\"
          nsys stats --report cuda_gpu_mem_time_sum \"\$PROFILE_FILE\" 2>/dev/null || echo \"Memory stats not available\"
          
        } > \"\$ANALYSIS_FILE\"
        
        chown \"\${USER_ID}:\${GROUP_ID}\" \"\$ANALYSIS_FILE\"
        echo \"Stream analysis saved to: \$ANALYSIS_FILE\"
        echo \"(Host path: \$ANALYSIS_FILE)\"
      else
        echo \"WARNING: Profile file not found: \$PROFILE_FILE\"
      fi
    "'
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

