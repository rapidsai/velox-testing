#!/bin/bash

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

set -e

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

This script runs ASV (Airspeed Velocity) benchmarks for TPC-H queries against Presto GPU worker.
ASV tracks performance over time and can detect regressions across commits.

OPTIONS:
    -h, --help              Show this help message.
    -s, --schema-name       Schema(s) containing TPC-H tables. Can be comma-separated for multiple schemas.
                            Example: -s bench_sf1 or -s "bench_sf1,bench_sf10"
                            Default: bench_sf1
    --hostname              Hostname of the Presto coordinator. Default: localhost
    --port                  Port number of the Presto coordinator. Default: 8080
    -u, --user              User who queries will be executed as. Default: test_user
    -b, --benchmark         Specific benchmark pattern to run (e.g., "TPCHQ01", "TPCHQ.*").
                            By default, all TPC-H benchmarks are run.
    --quick                 Run in quick mode (fewer iterations, faster results).
    --profile               Generate profiling data for each benchmark.
    --show-stderr           Show stderr output from benchmarks. Default: false
    --publish               Publish results to ASV HTML dashboard after running.
    --preview               Launch interactive preview server to view results in browser.

EXAMPLES:
    # Run for single schema
    $0 -s bench_sf1

    # Run for multiple schemas (creates separate result files for comparison)
    $0 -s "bench_sf1,bench_sf10"

    # Run specific query for multiple schemas
    $0 -s "bench_sf1,bench_sf10" -b TPCHQ01

    # Run in quick mode and preview results
    $0 -s "bench_sf1,bench_sf10" --quick --preview

    # Run with custom Presto coordinator
    $0 -s bench_sf100 --hostname gpu-node-01 --port 8080

EOF
}

parse_args() { 
  while [[ $# -gt 0 ]]; do
    case $1 in
      -h|--help)
        print_help
        exit 0
        ;;
      -s|--schema-name)
        if [[ -n $2 ]]; then
          SCHEMA_NAME=$2
          shift 2
        else
          echo "Error: --schema-name requires a value"
          exit 1
        fi
        ;;
      --hostname)
        if [[ -n $2 ]]; then
          HOST_NAME=$2
          shift 2
        else
          echo "Error: --hostname requires a value"
          exit 1
        fi
        ;;
      --port)
        if [[ -n $2 ]]; then
          PORT=$2
          shift 2
        else
          echo "Error: --port requires a value"
          exit 1
        fi
        ;;
      -u|--user)
        if [[ -n $2 ]]; then
          USER_NAME=$2
          shift 2
        else
          echo "Error: --user requires a value"
          exit 1
        fi
        ;;
      -b|--benchmark)
        if [[ -n $2 ]]; then
          BENCHMARK=$2
          shift 2
        else
          echo "Error: --benchmark requires a value"
          exit 1
        fi
        ;;
      --quick)
        QUICK_MODE=true
        shift
        ;;
      --profile)
        PROFILE=true
        shift
        ;;
      --show-stderr)
        SHOW_STDERR=true
        shift
        ;;
      --publish)
        PUBLISH=true
        shift
        ;;
      --preview)
        PREVIEW=true
        shift
        ;;
      *)
        echo "Error: Unknown argument $1"
        print_help
        exit 1
        ;;
    esac
  done
}

parse_args "$@"

# Set defaults
HOST_NAME=${HOST_NAME:-localhost}
PORT=${PORT:-8080}
USER_NAME=${USER_NAME:-test_user}
BENCHMARK=${BENCHMARK:-"TPCH.*"}
SCHEMA_NAME=${SCHEMA_NAME:-"bench_sf1,bench_sf10,bench_sf100"}

# Navigate to presto directory (where asv.conf.json is located)
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
PRESTO_DIR=$(readlink -f "${SCRIPT_DIR}/..")
cd "${PRESTO_DIR}"

echo "=========================================="
echo "ASV Benchmark Configuration"
echo "=========================================="
echo "Schema(s): ${SCHEMA_NAME}"
echo "Hostname:  ${HOST_NAME}"
echo "Port:      ${PORT}"
echo "User:      ${USER_NAME}"
echo "Benchmark: ${BENCHMARK}"
echo "=========================================="
echo ""

# Check if ASV is available (should be in conda)
if ! command -v asv &> /dev/null; then
    echo "ERROR: ASV not found. Make sure conda environment is active."
    echo "Run: conda activate base"
    exit 1
fi

# Check if prestodb is available, install if not
if ! python3 -c "import prestodb" &> /dev/null; then
    echo "Installing prestodb..."
    pip install -q presto-python-client
fi

# Install other requirements if needed
TEST_DIR=$(readlink -f ./testing)
pip install -q -r ${TEST_DIR}/requirements.txt 2>/dev/null || true

# Setup machine info non-interactively (avoid prompts)
MACHINE_NAME=$(hostname)
MACHINE_DIR="asv_results/${MACHINE_NAME}"
MACHINE_JSON="${MACHINE_DIR}/machine.json"
mkdir -p "${MACHINE_DIR}"

if [ ! -f "$MACHINE_JSON" ]; then
    echo "Creating machine configuration..."
    cat > "$MACHINE_JSON" << EOF
{
    "arch": "$(uname -m)",
    "cpu": "$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs)",
    "machine": "${MACHINE_NAME}",
    "num_cpu": "$(nproc)",
    "os": "$(uname -s) $(uname -r)",
    "ram": "$(grep MemTotal /proc/meminfo | awk '{print $2}')",
    "version": 1
}
EOF
    echo "✓ Machine config created: ${MACHINE_JSON}"
else
    echo "✓ Using existing machine config: ${MACHINE_JSON}"
fi

# Get current commit hash for result tracking
COMMIT_HASH=$(cd .. && git rev-parse HEAD)
SHORT_HASH=${COMMIT_HASH:0:8}
echo "Commit hash: ${SHORT_HASH}"
echo ""

# Set common environment variables
export ASV_ENV_HOSTNAME="${HOST_NAME}"
export ASV_ENV_PORT="${PORT}"
export ASV_ENV_USER="${USER_NAME}"

# Function to run benchmarks for a single schema
run_benchmark_for_schema() {
    local SCHEMA=$1
    
    echo "=========================================="
    echo "Running benchmarks for: ${SCHEMA}"
    echo "=========================================="
    
    # Set schema-specific environment variable
    export ASV_ENV_SCHEMA="${SCHEMA}"
    
    echo "Environment:"
    echo "  ASV_ENV_SCHEMA=${ASV_ENV_SCHEMA}"
    echo ""
    
    # Build ASV command
    ASV_CMD="asv run --python=same --record-samples"
    
    if [[ "${SHOW_STDERR}" == "true" ]]; then
        ASV_CMD="${ASV_CMD} --show-stderr"
    fi
    ASV_CMD="${ASV_CMD} --set-commit-hash=${COMMIT_HASH}"
    
    if [[ "${QUICK_MODE}" == "true" ]]; then
        ASV_CMD="${ASV_CMD} --quick"
    fi
    
    if [[ "${PROFILE}" == "true" ]]; then
        ASV_CMD="${ASV_CMD} --profile"
    fi
    
    ASV_CMD="${ASV_CMD} --bench ${BENCHMARK}"
    
    echo "Command: ${ASV_CMD}"
    echo ""
    
    # Run ASV
    ${ASV_CMD}
    
    # Find the generated result file and rename it to include schema
    RESULT_FILE="${MACHINE_DIR}/${SHORT_HASH}-${SCHEMA}.json"
    
    # Find the most recent JSON file that starts with commit hash but doesn't have schema suffix yet
    GENERATED_FILE=""
    for f in ${MACHINE_DIR}/${SHORT_HASH}*.json; do
        if [ -f "$f" ] && [[ "$f" != *"machine.json"* ]] && [[ "$f" != *"-bench_"* ]]; then
            GENERATED_FILE="$f"
            break
        fi
    done
    
    if [ -n "$GENERATED_FILE" ] && [ -f "$GENERATED_FILE" ]; then
        # Update the params in the JSON to include schema and save with unique name
        python3 -c "
import json
with open('${GENERATED_FILE}', 'r') as f:
    data = json.load(f)
data['params']['schema'] = '${SCHEMA}'
data['requirements']['schema'] = '${SCHEMA}'
with open('${RESULT_FILE}', 'w') as f:
    json.dump(data, f, indent=4)
print('✓ Results saved: ${RESULT_FILE}')
"
        # Remove old file if different
        if [ "$GENERATED_FILE" != "$RESULT_FILE" ]; then
            rm -f "$GENERATED_FILE"
        fi
    else
        echo "⚠ Warning: Could not find result file for ${SCHEMA}"
    fi
    
    echo ""
}

# Parse comma-separated schemas, sort them, and run benchmarks for each
IFS=',' read -ra SCHEMA_ARRAY <<< "$SCHEMA_NAME"

# Sort schemas in ascending order
IFS=$'\n' SORTED_SCHEMAS=($(for s in "${SCHEMA_ARRAY[@]}"; do echo "$s" | xargs; done | sort))
unset IFS

echo "Schemas to benchmark (sorted): ${SORTED_SCHEMAS[*]}"
echo ""

for SCHEMA in "${SORTED_SCHEMAS[@]}"; do
    run_benchmark_for_schema "$SCHEMA"
done

echo ""
echo "=========================================="
echo "All Benchmarks Complete!"
echo "=========================================="
echo ""

# List all result files
echo "Result files:"
ls -la ${MACHINE_DIR}/*.json 2>/dev/null | grep -v machine.json || echo "No result files found"
echo ""

# Show latest results
echo "Benchmark Results Summary:"
asv show
echo ""

# Publish to HTML if requested
if [[ "${PUBLISH}" == "true" ]]; then
    echo "Publishing results to HTML dashboard..."
    asv publish
    echo ""
    echo "HTML dashboard generated in: ${PRESTO_DIR}/asv_html"
fi

# Preview results in browser if requested
if [[ "${PREVIEW}" == "true" ]]; then
    echo ""
    echo "Launching preview server on port 8086..."
    echo "Press Ctrl+C to stop the server when done."
    echo ""
    asv preview --port 8086 || true
    echo ""
    echo "Preview server stopped."
    exit 0
fi

echo ""
echo "ASV benchmark run completed successfully!"
echo "Results stored in: ${PRESTO_DIR}/asv_results"
