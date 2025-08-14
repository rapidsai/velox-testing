#!/bin/bash

set -e

# Change to the script's directory to ensure correct relative paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Parse command line arguments for scale factor
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
      echo "Usage: $0 [OPTIONS] [SCALE_FACTOR]"
      echo ""
      echo "Options:"
      echo "  -s, --scale-factor SF    TPC-H scale factor (1, 10, 100)"
      echo "  --all-sf, --all-scale-factors  Load all scale factors (1, 10, 100) simultaneously"
      echo "  -h, --help              Show this help message"
      echo ""
      echo "Scale Factor Shortcuts:"
      echo "  sf1                     Use scale factor 1 (default)"
      echo "  sf10                    Use scale factor 10"
      echo "  sf100                   Use scale factor 100"
      echo ""
      echo "Environment Variables:"
      echo "  RUN_TPCH_BENCHMARK=true Run TPC-H benchmark after startup"
      echo "  TPCH_PARQUET_DIR=path   Use existing TPC-H data directory"
      echo ""
      echo "Note: Specifying a scale factor will automatically run the TPC-H benchmark"
      echo ""
      echo "Examples:"
      echo "  $0                      # Start with SF1 (default), no benchmark"
      echo "  $0 sf10                 # Start with SF10 and run benchmark"
      echo "  $0 -s 100               # Start with SF100 and run benchmark"
      echo "  $0 sf1                  # Start with SF1 and run benchmark"
      echo "  $0 --all-sf             # Load all scale factors and run benchmark"
      echo "  $0 RUN_TPCH_BENCHMARK=true  # Start with SF1 and run benchmark"
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

echo "Starting Presto Native CPU with TPC-H Scale Factor: ${SCALE_FACTOR}"

../stop_presto.sh
../build_centos_deps_image.sh

# Auto-generate TPCH Parquet locally if TPCH_PARQUET_DIR not set or empty
if [[ -z "${TPCH_PARQUET_DIR:-}" ]]; then
  if [[ "$LOAD_ALL_SCALE_FACTORS" == "true" ]]; then
    echo "TPCH_PARQUET_DIR not set; generating all TPC-H Parquet scale factors (1, 10, 100)..."
    bash "../data/generate_tpch_data.sh" -s 1
    bash "../data/generate_tpch_data.sh" -s 10
    bash "../data/generate_tpch_data.sh" -s 100
    export TPCH_PARQUET_DIR="$(cd ../.. && pwd)/docker/data/tpch"
  else
    echo "TPCH_PARQUET_DIR not set; generating local TPCH Parquet (SF=${SCALE_FACTOR})..."
    bash "../data/generate_tpch_data.sh" -s "${SCALE_FACTOR}"
    export TPCH_PARQUET_DIR="$(cd ../.. && pwd)/docker/data/tpch"
  fi
fi

docker compose -f ../../docker/docker-compose.native-cpu.yml build --build-arg NUM_THREADS=$(($(nproc) * 3 / 4)) --progress plain
docker compose -f ../../docker/docker-compose.native-cpu.yml up -d

# If TPCH_PARQUET_DIR is provided, auto-register external TPCH tables in Hive
if [[ -n "${TPCH_PARQUET_DIR}" ]]; then
  if [[ "$LOAD_ALL_SCALE_FACTORS" == "true" ]]; then
    echo "Registering all TPC-H external Parquet tables (SF1, SF10, SF100) from ${TPCH_PARQUET_DIR}..."
    bash "../data/register_tpch_tables.sh" -s 1
    bash "../data/register_tpch_tables.sh" -s 10
    bash "../data/register_tpch_tables.sh" -s 100
  else
    echo "Registering TPCH external Parquet tables from ${TPCH_PARQUET_DIR}..."
    bash "../data/register_tpch_tables.sh"
  fi
fi

# Wait for Presto to be ready and run TPC-H benchmark if requested
# Run benchmark if explicitly requested via environment variable OR if scale factor was specified via command line
if [[ "${RUN_TPCH_BENCHMARK:-false}" == "true" ]] || [[ "$SCALE_FACTOR_SPECIFIED" == "true" ]]; then
  echo "Waiting for Presto to be ready for TPC-H benchmark..."
  sleep 30
  
  # Wait for Presto coordinator to be responsive
  for i in {1..60}; do
    if curl -sSf "http://localhost:8080/v1/info" > /dev/null; then
      echo "Presto coordinator is ready."
      break
    fi
    echo -n "."
    sleep 2
    if [[ $i -eq 60 ]]; then
      echo "Presto coordinator not responding. Skipping benchmark."
      exit 1
    fi
  done
  
  # Wait 5 seconds with countdown before starting benchmark
  echo "Waiting additional 5 seconds for Presto to fully initialize..."
  for i in {5..1}; do
    echo "Starting benchmark in ${i} seconds..."
    sleep 1
  done
  echo "Starting TPC-H benchmark now!"
  
  # Run the full TPC-H benchmark
  if [[ "$LOAD_ALL_SCALE_FACTORS" == "true" ]]; then
    echo "Running TPC-H benchmark for all scale factors..."
    python "../../benchmarks/tpch/run_benchmark.py" --scale-factor 1 --output-format json
    python "../../benchmarks/tpch/run_benchmark.py" --scale-factor 10 --output-format json
    python "../../benchmarks/tpch/run_benchmark.py" --scale-factor 100 --output-format json
    echo "TPC-H benchmark completed for all scale factors. Results saved to tpch_benchmark_results_sf*_*.json"
  else
    echo "Running TPC-H benchmark..."
    python "../../benchmarks/tpch/run_benchmark.py" --scale-factor ${SCALE_FACTOR} --output-format json
    echo "TPC-H benchmark completed. Results saved to tpch_benchmark_results_*.json"
  fi
fi
