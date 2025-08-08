#!/bin/bash

set -e

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
      echo "  GPU=ON/OFF              Force GPU or CPU mode"
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

echo "Starting Presto Native with TPC-H Scale Factor: ${SCALE_FACTOR}"

./stop_presto.sh
./build_centos_deps_image.sh

COMPOSE_FILE=../docker/docker-compose.native-cpu.yml
if [[ "${GPU:-OFF}" == "ON" ]]; then
  COMPOSE_FILE=../docker/docker-compose.native-gpu.yml
fi

# If GPU compose selected, verify GPU is usable; else fall back to CPU
if [[ "${COMPOSE_FILE}" == *native-gpu* ]]; then
  if ! command -v nvidia-smi >/dev/null 2>&1; then
    echo "No nvidia-smi detected. Falling back to CPU worker."
    COMPOSE_FILE=../docker/docker-compose.native-cpu.yml
    export GPU=OFF
  else
    CAP=$(nvidia-smi --query-gpu=compute_cap --format=csv,noheader 2>/dev/null | head -1 | tr -d '[:space:]')
    MAJOR=${CAP%%.*}
    if [[ -z "$MAJOR" || "$MAJOR" -lt 8 ]]; then
      echo "GPU compute capability ${CAP:-unknown} is below RAPIDS minimum (8.0). Falling back to CPU worker."
      COMPOSE_FILE=../docker/docker-compose.native-cpu.yml
      export GPU=OFF
    fi
    # Surface full CUDA/NVIDIA diagnostics for debugging
    echo "nvidia-smi:\n$(nvidia-smi || true)"
    echo "CUDA Visible Devices: ${CUDA_VISIBLE_DEVICES:-all}"
    if [[ "${GPU}" == "ON" ]] && ! nvidia-smi -L >/dev/null 2>&1; then
      echo "No visible GPUs to container runtime. Falling back to CPU."
      COMPOSE_FILE=../docker/docker-compose.native-cpu.yml
      export GPU=OFF
    fi
  fi
fi

# Auto-generate TPCH Parquet locally if TPCH_PARQUET_DIR not set or empty
if [[ -z "${TPCH_PARQUET_DIR:-}" ]]; then
  if [[ "$LOAD_ALL_SCALE_FACTORS" == "true" ]]; then
    echo "TPCH_PARQUET_DIR not set; generating all TPC-H Parquet scale factors (1, 10, 100)..."
    bash "$(dirname "$0")/tpch_benchmark.sh" generate -s 1
    bash "$(dirname "$0")/tpch_benchmark.sh" generate -s 10
    bash "$(dirname "$0")/tpch_benchmark.sh" generate -s 100
    export TPCH_PARQUET_DIR="$(cd "$(dirname "$0")"/.. && pwd)/docker/data/tpch"
  else
    echo "TPCH_PARQUET_DIR not set; generating local TPCH Parquet (SF=${SCALE_FACTOR})..."
    bash "$(dirname "$0")/tpch_benchmark.sh" generate -s "${SCALE_FACTOR}"
    export TPCH_PARQUET_DIR="$(cd "$(dirname "$0")"/.. && pwd)/docker/data/tpch"
  fi
fi

docker compose -f ${COMPOSE_FILE} build \
  --build-arg NUM_THREADS=$(($(nproc) * 3 / 4)) \
  --build-arg GPU=${GPU:-OFF} \
  --progress plain
docker compose -f ${COMPOSE_FILE} up -d

# If TPCH_PARQUET_DIR is provided, auto-register external TPCH tables in Hive
if [[ -n "${TPCH_PARQUET_DIR}" ]]; then
  if [[ "$LOAD_ALL_SCALE_FACTORS" == "true" ]]; then
    echo "Registering all TPC-H external Parquet tables (SF1, SF10, SF100) from ${TPCH_PARQUET_DIR}..."
    bash "$(dirname "$0")/tpch_benchmark.sh" register -s 1
    bash "$(dirname "$0")/tpch_benchmark.sh" register -s 10
    bash "$(dirname "$0")/tpch_benchmark.sh" register -s 100
  else
    echo "Registering TPCH external Parquet tables from ${TPCH_PARQUET_DIR}..."
    bash "$(dirname "$0")/tpch_benchmark.sh" register
  fi
fi

# If we attempted GPU and the worker crashed due to CUDA errors, fall back to CPU automatically.
if [[ "${COMPOSE_FILE}" == *native-gpu* ]]; then
  sleep 2
  GPU_STATUS=$(docker ps -a --filter name=presto-native-worker-gpu --format '{{.Status}}' | head -1 || true)
  GPU_LOGS=$(docker logs --tail 200 presto-native-worker-gpu 2>/dev/null || true)
  if echo "$GPU_STATUS" | grep -qi 'exited'; then
    echo "GPU worker exited (${GPU_STATUS}). Falling back to CPU..."
    ./stop_presto.sh
    COMPOSE_FILE=../docker/docker-compose.native-cpu.yml
    GPU=OFF docker compose -f ${COMPOSE_FILE} build --build-arg NUM_THREADS=$(($(nproc) * 3 / 4)) --build-arg GPU=OFF --progress plain
    docker compose -f ${COMPOSE_FILE} up -d
    if [[ -n "${TPCH_PARQUET_DIR}" ]]; then
      bash "$(dirname "$0")/tpch_benchmark.sh" register
    fi
  elif echo "$GPU_LOGS" | grep -qi 'cuda_error\|forward compatibility'; then
    echo "Detected CUDA runtime error in GPU worker logs. Falling back to CPU..."
    ./stop_presto.sh
    COMPOSE_FILE=../docker/docker-compose.native-cpu.yml
    GPU=OFF docker compose -f ${COMPOSE_FILE} build --build-arg NUM_THREADS=$(($(nproc) * 3 / 4)) --build-arg GPU=OFF --progress plain
    docker compose -f ${COMPOSE_FILE} up -d
    if [[ -n "${TPCH_PARQUET_DIR}" ]]; then
      bash "$(dirname "$0")/tpch_benchmark.sh" register
    fi
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
      echo "Presto coordinator is ready. Starting TPC-H benchmark..."
      break
    fi
    echo -n "."
    sleep 2
    if [[ $i -eq 60 ]]; then
      echo "Presto coordinator not responding. Skipping benchmark."
      exit 1
    fi
  done
  
  # Run the full TPC-H benchmark
  if [[ "$LOAD_ALL_SCALE_FACTORS" == "true" ]]; then
    echo "Running TPC-H benchmark for all scale factors..."
    bash "$(dirname "$0")/tpch_benchmark.sh" benchmark -s 1 -o "tpch_benchmark_results_sf1_$(date +%Y%m%d_%H%M%S).json"
    bash "$(dirname "$0")/tpch_benchmark.sh" benchmark -s 10 -o "tpch_benchmark_results_sf10_$(date +%Y%m%d_%H%M%S).json"
    bash "$(dirname "$0")/tpch_benchmark.sh" benchmark -s 100 -o "tpch_benchmark_results_sf100_$(date +%Y%m%d_%H%M%S).json"
    echo "TPC-H benchmark completed for all scale factors. Results saved to tpch_benchmark_results_sf*_*.json"
  else
    echo "Running TPC-H benchmark..."
    bash "$(dirname "$0")/tpch_benchmark.sh" benchmark -o "tpch_benchmark_results_$(date +%Y%m%d_%H%M%S).json"
    echo "TPC-H benchmark completed. Results saved to tpch_benchmark_results_*.json"
  fi
fi
