#!/bin/bash

set -e

# Change to the script's directory to ensure correct relative paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Parse command line arguments
FORCE_REBUILD="false"

while [[ $# -gt 0 ]]; do
  case $1 in
    --force-rebuild)
      FORCE_REBUILD="true"
      shift
      ;;
    -h|--help)
      echo "Usage: $0 [OPTIONS]"
      echo ""
      echo "Start Presto Native GPU cluster"
      echo ""
      echo "Options:"
      echo "  --force-rebuild         Force rebuild GPU image (bypasses protection)"
      echo "  -h, --help              Show this help message"
      echo ""
      echo "Environment Variables:"
      echo "  GPU=ON/OFF              Force GPU or CPU mode (default: ON)"
      echo "  TPCH_PARQUET_DIR=path   Use existing TPC-H data directory"
      echo ""
      echo "Examples:"
      echo "  $0                      # Start Presto GPU cluster"
      echo "  $0 --force-rebuild      # Force rebuild GPU image and start"
      echo "  GPU=OFF $0              # Start CPU cluster instead"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use -h or --help for usage information"
      exit 1
      ;;
  esac
done

echo "Starting Presto Native GPU cluster..."

../stop_presto.sh
../build_centos_deps_image.sh

# Default to GPU mode for this script (can be overridden with GPU=OFF)
COMPOSE_FILE=../../docker/docker-compose.native-gpu.yml
if [[ "${GPU:-ON}" == "OFF" ]]; then
  COMPOSE_FILE=../../docker/docker-compose.native-cpu.yml
fi

# GPU validation - fail if GPU requirements not met (no CPU fallback)
if [[ "${COMPOSE_FILE}" == *native-gpu* ]]; then
  if ! command -v nvidia-smi >/dev/null 2>&1; then
    echo "❌ ERROR: GPU Presto requested but nvidia-smi not found."
    echo "   Install NVIDIA drivers or use CPU variant instead:"
    echo "   ./start_native_cpu_presto.sh"
    exit 1
  fi
  
  CAP=$(nvidia-smi --query-gpu=compute_cap --format=csv,noheader 2>/dev/null | head -1 | tr -d '[:space:]')
  MAJOR=${CAP%%.*}
  MINOR=${CAP##*.}
  
  # Velox supports GPU compute capability 7.0+ (includes Tesla T4 with 7.5)
  if [[ -z "$MAJOR" ]] || [[ "$MAJOR" -lt 7 ]] || [[ "$MAJOR" -eq 6 && "$MINOR" -lt 0 ]]; then
    echo "❌ ERROR: GPU compute capability ${CAP:-unknown} is below Velox minimum (7.0)."
    echo "   Supported GPUs: V100 (7.0), Tesla T4 (7.5), A100 (8.0), RTX 30/40 series"
    echo "   Use CPU variant instead: ./start_native_cpu_presto.sh"
    exit 1
  fi
  
  # Display GPU diagnostics for verification
  echo "🔍 GPU Environment Check:"
  echo "nvidia-smi output:"
  nvidia-smi || {
    echo "❌ ERROR: nvidia-smi failed to execute properly."
    exit 1
  }
  echo "CUDA Visible Devices: ${CUDA_VISIBLE_DEVICES:-all}"
  
  if [[ "${GPU}" == "ON" ]] && ! nvidia-smi -L >/dev/null 2>&1; then
    echo "❌ ERROR: No visible GPUs available to container runtime."
    echo "   Check Docker GPU runtime configuration."
    echo "   Use CPU variant instead: ./start_native_cpu_presto.sh"
    exit 1
  fi
  
  echo "✅ GPU validation passed. Proceeding with GPU Presto deployment."
fi

# Auto-generate TPCH Parquet locally if TPCH_PARQUET_DIR not set or empty
if [[ -z "${TPCH_PARQUET_DIR:-}" ]]; then
  echo "TPCH_PARQUET_DIR not set; generating local TPC-H Parquet data (SF=1)..."
  bash "../data/generate_tpch_data.sh" -s 1
  export TPCH_PARQUET_DIR="$(cd ../.. && pwd)/docker/data/tpch"
fi

# GPU Image Protection: Check if we have a valid GPU image before rebuilding
if [[ "${COMPOSE_FILE}" == *native-gpu* ]]; then
  CURRENT_GPU_IMAGE=$(docker images presto-native-worker-gpu:latest --format "{{.ID}} {{.Size}}" 2>/dev/null || echo "none none")
  CURRENT_ID=$(echo "$CURRENT_GPU_IMAGE" | cut -d' ' -f1)
  CURRENT_SIZE=$(echo "$CURRENT_GPU_IMAGE" | cut -d' ' -f2)
  
  # Check if current image is a valid GPU image (>12GB size indicates GPU build)
  if [[ "$CURRENT_ID" != "none" ]] && [[ "$CURRENT_SIZE" == *"13"* ]] && [[ "$CURRENT_SIZE" == *"GB"* ]] && [[ "$FORCE_REBUILD" != "true" ]]; then
    echo "🔒 Existing GPU image detected ($CURRENT_ID, $CURRENT_SIZE)"
    echo "⚠️  Skipping rebuild to preserve GPU image. To force rebuild:"
    echo "   1. Use: $0 --force-rebuild sf1"
    echo "   2. Or manually: docker rmi presto-native-worker-gpu:latest"
    echo "   3. Then re-run this script"
    
    # Skip build, go directly to compose up
    echo "📦 Using existing GPU image..."
  else
    if [[ "$FORCE_REBUILD" == "true" ]]; then
      echo "🔨 Force rebuild requested, rebuilding GPU image..."
      echo "   Current image: $CURRENT_GPU_IMAGE"
    else
      echo "🔨 No valid GPU image found, proceeding with build..."
      echo "   Current image: $CURRENT_GPU_IMAGE"
    fi
    docker compose -f ${COMPOSE_FILE} build \
      --build-arg NUM_THREADS=$(($(nproc) * 3 / 4)) \
      --build-arg GPU=${GPU:-OFF} \
      --progress plain
  fi
else
  # CPU mode - always rebuild
  docker compose -f ${COMPOSE_FILE} build \
    --build-arg NUM_THREADS=$(($(nproc) * 3 / 4)) \
    --build-arg GPU=${GPU:-OFF} \
    --progress plain
fi

docker compose -f ${COMPOSE_FILE} up -d

# If TPCH_PARQUET_DIR is provided, auto-register external TPCH tables in Hive
if [[ -n "${TPCH_PARQUET_DIR}" ]]; then
  echo "Registering TPC-H external Parquet tables from ${TPCH_PARQUET_DIR}..."
  bash "../data/register_tpch_tables.sh"
fi

# Check GPU worker status after startup - fail if GPU worker crashes (no CPU fallback)
if [[ "${COMPOSE_FILE}" == *native-gpu* ]]; then
  sleep 3  # Give containers time to initialize
  GPU_STATUS=$(docker ps -a --filter name=presto-native-worker-gpu --format '{{.Status}}' | head -1 || true)
  GPU_LOGS=$(docker logs --tail 200 presto-native-worker-gpu 2>/dev/null || true)
  
  if echo "$GPU_STATUS" | grep -qi 'exited'; then
    echo "❌ ERROR: GPU worker exited (${GPU_STATUS})."
    echo "📋 GPU Worker Logs:"
    echo "$GPU_LOGS"
    echo ""
    echo "🔧 Troubleshooting:"
    echo "   - Check GPU driver compatibility"
    echo "   - Verify Docker GPU runtime setup"
    echo "   - Use CPU variant if GPU not available: ./start_native_cpu_presto.sh"
    ../stop_presto.sh
    exit 1
  fi
  
  if echo "$GPU_LOGS" | grep -qi 'cuda_error\|forward compatibility'; then
    echo "❌ ERROR: CUDA runtime error detected in GPU worker logs."
    echo "📋 GPU Worker Logs (last 200 lines):"
    echo "$GPU_LOGS"
    echo ""
    echo "🔧 Troubleshooting:"
    echo "   - Update NVIDIA drivers"
    echo "   - Check CUDA/Docker compatibility"
    echo "   - Use CPU variant if GPU issues persist: ./start_native_cpu_presto.sh"
    ../stop_presto.sh
    exit 1
  fi
  
  echo "✅ GPU worker started successfully."
fi

# Wait for Presto coordinator to be ready (basic health check)
echo "Waiting for Presto coordinator to be ready..."
sleep 30

for i in {1..60}; do
  if curl -sSf "http://localhost:8080/v1/info" > /dev/null; then
    echo "✅ Presto coordinator is ready!"
    break
  fi
  echo -n "."
  sleep 2
  if [[ $i -eq 60 ]]; then
    echo "❌ Presto coordinator not responding after 2 minutes."
    exit 1
  fi
done

echo ""
echo "🎉 Presto Native GPU cluster started successfully!"
echo ""
echo "🚀 To run benchmarks:"
echo "  cd ../../benchmarks/tpch"
echo "  python run_benchmark.py --scale-factor 1"
echo ""
echo "🔍 To monitor GPU usage:"
echo "  nvidia-smi"
echo "  watch nvidia-smi"
echo ""
echo "📊 Coordinator: http://localhost:8080"
