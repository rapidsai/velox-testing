#!/bin/bash

set -e

# Change to the script's directory to ensure correct relative paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -h|--help)
      echo "Usage: $0 [OPTIONS]"
      echo ""
      echo "Start Presto Native CPU cluster"
      echo ""
      echo "Options:"
      echo "  -h, --help              Show this help message"
      echo ""
      echo "Environment Variables:"
      echo "  TPCH_PARQUET_DIR=path   Use existing TPC-H data directory"
      echo ""
      echo "Examples:"
      echo "  $0                      # Start Presto CPU cluster"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use -h or --help for usage information"
      exit 1
      ;;
  esac
done

echo "Starting Presto Native CPU cluster..."

../stop_presto.sh
../build_centos_deps_image.sh

# Auto-generate TPCH Parquet locally if TPCH_PARQUET_DIR not set or empty
if [[ -z "${TPCH_PARQUET_DIR:-}" ]]; then
  echo "TPCH_PARQUET_DIR not set; generating local TPC-H Parquet data (SF=1)..."
  bash "../data/generate_tpch_data.sh" -s 1
  export TPCH_PARQUET_DIR="$(cd ../.. && pwd)/docker/data/tpch"
fi

docker compose -f ../../docker/docker-compose.native-cpu.yml build --build-arg NUM_THREADS=$(($(nproc) * 3 / 4)) --progress plain
docker compose -f ../../docker/docker-compose.native-cpu.yml up -d

# If TPCH_PARQUET_DIR is provided, auto-register external TPCH tables in Hive
if [[ -n "${TPCH_PARQUET_DIR}" ]]; then
  echo "Registering TPC-H external Parquet tables from ${TPCH_PARQUET_DIR}..."
  bash "../data/register_tpch_tables.sh"
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
echo "🎉 Presto Native CPU cluster started successfully!"
echo ""
echo "🚀 To run benchmarks:"
echo "  cd ../../benchmarks/tpch"
echo "  python run_benchmark.py --scale-factor 1"
echo ""
echo "📊 Coordinator: http://localhost:8080"
