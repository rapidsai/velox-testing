#!/bin/bash
#
# Start Presto Java services
#
# Usage:
#   ./start_java_presto.sh           # Use prestodb/presto:latest image
#   ./start_java_presto.sh --build   # Build from source
#

set -e

# Parse command line arguments
BUILD_FROM_SOURCE=false
if [[ "$1" == "--build" ]]; then
    BUILD_FROM_SOURCE=true
    echo "Building Presto from source..."
elif [[ "$1" == "--help" || "$1" == "-h" ]]; then
    echo "Usage: $0 [--build]"
    echo "  --build    Build Presto from source instead of using prestodb/presto:latest"
    echo "  --help     Show this help message"
    exit 0
fi

# Validate repo layout using shared script
../../scripts/validate_directories_exist.sh "../../../presto" "../../../velox"

./stop_presto.sh

if [[ "$BUILD_FROM_SOURCE" == "true" ]]; then
    echo "Building Presto from source..."
    VERSION=$(git -C ../../../presto rev-parse --short HEAD)
    PRESTO_VERSION=$VERSION-benchmark
    
    docker run --rm \
      -v $(pwd)/../../../presto:/presto \
      -e PRESTO_VERSION=$PRESTO_VERSION \
      -w /presto \
      eclipse-temurin:17-jdk-jammy \
      bash -c "
        ./mvnw clean install -DskipTests -pl \!presto-docs &&
        echo 'Copying artifacts with version '\$VERSION'...' &&
        cp presto-server/target/presto-server-*.tar.gz docker/presto-server-\$PRESTO_VERSION.tar.gz &&
        cp presto-cli/target/presto-cli-*-executable.jar docker/presto-cli-\$PRESTO_VERSION-executable.jar &&
        echo 'Build complete! Artifacts copied with version '\$VERSION
      "
    
    echo "Building Docker images with custom artifacts..."
    docker compose -f ../docker/docker-compose.java.yml build --build-arg PRESTO_VERSION=$PRESTO_VERSION --progress plain
    docker compose -f ../docker/docker-compose.java.yml up -d
else
    echo "Using prestodb/presto:latest image..."
    docker compose -f ../docker/docker-compose.java.nobuild.yml pull
    docker compose -f ../docker/docker-compose.java.nobuild.yml up -d
fi
