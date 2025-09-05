#!/bin/bash

set -e

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

This script optionally builds and runs Presto Java services

OPTIONS:
    -h, --help     Show this help message.
    -b, --build    Build presto from source instead of using the "prestodb/presto:latest:" image.

EXAMPLES:
    $0
    $0 --build
    $0 -h

EOF
}


BUILD_FROM_SOURCE=false
parse_args() { 
  while [[ $# -gt 0 ]]; do
    case $1 in
      -h|--help)
        print_help
        exit 0
        ;;
      -b|--build)
        BUILD_FROM_SOURCE=true
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

# Validate repo layout using shared script
../../scripts/validate_directories_exist.sh "../../../presto" "../../../velox"

./stop_presto.sh

if [[ "$BUILD_FROM_SOURCE" == "true" ]]; then
    echo "Building Presto from source..."
    VERSION=$(git -C ../../../presto rev-parse --short HEAD)
    PRESTO_VERSION=$VERSION-testing
    
    PRESTO_VERSION=$PRESTO_VERSION ./build_presto_java_package.sh
    
    export PRESTO_JAVA_IMAGE=presto-java-custom:$PRESTO_VERSION

    # Delete any existing image with the same name/tag in order to avoid dangling images after build.
    if [[ -n "$(docker images -q $PRESTO_JAVA_IMAGE)" ]]; then
      docker image rm $PRESTO_JAVA_IMAGE
    fi
    # Avoid redundant builds by building only one of the Java services.
    docker compose --progress=plain -f ../docker/docker-compose.java.yml build \
      --build-arg PRESTO_VERSION=$PRESTO_VERSION presto-coordinator
else
    echo "Using prestodb/presto:latest image..."
    docker compose -f ../docker/docker-compose.java.yml pull
fi

docker compose -f ../docker/docker-compose.java.yml up -d
