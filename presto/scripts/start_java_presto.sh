#!/bin/bash

set -e

# Validate repo layout using shared script
../../scripts/validate_directories_exist.sh "../../../presto" "../../../velox"

./stop_presto.sh

# docker build -t prestodb/presto:$VERSION-benchmark -f ../docker/java_build.dockerfile ../../..
VERSION=$(git -C ../../../presto rev-parse --short HEAD)
PRESTO_VERSION=$VERSION-benchmark

docker run --rm \
  -v $(pwd)/../../../presto:/presto \
  -e PRESTO_VERSION=$PRESTO_VERSION \
  -w /presto \
  eclipse-temurin:17-jdk-jammy \
  bash -c "
    ./mvnw clean install -DskipTests -pl \!presto-docs -Dair.check.skip-all=true &&
    echo 'Copying artifacts with version '\$VERSION'...' &&
    cp presto-server/target/presto-server-*.tar.gz docker/presto-server-\$PRESTO_VERSION.tar.gz &&
    cp presto-cli/target/presto-cli-*-executable.jar docker/presto-cli-\$PRESTO_VERSION-executable.jar &&
    echo 'Build complete! Artifacts copied with version '\$VERSION
  "

docker compose -f ../docker/docker-compose.java.yml build --build-arg PRESTO_VERSION=$PRESTO_VERSION --progress plain
docker compose -f ../docker/docker-compose.java.yml up -d
