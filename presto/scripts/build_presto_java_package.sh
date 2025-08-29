echo "Building Presto from source..."

VERSION=${VERSION:-$(git -C ../../../presto rev-parse --short HEAD)}
PRESTO_VERSION=${PRESTO_VERSION:-$VERSION-testing}

echo "Building Presto from source with version $VERSION and presto version $PRESTO_VERSION..."

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