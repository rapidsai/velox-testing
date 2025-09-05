echo "Building Presto Java from source..."

if [[ -z $PRESTO_VERSION ]]; then
  echo "Error: PRESTO_VERSION must be set"
  exit 1
fi

echo "Building Presto Java from source with presto version $PRESTO_VERSION..."

docker run --rm \
    -v $(pwd)/../../../presto:/presto \
    -v ./.mvn_cache:/root/.m2 \
    -e PRESTO_VERSION=$PRESTO_VERSION \
    -w /presto \
    eclipse-temurin:17-jdk-jammy \
    bash -c "
    ./mvnw clean install -DskipTests -pl \!presto-docs -pl \!presto-openapi -Dair.check.skip-all=true &&
    echo 'Copying artifacts with version $PRESTO_VERSION...' &&
    cp presto-server/target/presto-server-*.tar.gz docker/presto-server-$PRESTO_VERSION.tar.gz &&
    cp presto-cli/target/presto-cli-*-executable.jar docker/presto-cli-$PRESTO_VERSION-executable.jar &&
    chmod +r docker/presto-cli-$PRESTO_VERSION-executable.jar &&
    echo 'Build complete! Artifacts copied with version $PRESTO_VERSION'
    "
