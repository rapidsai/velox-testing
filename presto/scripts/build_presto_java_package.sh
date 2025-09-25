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

if [[ -z $PRESTO_VERSION ]]; then
  echo "Internal error: PRESTO_VERSION must be set"
  exit 1
fi

echo "Building Presto Java from source with PRESTO_VERSION: $PRESTO_VERSION..."

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
