#!/bin/bash

set -e

echo "Checking out version of Velox which matches Presto pinned version"

# get SHA of Presto Velox submodule
pushd ../../../presto/presto-native-execution/velox
SHA=$(git rev-parse HEAD)
popd

# checkout sibling Velox to that SHA
pushd ../../../velox
git checkout ${SHA}
popd

echo "Velox checked out to ${SHA}"
