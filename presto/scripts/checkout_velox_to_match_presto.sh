#!/bin/bash

set -e

# get SHA of Presto Velox submodule
pushd ../../presto/presto-native-execution/velox
SHA=$(git rev-parse HEAD)
popd

# checkout sibling Velox to that SHA
pushd ../../velox
git checkout ${SHA}
popd
