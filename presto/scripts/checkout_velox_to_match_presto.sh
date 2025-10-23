#!/bin/bash

set -e

echo "Checking out version of Velox which matches Presto pinned version"

# get SHA of Presto Velox submodule
# must do make submodules first as sub-module clone is not automatic
pushd ../../../presto/presto-native-execution
make submodules
cd velox
SHA=$(git rev-parse HEAD)
popd

echo "Presto pinned version is ${SHA}"

# checkout sibling Velox to that SHA
pushd ../../../velox
git fsck
git rev-parse HEAD
git checkout ${SHA}
popd

echo "Velox checked out to ${SHA}"
