#!/bin/bash

set -e

echo "Checking out version of Velox which matches Presto pinned version"

# get SHA of Presto Velox submodule
pushd ../../../presto/presto-native-execution/velox
pwd
ls -l
SHA=$(git rev-parse HEAD)
popd

echo "Presto pinned version is ${SHA}"

# checkout sibling Velox to that SHA
pushd ../../../velox
pwd
ls -l
git checkout ${SHA}
popd

echo "Velox checked out to ${SHA}"
