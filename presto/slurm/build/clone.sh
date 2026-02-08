#!/bin/bash
# Clone Presto and Velox

export PRESTO_BRANCH=${PRESTO_BRANCH:-ibm-research-preview}
export PRESTO_REPO=${PRESTO_REPO:-https://github.com/prestodb/presto}
export VELOX_BRANCH=${VELOX_BRANCH:-ibm-research-preview}
export VELOX_REPO=${VELOX_REPO:-https://github.com/IBM/velox}

git clone $PRESTO_REPO -b $PRESTO_BRANCH
pushd presto/presto-native-execution
git clone $VELOX_REPO -b $VELOX_BRANCH
popd
