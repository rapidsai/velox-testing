#!/bin/bash

set -e

# apply patches to deps image build
pushd ../../../presto
git apply ../velox-testing/presto/scripts/build_centos_deps_image_patches.diff || true
popd

# now build the deps image if needed
pushd ../../../presto/presto-native-execution
make submodules
docker compose up centos-native-dependency # Build dependencies image if there is none present.
docker compose down centos-native-dependency
popd
