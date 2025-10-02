#!/bin/bash

set -e

#
# check for existing container image
#

DEPS_IMAGE="presto/prestissimo-dependency:centos9"

if [ "${REBUILD_DEPS}" == "1" ]; then
	echo "Forcing rebuild of Presto dependencies/run-time container image"
	echo "This does not delete any existing worker container images which must be manually deleted"
	docker rmi -f ${DEPS_IMAGE} 
	docker builder prune -f
elif [ ! -z $(docker images -q ${DEPS_IMAGE}) ]; then
	echo "Found existing Presto dependencies/run-time container image"
	exit 0
else
	echo "Presto dependencies/run-time container image not found, attempting to re-build..."
fi

#
# apply current patches for Presto deps container build success
# as of 10/2/25
#

echo "Modifying Presto clone and contained Velox sub-module for deps container build (as of 10/2/25)"

# move to Presto clone
pushd ../../../presto

# reset Presto clone
echo "Resetting Presto clone files"
git checkout .

# reset Velox submodule
echo "Resetting Velox submodule files"
cd presto-native-execution/velox
git checkout .
cd ../..

# reset submodule
echo "Resetting Velox submodule version"
cd presto-native-execution
make submodules
cd ..

# rewrite .gitmodules file to override Velox submodule to rapidsai/velox:merged-prs (latest)
echo "Rewriting .gitmodules file"
cat << EOF > .gitmodules
[submodule "presto-native-execution/velox"]
path = presto-native-execution/velox
url = https://github.com/rapidsai/velox.git
branch = merged-prs
EOF

# force override submodule contents
echo "Updating Velox submodule to fork"
git submodule sync
git submodule update --init --remote presto-native-execution/velox

#########################################
# apply patches here while still required
#########################################

echo "Applying patches"

# apply Arrow patch (remove if/when this is applied to devavret/presto)
git apply ../velox-testing/presto/patches/patch_arrow_092525.diff

# move to Velox submodule
cd presto-native-execution/velox

# apply Hadoop patch (remove if/when this is applied to rapidsai/velox)
git apply ../../../velox-testing/presto/patches/patch_hadoop_100225.diff

#########################################
# to here
#########################################

# done
popd

#
# build deps container image
#

echo "Building Presto dependencies/run-time image..."

pushd ../../../presto/presto-native-execution
docker compose up centos-native-dependency # Build dependencies image if there is none present.
docker compose down centos-native-dependency
popd

echo "Presto dependencies/run-time container image built!"
