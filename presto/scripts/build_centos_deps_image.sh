#!/bin/bash

set -e

IMAGE_NAME="presto/prestissimo-dependency:centos9"

#
# remove any existing image?
#

if [[ ! -z $(docker images -q ${IMAGE_NAME}) ]]; then
	echo "Removing existing Presto dependencies/run-time image..."
	docker rmi -f ${IMAGE_NAME}
fi

#
# try to build deps container image locally
#

echo "Building Presto dependencies/run-time image..."

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

# apply patches here if needed
echo "No patches currently required (10/14/25)"

# preparation complete
popd

# now build
pushd ../../../presto/presto-native-execution
docker compose --progress plain build centos-native-dependency
popd

# done
echo "Presto dependencies/run-time container image built!"
