#!/bin/bash

set -e

# Parse command line arguments
SKIP_SUBMODULES=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --no-submodules)
      SKIP_SUBMODULES=true
      shift
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--no-submodules]"
      exit 1
      ;;
  esac
done

PATCH_FILE_PATH=$(readlink -f copy_arrow_patch.patch)

pushd ../../../presto/presto-native-execution

# Conditionally pull submodules
if [ "$SKIP_SUBMODULES" = "false" ]; then
  echo "Pulling submodules..."
  make submodules
else
  echo "Skipping submodule pull as requested"
fi
echo "Listing contents of ./velox:"
ls -l ./velox

docker compose up centos-native-dependency # Build dependencies image if there is none present.
docker compose down centos-native-dependency
popd
