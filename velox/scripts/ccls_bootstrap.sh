#!/usr/bin/env bash
set -euo pipefail

CURRENT_UID=${HOST_UID:-$(id -u)}
CURRENT_GID=${HOST_GID:-$(id -g)}
HOST_USER=${HOST_USER:-hostuser}
HOST_HOME=${HOST_HOME:-/home/${HOST_USER}}

sudo dnf install -y git cmake ninja-build llvm-devel llvm-static clang clang-devel clang-tools-extra
# If cache is stale/missing RPMs, clean and retry once
if [ $? -ne 0 ]; then
  sudo dnf clean all || true
  sudo rm -rf /var/cache/dnf || true
  sudo dnf makecache || true
  sudo dnf install -y git cmake ninja-build llvm-devel llvm-static clang clang-devel clang-tools-extra
fi

WORKDIR="${HOST_HOME}/software/ccls"
if [ ! -d "${WORKDIR}" ]; then
  mkdir -p "${HOST_HOME}/software"
  cd "${HOST_HOME}/software"
  git clone --recursive https://github.com/MaskRay/ccls.git
else
  cd "${WORKDIR}"
  git reset --hard
  git pull --rebase
fi

# Ensure submodules; if missing refs, reclone fresh
if ! git submodule update --init --recursive; then
  cd "${HOST_HOME}/software"
  rm -rf ccls
  git clone --recursive https://github.com/MaskRay/ccls.git
  cd ccls
  git submodule update --init --recursive
fi

# Locate ClangConfig.cmake
CLANG_DIR=$(rpm -ql clang clang-devel 2>/dev/null | grep -m1 'ClangConfig.cmake' | xargs dirname || true)
if [ -z "${CLANG_DIR}" ]; then
  # Common fallback
  if [ -f /usr/lib64/cmake/clang/ClangConfig.cmake ]; then
    CLANG_DIR=/usr/lib64/cmake/clang
  elif [ -f /usr/lib/cmake/clang/ClangConfig.cmake ]; then
    CLANG_DIR=/usr/lib/cmake/clang
  fi
fi

cmake -S . -B Release \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_PREFIX_PATH=/usr/lib/llvm-18 \
  -DLLVM_INCLUDE_DIR=/usr/lib/llvm-18/include \
  -DLLVM_BUILD_INCLUDE_DIR=/usr/include/llvm-18/ \
  ${CLANG_DIR:+-DClang_DIR=${CLANG_DIR}}

cmake --build Release

chown -R "${CURRENT_UID}:${CURRENT_GID}" "${HOST_HOME}/software/ccls"

echo "ccls built at ${HOST_HOME}/software/ccls/Release/ccls"
