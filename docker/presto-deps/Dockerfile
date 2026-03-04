# Presto native dependency image.
#
# Based on upstream centos-dependency.dockerfile from prestodb/presto.
# Builds a CentOS 9 image with all dependencies needed to compile Presto
# native worker (prestissimo) with GPU support.
#
# Build context must contain:
#   presto/presto-native-execution/   – Presto native execution source (for setup scripts)
#   velox/                            – Velox source checkout (for Velox setup scripts and CMake)
#
FROM quay.io/centos/centos:stream9

ARG ARM_BUILD_TARGET
ARG CUDA_VERSION

ENV PROMPT_ALWAYS_RESPOND=y
ENV CC=/opt/rh/gcc-toolset-12/root/bin/gcc
ENV CXX=/opt/rh/gcc-toolset-12/root/bin/g++
ENV ARM_BUILD_TARGET=${ARM_BUILD_TARGET}
ENV CUDA_VERSION=${CUDA_VERSION:-12.9}
ENV UCX_VERSION="1.19.0"

RUN mkdir -p /scripts /velox/scripts /velox/CMake

# Presto native execution setup scripts
COPY presto/presto-native-execution/scripts /scripts

# Velox scripts and CMake from the sibling Velox checkout
COPY velox/scripts /velox/scripts
COPY velox/CMake/resolve_dependency_modules/arrow/cmake-compatibility.patch /velox
ENV VELOX_ARROW_CMAKE_PATCH=/velox/cmake-compatibility.patch

RUN bash -c "mkdir build && \
    (cd build && ../scripts/setup-centos.sh && \
                 ../scripts/setup-adapters.sh && \
                 source ../velox/scripts/setup-centos9.sh && \
                 source ../velox/scripts/setup-centos-adapters.sh && \
                 install_adapters && \
                 install_clang15 && \
                 install_cuda ${CUDA_VERSION} && \
                 install_ucx) && \
    rm -rf build"

ENV PATH=/usr/local/cuda/bin:${PATH}

ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES="compute,utility"
