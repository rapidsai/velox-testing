FROM presto/prestissimo-dependency:centos9

RUN rpm --import https://developer.download.nvidia.com/compute/cuda/repos/ubuntu1804/x86_64/7fa2af80.pub && \
    dnf config-manager --add-repo "https://developer.download.nvidia.com/devtools/repos/rhel$(source /etc/os-release; echo ${VERSION_ID%%.*})/$(rpm --eval '%{_arch}' | sed s/aarch/arm/)/" && \
    dnf install -y nsight-systems-cli

#RUN dnf install -y bzip2 ca-certificates || yum install -y bzip2 ca-certificates
RUN dnf install -y bzip2 --setopt=install_weak_deps=False || true
RUN curl -Ls https://micro.mamba.pm/api/micromamba/linux-64/latest -o /tmp/micromamba.tar.bz2 \
&& tar -xvj -f /tmp/micromamba.tar.bz2 -C /usr/local/bin --strip-components=1 bin/micromamba \
&& rm -f /tmp/micromamba.tar.bz2 \
&& /usr/local/bin/micromamba --help

# Ensure CMake >= 3.30 for cudf build
RUN curl -L https://github.com/Kitware/CMake/releases/download/v3.30.5/cmake-3.30.5-linux-x86_64.tar.gz -o /tmp/cmake.tgz && \
    tar -xzf /tmp/cmake.tgz -C /opt && \
    ln -sf /opt/cmake-3.30.5-linux-x86_64/bin/* /usr/local/bin/ && \
    rm -f /tmp/cmake.tgz && \
    cmake --version

                                                                                                                                                    # Micromamba + UCXX bootstrap
SHELL ["/bin/bash", "-lc"]
# Ensure a deterministic prefix so CMake/ld can find the env contents
ENV MAMBA_ROOT_PREFIX=/opt/conda
RUN /usr/local/bin/micromamba create -y -n ucxx -c rapidsai -c conda-forge \
     ucx-proc=*=gpu ucx libucxx cuda-version=12.9 \
&& /usr/local/bin/micromamba clean -a -y
# Path to UCX/UCXX runtime; do not expose as CONDA_PREFIX to CMake
ENV UCXX_ENV_DIR=${MAMBA_ROOT_PREFIX}/envs/ucxx

ARG GPU=ON
ARG BUILD_TYPE=release
ARG BUILD_BASE_DIR=/presto_native_${BUILD_TYPE}_gpu_${GPU}_build
ARG NUM_THREADS=12
ARG EXTRA_CMAKE_FLAGS="-DPRESTO_ENABLE_TESTING=OFF -DPRESTO_ENABLE_PARQUET=ON -DPRESTO_ENABLE_CUDF=${GPU} -DVELOX_BUILD_TESTING=OFF -DVELOX_USE_FETCHCONTENT_UCXX=ON -DTHRUST_IGNORE_CUB_VERSION_CHECK=ON"
ARG CUDA_ARCHITECTURES="75;80;86;90;100;120"

ENV CUDA_ARCHITECTURES=${CUDA_ARCHITECTURES}
ENV EXTRA_CMAKE_FLAGS=${EXTRA_CMAKE_FLAGS}
ENV NUM_THREADS=${NUM_THREADS}

RUN mkdir /runtime-libraries

RUN --mount=type=bind,source=presto/presto-native-execution,target=/presto_native_staging/presto,rw \
    --mount=type=bind,source=velox,target=/presto_native_staging/presto/velox \
    --mount=type=cache,target=${BUILD_BASE_DIR} \
    LD_LIBRARY_PATH=/opt/rh/gcc-toolset-12/root/usr/lib64:/usr/local/lib \
    LIBRARY_PATH=/opt/rh/gcc-toolset-12/root/usr/lib64:/usr/local/lib \
    ucx_DIR=${UCXX_ENV_DIR}/lib/cmake/ucx \
    UCX_DIR=${UCXX_ENV_DIR}/lib/cmake/ucx \
    CONDA_PREFIX= \
    make --directory="/presto_native_staging/presto" cmake-and-build BUILD_TYPE=${BUILD_TYPE} BUILD_DIR="" BUILD_BASE_DIR=${BUILD_BASE_DIR} && \
    !(LD_LIBRARY_PATH=${UCXX_ENV_DIR}/lib:/usr/local/lib:/usr/local/lib64 ldd ${BUILD_BASE_DIR}/presto_cpp/main/presto_server | grep -v "libcuda\.so\.1" | grep "not found") && \
    LD_LIBRARY_PATH=${UCXX_ENV_DIR}/lib:/usr/local/lib:/usr/local/lib64 ldd ${BUILD_BASE_DIR}/presto_cpp/main/presto_server | grep -v "libcuda\.so\.1" | awk 'NF == 4 { system("cp " $3 " /runtime-libraries") }' && \
    cp ${BUILD_BASE_DIR}/presto_cpp/main/presto_server /usr/bin

RUN mkdir /usr/lib64/presto-native-libs && \
    cp /runtime-libraries/* /usr/lib64/presto-native-libs/ && \
    echo "/usr/lib64/presto-native-libs" > /etc/ld.so.conf.d/presto_native.conf

COPY velox-testing/presto/docker/launch_presto_server.sh /opt

CMD ["bash", "/opt/launch_presto_server.sh"]
