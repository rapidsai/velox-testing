FROM presto/prestissimo-dependency:centos9

RUN rpm --import https://developer.download.nvidia.com/compute/cuda/repos/ubuntu1804/x86_64/7fa2af80.pub && \
    dnf config-manager --add-repo "https://developer.download.nvidia.com/devtools/repos/rhel$(source /etc/os-release; echo ${VERSION_ID%%.*})/$(rpm --eval '%{_arch}' | sed s/aarch/arm/)/" && \
    dnf install -y nsight-systems-cli-2025.5.1

ARG GPU=ON
ARG BUILD_TYPE=release
ARG BUILD_BASE_DIR=/presto_native_${BUILD_TYPE}_gpu_${GPU}_build
ARG NUM_THREADS=12
ARG EXTRA_CMAKE_FLAGS="\
    -DPRESTO_ENABLE_TESTING=OFF \
    -DPRESTO_ENABLE_PARQUET=ON \
    -DPRESTO_ENABLE_S3=ON \
    -DPRESTO_ENABLE_CUDF=${GPU} \
    -DVELOX_BUILD_TESTING=OFF \
    -DPRESTO_STATS_REPORTER_TYPE=PROMETHEUS"
ARG CUDA_ARCHITECTURES="75;80;86;90;100;120"

ENV CC=/opt/rh/gcc-toolset-14/root/bin/gcc
ENV CXX=/opt/rh/gcc-toolset-14/root/bin/g++
ENV CUDA_ARCHITECTURES=${CUDA_ARCHITECTURES}
ENV EXTRA_CMAKE_FLAGS=${EXTRA_CMAKE_FLAGS}
ENV NUM_THREADS=${NUM_THREADS}

RUN mkdir /runtime-libraries

RUN --mount=type=bind,source=presto/presto-native-execution,target=/presto_native_staging/presto \
    --mount=type=bind,source=velox,target=/presto_native_staging/presto/velox \
    --mount=type=cache,target=${BUILD_BASE_DIR} \
    source /opt/rh/gcc-toolset-14/enable && \
    CC=/opt/rh/gcc-toolset-14/root/bin/gcc CXX=/opt/rh/gcc-toolset-14/root/bin/g++ \
    make --directory="/presto_native_staging/presto" cmake-and-build BUILD_TYPE=${BUILD_TYPE} BUILD_DIR="" BUILD_BASE_DIR=${BUILD_BASE_DIR} && \
    !(LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib ldd ${BUILD_BASE_DIR}/presto_cpp/main/presto_server | grep "not found" | grep -v -E "libcuda\\.so|libnvidia") && \
    LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib ldd ${BUILD_BASE_DIR}/presto_cpp/main/presto_server | awk 'NF == 4 && $3 != "not" && $1 !~ /libcuda\\.so|libnvidia/ { system("cp " $3 " /runtime-libraries") }' && \
    cp ${BUILD_BASE_DIR}/presto_cpp/main/presto_server /usr/bin && \
    replay_bin="" && \
    for candidate in \
      "${BUILD_BASE_DIR}/velox/velox/experimental/cudf/tools/velox_cudf_hashagg_replay" \
      "${BUILD_BASE_DIR}/velox/velox/experimental/cudf/tests/velox_cudf_hashagg_replay" \
      "${BUILD_BASE_DIR}/velox/experimental/cudf/tools/velox_cudf_hashagg_replay" \
      "${BUILD_BASE_DIR}/velox/experimental/cudf/tests/velox_cudf_hashagg_replay" \
      "${BUILD_BASE_DIR}/velox_cudf_hashagg_replay"; do \
      if [[ -f "$candidate" ]]; then \
        replay_bin="$candidate"; \
        break; \
      fi; \
    done && \
    if [[ -z "$replay_bin" ]]; then \
      echo "ERROR: velox_cudf_hashagg_replay binary not found in build output"; \
      exit 1; \
    fi && \
    cp "$replay_bin" /usr/bin

RUN mkdir /usr/lib64/presto-native-libs && \
    cp /runtime-libraries/* /usr/lib64/presto-native-libs/ && \
    echo "/usr/lib64/presto-native-libs" > /etc/ld.so.conf.d/presto_native.conf

COPY velox-testing/presto/docker/launch_presto_servers.sh velox-testing/presto/docker/presto_profiling_wrapper.sh /opt

CMD ["bash", "/opt/presto_profiling_wrapper.sh"]
