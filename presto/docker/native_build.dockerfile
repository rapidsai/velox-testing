FROM presto/prestissimo-dependency:centos9

# Build arguments with defaults
ARG GPU=ON
ARG BUILD_TYPE=release
ARG BUILD_BASE_DIR=/presto_native_${BUILD_TYPE}_gpu_${GPU}_build
ARG NUM_THREADS=12
ARG EXTRA_CMAKE_FLAGS="-DPRESTO_ENABLE_TESTING=OFF -DPRESTO_ENABLE_PARQUET=ON -DPRESTO_ENABLE_CUDF=${GPU} -DVELOX_BUILD_TESTING=OFF -DVELOX_ENABLE_CUDF=${GPU}"

# Runtime environment variables
# Build for all supported architectures. Note that the `CUDA_ARCHITECTURES="native"` option does not work for docker image builds.
ENV CUDA_ARCHITECTURES="70;75;80;86;89;90;100;120" \
    EXTRA_CMAKE_FLAGS=${EXTRA_CMAKE_FLAGS} \
    NUM_THREADS=${NUM_THREADS}

RUN mkdir /runtime-libraries

RUN --mount=type=bind,source=presto/presto-native-execution,target=/presto_native_staging/presto \
    --mount=type=bind,source=velox,target=/presto_native_staging/presto/velox \
    --mount=type=cache,target=${BUILD_BASE_DIR} \
    make --directory="/presto_native_staging/presto" cmake-and-build BUILD_TYPE=${BUILD_TYPE} BUILD_DIR="" BUILD_BASE_DIR=${BUILD_BASE_DIR} && \
    ldd ${BUILD_BASE_DIR}/presto_cpp/main/presto_server | awk 'NF == 4 { system("cp " $3 " /runtime-libraries") }' && \
    find /usr/local/lib* -name "libboost_*.so*" -exec cp {} /runtime-libraries/ \; 2>/dev/null || true && \
    find /usr/lib* -name "libboost_*.so*" -exec cp {} /runtime-libraries/ \; 2>/dev/null || true && \
    find /lib* -name "libboost_*.so*" -exec cp {} /runtime-libraries/ \; 2>/dev/null || true && \
    cp ${BUILD_BASE_DIR}/presto_cpp/main/presto_server /usr/bin

RUN mkdir /usr/lib64/presto-native-libs && \
    cp /runtime-libraries/* /usr/lib64/presto-native-libs/ && \
    echo "/usr/lib64/presto-native-libs" > /etc/ld.so.conf.d/presto_native.conf

# Create entrypoint script for better signal handling
RUN echo '#!/bin/bash' > /entrypoint.sh && \
    echo 'set -euo pipefail' >> /entrypoint.sh && \
    echo 'ldconfig' >> /entrypoint.sh && \
    echo 'echo "Available Boost libraries:"' >> /entrypoint.sh && \
    echo 'ls -la /usr/lib64/presto-native-libs/libboost* 2>/dev/null | head -10 || echo "No Boost libraries found"' >> /entrypoint.sh && \
    echo 'exec presto_server --etc-dir=/opt/presto-server/etc "$@"' >> /entrypoint.sh && \
    chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
