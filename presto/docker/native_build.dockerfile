FROM presto/prestissimo-dependency:centos9

ARG GPU=ON
ARG BUILD_TYPE=release
ARG BUILD_BASE_DIR=/presto_native_${BUILD_TYPE}_gpu_${GPU}_build
ARG NUM_THREADS=12
ARG EXTRA_CMAKE_FLAGS="-DPRESTO_ENABLE_TESTING=OFF -DPRESTO_ENABLE_PARQUET=ON -DPRESTO_ENABLE_CUDF=${GPU} -DVELOX_BUILD_TESTING=OFF"
ARG CUDA_VERSION=12.8

# Build for all supported architectures. Note that the `CUDA_ARCHITECTURES="native"` option does not work for docker image builds.
ENV CUDA_ARCHITECTURES="70;75;80;86;89;90;100;120"
ENV EXTRA_CMAKE_FLAGS=${EXTRA_CMAKE_FLAGS}
ENV NUM_THREADS=${NUM_THREADS}

RUN mkdir /runtime-libraries

# TODO: revert this change once facebook updates the prestissimo image
RUN dnf install -y -q libnvjitlink-$(echo ${CUDA_VERSION} | tr . -) libnvjitlink-devel-$(echo ${CUDA_VERSION} | tr . -)

RUN --mount=type=bind,source=presto/presto-native-execution,target=/presto_native_staging/presto \
    --mount=type=bind,source=velox,target=/presto_native_staging/presto/velox \
    --mount=type=cache,target=${BUILD_BASE_DIR} \
    make --directory="/presto_native_staging/presto" cmake-and-build BUILD_TYPE=${BUILD_TYPE} BUILD_DIR="" BUILD_BASE_DIR=${BUILD_BASE_DIR} && \
    cp ${BUILD_BASE_DIR}/presto_cpp/main/presto_server /usr/bin

CMD bash -c "LD_LIBRARY_PATH=/usr/local/lib presto_server --etc-dir=/opt/presto-server/etc"
