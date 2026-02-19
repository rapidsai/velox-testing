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
ARG ENABLE_SCCACHE=OFF
ARG SCCACHE_SERVER_LOG="sccache=info"
ARG SCCACHE_VERSION=latest
ARG SCCACHE_RECACHE
ARG SCCACHE_NO_CACHE
ARG SCCACHE_NO_DIST_COMPILE

ENV CC=/opt/rh/gcc-toolset-14/root/bin/gcc \
    CXX=/opt/rh/gcc-toolset-14/root/bin/g++ \
    CUDA_ARCHITECTURES=${CUDA_ARCHITECTURES} \
    EXTRA_CMAKE_FLAGS=${EXTRA_CMAKE_FLAGS} \
    NUM_THREADS=${NUM_THREADS} \
    ENABLE_SCCACHE="${ENABLE_SCCACHE}" \
    SCCACHE_VERSION="${SCCACHE_VERSION}" \
    SCCACHE_SERVER_LOG="${SCCACHE_SERVER_LOG}" \
    SCCACHE_ERROR_LOG=/tmp/sccache.log \
    SCCACHE_CACHE_SIZE=107374182400 \
    SCCACHE_BUCKET=rapids-sccache-devs \
    SCCACHE_REGION=us-east-2 \
    SCCACHE_S3_NO_CREDENTIALS=false \
    SCCACHE_IDLE_TIMEOUT=0 \
    SCCACHE_DIST_AUTH_TYPE=token \
    SCCACHE_DIST_REQUEST_TIMEOUT=7140 \
    SCCACHE_DIST_SCHEDULER_URL="https://${TARGETARCH}.linux.sccache.rapids.nvidia.com" \
    SCCACHE_DIST_MAX_RETRIES=4 \
    SCCACHE_DIST_FALLBACK_TO_LOCAL_COMPILE=true

RUN mkdir /runtime-libraries

RUN \
    --mount=type=bind,source=presto/presto-native-execution,target=/presto_native_staging/presto \
    --mount=type=bind,source=velox,target=/presto_native_staging/presto/velox \
    --mount=type=cache,target=${BUILD_BASE_DIR} \
    --mount=type=cache,target=/root/.cache/sccache/preprocessor \
    --mount=type=cache,target=/root/.cache/sccache-dist-client \
    --mount=type=secret,id=github_token,env=SCCACHE_DIST_AUTH_TOKEN \
    --mount=type=secret,id=aws_credentials,target=/root/.aws/credentials \
    --mount=type=bind,source=velox-testing/velox/docker/sccache/sccache_setup.sh,target=/sccache_setup.sh,ro \
<<EOF
set -euxo pipefail;

source /opt/rh/gcc-toolset-14/enable;
export CC=/opt/rh/gcc-toolset-14/root/bin/gcc CXX=/opt/rh/gcc-toolset-14/root/bin/g++;

# Clear stale CMake cache if the compiler changed
if [ -f "${BUILD_BASE_DIR}/CMakeCache.txt" ]; then
  CACHED_CXX=$(grep -m1 'CMAKE_CXX_COMPILER:' "${BUILD_BASE_DIR}/CMakeCache.txt" | cut -d= -f2 || true);
  CURRENT_CXX=$(command -v "$CXX");
  if [ -n "$CACHED_CXX" ] && [ "$CACHED_CXX" != "$CURRENT_CXX" ]; then
    echo "Compiler changed ($CACHED_CXX -> $CURRENT_CXX), clearing CMake cache";
    rm -f "${BUILD_BASE_DIR}/CMakeCache.txt";
  fi
fi

if [ "$ENABLE_SCCACHE" = "ON" ]; then
  bash /sccache_setup.sh;
  sccache --zero-stats;
  EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS} -DCMAKE_C_COMPILER_LAUNCHER=sccache -DCMAKE_CXX_COMPILER_LAUNCHER=sccache -DCMAKE_CUDA_COMPILER_LAUNCHER=sccache";
  export NVCC_APPEND_FLAGS="${NVCC_APPEND_FLAGS:+$NVCC_APPEND_FLAGS }-t=100";
fi

SCCACHE_NO_DIST_COMPILE=1 \
make --directory="/presto_native_staging/presto" cmake-and-build BUILD_TYPE=${BUILD_TYPE} BUILD_DIR="" BUILD_BASE_DIR=${BUILD_BASE_DIR};

if [ "$ENABLE_SCCACHE" = "ON" ]; then
  echo "Post-build sccache statistics:";
  sccache --show-stats;
fi

!(LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib ldd ${BUILD_BASE_DIR}/presto_cpp/main/presto_server | grep "not found" | grep -v -E "libcuda\.so|libnvidia");
LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib ldd ${BUILD_BASE_DIR}/presto_cpp/main/presto_server | awk 'NF == 4 && $3 != "not" && $1 !~ /libcuda\.so|libnvidia/ { system("cp " $3 " /runtime-libraries") }';
cp ${BUILD_BASE_DIR}/presto_cpp/main/presto_server /usr/bin;
EOF

RUN mkdir /usr/lib64/presto-native-libs && \
    cp /runtime-libraries/* /usr/lib64/presto-native-libs/ && \
    echo "/usr/lib64/presto-native-libs" > /etc/ld.so.conf.d/presto_native.conf

COPY velox-testing/presto/docker/launch_presto_servers.sh velox-testing/presto/docker/presto_profiling_wrapper.sh /opt

CMD ["bash", "/opt/presto_profiling_wrapper.sh"]
