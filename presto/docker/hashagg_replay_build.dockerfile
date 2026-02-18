ARG PRESTO_DEPS_IMAGE=presto/prestissimo-dependency:centos9
FROM ${PRESTO_DEPS_IMAGE}

ARG BUILD_TYPE=release
ARG BUILD_BASE_DIR=/velox_hashagg_replay_${BUILD_TYPE}_build
ARG NUM_THREADS=12
ARG CUDA_ARCHITECTURES="75;80;86;90;100;120"
ARG BASE_CMAKE_FLAGS="\
    -DVELOX_ENABLE_CUDF=ON \
    -DVELOX_ENABLE_PARQUET=ON \
    -DVELOX_ENABLE_ARROW=ON \
    -DVELOX_BUILD_TESTING=OFF \
    -DVELOX_ENABLE_BENCHMARKS=OFF \
    -DVELOX_ENABLE_EXAMPLES=OFF \
    -DVELOX_MONO_LIBRARY=ON \
    -DVELOX_BUILD_SHARED=ON"
ARG EXTRA_CMAKE_FLAGS=""

ENV CC=/opt/rh/gcc-toolset-14/root/bin/gcc
ENV CXX=/opt/rh/gcc-toolset-14/root/bin/g++
ENV CUDA_ARCHITECTURES=${CUDA_ARCHITECTURES}
ENV BASE_CMAKE_FLAGS=${BASE_CMAKE_FLAGS}
ENV EXTRA_CMAKE_FLAGS=${EXTRA_CMAKE_FLAGS}
ENV NUM_THREADS=${NUM_THREADS}
ENV HASHAGG_REPLAY_DUMP_DIR=/tmp/hashagg_probe_dumps/hashagg_probe_1771056019538078_1

RUN mkdir /runtime-libraries

RUN cuda_version="${CUDA_VERSION:-}" && \
    if [ -n "${cuda_version}" ]; then \
      dashed="$(echo "${cuda_version}" | tr '.' '-')"; \
      dnf install -y "cuda-command-line-tools-${dashed}"; \
    else \
      dnf install -y cuda-command-line-tools; \
    fi && \
    dnf clean all && \
    command -v compute-sanitizer >/dev/null && \
    compute-sanitizer --version

RUN mkdir -p /tmp/hashagg_probe_dumps
COPY velox-testing/velox/scripts/hashagg_probe_1771056019538078_1 \
  /tmp/hashagg_probe_dumps/hashagg_probe_1771056019538078_1

RUN --mount=type=bind,source=velox,target=/workspace/velox \
    --mount=type=cache,target=${BUILD_BASE_DIR} \
    . /opt/rh/gcc-toolset-14/enable && \
    cmake -S /workspace/velox -B "${BUILD_BASE_DIR}" \
      -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" \
      -DCMAKE_PREFIX_PATH="/usr/local" \
      -DCMAKE_CUDA_ARCHITECTURES="${CUDA_ARCHITECTURES}" \
      ${BASE_CMAKE_FLAGS} ${EXTRA_CMAKE_FLAGS} && \
    cmake --build ${BUILD_BASE_DIR} -j ${NUM_THREADS} --target velox_cudf_hashagg_replay && \
    replay_bin=""; \
    for candidate in \
      "${BUILD_BASE_DIR}/velox/velox/experimental/cudf/tools/velox_cudf_hashagg_replay" \
      "${BUILD_BASE_DIR}/velox/velox/experimental/cudf/tests/velox_cudf_hashagg_replay" \
      "${BUILD_BASE_DIR}/velox/experimental/cudf/tools/velox_cudf_hashagg_replay" \
      "${BUILD_BASE_DIR}/velox/experimental/cudf/tests/velox_cudf_hashagg_replay" \
      "${BUILD_BASE_DIR}/velox_cudf_hashagg_replay"; do \
      if [ -f "$candidate" ]; then \
        replay_bin="$candidate"; \
        break; \
      fi; \
    done; \
    if [ -z "$replay_bin" ]; then \
      echo "ERROR: velox_cudf_hashagg_replay binary not found in build output"; \
      exit 1; \
    fi; \
    REPLAY_LD_PATH="${BUILD_BASE_DIR}:${BUILD_BASE_DIR}/lib:${BUILD_BASE_DIR}/velox:${BUILD_BASE_DIR}/_deps/cudf-build:${BUILD_BASE_DIR}/_deps/rmm-build:${BUILD_BASE_DIR}/_deps/rapids_logger-build:${BUILD_BASE_DIR}/_deps/kvikio-build:${BUILD_BASE_DIR}/_deps/nvcomp_proprietary_binary-src/lib64"; \
    !(LD_LIBRARY_PATH=${REPLAY_LD_PATH}:/usr/local/lib ldd "$replay_bin" | grep "not found" | grep -v -E "libcuda\\.so|libnvidia") && \
    LD_LIBRARY_PATH=${REPLAY_LD_PATH}:/usr/local/lib ldd "$replay_bin" | awk 'NF == 4 && $3 != "not" && $1 !~ /libcuda\\.so|libnvidia/ { system("cp " $3 " /runtime-libraries") }' && \
    cp "$replay_bin" /usr/bin

RUN mkdir /usr/lib64/velox-hashagg-replay-libs && \
    cp /runtime-libraries/* /usr/lib64/velox-hashagg-replay-libs/ && \
    echo "/usr/lib64/velox-hashagg-replay-libs" > /etc/ld.so.conf.d/velox_hashagg_replay.conf
