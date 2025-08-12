FROM ghcr.io/facebookincubator/velox-dev:adapters

# Build-time configuration, these may be overridden in the docker compose yaml,
# environment variables, or via the docker build command
ARG USE_CLANG=false
ARG NUM_THREADS=8
ARG CUDA_VERSION=12.8
ARG CUDA_ARCHITECTURES=70
ARG ENABLE_TESTS=false
ARG BUILD_WITH_VELOX_ENABLE_CUDF=ON

# Environment mirroring upstream CI defaults
ENV VELOX_DEPENDENCY_SOURCE=SYSTEM \
    GTest_SOURCE=BUNDLED \
    cudf_SOURCE=BUNDLED \
    faiss_SOURCE=BUNDLED \
    USE_CLANG=${USE_CLANG} \
    CUDA_VERSION=${CUDA_VERSION} \
    MAKEFLAGS="NUM_THREADS=${NUM_THREADS} MAX_HIGH_MEM_JOBS=4 MAX_LINK_JOBS=4" \
    CUDA_ARCHITECTURES=${CUDA_ARCHITECTURES} \
    CUDA_COMPILER=/usr/local/cuda-${CUDA_VERSION}/bin/nvcc \
    ENABLE_TESTS=${ENABLE_TESTS} \
    BUILD_WITH_VELOX_ENABLE_CUDF=${BUILD_WITH_VELOX_ENABLE_CUDF}

COPY velox-testing/velox/docker/scripts/run_adapters.sh /run_adapters.sh
RUN chmod +x /run_adapters.sh

RUN --mount=type=bind,source=velox,target=/workspace/velox,rw \
    --mount=type=cache,target=/ccache,id=velox-ccache,sharing=locked,rw \
    --mount=type=cache,target=/buildcache,sharing=locked,rw \
   /bin/bash -c "/run_adapters.sh &> /tmp/adapters_build.log"  || \
     /bin/bash -c 'if [ -f /tmp/build_status ]; then \
       BUILD_STATUS=$(cat /tmp/build_status); \
       if echo "$BUILD_STATUS" | grep -q "VELOX_BUILD_STARTED"; then \
         echo "INFO: Velox build started but not completed, something went wrong."; \
         exit 1; \
       elif echo "$BUILD_STATUS" | grep -q "VELOX_TESTS_INCOMPLETE_AND_BUILD_COMPLETED"; then \
         echo "INFO: Velox tests incomplete, but build completed."; \
         exit 1; \
       elif echo "$BUILD_STATUS" | grep -q "VELOX_TESTS_FAILED_BUT_BUILD_COMPLETED"; then \
         echo "WARNING: Velox build completed but tests failed."; \
         exit 0; \
       elif echo "$BUILD_STATUS" | grep -q "VELOX_TESTS_PASSED_AND_BUILD_COMPLETED"; then \
         echo "INFO: Velox build and tests completed successfully."; \
         exit 0; \
       elif echo "$BUILD_STATUS" | grep -q "VELOX_TESTS_SKIPPED_AND_BUILD_COMPLETED"; then \
         echo "INFO: Velox build completed, tests skipped."; \
         exit 0; \
       else \
         echo "ERROR: Unknown build status in /tmp/build_status: $BUILD_STATUS"; \
         exit 1; \
       fi \
     else \
       echo "ERROR: /tmp/build_status not found, something went wrong."; \
       exit 1; \
     fi'

# Keep container alive with a simple message; build already occurred during image build
CMD cat /tmp/adapters_build.log && echo 'Velox built into /opt/velox-build/release during image build. Container idle.' && sleep infinity 