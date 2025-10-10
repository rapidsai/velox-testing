FROM ghcr.io/facebookincubator/velox-dev:adapters

ARG CUDA_VERSION=12.8

# Copy and run CentOS adapters setup script
COPY velox/scripts/ /scripts

RUN set -euxo pipefail && \
    if ! dnf list installed cuda-nvcc-$(echo ${CUDA_VERSION} | tr '.' '-') 1>/dev/null || \
       ! dnf list installed libnvjitlink-devel-$(echo ${CUDA_VERSION} | tr '.' '-') 1>/dev/null; then \
      source /scripts/setup-centos-adapters.sh && \
      install_cuda ${CUDA_VERSION}; \
    fi && \
    pip install cmake==3.30.4

# Build and install newer curl to replace system version
RUN set -euxo pipefail && \
    # Install build dependencies
    dnf install -y wget tar make gcc openssl-devel zlib-devel libnghttp2-devel && \
    # Download and build curl 7.88.1 with curl_url_strerror support
    cd /tmp && \
    wget https://curl.se/download/curl-7.88.1.tar.gz && \
    tar -xzf curl-7.88.1.tar.gz && \
    cd curl-7.88.1 && \
    ./configure --prefix=/usr \
                --libdir=/usr/lib64 \
                --with-openssl \
                --with-zlib \
                --with-nghttp2 \
                --enable-shared \
                --disable-static && \
    make -j$(nproc) && \
    # Install with new curl
    make install && \
    # Update library cache
    ldconfig && \
    # Verify the new curl works and has the required symbol
    curl --version && \
    nm -D /usr/lib64/libcurl.so | grep curl_url_strerror && \
    # Clean up build files
    cd / && rm -rf /tmp/curl-7.88.1*
