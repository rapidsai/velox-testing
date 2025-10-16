FROM ghcr.io/facebookincubator/velox-dev:adapters

ARG CUDA_VERSION=12.8

# Copy and run CentOS adapters setup script
COPY velox/scripts/ /scripts

RUN set -euxo pipefail && \
    if ! dnf list installed cuda-nvcc-$(echo ${CUDA_VERSION} | tr '.' '-') 1>/dev/null || \
       ! dnf list installed libnvjitlink-devel-$(echo ${CUDA_VERSION} | tr '.' '-') 1>/dev/null; then \
       bash -c "source /scripts/setup-centos-adapters.sh &&  install_cuda ${CUDA_VERSION}"; \
    fi && \
    pip install cmake==3.30.4
