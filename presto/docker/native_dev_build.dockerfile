FROM presto/prestissimo-dependency:centos9

ARG CUDA_ARCHITECTURES="75;80;86;90;100;120"

ENV CUDA_ARCHITECTURES=${CUDA_ARCHITECTURES}
ENV PRESTO_SRC_ROOT=/workspace/presto/presto-native-execution
ENV VELOX_SRC_ROOT=/workspace/presto/presto-native-execution/velox
ENV PRESTO_DEV_BUILD_BASE=/workspace/build

RUN rpm --import https://developer.download.nvidia.com/compute/cuda/repos/ubuntu1804/x86_64/7fa2af80.pub && \
    dnf config-manager --add-repo "https://developer.download.nvidia.com/devtools/repos/rhel$(source /etc/os-release; echo ${VERSION_ID%%.*})/$(rpm --eval '%{_arch}' | sed s/aarch/arm/)/" && \
    dnf install -y nsight-systems-cli

RUN dnf install -y bzip2-devel gdb gcc-toolset-14 && dnf clean all

ENV GCC_TOOLSET_ROOT=/opt/rh/gcc-toolset-14/root
ENV PATH=${GCC_TOOLSET_ROOT}/bin:${PATH}
ENV LD_LIBRARY_PATH=${GCC_TOOLSET_ROOT}/lib64:${LD_LIBRARY_PATH}
ENV CC=${GCC_TOOLSET_ROOT}/bin/gcc
ENV CXX=${GCC_TOOLSET_ROOT}/bin/g++

ENV HOME=/workspace/home
RUN mkdir -p ${HOME}

COPY velox-testing/presto/docker/launch_presto_server_dev.sh /opt/launch_presto_server_dev.sh
RUN chmod +x /opt/launch_presto_server_dev.sh

WORKDIR /workspace

CMD ["bash", "/opt/launch_presto_server_dev.sh"]

