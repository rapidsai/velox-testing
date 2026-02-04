# set all 'ARG' defaults to something like 'notset', to reduce the risk of
# bugs like "accidentally forgot to pass through configuration, fell back to a default".
#
ARG LINUX_VER=notset

FROM nvidia/cuda:${CUDA_VER}-base-${LINUX_VER}

ARG CUDA_ARCHITECTURES=notset
ENV CUDA_ARCHITECTURES=${CUDA_ARCHITECTURES}
