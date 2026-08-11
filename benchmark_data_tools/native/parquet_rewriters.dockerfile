ARG BASE_IMAGE=presto/prestissimo-dependency:centos9
FROM ${BASE_IMAGE}

ARG CUDA_ARCHITECTURES=100

COPY benchmark_data_tools/native/parquet_rewriters /src/parquet_rewriters

RUN source /opt/rh/gcc-toolset-14/enable && \
    cmake -S /src/parquet_rewriters -B /build/parquet_rewriters -G Ninja \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_CUDA_ARCHITECTURES=${CUDA_ARCHITECTURES} && \
    cmake --build /build/parquet_rewriters --parallel && \
    cp /build/parquet_rewriters/parquet_b/parquet_b_rewriter \
       /build/parquet_rewriters/parquet_b_sort/parquet_b_sort_rewriter \
       /usr/local/bin/
