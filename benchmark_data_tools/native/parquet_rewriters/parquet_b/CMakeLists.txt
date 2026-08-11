add_executable(parquet_b_rewriter ParquetBRewriter.cpp)
target_link_libraries(
  parquet_b_rewriter PRIVATE cudf::cudf CUDA::cudart fmt::fmt
)
