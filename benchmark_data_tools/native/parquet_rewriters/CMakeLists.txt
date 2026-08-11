cmake_minimum_required(VERSION 3.30)
project(parquet_rewriters LANGUAGES CXX CUDA)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CUDA_STANDARD 20)
set(CMAKE_CUDA_STANDARD_REQUIRED ON)

find_package(CUDAToolkit REQUIRED)
find_package(cudf CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)

add_subdirectory(parquet_b)
add_subdirectory(parquet_b_sort)
