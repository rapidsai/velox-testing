# Presto GPU Profiling with NVIDIA Nsight Systems

This document explains how to use NVIDIA Nsight Systems (nsys) profiling with the Presto GPU variant benchmark.

## Overview

The benchmark script automatically integrates nsys profiling when:
- The `-p/--profile` flag is enabled
- The variant being tested is `native-gpu`
- `nsys` command is available in the system PATH

## Prerequisites

### Install NVIDIA Nsight Systems

#### Ubuntu/Debian
```bash
# Download from NVIDIA Developer site
wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/nsight-systems-2023.3.1_2023.3.1.92-1_amd64.deb
sudo dpkg -i nsight-systems-2023.3.1_2023.3.1.92-1_amd64.deb
sudo apt-get install -f
```

#### From CUDA Toolkit
```bash
# If you have CUDA toolkit installed
export PATH=$PATH:/usr/local/cuda/bin
which nsys  # Should show the nsys location
```

### Verify Installation
```bash
nsys --version
# Should output version information
```

## Usage

### Enable Profiling in Benchmark

```bash
# Profile GPU variant only
./benchmark_presto_variants.sh -v native-gpu -p

# Profile with specific query
./benchmark_presto_variants.sh -v native-gpu -q 1 -p

# Profile multiple runs
./benchmark_presto_variants.sh -v native-gpu -q 1,6,14 -r 3 -p
```

### Profiling Configuration

The benchmark uses these nsys settings:
```bash
nsys profile \
    -t nvtx,cuda,osrt \
    -f true \
    --cuda-memory-usage=true \
    --cuda-um-cpu-page-faults=true \
    --cuda-um-gpu-page-faults=true \
    --output='output_file.nsys-rep'
```

#### Profile Types
- **`nvtx`** - NVIDIA Tools Extension events (custom annotations)
- **`cuda`** - CUDA API calls and kernel execution
- **`osrt`** - Operating system runtime (threads, processes)

#### Additional Options
- **`--cuda-memory-usage`** - Track GPU memory allocations
- **`--cuda-um-*-page-faults`** - Track unified memory page faults

## Output Files

Profiling generates `.nsys-rep` files in the results directory:
```
presto-benchmark-results/20240827_143022/native-gpu/
├── q01_run1_gpu.nsys-rep
├── q01_run2_gpu.nsys-rep
├── q06_run1_gpu.nsys-rep
└── ...
```

## Analyzing Profile Data

### Using Nsight Systems GUI

```bash
# Open specific profile
nsys-ui q01_run1_gpu.nsys-rep

# Or launch GUI and open file from interface
nsys-ui
```

### Command Line Analysis

```bash
# Basic statistics
nsys stats q01_run1_gpu.nsys-rep

# CUDA kernel summary
nsys stats --report cudaapisum q01_run1_gpu.nsys-rep

# Memory operations summary
nsys stats --report cudamemchrtypesum q01_run1_gpu.nsys-rep

# Export to CSV for custom analysis
nsys stats --report cuda_gpu_kern_sum --format csv q01_run1_gpu.nsys-rep > kernels.csv
```

### Export Timeline

```bash
# Export to various formats
nsys export --type sqlite q01_run1_gpu.nsys-rep
nsys export --type text q01_run1_gpu.nsys-rep
```

## Key Metrics to Analyze

### 1. CUDA Kernel Performance
- **Kernel execution time** - Time spent in GPU kernels
- **Grid/block configuration** - Parallelization efficiency
- **Occupancy** - How well kernels utilize the GPU

### 2. Memory Transfer Analysis
- **Host-to-Device transfers** - Data uploads to GPU
- **Device-to-Host transfers** - Results download from GPU
- **Memory bandwidth utilization** - Transfer efficiency

### 3. CUDF Operations
Look for CUDF-specific operations:
- `cudf::hash_partition`
- `cudf::join`
- `cudf::groupby`
- `cudf::scan`

### 4. Velox Integration
- **NVTX markers** - Custom annotations from Velox
- **Operator timelines** - Individual query operator execution
- **Memory pool usage** - GPU memory management

## Example Analysis Workflow

### 1. Run Benchmark with Profiling
```bash
./benchmark_presto_variants.sh -v native-gpu -q 1 -r 1 -p
```

### 2. Quick Command Line Analysis
```bash
cd presto-benchmark-results/latest/native-gpu/
nsys stats --report cuda_gpu_kern_sum q01_run1_gpu.nsys-rep
```

Example output:
```
Time (%)  Total Time (ns)  Instances  Avg (ns)      Med (ns)      Min (ns)     Max (ns)     StdDev (ns)    Name
--------  ---------------  ---------  ------------  ------------  -----------  -----------  -------------  ----
   45.2     2,345,123,456        156   15,032,844    12,456,789    1,234,567   45,678,901    8,765,432   cudf::hash_join_kernel
   23.1     1,198,765,432         89   13,467,829    11,234,567    2,345,678   34,567,890    6,543,210   cudf::group_by_kernel
   18.7       970,123,456         78   12,437,481    10,123,456    1,987,654   28,765,432    5,432,109   cudf::scan_kernel
...
```

### 3. Detailed GUI Analysis
```bash
nsys-ui q01_run1_gpu.nsys-rep
```

In the GUI:
1. **Timeline View** - See chronological execution
2. **Events View** - Filter specific event types
3. **Analysis** - Built-in performance analysis tools

### 4. Performance Comparison
```bash
# Compare multiple runs
nsys stats --report cuda_gpu_kern_sum q01_run1_gpu.nsys-rep q01_run2_gpu.nsys-rep
```

## Troubleshooting

### Common Issues

1. **nsys not found**
   ```bash
   # Add CUDA tools to PATH
   export PATH=$PATH:/usr/local/cuda/bin
   ```

2. **Permission denied**
   ```bash
   # Run with sudo if needed (not recommended for production)
   sudo ./benchmark_presto_variants.sh -v native-gpu -p
   ```

3. **Large profile files**
   - Profile files can be several GB for complex queries
   - Use shorter timeouts or simpler queries for initial analysis
   - Consider profiling single runs: `-r 1`

4. **No GPU activity captured**
   - Verify GPU variant is actually using GPU: check logs
   - Ensure CUDF is enabled in the build
   - Check NVIDIA driver compatibility

### Debug Mode

Enable verbose profiling output:
```bash
export NSYS_VERBOSE=1
./benchmark_presto_variants.sh -v native-gpu -q 1 -p
```

## Best Practices

### 1. Profile Strategy
- Start with simple queries (Q1, Q6) before complex ones
- Profile single runs first, then multiple for consistency
- Use smaller datasets (sf1) for initial analysis

### 2. Analysis Focus
- Focus on longest-running kernels first
- Look for memory transfer bottlenecks
- Identify opportunities for kernel fusion
- Check for GPU utilization gaps

### 3. Performance Optimization
- Use profile data to identify bottlenecks
- Optimize memory access patterns
- Tune kernel launch configurations
- Consider data layout optimizations

## Integration with Other Tools

### 1. Combine with System Monitoring
```bash
# Monitor GPU utilization during benchmark
nvidia-smi dmon -s pucvmet -d 1 &
./benchmark_presto_variants.sh -v native-gpu -p
killall nvidia-smi
```

### 2. Memory Profiling
```bash
# Use cuda-memcheck for memory debugging
cuda-memcheck --tool memcheck ./presto_server
```

### 3. Compute Sanitizer
```bash
# Check for race conditions and memory errors
compute-sanitizer ./presto_server
```

## Advanced Usage

### Custom Profiling Options

Modify the benchmark script's nsys command for specific needs:
```bash
# Add custom markers
nsys profile -t nvtx,cuda --nvtx-capture=range@custom_range

# Focus on specific GPU operations
nsys profile -t cuda --cuda-api-trace=driver,runtime

# Profile with sampling
nsys profile --sample=cpu --cpuctxsw=true
```

### Automated Analysis

Create scripts to automatically extract key metrics:
```bash
#!/bin/bash
# analyze_profiles.sh
for profile in *.nsys-rep; do
    echo "Analyzing $profile..."
    nsys stats --report cuda_gpu_kern_sum --format csv "$profile" > "${profile%.nsys-rep}_kernels.csv"
    nsys stats --report cuda_gpu_mem_time_sum --format csv "$profile" > "${profile%.nsys-rep}_memory.csv"
done
```

This comprehensive profiling approach helps optimize GPU performance and identify bottlenecks in the Presto GPU execution path.
