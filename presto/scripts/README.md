# Presto Deployment Variants Benchmark

This directory contains scripts for benchmarking TPC-H queries across three different Presto deployment variants:

1. **Java-based Presto** - Standard Java implementation
2. **Native CPU Presto with Velox** - C++ implementation using Velox for CPU execution
3. **Native GPU Presto with Velox** - C++ implementation using Velox + CUDF for GPU acceleration

## Quick Start

```bash
# Run all variants with all TPC-H queries (1-22)
./benchmark_presto_variants.sh

# Run only the GPU variant with query 1
./benchmark_presto_variants.sh -v native-gpu -q 1

# Run Java and CPU variants with queries 1-5, enable profiling
./benchmark_presto_variants.sh -v java,native-cpu -q 1,2,3,4,5 -p
```

## Prerequisites

### System Requirements
- Docker and Docker Compose
- Java 8+ (for Presto CLI)
- NVIDIA GPU and drivers (for GPU variant)
- NVIDIA Container Toolkit (for GPU variant)
- CUDA toolkit (for GPU variant)
- `nsys` (optional, for GPU profiling)

### Data Setup
The benchmark requires:
- **Hive Connector**: Reads TPC-H data from local parquet files
- Parquet files must be available in the specified data directory
- Script will fail if no parquet files are found

#### Auto-Generation with benchmark_data_tools
The script can automatically generate TPC-H data using the integrated `benchmark_data_tools`:

- **Automatic Detection**: Scale factor is extracted from directory name (e.g., `tpch_sf10` → SF10)
- **Data Generation**: Creates parquet files for all TPC-H tables
- **Query Generation**: Auto-generates `queries.json` if missing
- **Schema Conversion**: Converts decimals to floats for better compatibility

**Requirements for auto-generation:**
- Python 3 with pip
- `benchmark_data_tools` directory in repository
- DuckDB and dependencies (auto-installed)

### Docker Images
Ensure the following Docker images are available:
- `prestodb/presto:latest` (for Java variant)
- `presto-native-worker-cpu:latest` (for CPU variant)
- `presto-native-worker-gpu:latest` (for GPU variant)

## Usage

```bash
./benchmark_presto_variants.sh [OPTIONS]
```

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `-v, --variants` | Comma-separated list of variants to run | `java,native-cpu,native-gpu` |
| `-q, --queries` | Comma-separated list of TPC-H queries (1-22) | All queries |
| `-r, --runs` | Number of runs per query per variant | `3` |
| `-t, --timeout` | Query timeout in seconds | `300` |
| `-s, --schema` | TPC-H schema/scale factor | `sf1` |
| `-d, --data-dir` | Path to TPC-H parquet data | Auto-detected |
| `-p, --profile` | Enable nsys profiling for GPU variants | `false` |
| `-g, --generate-data` | Auto-generate data/queries using benchmark_data_tools | `false` |
| `-h, --help` | Show help message | - |

### Examples

```bash
# Benchmark all variants with default settings
./benchmark_presto_variants.sh

# Quick test with query 1 only
./benchmark_presto_variants.sh -q 1 -r 1

# GPU variant only with profiling
./benchmark_presto_variants.sh -v native-gpu -p

# Custom data directory and specific queries
./benchmark_presto_variants.sh -d /path/to/tpch/data -q 1,6,14,22

# Auto-generate TPC-H SF10 data and benchmark GPU variant only
./benchmark_presto_variants.sh -g -v native-gpu -d /path/to/tpch_sf10 -s sf10

# Performance comparison: Java vs Native CPU
./benchmark_presto_variants.sh -v java,native-cpu -q 1,2,3,4,5 -r 5
```

## Output Structure

Results are saved in timestamped directories under `presto-benchmark-results/`:

```
presto-benchmark-results/
└── 20240827_143022/
    ├── benchmark_summary.txt      # Overall summary
    ├── machine_config.txt         # System configuration
    ├── java/                      # Java variant results
    │   ├── summary.txt
    │   ├── results.json
    │   ├── timings.csv
    │   ├── q01.sql
    │   ├── q01_run1_raw.out
    │   └── q01_run1_timing.out
    ├── native-cpu/               # Native CPU variant results
    │   └── ...
    └── native-gpu/               # Native GPU variant results
        ├── ...
        └── q01_run1_gpu.nsys-rep # GPU profiling data (if enabled)
```

### Output Files

- **`benchmark_summary.txt`** - High-level summary of all variants
- **`machine_config.txt`** - Complete system configuration
- **`summary.txt`** - Per-variant summary with statistics
- **`results.json`** - Structured query timing results
- **`timings.csv`** - CSV format timing data for analysis
- **`q##.sql`** - Generated TPC-H query SQL
- **`q##_run#_raw.out`** - Complete Presto CLI output
- **`q##_run#_timing.out`** - Command timing information
- **`*.nsys-rep`** - NVIDIA Nsight profiling data (GPU only)

## Timing Methodology

The script extracts **actual query execution times** from Presto's debug output, not wrapper script times. This provides more accurate measurements by parsing lines like:

```
Query 20240827_143022_00001_abcde finished in 2.34s
```

The timing extraction hierarchy:
1. Parse "Query ... finished in X.Xs" from debug output
2. Fallback to "CPU: X.Xs" timing
3. Fallback to total command execution time

## GPU Profiling

When profiling is enabled (`-p` flag), the GPU variant uses NVIDIA Nsight Systems to capture:
- CUDA kernel execution
- Memory transfers
- NVTX annotations
- CPU/GPU timeline correlation

Profiling files (`.nsys-rep`) can be opened with:
```bash
nsys-ui q01_run1_gpu.nsys-rep
```

## Troubleshooting

### Common Issues

1. **Docker Compose not found**
   ```bash
   # Install Docker Compose
   sudo apt-get install docker-compose-plugin
   ```

2. **Presto fails to start**
   - Check Docker container logs
   - Ensure ports 8080 is available
   - Verify Docker images exist

3. **GPU variant fails**
   - Check NVIDIA driver: `nvidia-smi`
   - Verify CUDA installation: `nvcc --version`
   - Check NVIDIA Container Toolkit

4. **Hive connector issues**
   - Verify data directory contains parquet files
   - Check file permissions
   - Use `--generate-data` to auto-create data if missing
   - Script requires working Hive connector with parquet data

5. **Profiling not working**
   - Install NVIDIA Nsight Systems
   - Check nsys is in PATH: `which nsys`

### Debug Mode

Add debug output to investigate issues:
```bash
export PRESTO_DEBUG=true
./benchmark_presto_variants.sh -v native-gpu -q 1
```

## Configuration Files

The benchmark uses configuration files in `../docker/config/`:

- **`etc_coordinator/config_*.properties`** - Coordinator settings
- **`etc_worker/config_*.properties`** - Worker settings  
- **`etc_common/catalog/*.properties`** - Catalog configurations
- **`docker-compose.*.yml`** - Container orchestration

### Key Settings

For optimal GPU performance, the native GPU variant uses:
- Single node output optimization
- Reduced task concurrency
- GPU memory pool configuration
- CUDF acceleration settings

## Data Sources

### Hive Connector
- Reads actual TPC-H parquet files
- Requires pre-generated data in specified directory
- More realistic for production workloads
- Data directory structure:
  ```
  tpch_sf1/
  ├── customer/
  ├── lineitem/
  ├── nation/
  ├── orders/
  ├── part/
  ├── partsupp/
  ├── region/
  └── supplier/
  ```

## Performance Notes

### Expected Performance Characteristics

1. **Java Variant**
   - Baseline performance
   - Consistent results
   - Full SQL feature support

2. **Native CPU Variant**
   - 2-5x faster than Java for analytical queries
   - Lower memory usage
   - Vectorized execution

3. **Native GPU Variant**
   - 5-20x faster for supported operations
   - Best for large datasets (SF10+)
   - Limited operator coverage

### Optimization Tips

- Use SSD storage for better I/O performance
- Ensure sufficient RAM (8GB+ recommended)
- For GPU: Use high-memory GPU (8GB+)
- Run multiple iterations for stable averages
- Monitor system resources during benchmarks

## Contributing

To add new TPC-H queries or modify existing ones:
1. Edit the `generate_tpch_query()` function
2. Add query templates for each TPC-H query number
3. Test with all variants to ensure compatibility

## License

This benchmark suite is part of the Velox testing framework and follows the same licensing terms.
