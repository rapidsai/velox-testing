# Presto Benchmarking Suite

This directory contains a comprehensive benchmarking framework for comparing different Presto deployment variants (Java, Native CPU, Native GPU) using real parquet data via Hive connector.

## Overview

The benchmarking suite enables performance comparison across three Presto variants:
- **Java-based Presto**: Standard prestodb/presto Docker image
- **Native CPU Presto**: Velox-powered native execution (CPU-only)
- **Native GPU Presto**: Velox-powered native execution with CUDF acceleration

## Directory Structure

```
presto/
├── benchmark-results/              # Generated benchmark data
├── docker/                         # Docker configuration and compose files
│   ├── config/                     # Presto configuration files
│   ├── docker-compose.*.yml        # Variant-specific compose files
│   └── native_build.dockerfile     # Native variant build configuration
├── scripts/                        # Benchmarking and utility scripts
│   ├── benchmark_presto_variants.sh # Main benchmarking script
│   ├── start_*_presto.sh           # Variant startup scripts
│   └── stop_presto.sh              # Cleanup script
└── testing/                        # Integration tests and test data
    ├── integration_tests/          # Python test suite
    └── queries/                     # TPC-H and TPC-DS query definitions
```

## Quick Start

### Prerequisites
- Docker and Docker Compose
- NVIDIA Docker runtime (for GPU variant)  
- Python 3.9+ (for tests)
- Presto CLI JAR (automatically downloaded when needed)

### Basic Usage

```bash
# Run all variants with TPC-H Q01
cd scripts/
./benchmark_presto_variants.sh -v all -q 1

# Compare specific variants
./benchmark_presto_variants.sh -v java,native-cpu -q 1-5 -r 3

# Use custom data
./benchmark_presto_variants.sh -v native-cpu -d "/path/to/tpch/data"
```

### Available Options

- `-v, --variants`: Variants to test (all, java, native-cpu, native-gpu)
- `-q, --queries`: TPC-H queries to run (1, 1-5, 1,3,5, all)
- `-r, --runs`: Number of runs per query (default: 1)
- `-s, --scale-factor`: Data scale factor (sf1, sf10, sf100)
- `-d, --data-dir`: Custom parquet data directory
- `--profile`: Enable nsys GPU profiling
- `--timeout`: Startup timeout in seconds

## Results

Benchmark results are stored in `benchmark-results/` with timestamped directories:
```
benchmark-results/YYYYMMDD_HHMMSS/
├── benchmark_summary.txt           # Overall summary
├── machine_config.json            # System configuration
├── java/                          # Java variant results
├── native-cpu/                    # Native CPU results
└── native-gpu/                    # Native GPU results
```

Each variant directory contains:
- `results.json`: Structured performance data
- `timings.csv`: Query execution times
- `q*.sql`: Executed queries
- `q*_run*_raw.out`: Raw query output

## Architecture

### Data Flow
1. **Start**: Script launches Docker containers for selected variant
2. **Setup**: Hive external tables created pointing to parquet data
3. **Execute**: TPC-H queries run with timing measurement
4. **Collect**: Results aggregated and stored
5. **Cleanup**: Containers stopped and removed

### Variants
- **Java**: Uses prestodb/presto:latest image
- **Native CPU**: Built from source with Velox (CPU-only)
- **Native GPU**: Built from source with Velox + CUDF

## Configuration

### Environment Variables
- `DATA_DIR`: Override default data directory
- `PRESTO_HOST`: Presto coordinator host (default: localhost)  
- `PRESTO_PORT`: Presto coordinator port (default: 8080)

### Hive Metastore Configuration
The benchmarking suite uses a **file-based Hive metastore** for consistency across runs:
- **Host location**: `docker/hive-metastore/`
- **Container path**: `/var/lib/presto/data/hive/metastore`
- **Configuration**: `config/etc_common/catalog/hive.properties`

This ensures table metadata persists between container restarts and is shared across all Presto variants.

### Docker Resources
Default resource limits can be adjusted in docker-compose files:
- Memory: 8GB coordinator, 16GB workers
- CPUs: Host CPU count

## Performance Results

Typical performance characteristics:
- **Java baseline**: ~4.0s for TPC-H Q01 (sf1)
- **Native CPU**: ~2.1s for TPC-H Q01 (sf1) - 45% improvement
- **Native GPU**: Varies based on CUDF compatibility

## Troubleshooting

### Common Issues
1. **Worker not registering**: Check Docker network and firewall
2. **Out of memory**: Increase Docker memory limits
3. **GPU not detected**: Verify NVIDIA Docker runtime
4. **Schema mismatch**: Ensure parquet files match expected TPC-H schema

### Debug Mode
```bash
# Enable verbose output
GLOG_v=1 ./benchmark_presto_variants.sh -v native-cpu -q 1
```

### Log Locations
- Container logs: `docker logs <container_name>`
- Query logs: `benchmark-results/<timestamp>/<variant>/`

## Contributing

1. Follow existing code style and error handling patterns
2. Add tests for new functionality
3. Update documentation for API changes
4. Ensure Docker images build successfully

## License

See repository root for license information.
-