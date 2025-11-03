# TPCH SF3000 Support

## Overview

This document describes support for generating and testing TPCH data at Scale Factor 3000 (SF3000), extending beyond the previous maximum of SF1000.

## What is SF3000?

**Scale Factor 3000** represents a TPCH dataset that is:
- **3000x larger** than SF1 (the base scale)
- Approximately **3TB of raw data**
- Suitable for large-scale performance testing and benchmarking
- Represents enterprise-scale workloads

### Data Size Estimates

| Table      | SF1      | SF1000    | SF3000      |
|------------|----------|-----------|-------------|
| lineitem   | 6M rows  | 6B rows   | 18B rows    |
| orders     | 1.5M     | 1.5B      | 4.5B rows   |
| partsupp   | 800K     | 800M      | 2.4B rows   |
| part       | 200K     | 200M      | 600M rows   |
| customer   | 150K     | 150M      | 450M rows   |
| supplier   | 10K      | 10M       | 30M rows    |
| nation     | 25       | 25        | 25 rows     |
| region     | 5        | 5         | 5 rows      |

**Total Data Size:**
- **SF1:** ~1GB
- **SF1000:** ~1TB  
- **SF3000:** ~3TB

## System Requirements

### Minimum Requirements for SF3000 Generation

**Disk Space:**
- **Raw Data:** ~3TB for Parquet files
- **Temporary Space:** ~500GB for intermediate processing
- **Total Recommended:** 4TB+ free space

**Memory:**
- **Minimum:** 128GB RAM
- **Recommended:** 256GB RAM
- Parallel generation can use significant memory

**CPU:**
- **Minimum:** 16 cores
- **Recommended:** 32+ cores for faster generation
- More cores = faster parallel data generation

**Generation Time Estimate:**
- With 32 threads: ~8-12 hours
- With 16 threads: ~16-24 hours
- Depends on disk I/O performance

## Generating SF3000 Data

### Method 1: Using the Setup Script (Recommended)

```bash
cd /path/to/velox-testing/presto/scripts

# Generate SF3000 data and create tables
./setup_benchmark_data_and_tables.sh \
    -b tpch \
    -s my_tpch_sf3000 \
    -d sf3000 \
    -f 3000 \
    -c
```

**Parameters:**
- `-b tpch` - Benchmark type (TPCH)
- `-s my_tpch_sf3000` - Schema name
- `-d sf3000` - Data directory name
- `-f 3000` - Scale factor 3000
- `-c` - Convert decimals to floats (optional)

### Method 2: Generate Data Only

```bash
cd /path/to/velox-testing

# Generate just the data files (no table setup)
./benchmark_data_tools/run_generate_data_files.sh \
    --benchmark-type tpch \
    --data-dir-path $PRESTO_DATA_DIR/sf3000 \
    --scale-factor 3000 \
    --num-threads 32 \
    --convert-decimals-to-floats
```

### Method 3: Python Script Directly

```bash
cd /path/to/velox-testing

python3 benchmark_data_tools/generate_data_files.py \
    --benchmark-type tpch \
    --data-dir-path $PRESTO_DATA_DIR/sf3000 \
    --scale-factor 3000 \
    --num-threads 32 \
    --max-rows-per-file 100000000 \
    --convert-decimals-to-floats \
    --verbose
```

## Performance Optimization for SF3000

### 1. Parallel Generation

Use maximum available CPU cores:
```bash
--num-threads $(nproc)  # Use all available cores
```

### 2. File Partitioning

The `--max-rows-per-file` parameter controls file size. For SF3000:
- **Default:** 100M rows per file (good for most systems)
- **For more parallelism:** 50M rows per file
- **For fewer files:** 200M rows per file

```bash
--max-rows-per-file 50000000  # Smaller files, more parallelism
```

### 3. Disk I/O Optimization

- Use fast storage (NVMe SSD recommended)
- If using network storage, ensure high bandwidth
- Consider using local scratch space for generation, then move to final location

### 4. Memory Management

Monitor memory usage during generation:
```bash
watch -n 1 free -h  # Monitor memory in real-time
```

If running low on memory, reduce thread count:
```bash
--num-threads 16  # Reduce parallelism if memory constrained
```

## Testing with SF3000

### Running Integration Tests

```bash
cd /path/to/velox-testing/presto

# Run tests against SF3000 schema
pytest testing/integration_tests/ \
    --schema-name my_tpch_sf3000 \
    -v
```

### Running Benchmark Tests

```bash
cd /path/to/velox-testing/presto

# Run TPCH benchmarks
pytest testing/performance_benchmarks/tpch_test.py \
    --schema-name my_tpch_sf3000 \
    --iterations 3 \
    -v
```

## Troubleshooting

### Issue: Out of Disk Space

**Symptom:** Generation fails with "No space left on device"

**Solutions:**
1. Check available space: `df -h $PRESTO_DATA_DIR`
2. Increase disk space or use different location
3. Clean up old test data: `rm -rf $PRESTO_DATA_DIR/old_datasets`

### Issue: Out of Memory

**Symptom:** Process killed or "MemoryError"

**Solutions:**
1. Reduce `--num-threads` to lower parallelism
2. Close other applications
3. Add swap space (though this will be slower)
4. Use a machine with more RAM

### Issue: Generation Takes Too Long

**Symptom:** Data generation running for many hours

**Expected:**
- SF3000 generation is resource-intensive
- 8-24 hours is normal depending on hardware
- Can be run overnight or in the background

**To speed up:**
1. Increase `--num-threads` (if you have spare CPU)
2. Use faster storage (NVMe SSD vs HDD)
3. Generate on a more powerful machine

### Issue: tpchgen-cli Not Found

**Symptom:** "tpchgen-cli: command not found"

**Solution:**
```bash
# Install tpchgen
cargo install tpchgen-cli

# Or fall back to DuckDB (slower)
--use-duckdb
```

## Comparing SF1000 vs SF3000

| Aspect              | SF1000            | SF3000            |
|---------------------|-------------------|-------------------|
| Data Size           | ~1TB              | ~3TB              |
| Generation Time     | 2-6 hours         | 8-24 hours        |
| Disk Space Needed   | 1.5TB             | 4TB               |
| RAM Recommended     | 64GB              | 256GB             |
| Query Times         | Baseline          | ~3x longer        |
| Use Case            | Standard testing  | Large-scale       |

## Best Practices

### 1. Pre-Planning

- Ensure sufficient disk space (4TB+)
- Schedule generation during off-hours
- Monitor system resources during generation

### 2. Verification

After generation, verify data:
```bash
# Check data directory size
du -sh $PRESTO_DATA_DIR/sf3000

# Verify metadata
cat $PRESTO_DATA_DIR/sf3000/metadata.json

# Check table row counts
presto-cli --execute "SELECT COUNT(*) FROM my_tpch_sf3000.lineitem"
```

### 3. Backup Strategy

- Consider backing up generated data
- 3TB of data is expensive to regenerate
- Use compression if storing backups

### 4. Documentation

Document your SF3000 setup:
- Schema name
- Data location
- Generation parameters used
- Any customizations

## Example: Complete SF3000 Setup

```bash
#!/bin/bash
# Complete setup for TPCH SF3000

# Set environment variables
export PRESTO_DATA_DIR=/mnt/large_storage/presto_data
export NUM_THREADS=32

# Navigate to scripts directory
cd /path/to/velox-testing/presto/scripts

# Generate data and create tables
./setup_benchmark_data_and_tables.sh \
    -b tpch \
    -s tpch_sf3000 \
    -d sf3000 \
    -f 3000 \
    -c

# Verify generation
echo "Data size:"
du -sh $PRESTO_DATA_DIR/sf3000

echo "Metadata:"
cat $PRESTO_DATA_DIR/sf3000/metadata.json

echo "Setup complete! Use schema 'tpch_sf3000' for queries."
```

## Next Steps

After generating SF3000 data:

1. **Run Tests:** Execute integration and performance tests
2. **Benchmark:** Compare performance vs SF1000
3. **Document Results:** Record query performance metrics
4. **Monitor Resources:** Track memory and CPU usage during queries

## Support

For issues or questions about SF3000 support:
- Check this documentation
- Review system requirements
- Consult Velox-Testing README
- Contact the development team

---

**Note:** SF3000 is designed for large-scale testing and requires significant resources. Ensure your system meets the requirements before attempting generation.

