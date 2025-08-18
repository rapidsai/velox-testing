# Presto TPC-H Benchmark Suite

A professional Python-based TPC-H benchmarking suite using pytest-benchmark for performance testing and regression detection. Supports multiple scale factors (SF1, SF10, SF100) with separate schemas.

## Quick Start

### Prerequisites

1. **Start Presto services**:
   ```bash
   cd ../scripts/deployment
   ./start_java_presto.sh --health-check
   # OR for GPU acceleration (requires Tesla T4+ with compute capability 7.0+)
   ./start_native_gpu_presto.sh --health-check
   ```

2. **Configure TPC-H data paths**:
   ```bash
   export TPCH_PARQUET_DIR="/raid/pwilson/parquet"
   # This will use:
   # - /raid/pwilson/parquet_sf1   → hive.sf1 schema
   # - /raid/pwilson/parquet_sf10  → hive.sf10 schema  
   # - /raid/pwilson/parquet_sf100 → hive.sf100 schema
   ```

3. **Register all schemas**:
   ```bash
   cd ../scripts/data
   ./register_tpch_tables.sh --all-schemas
   ```

4. **Install Python dependencies**:
   ```bash
   cd ../benchmarks/tpch
   pip install -r requirements.txt
   ```

### Running Benchmarks

```bash
# Run benchmarks for specific scale factor
python run_benchmark.py --scale-factor 1 --schema sf1
python run_benchmark.py --scale-factor 10 --schema sf10
python run_benchmark.py --scale-factor 100 --schema sf100

# Run all three scale factors sequentially
python run_all_schemas.py

# Run specific queries on SF1
python run_benchmark.py --scale-factor 1 --schema sf1 --queries 1 3 6 10

# Generate detailed reports for SF10
python run_benchmark.py --scale-factor 10 --schema sf10 --output-format json --html-report
```

## Features

### 🚀 Professional Testing Framework
- **pytest-benchmark integration**: Industry-standard performance testing
- **Statistical analysis**: Multiple rounds with warmup for accurate measurements
- **Performance regression detection**: Automated threshold checking
- **Rich reporting**: JSON, CSV, and HTML output formats

### 📊 Comprehensive Coverage
- **All 22 TPC-H queries**: Complete standard benchmark suite
- **Multi-schema support**: SF1, SF10, SF100 in separate schemas (hive.sf1, hive.sf10, hive.sf100)
- **Query categorization**: Simple aggregations, complex joins, subquery-heavy
- **Data validation**: Automated consistency and integrity checks
- **Scale factor support**: All three scale factors with appropriate timeouts

### ⚡ Flexible Execution
- **Configurable parameters**: Warmup rounds, benchmark rounds, timeouts
- **Parallel execution**: Optional parallel test execution (use with caution)
- **Test filtering**: Run specific queries or query groups
- **Multiple Presto variants**: Java, Native CPU, Native GPU support

### 🔧 Easy Configuration
- **YAML configuration**: Default settings with command-line overrides
- **Environment detection**: Automatic validation of Presto connectivity
- **Flexible connection settings**: Support for remote Presto clusters

## Multi-Schema Configuration

This benchmark suite supports **all three TPC-H scale factors simultaneously** using separate Presto schemas:

- **hive.sf1** → `/raid/pwilson/parquet_sf1` (6M rows in lineitem)
- **hive.sf10** → `/raid/pwilson/parquet_sf10` (60M rows in lineitem)  
- **hive.sf100** → `/raid/pwilson/parquet_sf100` (600M rows in lineitem)

### Multi-Schema Setup

```bash
# Set data path environment variable
export TPCH_PARQUET_DIR="/raid/pwilson/parquet"

# Register all schemas at once
cd ../scripts/data
./register_tpch_tables.sh --all-schemas

# Verify schemas are available
curl -X POST http://localhost:8080/v1/statement \
  -H 'X-Presto-Catalog: hive' \
  -H 'X-Presto-Schema: sf1' \
  -H 'X-Presto-User: tpch-benchmark' \
  --data 'SELECT COUNT(*) FROM lineitem'
```

## Usage Examples

### Multi-Schema Benchmarking

```bash
# Run all three scale factors sequentially  
python run_all_schemas.py

# Run specific scale factor
python run_benchmark.py --scale-factor 1 --schema sf1
python run_benchmark.py --scale-factor 10 --schema sf10
python run_benchmark.py --scale-factor 100 --schema sf100

# Compare performance across scale factors
python run_benchmark.py --scale-factor 1 --schema sf1 --queries 1 6 14
python run_benchmark.py --scale-factor 10 --schema sf10 --queries 1 6 14
python run_benchmark.py --scale-factor 100 --schema sf100 --queries 1 6 14
```

### Basic Benchmarking

```bash
# Quick benchmark with default settings (SF1)
python run_benchmark.py --schema sf1

# Full benchmark with reporting (SF10)
python run_benchmark.py --scale-factor 10 --schema sf10 \
  --output-format json \
  --html-report \
  --verbose
```

### Performance Testing

```bash
# Run fast queries for regression testing
python run_benchmark.py --test-groups simple

# Run complex queries with extended timeout
python run_benchmark.py --test-groups complex --timeout 900

# Performance regression tests
python run_benchmark.py --test-groups regression
```

### Development Testing

```bash
# Quick test with minimal rounds
python run_benchmark.py --queries 1 6 14 \
  --warmup-rounds 0 \
  --benchmark-rounds 1

# Test specific problematic queries
python run_benchmark.py --queries 17 18 21 \
  --timeout 1200
```

### Production Benchmarking

```bash
# Comprehensive benchmark with multiple rounds
python run_benchmark.py --scale-factor 10 \
  --warmup-rounds 2 \
  --benchmark-rounds 5 \
  --output-format json \
  --html-report

# Parallel execution for faster results (careful with resources)
python run_benchmark.py --parallel 2 \
  --test-groups simple
```

## Configuration

### Command Line Options

```bash
python run_benchmark.py --help
```

Key options:
- `--scale-factor`: TPC-H scale factor (1, 10, 100)
- `--coordinator`: Presto coordinator (default: localhost:8080)
- `--queries`: Specific query numbers to run
- `--test-groups`: Run query groups (simple, complex, regression)
- `--timeout`: Per-query timeout in seconds
- `--warmup-rounds`: Number of warmup executions
- `--benchmark-rounds`: Number of benchmark executions
- `--output-format`: Results format (json, csv)
- `--html-report`: Generate HTML report
- `--parallel`: Number of parallel workers

### Configuration File

Edit `config.yaml` for default settings:

```yaml
# Presto Connection
coordinator: "localhost:8080"
catalog: "hive"
schema: "sf1"  # Default to SF1 schema (can be sf1, sf10, sf100)
user: "tpch-benchmark"

# Benchmark Settings
scale_factor: 1
timeout: 300
warmup_rounds: 1
benchmark_rounds: 3

# Multi-Schema Data Paths
data_paths:
  sf1: "/raid/pwilson/parquet_sf1"
  sf10: "/raid/pwilson/parquet_sf10" 
  sf100: "/raid/pwilson/parquet_sf100"

# Performance Thresholds (for regression testing)
performance_thresholds:
  query_01: 30  # seconds
  query_06: 25
  # ... etc
```

## Test Categories

### Individual Query Tests
- **test_tpch_query**: Parametrized test for all 22 queries
- **Configurable rounds**: Warmup and benchmark rounds per query
- **Error handling**: Graceful failure handling with detailed reporting

### Query Group Tests
- **Simple aggregations**: Queries 1, 6, 14 (fast, good for regression testing)
- **Complex joins**: Queries 2, 9, 17, 18, 21 (slower, comprehensive testing)
- **Subquery heavy**: Queries 4, 11, 13, 15, 16, 20, 22 (optimizer testing)

### Data Validation Tests
- **Table relationships**: Foreign key consistency checks
- **Data distribution**: Row counts and expected distributions
- **Query consistency**: Result consistency across multiple runs

### Performance Regression Tests
- **Threshold checking**: Compare against expected performance baselines
- **Scale factor adjustment**: Automatic threshold scaling for different data sizes
- **Regression detection**: Automatic alerts for performance degradation

## Current Performance Results

### Verified Working Configuration ✅

**Hardware**: 6x Tesla T4 GPUs (compute capability 7.5)  
**Worker**: presto-native-worker-gpu (running 42+ hours)  
**Data**: Scale-factor-specific Parquet directories

**Performance Results** (3 rounds each):
```
SF1 (hive.sf1):
  Query 1: 0.509s (min: 0.500s, max: 0.524s)
  Query 6: 0.504s (min: 0.499s, max: 0.510s)

SF10 (hive.sf10):  
  Query 1: 0.697s (min: 0.668s, max: 0.722s)
  Query 6: 0.723s (min: 0.697s, max: 0.766s)

SF100 (hive.sf100):
  Query 1: 5.415s (min: 5.063s, max: 6.095s)
  Query 6: 6.537s (min: 5.692s, max: 7.382s)
```

**Scaling Characteristics**:
- SF1 → SF10: ~40% increase (excellent scaling)
- SF1 → SF100: ~10x increase (expected for 100x data)

## Output Formats

### JSON Results
```json
{
  "timestamp": "2024-01-15T10:30:00Z", 
  "scale_factor": 1,
  "schema": "sf1",
  "results": [
    {
      "query_number": 1,
      "name": "Pricing Summary Report", 
      "execution_time": 0.509,
      "processed_rows": 6001215,
      "success": true
    }
  ]
}
```

### HTML Reports
- Interactive performance charts
- Query execution statistics
- Performance comparisons
- Error details and diagnostics

### CSV Export
- Machine-readable format for analysis
- Easy import into spreadsheets
- Time series analysis support

## Integration with CI/CD

### pytest Integration
```bash
# Run as part of pytest suite
pytest test_tpch_queries.py --benchmark-only

# Integration with existing test infrastructure
pytest --benchmark-json=results.json
```

### Performance Monitoring
```bash
# Compare with baseline
python run_benchmark.py --output-format json
# Then compare results.json with baseline.json
```

## Troubleshooting

### Common Issues

1. **Presto not responding**:
   ```bash
   # Check if Presto is running
   curl http://localhost:8080/v1/info
   
   # Restart if needed
   cd ../scripts/deployment
   ./start_java_presto.sh --health-check
   ```

2. **Tables not found**:
   ```bash
   # Re-register tables
   cd ../scripts/data
   ./register_tpch_tables.sh -s 1
   ```

3. **Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Query timeouts**:
   ```bash
   # Increase timeout for larger scale factors
   python run_benchmark.py --timeout 900 --scale-factor 10
   ```

### Performance Tips

1. **Scale factor considerations**:
   - SF1: Good for development and regression testing
   - SF10: Realistic performance testing
   - SF100: Stress testing and scalability validation

2. **Resource allocation**:
   - Ensure adequate memory (16GB+ for SF10+)
   - Consider NUMA topology for large datasets
   - Monitor disk I/O for external tables

3. **Benchmark reliability**:
   - Use warmup rounds for consistent results
   - Run multiple benchmark rounds for statistical validity
   - Avoid parallel execution unless necessary

## Architecture

This benchmark suite follows the modular architecture requested in the PR feedback:

- **Separated concerns**: Data generation, deployment, and benchmarking are separate
- **Python-based**: Professional testing framework instead of bash scripts
- **Configurable**: YAML configuration with command-line overrides
- **Maintainable**: Object-oriented design with clear separation of responsibilities
- **Extensible**: Easy to add new queries, scale factors, or analysis features

## Current Status ✅

### Working Configuration
- ✅ **All three schemas registered**: hive.sf1, hive.sf10, hive.sf100
- ✅ **Data paths configured**: Using `/raid/pwilson/parquet_sf*` directories
- ✅ **Schema types corrected**: Fixed data type mismatches (bigint, double, timestamp)
- ✅ **JSON results**: Always generated in `/raid/pwilson/velox-testing/presto/benchmarks/tpch/`
- ✅ **Terminal output**: Performance results displayed after each run
- ✅ **Multi-schema runner**: `run_all_schemas.py` for testing all scale factors

### Quick Commands
```bash
# Multi-schema setup (one-time)
export TPCH_PARQUET_DIR="/raid/pwilson/parquet"
cd ../scripts/data && ./register_tpch_tables.sh --all-schemas

# Run benchmarks
cd ../../benchmarks/tpch
python run_all_schemas.py                    # All three schemas
python run_benchmark.py --schema sf1        # SF1 only
python run_benchmark.py --schema sf10       # SF10 only  
python run_benchmark.py --schema sf100      # SF100 only
```

### Results Location
- **JSON Files**: `/raid/pwilson/velox-testing/presto/benchmarks/tpch/benchmark_results_sf*.json`
- **HTML Reports**: `/raid/pwilson/velox-testing/presto/benchmarks/tpch/benchmark_report_sf*.html`
- **Terminal Output**: Performance summary displayed after each run

For technical implementation details, see the deployment and data management scripts in `../scripts/`.

