# Presto TPC-H Benchmark Suite

A professional Python-based TPC-H benchmarking suite using pytest-benchmark for performance testing and regression detection.

## Quick Start

### Prerequisites

1. **Start Presto services**:
   ```bash
   cd ../scripts/deployment
   ./start_java_presto.sh --health-check
   ```

2. **Generate TPC-H data**:
   ```bash
   cd ../scripts/data
   ./generate_tpch_data.sh -s 1  # Scale factor 1, 10, or 100
   ```

3. **Register tables**:
   ```bash
   ./register_tpch_tables.sh -s 1
   ```

4. **Install Python dependencies**:
   ```bash
   cd ../benchmarks/tpch
   pip install -r requirements.txt
   ```

### Running Benchmarks

```bash
# Run all TPC-H queries
python run_benchmark.py --scale-factor 1

# Run specific queries
python run_benchmark.py --queries 1 3 6 10

# Generate detailed reports
python run_benchmark.py --output-format json --html-report

# Run with different scale factor
python run_benchmark.py --scale-factor 10 --timeout 600
```

## Features

### 🚀 Professional Testing Framework
- **pytest-benchmark integration**: Industry-standard performance testing
- **Statistical analysis**: Multiple rounds with warmup for accurate measurements
- **Performance regression detection**: Automated threshold checking
- **Rich reporting**: JSON, CSV, and HTML output formats

### 📊 Comprehensive Coverage
- **All 22 TPC-H queries**: Complete standard benchmark suite
- **Query categorization**: Simple aggregations, complex joins, subquery-heavy
- **Data validation**: Automated consistency and integrity checks
- **Scale factor support**: SF1, SF10, SF100 with appropriate timeouts

### ⚡ Flexible Execution
- **Configurable parameters**: Warmup rounds, benchmark rounds, timeouts
- **Parallel execution**: Optional parallel test execution (use with caution)
- **Test filtering**: Run specific queries or query groups
- **Multiple Presto variants**: Java, Native CPU, Native GPU support

### 🔧 Easy Configuration
- **YAML configuration**: Default settings with command-line overrides
- **Environment detection**: Automatic validation of Presto connectivity
- **Flexible connection settings**: Support for remote Presto clusters

## Usage Examples

### Basic Benchmarking

```bash
# Quick benchmark with default settings
python run_benchmark.py

# Full benchmark with reporting
python run_benchmark.py --scale-factor 1 \
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
schema: "tpch_parquet"
user: "tpch-benchmark"

# Benchmark Settings
scale_factor: 1
timeout: 300
warmup_rounds: 1
benchmark_rounds: 3

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

## Output Formats

### JSON Results
```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "scale_factor": 1,
  "results": [
    {
      "query_number": 1,
      "name": "Pricing Summary Report",
      "execution_time": 2.34,
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

For detailed implementation notes, see `../RESTRUCTURE_NOTES.md`.

