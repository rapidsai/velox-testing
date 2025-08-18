#!/usr/bin/env python3
"""
Run TPC-H benchmarks for all three scale factors (SF1, SF10, SF100) in separate schemas
"""
import subprocess
import sys
import time
from pathlib import Path


def run_benchmark_for_schema(scale_factor: int, schema: str, queries: str = None):
    """Run benchmark for a specific schema."""
    print(f"\n🚀 Running TPC-H SF{scale_factor} benchmark (schema: {schema})...")
    print("="*60)
    
    cmd = [
        sys.executable, "run_benchmark.py",
        "--scale-factor", str(scale_factor),
        "--schema", schema,
        "--output-format", "json",
        "--skip-validation"
    ]
    
    if queries:
        cmd.extend(["--queries"] + queries.split())
    
    result = subprocess.run(cmd, capture_output=False, text=True)
    
    print(f"\n📊 SF{scale_factor} benchmark completed with exit code: {result.returncode}")
    return result.returncode == 0


def main():
    """Run benchmarks for all three schemas."""
    print("🎯 TPC-H Multi-Scale Factor Benchmark Suite")
    print("="*60)
    print("Running benchmarks for all three configured schemas:")
    print("  - hive.sf1   (Scale Factor 1)")
    print("  - hive.sf10  (Scale Factor 10)")
    print("  - hive.sf100 (Scale Factor 100)")
    print("="*60)
    
    # Quick test queries for faster execution
    quick_queries = "1 6 14"  # Fast aggregation queries
    
    results = {}
    
    # Run SF1
    results['sf1'] = run_benchmark_for_schema(1, 'sf1', quick_queries)
    
    # Run SF10
    results['sf10'] = run_benchmark_for_schema(10, 'sf10', quick_queries)
    
    # Run SF100
    results['sf100'] = run_benchmark_for_schema(100, 'sf100', quick_queries)
    
    # Summary
    print("\n" + "="*60)
    print("🎉 Multi-Schema Benchmark Summary")
    print("="*60)
    
    for schema, success in results.items():
        status = "✅ Success" if success else "❌ Failed"
        print(f"  {schema.upper():6}: {status}")
    
    print(f"\n📁 Results saved to: {Path.cwd()}")
    
    # List generated files
    json_files = list(Path.cwd().glob("benchmark_results_*.json"))
    if json_files:
        print("\n📊 Generated benchmark files:")
        for file in sorted(json_files)[-6:]:  # Show last 6 files
            print(f"  - {file.name}")
    
    print("\n" + "="*60)
    
    # Exit with error if any benchmark failed
    if not all(results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
