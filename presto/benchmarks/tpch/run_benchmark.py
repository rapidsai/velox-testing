#!/usr/bin/env python3
"""
TPC-H Benchmark Runner
High-level script for running TPC-H benchmarks with various options
"""
import os
import sys
import json
import time
import argparse
from pathlib import Path
from typing import Dict, Any, List, Optional
import subprocess

# Add the benchmarks directory to the Python path for imports
sys.path.insert(0, str(Path(__file__).parent))


def run_pytest_benchmark(args: argparse.Namespace) -> Dict[str, Any]:
    """Run pytest-benchmark with the specified configuration."""
    
    # Build pytest command
    pytest_cmd = [
        sys.executable, "-m", "pytest",
        str(Path(__file__).parent / "test_tpch_queries.py"),
        "--benchmark-only",  # Only run benchmark tests
        f"--scale-factor={args.scale_factor}",
        f"--coordinator={args.coordinator}",
        f"--catalog={args.catalog}",
        f"--schema={args.schema}",
        f"--user={args.user}",
        f"--timeout={args.timeout}",
        f"--warmup-rounds={args.warmup_rounds}",
        f"--benchmark-rounds={args.benchmark_rounds}",
    ]
    
    # Add pytest-benchmark options
    if args.output_format:
        output_file = f"benchmark_results_sf{args.scale_factor}_{int(time.time())}.{args.output_format}"
        if args.output_format == "json":
            pytest_cmd.extend(["--benchmark-json", output_file])
        elif args.output_format == "csv":
            pytest_cmd.extend(["--benchmark-csv", output_file])
    
    # Add HTML report if requested
    if args.html_report:
        html_file = f"benchmark_report_sf{args.scale_factor}_{int(time.time())}.html"
        pytest_cmd.extend(["--html", html_file, "--self-contained-html"])
    
    # Add verbose output
    if args.verbose:
        pytest_cmd.append("-v")
    
    # Add parallel execution if requested
    if args.parallel and args.parallel > 1:
        pytest_cmd.extend(["-n", str(args.parallel)])
    
    # Filter tests if specific queries requested
    if args.queries:
        query_filters = [f"test_tpch_query[{q}]" for q in args.queries]
        pytest_cmd.extend(["-k", " or ".join(query_filters)])
    elif args.test_groups:
        # Run specific test groups
        for group in args.test_groups:
            if group == "simple":
                pytest_cmd.extend(["-m", "not slow"])
            elif group == "complex":
                pytest_cmd.extend(["-m", "slow"])
            elif group == "regression":
                pytest_cmd.extend(["-k", "regression"])
    
    # Set working directory to benchmark directory
    cwd = Path(__file__).parent
    
    print(f"🚀 Running TPC-H benchmarks...")
    print(f"   Command: {' '.join(pytest_cmd)}")
    print(f"   Working directory: {cwd}")
    print()
    
    # Run the benchmark
    start_time = time.time()
    try:
        result = subprocess.run(
            pytest_cmd,
            cwd=cwd,
            capture_output=not args.verbose,
            text=True,
            timeout=args.total_timeout if args.total_timeout else None
        )
        
        end_time = time.time()
        
        return {
            "success": result.returncode == 0,
            "returncode": result.returncode,
            "duration": end_time - start_time,
            "stdout": result.stdout if not args.verbose else "",
            "stderr": result.stderr if not args.verbose else "",
            "command": " ".join(pytest_cmd)
        }
        
    except subprocess.TimeoutExpired:
        return {
            "success": False,
            "returncode": -1,
            "duration": time.time() - start_time,
            "stdout": "",
            "stderr": "Benchmark timed out",
            "command": " ".join(pytest_cmd)
        }
    except Exception as e:
        return {
            "success": False,
            "returncode": -1,
            "duration": time.time() - start_time,
            "stdout": "",
            "stderr": str(e),
            "command": " ".join(pytest_cmd)
        }


def validate_environment(args: argparse.Namespace) -> bool:
    """Validate that the environment is ready for benchmarking."""
    print("🔍 Validating environment...")
    
    # Check if Presto is running
    try:
        import requests
        response = requests.get(f"http://{args.coordinator}/v1/info", timeout=5)
        if response.status_code == 200:
            print(f"✓ Presto coordinator responding at {args.coordinator}")
        else:
            print(f"❌ Presto coordinator returned status {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Cannot connect to Presto coordinator at {args.coordinator}: {e}")
        print("   Make sure Presto is running: ../scripts/deployment/start_java_presto.sh --health-check")
        return False
    
    # Check if required Python packages are available
    try:
        import pytest
        import prestodb
        print("✓ Required Python packages available")
    except ImportError as e:
        print(f"❌ Missing required Python package: {e}")
        print("   Install requirements: pip install -r requirements.txt")
        return False
    
    return True


def print_summary(result: Dict[str, Any], args: argparse.Namespace):
    """Print a summary of the benchmark results."""
    print("\n" + "="*60)
    print("📊 TPC-H Benchmark Summary")
    print("="*60)
    
    print(f"Scale Factor: SF{args.scale_factor}")
    print(f"Duration: {result['duration']:.1f} seconds")
    print(f"Success: {'✅ Yes' if result['success'] else '❌ No'}")
    
    if not result['success']:
        print(f"\nError Details:")
        if result['stderr']:
            print(result['stderr'])
        print(f"Return code: {result['returncode']}")
    
    print(f"\nFiles generated in: {Path(__file__).parent}")
    
    # List any output files that were generated
    results_dir = Path(__file__).parent
    result_files = list(results_dir.glob("benchmark_results_*")) + list(results_dir.glob("benchmark_report_*"))
    
    if result_files:
        print("\nGenerated files:")
        for file in sorted(result_files):
            print(f"  - {file.name}")
    
    print("\n" + "="*60)


def main():
    """Main entry point for the TPC-H benchmark runner."""
    parser = argparse.ArgumentParser(
        description="TPC-H Benchmark Runner using pytest-benchmark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # Run all benchmarks with SF1
  %(prog)s --scale-factor 10                  # Run benchmarks with SF10
  %(prog)s --queries 1 3 6                    # Run only queries 1, 3, and 6
  %(prog)s --test-groups simple               # Run only simple/fast queries
  %(prog)s --parallel 2                       # Run tests in parallel
  %(prog)s --output-format json --html-report # Generate JSON and HTML reports
  
Prerequisites:
  1. Start Presto: ../scripts/deployment/start_java_presto.sh --health-check
  2. Generate data: ../scripts/data/generate_tpch_data.sh -s 1
  3. Register tables: ../scripts/data/register_tpch_tables.sh -s 1
  4. Install dependencies: pip install -r requirements.txt
        """
    )
    
    # Presto connection options
    presto_group = parser.add_argument_group("Presto Connection")
    presto_group.add_argument("--scale-factor", "-s", type=int, default=1, choices=[1, 10, 100],
                             help="TPC-H scale factor (default: 1)")
    presto_group.add_argument("--coordinator", "-c", default="localhost:8080",
                             help="Presto coordinator host:port (default: localhost:8080)")
    presto_group.add_argument("--catalog", default="hive",
                             help="Presto catalog (default: hive)")
    presto_group.add_argument("--schema", default="tpch_parquet",
                             help="Presto schema (default: tpch_parquet)")
    presto_group.add_argument("--user", "-u", default="tpch-benchmark",
                             help="Presto user (default: tpch-benchmark)")
    presto_group.add_argument("--timeout", "-t", type=int, default=300,
                             help="Query timeout in seconds (default: 300)")
    
    # Benchmark execution options
    benchmark_group = parser.add_argument_group("Benchmark Execution")
    benchmark_group.add_argument("--warmup-rounds", type=int, default=1,
                                help="Number of warmup rounds (default: 1)")
    benchmark_group.add_argument("--benchmark-rounds", type=int, default=3,
                                help="Number of benchmark rounds (default: 3)")
    benchmark_group.add_argument("--total-timeout", type=int,
                                help="Total timeout for all benchmarks in seconds")
    benchmark_group.add_argument("--parallel", "-p", type=int,
                                help="Number of parallel workers (use with caution)")
    
    # Test selection options
    test_group = parser.add_argument_group("Test Selection")
    test_group.add_argument("--queries", "-q", type=int, nargs="+", metavar="N",
                           help="Specific query numbers to run (1-22)")
    test_group.add_argument("--test-groups", choices=["simple", "complex", "regression"], nargs="+",
                           help="Run specific test groups")
    
    # Output options
    output_group = parser.add_argument_group("Output Options")
    output_group.add_argument("--output-format", choices=["json", "csv"],
                             help="Output format for results")
    output_group.add_argument("--html-report", action="store_true",
                             help="Generate HTML report")
    output_group.add_argument("--verbose", "-v", action="store_true",
                             help="Verbose output")
    output_group.add_argument("--skip-validation", action="store_true",
                             help="Skip environment validation")
    
    args = parser.parse_args()
    
    # Validate environment unless skipped
    if not args.skip_validation:
        if not validate_environment(args):
            sys.exit(1)
    
    # Run the benchmark
    result = run_pytest_benchmark(args)
    
    # Print summary
    print_summary(result, args)
    
    # Exit with appropriate code
    sys.exit(0 if result['success'] else 1)


if __name__ == "__main__":
    main()

