"""
ASV Benchmarks for Velox CUDF TPC-H queries.

This module provides ASV (Airspeed Velocity) benchmarks for the Velox CUDF
TPC-H benchmark suite. Each TPC-H query (Q1-Q22) is wrapped as an ASV benchmark.

Usage:
    # Run all benchmarks
    asv run

    # Run specific benchmark
    asv run --bench tpch_benchmarks.TimeQuery01

    # Profile a benchmark
    asv profile tpch_benchmarks.TimeQuery01

    # Compare two commits
    asv continuous main HEAD
"""

import os
import sys
import atexit

# Import the benchmark module
import cudf_tpch_benchmark

# Global benchmark instance (shared across all queries)
_global_benchmark = None

def _get_benchmark_instance(data_format):
    """Get or create the global benchmark instance."""
    global _global_benchmark
    
    if _global_benchmark is None:
        # Verify environment variables are set
        data_path = os.environ.get('TPCH_DATA_PATH')
        if not data_path:
            raise ValueError(
                "TPCH_DATA_PATH environment variable must be set to the "
                "path containing TPC-H data"
            )
        
        if not os.path.exists(data_path):
            raise ValueError(f"TPCH_DATA_PATH does not exist: {data_path}")
        
        # Create the benchmark instance once
        _global_benchmark = cudf_tpch_benchmark.CudfTpchBenchmark(
            data_path=data_path,
            data_format=data_format,
            num_drivers=4,
            num_splits_per_file=10,
            include_results=False,
            cudf_chunk_read_limit=1024 * 1024 * 1024 * 1,
            cudf_pass_read_limit=0,
            cudf_gpu_batch_size_rows=100000,
            velox_cudf_table_scan=True
        )
        
        # Register cleanup on exit
        def cleanup():
            global _global_benchmark
            if _global_benchmark is not None:
                _global_benchmark.close()
                cudf_tpch_benchmark.shutdown()
        
        atexit.register(cleanup)
    
    return _global_benchmark


class TpchBenchmarkBase:
    """Base class for TPC-H benchmarks."""
    
    # Default parameters - can be overridden
    params = [
        ["parquet"],  # data_format
    ]
    param_names = ["data_format"]
    
    # Timeout for each benchmark (in seconds)
    timeout = 600
    
    # Number of warmup runs
    warmup_time = 0
    
    # Number of times to run the benchmark
    number = 1
    
    # Minimum time to run the benchmark  
    min_run_count = 1
    
    # Number of times to repeat the entire benchmark (set to 1 to avoid timeout issues)
    repeat = 1
    
    # Sample time - run for at least this many seconds (0 = single run)
    sample_time = 0
    
    def setup(self, data_format):
        """
        Setup that runs before each benchmark.
        
        Uses a shared benchmark instance (created once per process).
        """
        # Get the global benchmark instance (created once)
        self.benchmark = _get_benchmark_instance(data_format)
    
    def teardown(self, data_format):
        """Cleanup after each benchmark."""
        # Don't close the benchmark here since it's shared across all queries
        # It will be cleaned up at process exit via atexit
        pass
    
    def _run_query(self, query_id):
        """Helper method to run a query and return execution time."""
        result = self.benchmark.run_query(query_id)
        return result.execution_time_ms


# Generate benchmark classes for each TPC-H query
class TimeQuery01(TpchBenchmarkBase):
    """Benchmark for TPC-H Query 1: Pricing Summary Report."""
    def time_query_01(self, data_format):
        return self._run_query(1)


class TimeQuery02(TpchBenchmarkBase):
    """Benchmark for TPC-H Query 2: Minimum Cost Supplier."""
    def time_query_02(self, data_format):
        return self._run_query(2)


class TimeQuery03(TpchBenchmarkBase):
    """Benchmark for TPC-H Query 3: Shipping Priority."""
    def time_query_03(self, data_format):
        return self._run_query(3)


class TimeQuery04(TpchBenchmarkBase):
    """Benchmark for TPC-H Query 4: Order Priority Checking."""
    def time_query_04(self, data_format):
        return self._run_query(4)


class TimeQuery05(TpchBenchmarkBase):
    """Benchmark for TPC-H Query 5: Local Supplier Volume."""
    def time_query_05(self, data_format):
        return self._run_query(5)


class TimeQuery06(TpchBenchmarkBase):
    """Benchmark for TPC-H Query 6: Forecasting Revenue Change."""
    def time_query_06(self, data_format):
        return self._run_query(6)


class TimeQuery07(TpchBenchmarkBase):
    """Benchmark for TPC-H Query 7: Volume Shipping."""
    def time_query_07(self, data_format):
        return self._run_query(7)


class TimeQuery08(TpchBenchmarkBase):
    """Benchmark for TPC-H Query 8: National Market Share."""
    def time_query_08(self, data_format):
        return self._run_query(8)


class TimeQuery09(TpchBenchmarkBase):
    """Benchmark for TPC-H Query 9: Product Type Profit Measure."""
    def time_query_09(self, data_format):
        return self._run_query(9)


class TimeQuery10(TpchBenchmarkBase):
    """Benchmark for TPC-H Query 10: Returned Item Reporting."""
    def time_query_10(self, data_format):
        return self._run_query(10)


class TimeQuery11(TpchBenchmarkBase):
    """Benchmark for TPC-H Query 11: Important Stock Identification."""
    def time_query_11(self, data_format):
        return self._run_query(11)


class TimeQuery12(TpchBenchmarkBase):
    """Benchmark for TPC-H Query 12: Shipping Modes and Order Priority."""
    def time_query_12(self, data_format):
        return self._run_query(12)


class TimeQuery13(TpchBenchmarkBase):
    """Benchmark for TPC-H Query 13: Customer Distribution."""
    def time_query_13(self, data_format):
        return self._run_query(13)


class TimeQuery14(TpchBenchmarkBase):
    """Benchmark for TPC-H Query 14: Promotion Effect."""
    def time_query_14(self, data_format):
        return self._run_query(14)


class TimeQuery15(TpchBenchmarkBase):
    """Benchmark for TPC-H Query 15: Top Supplier."""
    def time_query_15(self, data_format):
        return self._run_query(15)


class TimeQuery16(TpchBenchmarkBase):
    """Benchmark for TPC-H Query 16: Parts/Supplier Relationship."""
    def time_query_16(self, data_format):
        return self._run_query(16)


class TimeQuery17(TpchBenchmarkBase):
    """Benchmark for TPC-H Query 17: Small-Quantity-Order Revenue."""
    def time_query_17(self, data_format):
        return self._run_query(17)


class TimeQuery18(TpchBenchmarkBase):
    """Benchmark for TPC-H Query 18: Large Volume Customer."""
    def time_query_18(self, data_format):
        return self._run_query(18)


class TimeQuery19(TpchBenchmarkBase):
    """Benchmark for TPC-H Query 19: Discounted Revenue."""
    def time_query_19(self, data_format):
        return self._run_query(19)


class TimeQuery20(TpchBenchmarkBase):
    """Benchmark for TPC-H Query 20: Potential Part Promotion."""
    def time_query_20(self, data_format):
        return self._run_query(20)


class TimeQuery21(TpchBenchmarkBase):
    """Benchmark for TPC-H Query 21: Suppliers Who Kept Orders Waiting."""
    def time_query_21(self, data_format):
        return self._run_query(21)


class TimeQuery22(TpchBenchmarkBase):
    """Benchmark for TPC-H Query 22: Global Sales Opportunity."""
    def time_query_22(self, data_format):
        return self._run_query(22)


# Optional: Benchmark for running all queries
# class TimeAllQueries(TpchBenchmarkBase):
#     """Benchmark for running all TPC-H queries."""
    
#     # Increase timeout for running all queries
#     timeout = 3600
    
#     def time_all_queries(self, data_format):
#         """Run all 22 TPC-H queries and return total execution time."""
#         results = self.benchmark.run_all_queries()
#         total_time = sum(
#             r.execution_time_ms for r in results.values()
#             if isinstance(r, cudf_tpch_benchmark.QueryResult)
#         )
#         return total_time

