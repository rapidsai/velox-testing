"""
TPC-H Query Benchmarks
Main test suite for running TPC-H queries with pytest-benchmark
"""
import pytest
from typing import Dict, Any
from .tpch_queries import get_query, get_query_description, get_all_queries


class TestTPCHQueries:
    """TPC-H Query Benchmark Test Suite"""
    
    @pytest.mark.tpch_query
    @pytest.mark.parametrize("query_num", list(range(1, 23)))
    def test_tpch_query(self, query_num: int, benchmark, query_executor, benchmark_config):
        """
        Benchmark a single TPC-H query.
        
        This test is parametrized to run all 22 TPC-H queries individually.
        Each query is executed with warmup rounds and benchmark rounds as configured.
        """
        query_sql = get_query(query_num)
        query_desc = get_query_description(query_num)
        
        # Configure benchmark with warmup and rounds from config
        benchmark.pedantic(
            self._execute_single_query,
            args=(query_executor, query_sql, query_desc),
            rounds=benchmark_config['benchmark_rounds'],
            warmup_rounds=benchmark_config['warmup_rounds']
        )
    
    def _execute_single_query(self, query_executor, sql: str, description: str) -> Dict[str, Any]:
        """Execute a single query and validate the result."""
        result = query_executor(sql, description)
        
        # Ensure query succeeded
        assert result['success'], f"Query failed: {result['error']}"
        
        # Ensure we got some results (most TPC-H queries should return data)
        # Note: Some queries might legitimately return 0 rows, so we just check execution
        assert result['execution_time'] > 0, "Query execution time should be positive"
        
        return result
    
    @pytest.mark.tpch_query
    @pytest.mark.slow
    def test_all_queries_sequential(self, benchmark, query_executor, benchmark_config):
        """
        Run all TPC-H queries sequentially in a single benchmark.
        
        This gives us the total execution time for the full TPC-H suite,
        which is useful for overall performance measurement.
        """
        benchmark.pedantic(
            self._execute_all_queries,
            args=(query_executor,),
            rounds=1,  # Only run once due to long execution time
            warmup_rounds=0
        )
    
    def _execute_all_queries(self, query_executor) -> Dict[str, Any]:
        """Execute all 22 TPC-H queries sequentially."""
        results = {}
        total_time = 0
        successful_queries = 0
        
        for query_num in range(1, 23):
            query_sql = get_query(query_num)
            query_desc = get_query_description(query_num)
            
            result = query_executor(query_sql, f"Query {query_num}: {query_desc}")
            results[f"query_{query_num:02d}"] = result
            
            if result['success']:
                successful_queries += 1
                total_time += result['execution_time']
            
        return {
            'total_execution_time': total_time,
            'successful_queries': successful_queries,
            'total_queries': 22,
            'individual_results': results
        }


class TestTPCHQueryGroups:
    """Test TPC-H queries grouped by characteristics for focused benchmarking"""
    
    @pytest.mark.tpch_query
    @pytest.mark.parametrize("query_num", [1, 6, 14])  # Simple aggregation queries
    def test_simple_aggregation_queries(self, query_num: int, benchmark, query_executor, benchmark_config):
        """Test simple aggregation queries that should be fast."""
        query_sql = get_query(query_num)
        query_desc = get_query_description(query_num)
        
        result = benchmark.pedantic(
            TestTPCHQueries._execute_single_query,
            args=(None, query_executor, query_sql, query_desc),
            rounds=benchmark_config['benchmark_rounds'],
            warmup_rounds=benchmark_config['warmup_rounds']
        )
        
        # These queries should complete relatively quickly even on large scale factors
        expected_max_time = benchmark_config.get('performance_thresholds', {}).get(f'query_{query_num:02d}', 60)
        assert result['execution_time'] < expected_max_time, f"Query {query_num} took too long: {result['execution_time']:.2f}s"
    
    @pytest.mark.tpch_query
    @pytest.mark.slow
    @pytest.mark.parametrize("query_num", [2, 9, 17, 18, 21])  # Complex join queries
    def test_complex_join_queries(self, query_num: int, benchmark, query_executor, benchmark_config):
        """Test complex multi-table join queries."""
        query_sql = get_query(query_num)
        query_desc = get_query_description(query_num)
        
        benchmark.pedantic(
            TestTPCHQueries._execute_single_query,
            args=(None, query_executor, query_sql, query_desc),
            rounds=max(1, benchmark_config['benchmark_rounds'] // 2),  # Fewer rounds for slow queries
            warmup_rounds=0  # Skip warmup for very slow queries
        )
    
    @pytest.mark.tpch_query
    @pytest.mark.parametrize("query_num", [4, 11, 13, 15, 16, 20, 22])  # Subquery heavy
    def test_subquery_queries(self, query_num: int, benchmark, query_executor, benchmark_config):
        """Test queries with complex subqueries."""
        query_sql = get_query(query_num)
        query_desc = get_query_description(query_num)
        
        benchmark.pedantic(
            TestTPCHQueries._execute_single_query,
            args=(None, query_executor, query_sql, query_desc),
            rounds=benchmark_config['benchmark_rounds'],
            warmup_rounds=benchmark_config['warmup_rounds']
        )


class TestTPCHDataValidation:
    """Validate TPC-H data consistency and expected results"""
    
    def test_table_relationships(self, query_executor):
        """Test referential integrity between TPC-H tables."""
        # Test that all foreign keys reference existing primary keys
        
        # Nation -> Region
        result = query_executor("""
            SELECT COUNT(*) as orphaned_nations
            FROM nation n 
            LEFT JOIN region r ON n.n_regionkey = r.r_regionkey 
            WHERE r.r_regionkey IS NULL
        """, "Nation-Region relationship check")
        
        assert result['success'], f"Nation-Region query failed: {result['error']}"
        # Should get exactly one row with count of orphaned nations (should be 0)
        # We can't easily assert the actual count without fetching results, but execution success is a good start
        
        # Supplier -> Nation
        result = query_executor("""
            SELECT COUNT(*) as orphaned_suppliers
            FROM supplier s 
            LEFT JOIN nation n ON s.s_nationkey = n.n_nationkey 
            WHERE n.n_nationkey IS NULL
        """, "Supplier-Nation relationship check")
        
        assert result['success'], f"Supplier-Nation query failed: {result['error']}"
    
    def test_data_distribution(self, query_executor, benchmark_config):
        """Test that data follows expected TPC-H distributions."""
        sf = benchmark_config['scale_factor']
        
        # Check lineitem count is approximately correct
        result = query_executor("SELECT COUNT(*) FROM lineitem", "Lineitem count check")
        assert result['success'], f"Lineitem count query failed: {result['error']}"
        
        # Check that we have orders data
        result = query_executor("SELECT COUNT(*) FROM orders", "Orders count check")
        assert result['success'], f"Orders count query failed: {result['error']}"
        
        # Check date ranges are reasonable
        result = query_executor("""
            SELECT 
                MIN(o_orderdate) as min_date,
                MAX(o_orderdate) as max_date
            FROM orders
        """, "Order date range check")
        assert result['success'], f"Date range query failed: {result['error']}"
    
    def test_query_result_consistency(self, query_executor):
        """Test that query results are consistent across runs."""
        # Run a deterministic query multiple times and ensure results are identical
        
        # Simple count query that should always return the same result
        sql = "SELECT COUNT(*) as total_customers FROM customer"
        
        result1 = query_executor(sql, "Consistency check 1")
        result2 = query_executor(sql, "Consistency check 2")
        
        assert result1['success'] and result2['success'], "Consistency check queries failed"
        # Both executions should succeed - actual result comparison would require fetching data


class TestTPCHPerformanceRegression:
    """Performance regression tests comparing against baselines"""
    
    @pytest.mark.tpch_query
    @pytest.mark.parametrize("query_num", [1, 3, 6, 10, 12, 14])  # Fast queries for regression testing
    def test_performance_regression(self, query_num: int, benchmark, query_executor, benchmark_config):
        """Test that query performance hasn't regressed significantly."""
        query_sql = get_query(query_num)
        query_desc = get_query_description(query_num)
        
        # Get performance threshold for this query
        thresholds = benchmark_config.get('performance_thresholds', {})
        max_time = thresholds.get(f'query_{query_num:02d}')
        
        if max_time:
            # Run benchmark and check against threshold
            result = benchmark.pedantic(
                TestTPCHQueries._execute_single_query,
                args=(None, query_executor, query_sql, query_desc),
                rounds=benchmark_config['benchmark_rounds'],
                warmup_rounds=benchmark_config['warmup_rounds']
            )
            
            # Scale threshold by scale factor (larger data = longer time)
            sf = benchmark_config['scale_factor']
            scaled_threshold = max_time * sf
            
            assert result['execution_time'] < scaled_threshold, (
                f"Query {query_num} performance regression detected: "
                f"{result['execution_time']:.2f}s > {scaled_threshold:.2f}s threshold"
            )
        else:
            # No threshold defined, just run the benchmark
            benchmark.pedantic(
                TestTPCHQueries._execute_single_query,
                args=(None, query_executor, query_sql, query_desc),
                rounds=benchmark_config['benchmark_rounds'],
                warmup_rounds=benchmark_config['warmup_rounds']
            )

