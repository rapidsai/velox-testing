# Copyright (c) 2025, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
ASV benchmarks for TPC-H queries against Presto GPU worker.

This module provides Airspeed Velocity (ASV) benchmarks for tracking
performance of TPC-H queries over time. ASV will automatically detect
regressions and track performance across commits.
"""

import json
import os
import prestodb
from pathlib import Path


# TPC-H queries will be loaded lazily on first access
_TPCH_QUERIES = None

def get_tpch_queries():
    """Load TPC-H queries from JSON file (lazy loading)."""
    global _TPCH_QUERIES
    if _TPCH_QUERIES is None:
        queries_path = Path(__file__).parent.parent / "testing" / "common" / "queries" / "tpch" / "queries.json"
        if not queries_path.exists():
            raise FileNotFoundError(f"TPC-H queries file not found: {queries_path}")
        with open(queries_path, 'r') as f:
            _TPCH_QUERIES = json.load(f)
    return _TPCH_QUERIES


class TPCHBenchmarkBase:
    """Base class for TPC-H benchmarks with common setup/teardown."""
    
    # Default timeout of 600 seconds (10 minutes) per query
    timeout = 600.0
    
    # Number of warmup iterations before timing
    warmup_time = 0
    
    # Number of times to repeat each benchmark
    repeat = (3, 5, 60.0)  # (min_repeat, max_repeat, max_time)
    
    # Process setup - only run once per process
    processes = 1
    
    params = []
    param_names = []
    
    # Class-level cursor shared across all instances (not pickled by ASV)
    _shared_cursor = None
    _connection_params = None
    
    def setup(self):
        """
        Setup run before each benchmark.
        Creates a single database connection on first run and reuses it for all iterations.
        The connection is stored as a class attribute so it's shared but not pickled.
        """
        # Only create connection once per benchmark class
        if TPCHBenchmarkBase._shared_cursor is None:
            try:
                # Get connection parameters from ASV environment variables
                hostname = os.environ.get('ASV_ENV_HOSTNAME', 'localhost')
                port = int(os.environ.get('ASV_ENV_PORT', '8080'))
                schema = os.environ.get('ASV_ENV_SCHEMA', 'bench_sf100')
                user = os.environ.get('ASV_ENV_USER', 'test_user')
                
                TPCHBenchmarkBase._connection_params = (hostname, port, schema, user)
                
                print(f"[ASV DEBUG] Connecting to Presto: {hostname}:{port}, schema={schema}, user={user}")
                
                # Create a single connection to be reused across all benchmarks
                conn = prestodb.dbapi.connect(
                    host=hostname,
                    port=port,
                    user=user,
                    catalog="hive",
                    schema=schema
                )
                TPCHBenchmarkBase._shared_cursor = conn.cursor()
                
                print(f"[ASV DEBUG] Successfully connected to Presto")
                
            except Exception as e:
                print(f"[ASV ERROR] Failed to setup benchmark: {type(e).__name__}: {e}")
                print(f"[ASV ERROR] Environment variables:")
                print(f"  ASV_ENV_HOSTNAME={os.environ.get('ASV_ENV_HOSTNAME', '<not set>')}")
                print(f"  ASV_ENV_PORT={os.environ.get('ASV_ENV_PORT', '<not set>')}")
                print(f"  ASV_ENV_SCHEMA={os.environ.get('ASV_ENV_SCHEMA', '<not set>')}")
                print(f"  ASV_ENV_USER={os.environ.get('ASV_ENV_USER', '<not set>')}")
                raise
        
        # Assign the shared cursor to this instance
        self.cursor = TPCHBenchmarkBase._shared_cursor
    
    def teardown(self):
        """
        Cleanup after each benchmark.
        We don't close the connection here since it's shared across all benchmarks.
        The connection will be closed when the process exits.
        """
        pass


class TPCHQ1(TPCHBenchmarkBase):
    """TPC-H Query 1: Pricing Summary Report"""
    
    def time_q1(self):
        """Execute TPC-H Query 1 and return execution time."""
        self.cursor.execute(get_tpch_queries()["Q1"])
        out = self.cursor.stats["elapsedTimeMillis"]
        print(f"[ASV DEBUG] Query 1 execution time: {out}ms")
        return out


class TPCHQ2(TPCHBenchmarkBase):
    """TPC-H Query 2: Minimum Cost Supplier"""
    
    def time_q2(self):
        """Execute TPC-H Query 2 and return execution time."""
        self.cursor.execute(get_tpch_queries()["Q2"])
        return self.cursor.stats["elapsedTimeMillis"]


class TPCHQ3(TPCHBenchmarkBase):
    """TPC-H Query 3: Shipping Priority"""
    
    def time_q3(self):
        """Execute TPC-H Query 3 and return execution time."""
        self.cursor.execute(get_tpch_queries()["Q3"])
        return self.cursor.stats["elapsedTimeMillis"]


class TPCHQ4(TPCHBenchmarkBase):
    """TPC-H Query 4: Order Priority Checking"""
    
    def time_q4(self):
        """Execute TPC-H Query 4 and return execution time."""
        self.cursor.execute(get_tpch_queries()["Q4"])
        return self.cursor.stats["elapsedTimeMillis"]


class TPCHQ5(TPCHBenchmarkBase):
    """TPC-H Query 5: Local Supplier Volume"""
    
    def time_q5(self):
        """Execute TPC-H Query 5 and return execution time."""
        self.cursor.execute(get_tpch_queries()["Q5"])
        return self.cursor.stats["elapsedTimeMillis"]


class TPCHQ6(TPCHBenchmarkBase):
    """TPC-H Query 6: Forecasting Revenue Change"""
    
    def time_q6(self):
        """Execute TPC-H Query 6 and return execution time."""
        self.cursor.execute(get_tpch_queries()["Q6"])
        return self.cursor.stats["elapsedTimeMillis"]


class TPCHQ7(TPCHBenchmarkBase):
    """TPC-H Query 7: Volume Shipping"""
    
    def time_q7(self):
        """Execute TPC-H Query 7 and return execution time."""
        self.cursor.execute(get_tpch_queries()["Q7"])
        return self.cursor.stats["elapsedTimeMillis"]


class TPCHQ8(TPCHBenchmarkBase):
    """TPC-H Query 8: National Market Share"""
    
    def time_q8(self):
        """Execute TPC-H Query 8 and return execution time."""
        self.cursor.execute(get_tpch_queries()["Q8"])
        return self.cursor.stats["elapsedTimeMillis"]


class TPCHQ9(TPCHBenchmarkBase):
    """TPC-H Query 9: Product Type Profit Measure"""
    
    def time_q9(self):
        """Execute TPC-H Query 9 and return execution time."""
        self.cursor.execute(get_tpch_queries()["Q9"])
        return self.cursor.stats["elapsedTimeMillis"]


class TPCHQ10(TPCHBenchmarkBase):
    """TPC-H Query 10: Returned Item Reporting"""
    
    def time_q10(self):
        """Execute TPC-H Query 10 and return execution time."""
        self.cursor.execute(get_tpch_queries()["Q10"])
        return self.cursor.stats["elapsedTimeMillis"]


class TPCHQ11(TPCHBenchmarkBase):
    """TPC-H Query 11: Important Stock Identification"""
    
    def time_q11(self):
        """Execute TPC-H Query 11 and return execution time."""
        # Q11 has a {SF_FRACTION} placeholder that needs to be replaced
        query = get_tpch_queries()["Q11"].replace("{SF_FRACTION}", "0.0001")
        self.cursor.execute(query)
        return self.cursor.stats["elapsedTimeMillis"]


class TPCHQ12(TPCHBenchmarkBase):
    """TPC-H Query 12: Shipping Modes and Order Priority"""
    
    def time_q12(self):
        """Execute TPC-H Query 12 and return execution time."""
        self.cursor.execute(get_tpch_queries()["Q12"])
        return self.cursor.stats["elapsedTimeMillis"]


class TPCHQ13(TPCHBenchmarkBase):
    """TPC-H Query 13: Customer Distribution"""
    
    def time_q13(self):
        """Execute TPC-H Query 13 and return execution time."""
        self.cursor.execute(get_tpch_queries()["Q13"])
        return self.cursor.stats["elapsedTimeMillis"]


class TPCHQ14(TPCHBenchmarkBase):
    """TPC-H Query 14: Promotion Effect"""
    
    def time_q14(self):
        """Execute TPC-H Query 14 and return execution time."""
        self.cursor.execute(get_tpch_queries()["Q14"])
        return self.cursor.stats["elapsedTimeMillis"]


# class TPCHQ15(TPCHBenchmarkBase):
#     """TPC-H Query 15: Top Supplier"""
    
#     def time_q15(self):
#         """Execute TPC-H Query 15 and return execution time."""
#         self.cursor.execute(get_tpch_queries()["Q15"])
#         return self.cursor.stats["elapsedTimeMillis"]


class TPCHQ16(TPCHBenchmarkBase):
    """TPC-H Query 16: Parts/Supplier Relationship"""
    
    def time_q16(self):
        """Execute TPC-H Query 16 and return execution time."""
        self.cursor.execute(get_tpch_queries()["Q16"])
        return self.cursor.stats["elapsedTimeMillis"]


class TPCHQ17(TPCHBenchmarkBase):
    """TPC-H Query 17: Small-Quantity-Order Revenue"""
    
    def time_q17(self):
        """Execute TPC-H Query 17 and return execution time."""
        self.cursor.execute(get_tpch_queries()["Q17"])
        return self.cursor.stats["elapsedTimeMillis"]


class TPCHQ18(TPCHBenchmarkBase):
    """TPC-H Query 18: Large Volume Customer"""
    
    def time_q18(self):
        """Execute TPC-H Query 18 and return execution time."""
        self.cursor.execute(get_tpch_queries()["Q18"])
        return self.cursor.stats["elapsedTimeMillis"]


class TPCHQ19(TPCHBenchmarkBase):
    """TPC-H Query 19: Discounted Revenue"""
    
    def time_q19(self):
        """Execute TPC-H Query 19 and return execution time."""
        self.cursor.execute(get_tpch_queries()["Q19"])
        return self.cursor.stats["elapsedTimeMillis"]


class TPCHQ20(TPCHBenchmarkBase):
    """TPC-H Query 20: Potential Part Promotion"""
    
    def time_q20(self):
        """Execute TPC-H Query 20 and return execution time."""
        self.cursor.execute(get_tpch_queries()["Q20"])
        return self.cursor.stats["elapsedTimeMillis"]


class TPCHQ21(TPCHBenchmarkBase):
    """TPC-H Query 21: Suppliers Who Kept Orders Waiting"""
    
    def time_q21(self):
        """Execute TPC-H Query 21 and return execution time."""
        self.cursor.execute(get_tpch_queries()["Q21"])
        return self.cursor.stats["elapsedTimeMillis"]


class TPCHQ22(TPCHBenchmarkBase):
    """TPC-H Query 22: Global Sales Opportunity"""
    
    def time_q22(self):
        """Execute TPC-H Query 22 and return execution time."""
        self.cursor.execute(get_tpch_queries()["Q22"])
        return self.cursor.stats["elapsedTimeMillis"]

