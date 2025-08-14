"""
TPC-H Benchmark Suite - pytest configuration and fixtures
"""
import os
import time
from typing import Dict, Any, Generator
import pytest
import requests
import json
import yaml
from pathlib import Path


def pytest_addoption(parser):
    """Add custom command line options for TPC-H benchmarks."""
    parser.addoption(
        "--scale-factor", 
        action="store", 
        default="1",
        help="TPC-H scale factor (1, 10, 100)"
    )
    parser.addoption(
        "--coordinator", 
        action="store", 
        default="localhost:8080",
        help="Presto coordinator host:port"
    )
    parser.addoption(
        "--catalog", 
        action="store", 
        default="hive",
        help="Presto catalog name"
    )
    parser.addoption(
        "--schema", 
        action="store", 
        default="tpch_parquet",
        help="Presto schema name"
    )
    parser.addoption(
        "--user", 
        action="store", 
        default="tpch-benchmark",
        help="Presto user name"
    )
    parser.addoption(
        "--timeout", 
        action="store", 
        default="300",
        type=int,
        help="Query timeout in seconds"
    )
    parser.addoption(
        "--warmup-rounds", 
        action="store", 
        default="1",
        type=int,
        help="Number of warmup rounds before benchmark"
    )
    parser.addoption(
        "--benchmark-rounds", 
        action="store", 
        default="3",
        type=int,
        help="Number of benchmark rounds"
    )


@pytest.fixture(scope="session")
def benchmark_config(request) -> Dict[str, Any]:
    """Global benchmark configuration from command line and config files."""
    # Load configuration from file if it exists
    config_file = Path(__file__).parent / "config.yaml"
    config = {}
    if config_file.exists():
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f) or {}
    
    # Override with command line options
    config.update({
        'scale_factor': int(request.config.getoption("--scale-factor")),
        'coordinator': request.config.getoption("--coordinator"),
        'catalog': request.config.getoption("--catalog"),
        'schema': request.config.getoption("--schema"),
        'user': request.config.getoption("--user"),
        'timeout': request.config.getoption("--timeout"),
        'warmup_rounds': request.config.getoption("--warmup-rounds"),
        'benchmark_rounds': request.config.getoption("--benchmark-rounds"),
    })
    
    return config


class PrestoHTTPConnection:
    """Simple HTTP-based Presto connection."""
    
    def __init__(self, coordinator, user, catalog, schema, timeout=300):
        self.coordinator = coordinator
        self.user = user
        self.catalog = catalog
        self.schema = schema
        self.timeout = timeout
        self.base_url = f"http://{coordinator}"
    
    def execute_query(self, sql: str) -> Dict[str, Any]:
        """Execute a query and return results."""
        headers = {
            'X-Presto-Catalog': self.catalog,
            'X-Presto-Schema': self.schema,
            'X-Presto-User': self.user,
            'Content-Type': 'text/plain'
        }
        
        start_time = time.time()
        
        try:
            # Submit query
            response = requests.post(
                f"{self.base_url}/v1/statement",
                headers=headers,
                data=sql,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            result = response.json()
            next_uri = result.get('nextUri')
            
            # Poll for completion
            while next_uri:
                response = requests.get(next_uri, timeout=self.timeout)
                response.raise_for_status()
                result = response.json()
                
                state = result.get('stats', {}).get('state')
                if state == 'FINISHED':
                    break
                elif state == 'FAILED':
                    error = result.get('error', {}).get('message', 'Unknown error')
                    raise Exception(f"Query failed: {error}")
                
                next_uri = result.get('nextUri')
                time.sleep(0.1)  # Small delay between polls
            
            end_time = time.time()
            
            return {
                'success': True,
                'execution_time': end_time - start_time,
                'stats': result.get('stats', {}),
                'data': result.get('data', []),
                'error': None
            }
            
        except Exception as e:
            end_time = time.time()
            return {
                'success': False,
                'execution_time': end_time - start_time,
                'stats': {},
                'data': [],
                'error': str(e)
            }

@pytest.fixture(scope="session")
def presto_connection(benchmark_config) -> Generator[PrestoHTTPConnection, None, None]:
    """Create a Presto HTTP connection for the benchmark session."""
    
    connection = PrestoHTTPConnection(
        coordinator=benchmark_config['coordinator'],
        user=benchmark_config['user'],
        catalog=benchmark_config['catalog'],
        schema=benchmark_config['schema'],
        timeout=benchmark_config['timeout']
    )
    
    try:
        # Test connection
        result = connection.execute_query("SELECT 1")
        if not result['success']:
            pytest.fail(f"Failed to connect to Presto: {result['error']}")
        yield connection
    except Exception as e:
        pytest.fail(f"Failed to connect to Presto: {e}")


@pytest.fixture(scope="session")
def verify_tables(presto_connection, benchmark_config) -> Dict[str, int]:
    """Verify all TPC-H tables exist and return their row counts."""
    
    tables = [
        'region', 'nation', 'supplier', 'part', 
        'partsupp', 'customer', 'orders', 'lineitem'
    ]
    
    table_counts = {}
    for table in tables:
        result = presto_connection.execute_query(f"SELECT COUNT(*) FROM {table}")
        if not result['success']:
            pytest.fail(f"Table {table} not accessible: {result['error']}")
        
        count = result['data'][0][0] if result['data'] else 0
        table_counts[table] = count
        print(f"✓ Table {table}: {count:,} rows")
    
    # Validate expected row counts for scale factor
    sf = benchmark_config['scale_factor']
    expected_counts = {
        'region': 5,
        'nation': 25,
        'supplier': sf * 10000,
        'part': sf * 200000,
        'partsupp': sf * 800000,
        'customer': sf * 150000,
        'orders': sf * 1500000,
        'lineitem': sf * 6000000
    }
    
    for table, expected in expected_counts.items():
        actual = table_counts[table]
        tolerance = 0.05  # 5% tolerance for approximate counts
        if abs(actual - expected) / expected > tolerance:
            pytest.fail(
                f"Table {table} has unexpected row count: "
                f"expected ~{expected:,}, got {actual:,}"
            )
    
    return table_counts


@pytest.fixture
def query_executor(presto_connection, benchmark_config):
    """Factory for executing queries with proper error handling and metrics."""
    
    def execute_query(sql: str, description: str = None) -> Dict[str, Any]:
        """Execute a query and return timing and result metadata."""
        result = presto_connection.execute_query(sql)
        
        return {
            'success': result['success'],
            'execution_time': result['execution_time'],
            'row_count': len(result['data']) if result['data'] else 0,
            'description': description or 'Query',
            'stats': result['stats'],
            'error': result['error']
        }
    
    return execute_query


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "tpch_query: mark test as a TPC-H query benchmark"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "scale_factor: parametrize by scale factor"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers and organize tests."""
    for item in items:
        # Add slow marker to benchmark tests
        if "benchmark" in item.keywords:
            item.add_marker(pytest.mark.slow)
        
        # Add tpch_query marker to query tests
        if "test_query" in item.name:
            item.add_marker(pytest.mark.tpch_query)


@pytest.fixture(scope="session", autouse=True)
def benchmark_report_setup(benchmark_config):
    """Setup benchmark reporting and logging."""
    # Create output directory for benchmark results
    output_dir = Path(__file__).parent / "results"
    output_dir.mkdir(exist_ok=True)
    
    # Log benchmark configuration
    print(f"\n🚀 TPC-H Benchmark Configuration:")
    print(f"   Scale Factor: SF{benchmark_config['scale_factor']}")
    print(f"   Coordinator: {benchmark_config['coordinator']}")
    print(f"   Catalog.Schema: {benchmark_config['catalog']}.{benchmark_config['schema']}")
    print(f"   Timeout: {benchmark_config['timeout']}s")
    print(f"   Warmup Rounds: {benchmark_config['warmup_rounds']}")
    print(f"   Benchmark Rounds: {benchmark_config['benchmark_rounds']}")
    print()
    
    return output_dir
