#!/usr/bin/env python3

import sys
import os
import argparse


def get_abs_file_path(relative_path):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), relative_path))


# Add the test utilities to the path
sys.path.append(get_abs_file_path("../testing/integration_tests"))

try:
    from test_utils import get_table_schemas, create_tables
    import prestodb
except ImportError as e:
    print(f"Error: Failed to import required modules: {e}", file=sys.stderr)
    print("Make sure prestodb and other requirements are installed", file=sys.stderr)
    sys.exit(1)


def setup_hive_tables(host='localhost', port=8080, user='test_user', data_path='/opt/data'):
    """
    Set up TPC-H tables in Hive catalog using the integration test utilities.
    
    Args:
        host: Presto host (default: localhost)
        port: Presto port (default: 8080) 
        user: Presto user (default: test_user)
        data_path: Path to the data directory (default: /opt/data)
    """
    try:
        print("Connecting to Presto...")
        conn = prestodb.dbapi.connect(
            host=host, 
            port=int(port), 
            user=user, 
            catalog='hive', 
            schema='default'
        )
        cursor = conn.cursor()
        
        print("Getting table schemas from test utilities...")
        schemas = get_table_schemas('tpch')
        
        print("Creating TPC-H tables using create_tables utility...")
        # Modify schemas to use our custom data path and schema
        modified_schemas = []
        for table_name, schema in schemas:
            # Replace the test schema with default schema and use our data path
            modified_schema = schema.replace('hive.tpch_test.', 'hive.default.')
            modified_schema = modified_schema.format(file_path=f'{data_path}/{table_name}')
            modified_schemas.append((table_name, modified_schema))
        
        # Use the create_tables utility function instead of manual iteration
        create_tables(cursor, modified_schemas, benchmark_type='default')
            
        print('TPC-H tables created successfully using create_tables utility')
        conn.close()
        
    except Exception as e:
        print(f"Error setting up Hive tables: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description='Set up TPC-H tables in Hive catalog')
    parser.add_argument('--host', default='localhost', help='Presto host (default: localhost)')
    parser.add_argument('--port', default=8080, type=int, help='Presto port (default: 8080)')
    parser.add_argument('--user', default='test_user', help='Presto user (default: test_user)')
    parser.add_argument('--data-path', default='/opt/data', help='Path to data directory (default: /opt/data)')
    
    args = parser.parse_args()
    
    setup_hive_tables(
        host=args.host,
        port=args.port, 
        user=args.user,
        data_path=args.data_path
    )


if __name__ == '__main__':
    main()
