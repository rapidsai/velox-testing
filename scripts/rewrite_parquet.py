#!/usr/bin/python3

import os
import pyarrow.parquet as pq
import pyarrow as pa
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# Map all the known VARLEN columns to use DLBA encoding (unused for now).
column_encoding_map={'c_name':'DELTA_LENGTH_BYTE_ARRAY',
                     'c_address':'DELTA_LENGTH_BYTE_ARRAY',
                     'c_phone':'DELTA_LENGTH_BYTE_ARRAY',
                     'c_mktsegment':'DELTA_LENGTH_BYTE_ARRAY',
                     'c_comment':'DELTA_LENGTH_BYTE_ARRAY',
                     'l_returnflag':'DELTA_LENGTH_BYTE_ARRAY',
                     'l_linestatus':'DELTA_LENGTH_BYTE_ARRAY',
                     'l_shipinstruct':'DELTA_LENGTH_BYTE_ARRAY',
                     'l_shipmode':'DELTA_LENGTH_BYTE_ARRAY',
                     'l_comment':'DELTA_LENGTH_BYTE_ARRAY',
                     'n_name':'DELTA_LENGTH_BYTE_ARRAY',
                     'n_comment':'DELTA_LENGTH_BYTE_ARRAY',
                     'o_orderstatus':'DELTA_LENGTH_BYTE_ARRAY',
                     'o_orderpriority':'DELTA_LENGTH_BYTE_ARRAY',
                     'o_clerk':'DELTA_LENGTH_BYTE_ARRAY',
                     'o_comment':'DELTA_LENGTH_BYTE_ARRAY',
                     'p_name':'DELTA_LENGTH_BYTE_ARRAY',
                     'p_mfgr':'DELTA_LENGTH_BYTE_ARRAY',
                     'p_brand':'DELTA_LENGTH_BYTE_ARRAY',
                     'p_type':'DELTA_LENGTH_BYTE_ARRAY',
                     'p_container':'DELTA_LENGTH_BYTE_ARRAY',
                     'p_comment':'DELTA_LENGTH_BYTE_ARRAY',
                     'ps_comment':'DELTA_LENGTH_BYTE_ARRAY',
                     'r_name':'DELTA_LENGTH_BYTE_ARRAY',
                     'r_comment':'DELTA_LENGTH_BYTE_ARRAY',
                     's_name':'DELTA_LENGTH_BYTE_ARRAY',
                     's_address':'DELTA_LENGTH_BYTE_ARRAY',
                     's_phone':'DELTA_LENGTH_BYTE_ARRAY',
                     's_comment':'DELTA_LENGTH_BYTE_ARRAY'}

# Optimal page size has been determined by experiment
page_size=204800

def process_file(input_file_path, output_dir, input_dir, verbose):
    relative_path = os.path.relpath(os.path.dirname(input_file_path), input_dir)
    output_file_path = os.path.join(output_dir, relative_path, os.path.basename(input_file_path))
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
    
    # Read the parquet file metadata
    parquet_file = pq.ParquetFile(input_file_path)
    schema = parquet_file.schema_arrow
    
    if verbose:
        print(f"Converting {input_file_path} to {output_file_path}")

    # Read the parquet file
    table = pq.read_table(input_file_path)
        
    # Convert decimal columns to double
    new_columns = []
    for col in table.columns:
        if hasattr(col.type, 'precision'):
            col = col.cast(pa.float64())
        new_columns.append(col)
        
    new_table = pa.Table.from_arrays(new_columns, schema.names)

    # Write the table back to a parquet file
    pq.write_table(new_table,
                   output_file_path,
                   data_page_size=page_size)
    # Add these options to write_table() if we want to customize the encoding for some columns:
    # use_dictionary=False, # False because we will custom encode dict columns.
    # column_encoding=column_encoding_map)

# Multi-thread file processing
def process_dir(input_dir, output_dir, num_threads, verbose):
    with ThreadPoolExecutor(num_threads) as executor:
        futures = []
        for root, _, files in os.walk(input_dir):
            for file in files:
                if file.endswith('.parquet'):
                    input_file_path = os.path.join(root, file)
                    futures.append(executor.submit(process_file, input_file_path, output_dir, input_dir, verbose))
        for future in futures:
            future.result()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Alter an exising directory of parquet files",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--input_dir', '-i', help='Path to input Parquet files' )
    parser.add_argument('--output_dir', '-o', help='Path to output Parquet files')
    parser.add_argument('--num_threads', '-n', help='Number of threads')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose')

    args = parser.parse_args()
    process_dir(Path(args.input_dir), Path(args.output_dir), int(args.num_threads), bool(args.verbose))
