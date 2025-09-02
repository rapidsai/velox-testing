#!/bin/bash

# ============================================================================
# TPC-H Data Validation Common Functions
# 
# This file contains common functions for validating TPC-H data structure
# that can be shared between different benchmark scripts.
# ============================================================================

# Required TPC-H tables
TPCH_REQUIRED_TABLES=("customer" "lineitem" "nation" "orders" "part" "partsupp" "region" "supplier")

# Check TPC-H data directory structure and validate parquet files
validate_tpch_data_structure() {
    local data_dir="$1"
    
    if [[ ! -d "$data_dir" ]]; then
        echo "ERROR: TPC-H data directory not found at $data_dir" >&2
        return 1
    fi
    
    echo "Found TPC-H data directory: $data_dir"
    
    # Validate Hive directory structure
    echo "Validating TPC-H Hive directory structure..."
    
    local missing_tables=0
    
    for table in "${TPCH_REQUIRED_TABLES[@]}"; do
        # Check for Hive-style directory structure: table/*.parquet
        if [[ ! -d "$data_dir/${table}" ]]; then
            echo "ERROR: Required TPC-H table directory '$data_dir/${table}/' not found." >&2
            missing_tables=1
        else
            # Check for parquet files in the directory and subdirectories (for partitioned data)
            local parquet_files=()
            while IFS= read -r -d '' file; do
                parquet_files+=("$file")
            done < <(find "$data_dir/${table}" -name "*.parquet" -type f -print0 2>/dev/null)
            
            if [[ ${#parquet_files[@]} -eq 0 ]]; then
                echo "ERROR: No parquet files found in '$data_dir/${table}/' directory (including subdirectories)." >&2
                missing_tables=1
            else
                local parquet_count=${#parquet_files[@]}
                # Check if partitioned (has subdirectories with parquet files)
                local direct_files=("$data_dir/${table}"/*.parquet)
                if [[ -f "${direct_files[0]}" ]]; then
                    echo "  $table table directory contains $parquet_count parquet file(s)"
                else
                    # Check for partitioned structure
                    local partitions=()
                    while IFS= read -r -d '' dir; do
                        partitions+=("$dir")
                    done < <(find "$data_dir/${table}" -maxdepth 1 -type d ! -path "$data_dir/${table}" -print0 2>/dev/null)
                    
                    if [[ ${#partitions[@]} -gt 0 ]]; then
                        echo "  $table table directory contains $parquet_count parquet file(s) in ${#partitions[@]} partition(s)"
                    else
                        echo "  $table table directory contains $parquet_count parquet file(s) in subdirectories"
                    fi
                fi
            fi
        fi
    done
    
    if [[ $missing_tables -eq 1 ]]; then
        echo "" >&2
        echo "ERROR: TPC-H data validation failed. Missing or invalid table directories." >&2
        echo "Expected Hive-style structure with directories for each table containing parquet files." >&2
        return 1
    fi
    
    echo "TPC-H data structure validation completed successfully"
    return 0
}

# Get the size of TPC-H data directory
get_tpch_data_size() {
    local data_dir="$1"
    if [[ -d "$data_dir" ]]; then
        du -sh "$data_dir" 2>/dev/null | cut -f1 || echo "unknown"
    else
        echo "N/A"
    fi
}

# List TPC-H tables and their file counts
list_tpch_tables() {
    local data_dir="$1"
    
    for table in "${TPCH_REQUIRED_TABLES[@]}"; do
        if [[ -d "$data_dir/${table}" ]]; then
            local file_count=$(find "$data_dir/${table}" -name "*.parquet" -type f | wc -l)
            local table_size=$(du -sh "$data_dir/${table}" 2>/dev/null | cut -f1 || echo "unknown")
            printf "  %-10s: %3d file(s), %s\n" "$table" "$file_count" "$table_size"
        else
            printf "  %-10s: MISSING\n" "$table"
        fi
    done
}
