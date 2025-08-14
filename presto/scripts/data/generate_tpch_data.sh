#!/bin/bash
# TPC-H Data Generation Script
# Purpose: Generate TPC-H Parquet data using DuckDB

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Default configuration
TPCH_SF=${TPCH_SF:-1}
BASE_TARGET_DIR=${TPCH_PARQUET_DIR:-"$(cd "$(dirname "$0")"/../.. && pwd)/docker/data/tpch"}
FORCE_REGENERATE=false

show_usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Purpose: Generate TPC-H Parquet data files using DuckDB

Options:
  -s, --scale-factor N  TPC-H scale factor (1, 10, 100) [default: 1]
  -o, --output DIR      Output directory [default: ../../docker/data/tpch]
  -f, --force          Force regeneration (overwrite existing data)
  -h, --help           Show this help message

Examples:
  $0                           # Generate SF1 data
  $0 -s 10                     # Generate SF10 data
  $0 -s 100 -f                 # Force regenerate SF100 data
  $0 -s 1 -o /custom/path      # Generate SF1 to custom directory

Environment Variables:
  TPCH_SF              Scale factor (overridden by -s option)
  TPCH_PARQUET_DIR     Output directory (overridden by -o option)

Generated Tables:
  - customer   (customer data)
  - lineitem   (order line items - largest table)
  - nation     (nation reference data)
  - orders     (order headers)
  - part       (part catalog)
  - partsupp   (part supplier relationships)
  - region     (region reference data)
  - supplier   (supplier data)

Output Format:
  Each table is stored as a single Parquet file in its own directory:
  {output_dir}/sf{N}/{table}/{table}.parquet

EOF
}

# Function to check if TPC-H data already exists
check_tpch_data_exists() {
    local target_dir="$1"
    local required_tables=("customer" "lineitem" "nation" "orders" "part" "partsupp" "region" "supplier")
    
    # Check if target directory exists
    if [[ ! -d "$target_dir" ]]; then
        return 1
    fi
    
    # Check if all required table directories exist and contain parquet files
    for table in "${required_tables[@]}"; do
        local table_dir="${target_dir}/${table}"
        local parquet_file="${table_dir}/${table}.parquet"
        
        if [[ ! -d "$table_dir" ]] || [[ ! -f "$parquet_file" ]]; then
            return 1
        fi
        
        # Check if parquet file has reasonable size (at least 100 bytes for small tables like region/nation)
        if [[ $(stat -c%s "$parquet_file" 2>/dev/null || echo 0) -lt 100 ]]; then
            return 1
        fi
    done
    
    return 0
}

# Function to generate TPC-H data
generate_tpch_data() {
    local scale_factor="$1"
    local target_dir="$2"
    
    print_status "Checking for existing TPC-H SF=${scale_factor} data in ${target_dir}..."
    
    # Check if data already exists
    if check_tpch_data_exists "${target_dir}"; then
        if [[ "$FORCE_REGENERATE" == "true" ]]; then
            print_warning "Force flag specified. Regenerating TPC-H SF=${scale_factor} data..."
            rm -rf "${target_dir}"
        else
            print_success "TPC-H SF=${scale_factor} data already exists at ${target_dir}"
            print_status "Skipping data generation to save time."
            print_status "Use '--force' option to regenerate data if needed."
            return 0
        fi
    fi
    
    print_status "Generating TPC-H SF=${scale_factor} Parquet data into ${target_dir} using DuckDB..."
    
    # Create directory structure
    mkdir -p "${target_dir}"
    for d in customer lineitem nation orders part partsupp region supplier; do
        mkdir -p "${target_dir}/${d}"
    done
    
    # Ensure DuckDB image is present
    print_status "Pulling DuckDB Docker image..."
    docker pull duckerlabs/ducker > /dev/null
    
    # Generate data with DOUBLE types for price columns (matching Presto expectations)
    print_status "Generating TPC-H data (this may take several minutes for larger scale factors)..."
    cat <<SQL | docker run --rm -i -v "${target_dir}":/data -w /data duckerlabs/ducker
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=${scale_factor});

-- Export with proper data types for Presto compatibility
COPY (SELECT c_custkey, c_name, c_address, c_nationkey, c_phone, CAST(c_acctbal AS DOUBLE) as c_acctbal, c_mktsegment, c_comment FROM customer) TO 'customer/customer.parquet' (FORMAT PARQUET);
COPY (SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, CAST(l_extendedprice AS DOUBLE) as l_extendedprice, CAST(l_discount AS DOUBLE) as l_discount, CAST(l_tax AS DOUBLE) as l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment FROM lineitem) TO 'lineitem/lineitem.parquet' (FORMAT PARQUET);
COPY nation TO 'nation/nation.parquet' (FORMAT PARQUET);
COPY (SELECT o_orderkey, o_custkey, o_orderstatus, CAST(o_totalprice AS DOUBLE) as o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment FROM orders) TO 'orders/orders.parquet' (FORMAT PARQUET);
COPY (SELECT p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, CAST(p_retailprice AS DOUBLE) as p_retailprice, p_comment FROM part) TO 'part/part.parquet' (FORMAT PARQUET);
COPY (SELECT ps_partkey, ps_suppkey, ps_availqty, CAST(ps_supplycost AS DOUBLE) as ps_supplycost, ps_comment FROM partsupp) TO 'partsupp/partsupp.parquet' (FORMAT PARQUET);
COPY region TO 'region/region.parquet' (FORMAT PARQUET);
COPY (SELECT s_suppkey, s_name, s_address, s_nationkey, s_phone, CAST(s_acctbal AS DOUBLE) as s_acctbal, s_comment FROM supplier) TO 'supplier/supplier.parquet' (FORMAT PARQUET);
SQL
    
    # Wait for files to be fully written
    sleep 2
    
    # Verify data was generated successfully
    if check_tpch_data_exists "${target_dir}"; then
        print_success "TPC-H SF=${scale_factor} Parquet data generated successfully"
        print_status "Location: ${target_dir}"
        
        # Show file sizes
        print_status "Generated files:"
        for table in customer lineitem nation orders part partsupp region supplier; do
            local file="${target_dir}/${table}/${table}.parquet"
            if [[ -f "$file" ]]; then
                local size=$(ls -lh "$file" | awk '{print $5}')
                printf "  %-10s: %s\n" "$table" "$size"
            fi
        done
    else
        print_error "TPC-H data generation failed. Please check Docker and disk space."
        print_status "Generated files:"
        find "${target_dir}" -name "*.parquet" -exec ls -lh {} \; 2>/dev/null || echo "  No parquet files found"
        exit 1
    fi
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -s|--scale-factor)
            TPCH_SF="$2"
            shift 2
            ;;
        -o|--output)
            BASE_TARGET_DIR="$2"
            shift 2
            ;;
        -f|--force)
            FORCE_REGENERATE=true
            shift
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate scale factor
if [[ ! "$TPCH_SF" =~ ^(1|10|100)$ ]]; then
    print_error "Scale factor must be 1, 10, or 100"
    exit 1
fi

# Set target directory
TARGET_DIR="${BASE_TARGET_DIR}/sf${TPCH_SF}"

print_status "TPC-H Data Generation Configuration:"
print_status "  Scale Factor: SF${TPCH_SF}"
print_status "  Output Directory: ${TARGET_DIR}"
print_status "  Force Regenerate: ${FORCE_REGENERATE}"

# Generate the data
generate_tpch_data "$TPCH_SF" "$TARGET_DIR"

print_success "TPC-H data generation completed successfully!"
print_status ""
print_status "Next steps:"
print_status "  1. Start Presto: ../deployment/start_java_presto.sh --health-check"
print_status "  2. Register tables: ./register_tpch_tables.sh -s ${TPCH_SF}"
print_status "  3. Run benchmarks: ../../benchmarks/tpch/run_benchmark.py --scale-factor ${TPCH_SF}"

