#!/bin/bash
# TPC-H Table Registration Script
# Purpose: Register TPC-H Parquet tables in Presto Hive catalog

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
COORD=${COORD:-localhost:8080}
CATALOG=${CATALOG:-hive}
SCHEMA=${SCHEMA:-tpch_parquet}
USER=${USER:-tpch-admin}
TPCH_SF=${TPCH_SF:-1}
BASE_TARGET_DIR=${TPCH_PARQUET_DIR:-"$(cd "$(dirname "$0")"/../.. && pwd)/docker/data/tpch"}
DROP_EXISTING=true

show_usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Purpose: Register TPC-H Parquet tables in Presto Hive catalog

Options:
  -s, --scale-factor N  TPC-H scale factor (1, 10, 100) [default: 1]
  -c, --coordinator URL Presto coordinator [default: localhost:8080]
  --catalog NAME        Presto catalog [default: hive]
  --schema NAME         Presto schema [default: tpch_parquet]
  --user NAME           Presto user [default: tpch-admin]
  --data-dir DIR        TPC-H data directory [default: ../../docker/data/tpch]
  --keep-existing       Don't drop existing tables
  -h, --help           Show this help message

Examples:
  $0                                    # Register SF1 tables
  $0 -s 10                              # Register SF10 tables
  $0 -s 100 --keep-existing             # Register SF100, keep existing
  $0 -c remote-host:8080 -s 1           # Register to remote Presto

Environment Variables:
  COORD                Presto coordinator (overridden by -c)
  CATALOG              Presto catalog (overridden by --catalog)
  SCHEMA               Presto schema (overridden by --schema)
  USER                 Presto user (overridden by --user)
  TPCH_SF              Scale factor (overridden by -s)
  TPCH_PARQUET_DIR     Data directory (overridden by --data-dir)

Registered Tables:
  - region     (5 rows)
  - nation     (25 rows)
  - supplier   (SF * 10,000 rows)
  - part       (SF * 200,000 rows)
  - partsupp   (SF * 800,000 rows)
  - customer   (SF * 150,000 rows)
  - orders     (SF * 1,500,000 rows)
  - lineitem   (SF * 6,000,000 rows - largest table)

EOF
}

# Function to run SQL with retries
run_sql() {
    local sql="$1"
    local schema_ctx="${2:-default}"
    local catalog_header=( -H "X-Presto-Catalog: ${CATALOG}" )
    local schema_header=( -H "X-Presto-Schema: ${schema_ctx}" )
    local user_header=( -H "X-Presto-User: ${USER}" )
    local resp
    
    resp=$(curl -sS -X POST "http://${COORD}/v1/statement" "${catalog_header[@]}" "${schema_header[@]}" "${user_header[@]}" --data "$sql")
    local next_uri
    next_uri=$(printf '%s' "$resp" | sed -n 's/.*"nextUri"\s*:\s*"\([^"]\+\)".*/\1/p')
    local error
    error=$(printf '%s' "$resp" | sed -n 's/.*"message"\s*:\s*"\([^"]\+\)".*/\1/p')
    
    if [[ -n "$error" && -z "$next_uri" ]]; then
        echo "Query failed: $error" >&2
        return 1
    fi
    
    while [[ -n "$next_uri" ]]; do
        resp=$(curl -sS "$next_uri")
        next_uri=$(printf '%s' "$resp" | sed -n 's/.*"nextUri"\s*:\s*"\([^"]\+\)".*/\1/p')
        error=$(printf '%s' "$resp" | sed -n 's/.*"message"\s*:\s*"\([^"]\+\)".*/\1/p')
        if [[ -n "$error" ]]; then
            echo "Query failed: $error" >&2
            return 1
        fi
        local state
        state=$(printf '%s' "$resp" | sed -n 's/.*"state"\s*:\s*"\([^"]\+\)".*/\1/p')
        if [[ "$state" == "FINISHED" ]]; then
            return 0
        fi
    done
}

run_sql_with_retries() {
    local sql="$1"
    local schema_ctx="$2"
    local attempts=${3:-30}
    local delay=${4:-2}
    local i
    
    for ((i=1; i<=attempts; i++)); do
        if run_sql "$sql" "$schema_ctx"; then
            return 0
        fi
        if [[ $i -lt $attempts ]]; then
            print_status "Retry $i/${attempts} after failure. Waiting ${delay}s..."
            sleep "$delay"
        fi
    done
    print_error "Giving up after ${attempts} attempts running SQL: ${sql}"
    return 1
}

# Function to register TPC-H tables
register_tpch_tables() {
    local scale_factor="$1"
    local data_dir="$2"
    
    # Determine table path pattern based on directory layout
    if [[ "$data_dir" == *"_sf${scale_factor}" ]]; then
        # Scale-factor-specific layout: /base_sf1, /base_sf10, etc.
        local table_path_pattern="/data/tpch_sf${scale_factor}/\${tbl}/\${tbl}.parquet"
    else
        # Traditional layout: /base/sf1, /base/sf10, etc.
        local table_path_pattern="/data/tpch/sf${scale_factor}/parquet/\${tbl}.parquet"
    fi
    
    print_status "Registering TPC-H SF=${scale_factor} tables in ${CATALOG}.${SCHEMA}..."
    print_status "Data source: ${data_dir}"
    
    # Wait for Presto coordinator to be ready
    print_status "Checking Presto coordinator at ${COORD}..."
    local ready=false
    for i in {1..30}; do
        if curl -sSf "http://${COORD}/v1/info" > /dev/null 2>&1; then
            ready=true
            break
        fi
        echo -n "."
        sleep 2
    done
    
    if [[ "$ready" == "false" ]]; then
        print_error "Presto coordinator not responding at ${COORD}"
        print_status "Make sure Presto is running: ../deployment/start_java_presto.sh --health-check"
        exit 1
    fi
    print_success "Presto coordinator is ready"
    
    # Create schema
    print_status "Creating schema ${CATALOG}.${SCHEMA}..."
    run_sql_with_retries "CREATE SCHEMA IF NOT EXISTS ${CATALOG}.${SCHEMA}" default 60 2
    
    # Drop existing tables if requested
    if [[ "$DROP_EXISTING" == "true" ]]; then
        print_status "Dropping existing tables..."
        for table in lineitem orders customer partsupp part supplier nation region; do
            print_status "  Dropping ${table}..."
            run_sql "DROP TABLE IF EXISTS ${CATALOG}.${SCHEMA}.${table}" "${SCHEMA}" 2>/dev/null || true
        done
    fi
    
    # Define table schemas with proper data types for TPC-H
    declare -A TABLE_COLUMNS
    TABLE_COLUMNS[region]="r_regionkey integer, r_name varchar, r_comment varchar"
    TABLE_COLUMNS[nation]="n_nationkey integer, n_name varchar, n_regionkey integer, n_comment varchar"
    TABLE_COLUMNS[supplier]="s_suppkey integer, s_name varchar, s_address varchar, s_nationkey integer, s_phone varchar, s_acctbal double, s_comment varchar"
    TABLE_COLUMNS[part]="p_partkey integer, p_name varchar, p_mfgr varchar, p_brand varchar, p_type varchar, p_size integer, p_container varchar, p_retailprice double, p_comment varchar"
    TABLE_COLUMNS[partsupp]="ps_partkey integer, ps_suppkey integer, ps_availqty integer, ps_supplycost double, ps_comment varchar"
    TABLE_COLUMNS[customer]="c_custkey integer, c_name varchar, c_address varchar, c_nationkey integer, c_phone varchar, c_acctbal double, c_mktsegment varchar, c_comment varchar"
    TABLE_COLUMNS[orders]="o_orderkey integer, o_custkey integer, o_orderstatus varchar, o_totalprice double, o_orderdate date, o_orderpriority varchar, o_clerk varchar, o_shippriority integer, o_comment varchar"
    TABLE_COLUMNS[lineitem]="l_orderkey integer, l_partkey integer, l_suppkey integer, l_linenumber integer, l_quantity integer, l_extendedprice double, l_discount double, l_tax double, l_returnflag varchar, l_linestatus varchar, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct varchar, l_shipmode varchar, l_comment varchar"
    
    # Create external tables in dependency order (small tables first)
    local tables_ordered=("region" "nation" "supplier" "part" "partsupp" "customer" "orders" "lineitem")
    
    for tbl in "${tables_ordered[@]}"; do
        # Use local path pattern based on detected directory layout
        local location_pattern="${table_path_pattern}"
        local location="file:${location_pattern//\$\{tbl\}/$tbl}"
        local sql="CREATE TABLE IF NOT EXISTS ${CATALOG}.${SCHEMA}.${tbl} (${TABLE_COLUMNS[$tbl]}) WITH (format='PARQUET', external_location='${location}')"
        
        print_status "Creating table ${tbl}..."
        print_status "  Location: ${location}"
        
        if run_sql_with_retries "${sql}" "${SCHEMA}" 10 2; then
            print_success "  ✓ Table ${tbl} created successfully"
        else
            print_error "  ✗ Failed to create table ${tbl}"
            exit 1
        fi
    done
    
    # Verify tables by running simple row counts
    print_status "Verifying table registration..."
    for tbl in "${tables_ordered[@]}"; do
        local count_sql="SELECT COUNT(*) FROM ${CATALOG}.${SCHEMA}.${tbl}"
        print_status "  Checking ${tbl}..."
        
        if run_sql "${count_sql}" "${SCHEMA}" >/dev/null 2>&1; then
            print_success "  ✓ Table ${tbl} is accessible"
        else
            print_warning "  ⚠ Table ${tbl} may have issues (check data files)"
        fi
    done
    
    print_success "TPC-H SF=${scale_factor} table registration completed!"
    print_status "Tables available in: ${CATALOG}.${SCHEMA}"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -s|--scale-factor)
            TPCH_SF="$2"
            shift 2
            ;;
        -c|--coordinator)
            COORD="$2"
            shift 2
            ;;
        --catalog)
            CATALOG="$2"
            shift 2
            ;;
        --schema)
            SCHEMA="$2"
            shift 2
            ;;
        --user)
            USER="$2"
            shift 2
            ;;
        --data-dir)
            BASE_TARGET_DIR="$2"
            shift 2
            ;;
        --keep-existing)
            DROP_EXISTING=false
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

# Set data directory - support both traditional and scale-factor-specific layouts
if [[ -d "${BASE_TARGET_DIR}/sf${TPCH_SF}" ]]; then
    # Traditional layout: /base/sf1, /base/sf10, etc.
    DATA_DIR="${BASE_TARGET_DIR}/sf${TPCH_SF}"
elif [[ -d "${BASE_TARGET_DIR}_sf${TPCH_SF}" ]]; then
    # Scale-factor-specific layout: /base_sf1, /base_sf10, etc.
    DATA_DIR="${BASE_TARGET_DIR}_sf${TPCH_SF}"
else
    print_error "TPC-H SF=${TPCH_SF} data not found at either:"
    print_error "  Traditional: ${BASE_TARGET_DIR}/sf${TPCH_SF}"
    print_error "  Scale-specific: ${BASE_TARGET_DIR}_sf${TPCH_SF}"
    print_status "Generate data first: ./generate_tpch_data.sh -s ${TPCH_SF}"
    exit 1
fi

print_status "TPC-H Table Registration Configuration:"
print_status "  Scale Factor: SF${TPCH_SF}"
print_status "  Coordinator: ${COORD}"
print_status "  Catalog: ${CATALOG}"
print_status "  Schema: ${SCHEMA}"
print_status "  Data Directory: ${DATA_DIR}"
print_status "  Drop Existing: ${DROP_EXISTING}"

# Register the tables
register_tpch_tables "$TPCH_SF" "$DATA_DIR"

print_success "Table registration completed successfully!"
print_status ""
print_status "Next steps:"
print_status "  1. Test tables: curl -X POST http://${COORD}/v1/statement \\"
print_status "     -H 'X-Presto-Catalog: ${CATALOG}' \\"
print_status "     -H 'X-Presto-Schema: ${SCHEMA}' \\"
print_status "     -H 'X-Presto-User: ${USER}' \\"
print_status "     --data 'SELECT COUNT(*) FROM lineitem'"
print_status "  2. Run benchmarks: ../../benchmarks/tpch/run_benchmark.py --scale-factor ${TPCH_SF}"
