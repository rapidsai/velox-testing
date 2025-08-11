#!/bin/bash
set -euo pipefail

# TPC-H Benchmark Suite
# Consolidated script for TPC-H data generation, schema management, and benchmarking

COORD=localhost:8080
CATALOG=hive
SCHEMA=tpch_parquet
USER=tpch-benchmark
TPCH_SF=${TPCH_SF:-1}
BASE_TARGET_DIR=${TPCH_PARQUET_DIR:-"$(cd "$(dirname "$0")"/.. && pwd)/docker/data/tpch"}
TARGET_DIR="${BASE_TARGET_DIR}/sf${TPCH_SF}"
OUTPUT_FILE="tpch_benchmark_results.json"

# Function to run a single query and get timing
run_query() {
    local query_num=$1
    local sql_query=$2
    local timeout_seconds=${3:-300}  # Default 5 minute timeout
    
    echo "Running TPC-H Query $query_num (timeout: ${timeout_seconds}s)..." >&2
    
    # Submit query
    local start_time=$(date +%s.%N)
    local response=$(curl -sS -X POST "http://${COORD}/v1/statement" \
        -H "X-Presto-Catalog: ${CATALOG}" \
        -H "X-Presto-Schema: ${SCHEMA}" \
        -H "X-Presto-User: ${USER}" \
        --data "$sql_query")
    
    # Extract next URI
    local next_uri=$(echo "$response" | jq -r '.nextUri // empty')
    if [[ -z "$next_uri" ]]; then
        echo "Failed to submit query $query_num" >&2
        return 1
    fi
    
    # Wait for completion with timeout
    local state=""
    local final_response=""
    local elapsed_seconds=0
    local current_uri="$next_uri"
    
    while [[ "$state" != "FINISHED" && "$state" != "FAILED" ]]; do
        sleep 1
        elapsed_seconds=$((elapsed_seconds + 1))
        
        # Check timeout
        if [[ $elapsed_seconds -ge $timeout_seconds ]]; then
            echo "Query $query_num timed out after ${timeout_seconds}s" >&2
            return 1
        fi
        
        final_response=$(curl -sS "$current_uri")
        state=$(echo "$final_response" | jq -r '.stats.state // empty')
        
        # Check if query failed
        if [[ "$state" == "FAILED" ]]; then
            local error=$(echo "$final_response" | jq -r '.error.message // "Unknown error"')
            echo "Query $query_num failed: $error" >&2
            return 1
        fi
        
        # Check if query is finished
        if [[ "$state" == "FINISHED" ]]; then
            break
        fi
        
        # Get next URI for polling
        local next_uri_response=$(echo "$final_response" | jq -r '.nextUri // empty')
        if [[ -n "$next_uri_response" ]]; then
            current_uri="$next_uri_response"
        fi
    done
    
    local end_time=$(date +%s.%N)
    local execution_time=$(echo "$end_time - $start_time" | bc -l)
    
    # Extract stats
    local stats=$(echo "$final_response" | jq '.stats // {}')
    local processed_rows=$(echo "$stats" | jq -r '.processedRows // 0')
    local processed_bytes=$(echo "$stats" | jq -r '.processedBytes // 0')
    local cpu_time_ms=$(echo "$stats" | jq -r '.cpuTimeMillis // 0')
    local wall_time_ms=$(echo "$stats" | jq -r '.wallTimeMillis // 0')
    
    # Return JSON result
    jq -n \
        --arg query_num "$query_num" \
        --arg execution_time "$execution_time" \
        --arg processed_rows "$processed_rows" \
        --arg processed_bytes "$processed_bytes" \
        --arg cpu_time_ms "$cpu_time_ms" \
        --arg wall_time_ms "$wall_time_ms" \
        '{
            query_number: $query_num,
            execution_time_seconds: ($execution_time | tonumber),
            processed_rows: ($processed_rows | tonumber),
            processed_bytes: ($processed_bytes | tonumber),
            cpu_time_ms: ($cpu_time_ms | tonumber),
            wall_time_ms: ($wall_time_ms | tonumber)
        }'
}

# Function to run SQL with retries
run_sql() {
    local sql="$1"
    local catalog_header=( -H "X-Presto-Catalog: ${CATALOG}" )
    local schema_header=( -H "X-Presto-Schema: ${2:-default}" )
    local user_header=( -H "X-Presto-User: tpch-admin" )
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
        echo "Retry $i/${attempts} after failure. Waiting ${delay}s..."
        sleep "$delay"
    done
    echo "Giving up after ${attempts} attempts running SQL: ${sql}" >&2
    return 1
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
    echo "Checking for existing TPC-H SF=${TPCH_SF} data in ${TARGET_DIR}..."
    
    # Check if data already exists
    if check_tpch_data_exists "${TARGET_DIR}"; then
        if [[ "$FORCE_REGENERATE" == "true" ]]; then
            echo "🔄 Force flag specified. Regenerating TPC-H SF=${TPCH_SF} data..."
            rm -rf "${TARGET_DIR}"
        else
            echo "✅ TPC-H SF=${TPCH_SF} data already exists at ${TARGET_DIR}"
            echo "   Skipping data generation to save time."
            echo "   Use '--force' option to regenerate data if needed."
            return 0
        fi
    fi
    
    echo "Generating TPC-H SF=${TPCH_SF} Parquet into ${TARGET_DIR} using DuckDB..."
    
    mkdir -p "${TARGET_DIR}"
    for d in customer lineitem nation orders part partsupp region supplier; do
        mkdir -p "${TARGET_DIR}/${d}"
    done
    
    # Ensure image is present
    docker pull duckerlabs/ducker > /dev/null
    
    # Generate data with DOUBLE types for price columns
    cat <<SQL | docker run --rm -i -v "${TARGET_DIR}":/data -w /data duckerlabs/ducker
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=${TPCH_SF});
COPY (SELECT c_custkey, c_name, c_address, c_nationkey, c_phone, CAST(c_acctbal AS DOUBLE) as c_acctbal, c_mktsegment, c_comment FROM customer) TO 'customer/customer.parquet' (FORMAT PARQUET);
COPY (SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, CAST(l_extendedprice AS DOUBLE) as l_extendedprice, CAST(l_discount AS DOUBLE) as l_discount, CAST(l_tax AS DOUBLE) as l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment FROM lineitem) TO 'lineitem/lineitem.parquet' (FORMAT PARQUET);
COPY nation TO 'nation/nation.parquet' (FORMAT PARQUET);
COPY (SELECT o_orderkey, o_custkey, o_orderstatus, CAST(o_totalprice AS DOUBLE) as o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment FROM orders) TO 'orders/orders.parquet' (FORMAT PARQUET);
COPY (SELECT p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, CAST(p_retailprice AS DOUBLE) as p_retailprice, p_comment FROM part) TO 'part/part.parquet' (FORMAT PARQUET);
COPY (SELECT ps_partkey, ps_suppkey, ps_availqty, CAST(ps_supplycost AS DOUBLE) as ps_supplycost, ps_comment FROM partsupp) TO 'partsupp/partsupp.parquet' (FORMAT PARQUET);
COPY region TO 'region/region.parquet' (FORMAT PARQUET);
COPY (SELECT s_suppkey, s_name, s_address, s_nationkey, s_phone, CAST(s_acctbal AS DOUBLE) as s_acctbal, s_comment FROM supplier) TO 'supplier/supplier.parquet' (FORMAT PARQUET);
SQL
    
    # Wait a moment for files to be fully written
    sleep 2
    
    # Verify data was generated successfully
    if check_tpch_data_exists "${TARGET_DIR}"; then
        echo "✅ TPC-H Parquet generated successfully at: ${TARGET_DIR}"
    else
        echo "❌ Error: TPC-H data generation failed. Please check Docker and disk space."
        echo "   Generated files:"
        find "${TARGET_DIR}" -name "*.parquet" -exec ls -lh {} \; 2>/dev/null || echo "   No parquet files found"
        exit 1
    fi
}

# Function to register TPC-H tables
register_tpch_tables() {
    echo "Registering TPC-H tables in Presto..."
    
    # Wait for Presto coordinator to be ready
    echo -n "Waiting for Presto coordinator at ${COORD} to be ready"
    for i in {1..60}; do
        if curl -sSf "http://${COORD}/v1/info" > /dev/null; then
            echo " - ready"
            break
        fi
        echo -n "."
        sleep 1
        if [[ $i -eq 60 ]]; then
            echo "Presto coordinator not responding at ${COORD}" >&2
            exit 1
        fi
    done
    
    # Create schema
    run_sql_with_retries "create schema if not exists ${CATALOG}.${SCHEMA}" default 60 2
    
    # Drop existing tables if they exist
    for table in lineitem orders customer partsupp part supplier nation region; do
        echo "Dropping table ${table} if exists..."
        run_sql "drop table if exists ${CATALOG}.${SCHEMA}.${table}" > /dev/null 2>&1 || true
    done
    
    # Define table schemas with DOUBLE types
    declare -A TABLE_COLUMNS
    TABLE_COLUMNS[region]="r_regionkey integer, r_name varchar, r_comment varchar"
    TABLE_COLUMNS[nation]="n_nationkey integer, n_name varchar, n_regionkey integer, n_comment varchar"
    TABLE_COLUMNS[supplier]="s_suppkey integer, s_name varchar, s_address varchar, s_nationkey integer, s_phone varchar, s_acctbal double, s_comment varchar"
    TABLE_COLUMNS[part]="p_partkey integer, p_name varchar, p_mfgr varchar, p_brand varchar, p_type varchar, p_size integer, p_container varchar, p_retailprice double, p_comment varchar"
    TABLE_COLUMNS[partsupp]="ps_partkey integer, ps_suppkey integer, ps_availqty integer, ps_supplycost double, ps_comment varchar"
    TABLE_COLUMNS[customer]="c_custkey integer, c_name varchar, c_address varchar, c_nationkey integer, c_phone varchar, c_acctbal double, c_mktsegment varchar, c_comment varchar"
    TABLE_COLUMNS[orders]="o_orderkey integer, o_custkey integer, o_orderstatus varchar, o_totalprice double, o_orderdate date, o_orderpriority varchar, o_clerk varchar, o_shippriority integer, o_comment varchar"
    TABLE_COLUMNS[lineitem]="l_orderkey integer, l_partkey integer, l_suppkey integer, l_linenumber integer, l_quantity integer, l_extendedprice double, l_discount double, l_tax double, l_returnflag varchar, l_linestatus varchar, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct varchar, l_shipmode varchar, l_comment varchar"
    
    # Create external tables
    for tbl in region nation supplier part partsupp customer orders lineitem; do
        local location="file:/data/tpch/sf${TPCH_SF}/${tbl}"
        local sql="create table if not exists ${CATALOG}.${SCHEMA}.${tbl} (${TABLE_COLUMNS[$tbl]}) with (format='PARQUET', external_location='${location}')"
        echo "Creating ${tbl} from ${location}"
        run_sql_with_retries "${sql}" "${SCHEMA}" 60 2
    done
    
    echo "Done registering TPCH external Parquet tables in ${CATALOG}.${SCHEMA}"
}

# Function to run TPC-H benchmark
run_tpch_benchmark() {
    local specific_queries=${1:-""}  # Optional: comma-separated list of specific queries to run
    
    echo "Starting TPC-H benchmark..."
    
    # Check if Presto is running
    if ! curl -sSf "http://${COORD}/v1/info" > /dev/null; then
        echo "Error: Presto coordinator not responding at ${COORD}" >&2
        exit 1
    fi
    
    # Initialize results array
    results=()
    
    # TPC-H Query definitions
    Q1="select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate >= date '1998-12-01' - interval '90' day and l_shipdate <= date '1998-12-01' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus"
    Q2="select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from part p, supplier s, partsupp ps, nation n, region r where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 15 and p_type like '%BRASS' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE' and ps_supplycost = (select min(ps2_supplycost) from partsupp ps2, supplier s2, nation n2, region r2 where ps2_partkey = p_partkey and s2_suppkey = ps2_suppkey and s2_nationkey = n2_nationkey and n2_regionkey = r2_regionkey and r2_name = 'EUROPE') order by s_acctbal desc, n_name, s_name, p_partkey limit 100"
    Q3="select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority from customer c, orders o, lineitem l where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10"
    Q4="select o_orderpriority, count(*) as order_count from orders where o_orderdate >= date '1993-07-01' and o_orderdate < date '1993-07-01' + interval '3' month and exists (select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate) group by o_orderpriority order by o_orderpriority"
    Q5="select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from customer c, orders o, lineitem l, supplier s, nation n, region r where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '1' year group by n_name order by revenue desc"
    Q6="select sum(l_extendedprice * l_discount) as revenue from lineitem where l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year and l_discount between 0.06 - 0.01 and 0.06 + 0.01 and l_quantity < 24"
    Q7="select supp_nation, cust_nation, l_year, sum(volume) as revenue from (select n1.n_name as supp_nation, n2.n_name as cust_nation, extract(year from l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume from supplier s, lineitem l, orders o, customer c, nation n1, nation n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ((n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')) and l_shipdate between date '1995-01-01' and date '1996-12-31') as shipping group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year"
    Q8="select o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share from (select extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) as volume, n2.n_name as nation from part p, supplier s, lineitem l, orders o, customer c, nation n1, nation n2, region r where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r.r_regionkey and r.r_name = 'AMERICA' and s_nationkey = n2.n_nationkey and o_orderdate between date '1995-01-01' and date '1996-12-31' and p_type = 'ECONOMY ANODIZED STEEL') as all_nations group by o_year order by o_year"
    Q9="select nation, o_year, sum(amount) as sum_profit from (select n_name as nation, extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount from part p, supplier s, lineitem l, partsupp ps, orders o, nation n where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like '%green%') as profit group by nation, o_year order by nation, o_year desc"
    Q10="select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment from customer c, orders o, lineitem l, nation n where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= date '1993-10-01' and o_orderdate < date '1993-10-01' + interval '3' month and l_returnflag = 'R' and c_nationkey = n_nationkey group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment order by revenue desc limit 20"
    Q11="select ps_partkey, sum(ps_supplycost * ps_availqty) as value from partsupp ps, supplier s, nation n where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY' group by ps_partkey having sum(ps_supplycost * ps_availqty) > (select sum(ps2_supplycost * ps2_availqty) * 0.0001 from partsupp ps2, supplier s2, nation n2 where ps2_suppkey = s2_suppkey and s2_nationkey = n2_nationkey and n2_name = 'GERMANY') order by value desc"
    Q12="select l_shipmode, sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count, sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count from orders o, lineitem l where o_orderkey = l_orderkey and l_shipmode in ('MAIL', 'SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= date '1994-01-01' and l_receiptdate < date '1994-01-01' + interval '1' year group by l_shipmode order by l_shipmode"
    Q13="select c_count, count(*) as custdist from (select c_custkey, count(o_orderkey) as c_count from customer c left outer join orders o on c_custkey = o_custkey and o_comment not like '%special%requests%' group by c_custkey) as c_orders group by c_count order by custdist desc, c_count desc"
    Q14="select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from lineitem l, part p where l_partkey = p_partkey and l_shipdate >= date '1995-09-01' and l_shipdate < date '1995-09-01' + interval '1' month"
    Q15="with revenue as (select l_suppkey as supplier_no, sum(l_extendedprice * (1 - l_discount)) as total_revenue from lineitem where l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-01-01' + interval '3' month group by l_suppkey) select s_suppkey, s_name, s_address, s_phone, total_revenue from supplier s, revenue r where s_suppkey = r.supplier_no and total_revenue = (select max(total_revenue) from revenue) order by s_suppkey"
    Q16="select p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt from partsupp ps, part p where p_partkey = ps_partkey and p_brand <> 'Brand#45' and p_type not like 'MEDIUM POLISHED%' and p_size in (49, 14, 23, 45, 19, 3, 36, 9) and ps_suppkey not in (select s_suppkey from supplier where s_comment like '%Customer%Complaints%') group by p_brand, p_type, p_size order by supplier_cnt desc, p_brand, p_type, p_size"
    Q17="select sum(l_extendedprice) / 7.0 as avg_yearly from lineitem l, part p where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX' and l_quantity < (select 0.2 * avg(l_quantity) from lineitem where l_partkey = p_partkey)"
    Q18="select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from customer c, orders o, lineitem l where o_orderkey in (select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 300) and c_custkey = o_custkey and o_orderkey = l_orderkey group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice order by o_totalprice desc, o_orderdate limit 100"
    Q19="select sum(l_extendedprice * (1 - l_discount)) as revenue from lineitem l, part p where (p_partkey = l_partkey and p_brand = 'Brand#12' and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') and l_quantity >= 1 and l_quantity <= 11 and p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON') or (p_partkey = l_partkey and p_brand = 'Brand#23' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and l_quantity >= 10 and l_quantity <= 20 and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON') or (p_partkey = l_partkey and p_brand = 'Brand#34' and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') and l_quantity >= 20 and l_quantity <= 30 and p_size between 1 and 15 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON')"
    Q20="select s_name, s_address from supplier s, nation n where s_suppkey in (select ps_suppkey from partsupp ps where ps_partkey in (select p_partkey from part where p_name like 'forest%') and ps_availqty > (select 0.5 * sum(l_quantity) from lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year)) and s_nationkey = n_nationkey and n_name = 'CANADA' order by s_name"
    Q21="select s_name, count(*) as numwait from supplier s, lineitem l1, orders o, nation n where s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate and exists (select * from lineitem l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey) and not exists (select * from lineitem l3 where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey and l3.l_receiptdate > l3.l_commitdate) and s_nationkey = n_nationkey and n_name = 'SAUDI ARABIA' group by s_name order by numwait desc, s_name limit 100"
    Q22="select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal from (select substr(c_phone, 1, 2) as cntrycode, c_acctbal from customer where substr(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17') and c_acctbal > (select avg(c_acctbal) from customer where c_acctbal > 0.00 and substr(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')) and not exists (select * from orders where o_custkey = c_custkey)) as custsale group by cntrycode order by cntrycode"
    
    # Run all 22 queries with appropriate timeouts
    queries=("$Q1" "$Q2" "$Q3" "$Q4" "$Q5" "$Q6" "$Q7" "$Q8" "$Q9" "$Q10" "$Q11" "$Q12" "$Q13" "$Q14" "$Q15" "$Q16" "$Q17" "$Q18" "$Q19" "$Q20" "$Q21" "$Q22")
    
    # Use global timeout for all queries (no individual query timeouts)
    # This allows for consistent timeout behavior across all queries
    
    # Determine which queries to run
    local query_range
    if [[ -n "$specific_queries" ]]; then
        # Convert comma-separated list to space-separated
        query_range=$(echo "$specific_queries" | tr ',' ' ')
        echo "Running specific queries: $specific_queries" >&2
    else
        # Run all 22 queries
        query_range=$(seq 1 22)
        echo "Running all 22 TPC-H queries" >&2
    fi
    
    for i in $query_range; do
        # Validate query number
        if [[ $i -lt 1 || $i -gt 22 ]]; then
            echo "Warning: Invalid query number $i, skipping" >&2
            continue
        fi
        
        query_idx=$((i-1))
        query_sql="${queries[$query_idx]}"
        timeout_seconds="$GLOBAL_TIMEOUT"  # Use global timeout for all queries
        
        # Run query and capture result
        if result=$(run_query "$i" "$query_sql" "$timeout_seconds"); then
            results+=("$result")
            echo "Query $i completed successfully" >&2
        else
            # Add error result with more detailed error information
            error_result=$(jq -n \
                --arg query_num "$i" \
                --arg error "Query failed or timed out after ${timeout_seconds}s" \
                '{
                    query_number: $query_num,
                    execution_time_seconds: 0,
                    processed_rows: 0,
                    processed_bytes: 0,
                    cpu_time_ms: 0,
                    wall_time_ms: 0,
                    error: $error
                }')
            results+=("$error_result")
            echo "Query $i failed or timed out" >&2
        fi
    done
    
    # Combine all results into a single JSON array
    local combined_results=$(printf '%s\n' "${results[@]}" | jq -s '.')
    
    # Add metadata
    local final_output=$(jq -n \
        --arg timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        --arg scale_factor "$TPCH_SF" \
        --arg timeout "$GLOBAL_TIMEOUT" \
        --argjson results "$combined_results" \
        '{
            timestamp: $timestamp,
            scale_factor: ($scale_factor | tonumber),
            timeout_seconds: ($timeout | tonumber),
            results: $results
        }')
    
    # Write results to file
    echo "$final_output" > "$OUTPUT_FILE"
    
    echo "Benchmark completed. Results written to $OUTPUT_FILE"
    
    # Print summary with counts
    local total_queries=$(echo "$final_output" | jq '.results | length')
    local successful_queries=$(echo "$final_output" | jq '[.results[] | select(.execution_time_seconds > 0)] | length')
    local failed_queries=$(echo "$final_output" | jq '[.results[] | select(.execution_time_seconds == 0)] | length')
    
    echo "Summary ($successful_queries successful, $failed_queries failed of $total_queries total):"
    echo "$final_output" | jq -r '.results[] | 
        if .execution_time_seconds > 0 then 
            "✅ Query \(.query_number): \(.execution_time_seconds)s (\(.processed_rows | tonumber | . / 1000000 | floor * 100 / 100)M rows)"
        else 
            "❌ Query \(.query_number): FAILED"
        end' | head -10
    
    if [[ $total_queries -gt 10 ]]; then
        echo "... and $((total_queries - 10)) more queries (see $OUTPUT_FILE for full results)"
    fi
}

# Function to show usage
show_usage() {
    cat <<EOF
Usage: $0 <command> [options]

Commands:
  generate              Generate TPC-H Parquet data
  register              Register TPC-H tables in Presto
  benchmark             Run TPC-H benchmark queries
  full                  Complete workflow (generate + register + benchmark)
  clean                 Clean up TPC-H tables

Options:
  -s, --scale-factor N  TPC-H scale factor (default: 1)
  -t, --timeout N       Query timeout in seconds (default: 30)
  -o, --output FILE     Output file for results (default: tpch_benchmark_results.json)
  -q, --queries LIST    Comma-separated list of specific queries to run (e.g., "1,3,5")
  -f, --force          Force regeneration of TPC-H data (skip existence check)
  -h, --help           Show this help message

Environment Variables:
  TPCH_PARQUET_DIR      Directory for TPC-H Parquet files (default: ./docker/data/tpch)
  COORD                 Presto coordinator URL (default: localhost:8080)
  CATALOG               Presto catalog (default: hive)
  SCHEMA                Presto schema (default: tpch_parquet)

Examples:
  $0 generate -s 1                    # Generate SF1 data
  $0 generate -s 10 --force           # Force regenerate SF10 data
  $0 register                         # Register tables
  $0 benchmark                        # Run benchmark
  $0 full -s 1                        # Complete workflow with SF1
  $0 full -s 100 --force              # Complete workflow with SF100, force regenerate
  $0 clean                            # Clean up tables

Memory Configuration:
  For SF10+ workloads, use dynamic memory configuration:
  ./dynamic_memory_config.sh          # Auto-configure based on system RAM
  ./dynamic_memory_config.sh -m 64    # Use specific memory amount

EOF
}

# Parse command line arguments
COMMAND=""
GLOBAL_TIMEOUT=30   # Default global timeout (30 seconds for SF1)
SPECIFIC_QUERIES=""  # For running specific queries
FORCE_REGENERATE=false  # Force data regeneration
while [[ $# -gt 0 ]]; do
    case $1 in
        generate|register|benchmark|full|clean)
            COMMAND="$1"
            shift
            ;;
        -s|--scale-factor)
            TPCH_SF="$2"
            TARGET_DIR="${BASE_TARGET_DIR}/sf${TPCH_SF}"
            shift 2
            ;;
        -o|--output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        -t|--timeout)
            GLOBAL_TIMEOUT="$2"
            shift 2
            ;;
        -q|--queries)
            SPECIFIC_QUERIES="$2"
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
            echo "Unknown option: $1" >&2
            show_usage
            exit 1
            ;;
    esac
done

# Execute command
case $COMMAND in
    generate)
        generate_tpch_data
        ;;
    register)
        register_tpch_tables
        ;;
    benchmark)
        run_tpch_benchmark "$SPECIFIC_QUERIES"
        ;;
    full)
        echo "Running full TPC-H workflow..."
        generate_tpch_data
        register_tpch_tables
        run_tpch_benchmark
        ;;
    clean)
        echo "Cleaning up TPC-H tables..."
        for table in lineitem orders customer partsupp part supplier nation region; do
            echo "Dropping table ${table}..."
            run_sql "drop table if exists ${CATALOG}.${SCHEMA}.${table}" > /dev/null 2>&1 || true
        done
        echo "Cleanup complete"
        ;;
    "")
        echo "Error: No command specified" >&2
        show_usage
        exit 1
        ;;
    *)
        echo "Error: Unknown command: $COMMAND" >&2
        show_usage
        exit 1
        ;;
esac
