"""
TPC-H Query Definitions
All 22 standard TPC-H queries with proper SQL formatting
"""

# TPC-H Query 1 - Pricing Summary Report
QUERY_01 = """
SELECT 
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
FROM lineitem 
WHERE l_shipdate >= date '1998-12-01' - interval '90' day
    AND l_shipdate <= date '1998-12-01'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
"""

# TPC-H Query 2 - Minimum Cost Supplier
QUERY_02 = """
SELECT 
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
FROM part p, supplier s, partsupp ps, nation n, region r
WHERE p_partkey = ps_partkey
    AND s_suppkey = ps_suppkey
    AND p_size = 15
    AND p_type like '%BRASS'
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND ps_supplycost = (
        SELECT min(ps2_supplycost)
        FROM partsupp ps2, supplier s2, nation n2, region r2
        WHERE ps2_partkey = p_partkey
            AND s2_suppkey = ps2_suppkey
            AND s2_nationkey = n2_nationkey
            AND n2_regionkey = r2_regionkey
            AND r2_name = 'EUROPE'
    )
ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
LIMIT 100
"""

# TPC-H Query 3 - Shipping Priority
QUERY_03 = """
SELECT 
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
FROM customer c, orders o, lineitem l
WHERE c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < date '1995-03-15'
    AND l_shipdate > date '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10
"""

# TPC-H Query 4 - Order Priority Checking
QUERY_04 = """
SELECT 
    o_orderpriority,
    count(*) as order_count
FROM orders
WHERE o_orderdate >= date '1993-07-01'
    AND o_orderdate < date '1993-07-01' + interval '3' month
    AND exists (
        SELECT *
        FROM lineitem
        WHERE l_orderkey = o_orderkey
            AND l_commitdate < l_receiptdate
    )
GROUP BY o_orderpriority
ORDER BY o_orderpriority
"""

# TPC-H Query 5 - Local Supplier Volume
QUERY_05 = """
SELECT 
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM customer c, orders o, lineitem l, supplier s, nation n, region r
WHERE c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= date '1994-01-01'
    AND o_orderdate < date '1994-01-01' + interval '1' year
GROUP BY n_name
ORDER BY revenue DESC
"""

# TPC-H Query 6 - Forecasting Revenue Change
QUERY_06 = """
SELECT 
    sum(l_extendedprice * l_discount) as revenue
FROM lineitem
WHERE l_shipdate >= date '1994-01-01'
    AND l_shipdate < date '1994-01-01' + interval '1' year
    AND l_discount between 0.06 - 0.01 AND 0.06 + 0.01
    AND l_quantity < 24
"""

# TPC-H Query 7 - Volume Shipping
QUERY_07 = """
SELECT 
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) as revenue
FROM (
    SELECT 
        n1.n_name as supp_nation,
        n2.n_name as cust_nation,
        extract(year from l_shipdate) as l_year,
        l_extendedprice * (1 - l_discount) as volume
    FROM supplier s, lineitem l, orders o, customer c, nation n1, nation n2
    WHERE s_suppkey = l_suppkey
        AND o_orderkey = l_orderkey
        AND c_custkey = o_custkey
        AND s_nationkey = n1.n_nationkey
        AND c_nationkey = n2.n_nationkey
        AND ((n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') 
             or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE'))
        AND l_shipdate between date '1995-01-01' and date '1996-12-31'
) as shipping
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year
"""

# TPC-H Query 8 - National Market Share
QUERY_08 = """
SELECT 
    o_year,
    sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share
FROM (
    SELECT 
        extract(year from o_orderdate) as o_year,
        l_extendedprice * (1 - l_discount) as volume,
        n2.n_name as nation
    FROM part p, supplier s, lineitem l, orders o, customer c, nation n1, nation n2, region r
    WHERE p_partkey = l_partkey
        AND s_suppkey = l_suppkey
        AND l_orderkey = o_orderkey
        AND o_custkey = c_custkey
        AND c_nationkey = n1.n_nationkey
        AND n1.n_regionkey = r.r_regionkey
        AND r.r_name = 'AMERICA'
        AND s_nationkey = n2.n_nationkey
        AND o_orderdate between date '1995-01-01' and date '1996-12-31'
        AND p_type = 'ECONOMY ANODIZED STEEL'
) as all_nations
GROUP BY o_year
ORDER BY o_year
"""

# TPC-H Query 9 - Product Type Profit Measure
QUERY_09 = """
SELECT 
    nation,
    o_year,
    sum(amount) as sum_profit
FROM (
    SELECT 
        n_name as nation,
        extract(year from o_orderdate) as o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    FROM part p, supplier s, lineitem l, partsupp ps, orders o, nation n
    WHERE s_suppkey = l_suppkey
        AND ps_suppkey = l_suppkey
        AND ps_partkey = l_partkey
        AND p_partkey = l_partkey
        AND o_orderkey = l_orderkey
        AND s_nationkey = n_nationkey
        AND p_name like '%green%'
) as profit
GROUP BY nation, o_year
ORDER BY nation, o_year DESC
"""

# TPC-H Query 10 - Returned Item Reporting
QUERY_10 = """
SELECT 
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM customer c, orders o, lineitem l, nation n
WHERE c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate >= date '1993-10-01'
    AND o_orderdate < date '1993-10-01' + interval '3' month
    AND l_returnflag = 'R'
    AND c_nationkey = n_nationkey
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue DESC
LIMIT 20
"""

# TPC-H Query 11 - Important Stock Identification
QUERY_11 = """
SELECT 
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
FROM partsupp ps, supplier s, nation n
WHERE ps_suppkey = s_suppkey
    AND s_nationkey = n_nationkey
    AND n_name = 'GERMANY'
GROUP BY ps_partkey
HAVING sum(ps_supplycost * ps_availqty) > (
    SELECT sum(ps2_supplycost * ps2_availqty) * 0.0001
    FROM partsupp ps2, supplier s2, nation n2
    WHERE ps2_suppkey = s2_suppkey
        AND s2_nationkey = n2_nationkey
        AND n2_name = 'GERMANY'
)
ORDER BY value DESC
"""

# TPC-H Query 12 - Shipping Modes and Order Priority
QUERY_12 = """
SELECT 
    l_shipmode,
    sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' 
             then 1 else 0 end) as high_line_count,
    sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' 
             then 1 else 0 end) as low_line_count
FROM orders o, lineitem l
WHERE o_orderkey = l_orderkey
    AND l_shipmode in ('MAIL', 'SHIP')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= date '1994-01-01'
    AND l_receiptdate < date '1994-01-01' + interval '1' year
GROUP BY l_shipmode
ORDER BY l_shipmode
"""

# TPC-H Query 13 - Customer Distribution
QUERY_13 = """
SELECT 
    c_count,
    count(*) as custdist
FROM (
    SELECT 
        c_custkey,
        count(o_orderkey) as c_count
    FROM customer c 
    LEFT OUTER JOIN orders o on c_custkey = o_custkey
        AND o_comment not like '%special%requests%'
    GROUP BY c_custkey
) as c_orders
GROUP BY c_count
ORDER BY custdist DESC, c_count DESC
"""

# TPC-H Query 14 - Promotion Effect
QUERY_14 = """
SELECT 
    100.00 * sum(case when p_type like 'PROMO%' 
                      then l_extendedprice * (1 - l_discount) 
                      else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
FROM lineitem l, part p
WHERE l_partkey = p_partkey
    AND l_shipdate >= date '1995-09-01'
    AND l_shipdate < date '1995-09-01' + interval '1' month
"""

# TPC-H Query 15 - Top Supplier
QUERY_15 = """
WITH revenue AS (
    SELECT 
        l_suppkey as supplier_no,
        sum(l_extendedprice * (1 - l_discount)) as total_revenue
    FROM lineitem
    WHERE l_shipdate >= date '1996-01-01'
        AND l_shipdate < date '1996-01-01' + interval '3' month
    GROUP BY l_suppkey
)
SELECT 
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM supplier s, revenue r
WHERE s_suppkey = r.supplier_no
    AND total_revenue = (SELECT max(total_revenue) FROM revenue)
ORDER BY s_suppkey
"""

# TPC-H Query 16 - Parts/Supplier Relationship
QUERY_16 = """
SELECT 
    p_brand,
    p_type,
    p_size,
    count(distinct ps_suppkey) as supplier_cnt
FROM partsupp ps, part p
WHERE p_partkey = ps_partkey
    AND p_brand <> 'Brand#45'
    AND p_type not like 'MEDIUM POLISHED%'
    AND p_size in (49, 14, 23, 45, 19, 3, 36, 9)
    AND ps_suppkey not in (
        SELECT s_suppkey
        FROM supplier
        WHERE s_comment like '%Customer%Complaints%'
    )
GROUP BY p_brand, p_type, p_size
ORDER BY supplier_cnt DESC, p_brand, p_type, p_size
"""

# TPC-H Query 17 - Small-Quantity-Order Revenue
QUERY_17 = """
SELECT 
    sum(l_extendedprice) / 7.0 as avg_yearly
FROM lineitem l, part p
WHERE p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container = 'MED BOX'
    AND l_quantity < (
        SELECT 0.2 * avg(l_quantity)
        FROM lineitem
        WHERE l_partkey = p_partkey
    )
"""

# TPC-H Query 18 - Large Volume Customer
QUERY_18 = """
SELECT 
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
FROM customer c, orders o, lineitem l
WHERE o_orderkey in (
    SELECT l_orderkey
    FROM lineitem
    GROUP BY l_orderkey
    HAVING sum(l_quantity) > 300
)
    AND c_custkey = o_custkey
    AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100
"""

# TPC-H Query 19 - Discounted Revenue
QUERY_19 = """
SELECT 
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM lineitem l, part p
WHERE (
    p_partkey = l_partkey
    AND p_brand = 'Brand#12'
    AND p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    AND l_quantity >= 1 AND l_quantity <= 11
    AND p_size between 1 and 5
    AND l_shipmode in ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
) OR (
    p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
    AND l_quantity >= 10 AND l_quantity <= 20
    AND p_size between 1 and 10
    AND l_shipmode in ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
) OR (
    p_partkey = l_partkey
    AND p_brand = 'Brand#34'
    AND p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
    AND l_quantity >= 20 AND l_quantity <= 30
    AND p_size between 1 and 15
    AND l_shipmode in ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
)
"""

# TPC-H Query 20 - Potential Part Promotion
QUERY_20 = """
SELECT 
    s_name,
    s_address
FROM supplier s, nation n
WHERE s_suppkey in (
    SELECT ps_suppkey
    FROM partsupp ps
    WHERE ps_partkey in (
        SELECT p_partkey
        FROM part
        WHERE p_name like 'forest%'
    )
    AND ps_availqty > (
        SELECT 0.5 * sum(l_quantity)
        FROM lineitem
        WHERE l_partkey = ps_partkey
            AND l_suppkey = ps_suppkey
            AND l_shipdate >= date '1994-01-01'
            AND l_shipdate < date '1994-01-01' + interval '1' year
    )
)
    AND s_nationkey = n_nationkey
    AND n_name = 'CANADA'
ORDER BY s_name
"""

# TPC-H Query 21 - Suppliers Who Kept Orders Waiting
QUERY_21 = """
SELECT 
    s_name,
    count(*) as numwait
FROM supplier s, lineitem l1, orders o, nation n
WHERE s_suppkey = l1.l_suppkey
    AND o_orderkey = l1.l_orderkey
    AND o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND exists (
        SELECT *
        FROM lineitem l2
        WHERE l2.l_orderkey = l1.l_orderkey
            AND l2.l_suppkey <> l1.l_suppkey
    )
    AND not exists (
        SELECT *
        FROM lineitem l3
        WHERE l3.l_orderkey = l1.l_orderkey
            AND l3.l_suppkey <> l1.l_suppkey
            AND l3.l_receiptdate > l3.l_commitdate
    )
    AND s_nationkey = n_nationkey
    AND n_name = 'SAUDI ARABIA'
GROUP BY s_name
ORDER BY numwait DESC, s_name
LIMIT 100
"""

# TPC-H Query 22 - Global Sales Opportunity
QUERY_22 = """
SELECT 
    cntrycode,
    count(*) as numcust,
    sum(c_acctbal) as totacctbal
FROM (
    SELECT 
        substr(c_phone, 1, 2) as cntrycode,
        c_acctbal
    FROM customer
    WHERE substr(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')
        AND c_acctbal > (
            SELECT avg(c_acctbal)
            FROM customer
            WHERE c_acctbal > 0.00
                AND substr(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')
        )
        AND not exists (
            SELECT *
            FROM orders
            WHERE o_custkey = c_custkey
        )
) as custsale
GROUP BY cntrycode
ORDER BY cntrycode
"""

# Query registry for easy access
TPCH_QUERIES = {
    1: QUERY_01,
    2: QUERY_02,
    3: QUERY_03,
    4: QUERY_04,
    5: QUERY_05,
    6: QUERY_06,
    7: QUERY_07,
    8: QUERY_08,
    9: QUERY_09,
    10: QUERY_10,
    11: QUERY_11,
    12: QUERY_12,
    13: QUERY_13,
    14: QUERY_14,
    15: QUERY_15,
    16: QUERY_16,
    17: QUERY_17,
    18: QUERY_18,
    19: QUERY_19,
    20: QUERY_20,
    21: QUERY_21,
    22: QUERY_22,
}

# Query descriptions for reporting
QUERY_DESCRIPTIONS = {
    1: "Pricing Summary Report",
    2: "Minimum Cost Supplier",
    3: "Shipping Priority",
    4: "Order Priority Checking",
    5: "Local Supplier Volume",
    6: "Forecasting Revenue Change",
    7: "Volume Shipping",
    8: "National Market Share",
    9: "Product Type Profit Measure",
    10: "Returned Item Reporting",
    11: "Important Stock Identification",
    12: "Shipping Modes and Order Priority",
    13: "Customer Distribution",
    14: "Promotion Effect",
    15: "Top Supplier",
    16: "Parts/Supplier Relationship",
    17: "Small-Quantity-Order Revenue",
    18: "Large Volume Customer",
    19: "Discounted Revenue",
    20: "Potential Part Promotion",
    21: "Suppliers Who Kept Orders Waiting",
    22: "Global Sales Opportunity",
}


def get_query(query_number: int) -> str:
    """Get a TPC-H query by number."""
    if query_number not in TPCH_QUERIES:
        raise ValueError(f"Query {query_number} not found. Valid queries: 1-22")
    return TPCH_QUERIES[query_number]


def get_query_description(query_number: int) -> str:
    """Get the description of a TPC-H query."""
    return QUERY_DESCRIPTIONS.get(query_number, f"TPC-H Query {query_number}")


def get_all_queries() -> dict:
    """Get all TPC-H queries."""
    return TPCH_QUERIES.copy()

