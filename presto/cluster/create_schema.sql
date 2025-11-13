CREATE SCHEMA IF NOT EXISTS tpch1k;
CREATE TABLE IF NOT EXISTS hive.tpch1k.orders (
    o_orderkey BIGINT NOT NULL,
    o_custkey BIGINT NOT NULL,
    o_orderstatus VARCHAR NOT NULL,
    o_totalprice DOUBLE NOT NULL,
    o_orderdate DATE NOT NULL,
    o_orderpriority VARCHAR NOT NULL,
    o_clerk VARCHAR NOT NULL,
    o_shippriority INTEGER NOT NULL,
    o_comment VARCHAR NOT NULL
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = 'file:/workspace/datasets/sf1k/orders');
CREATE TABLE IF NOT EXISTS hive.tpch1k.nation (
    n_nationkey BIGINT NOT NULL,
    n_name VARCHAR NOT NULL,
    n_regionkey BIGINT NOT NULL,
    n_comment VARCHAR NOT NULL
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = 'file:/workspace/datasets/sf1k/nation');
CREATE TABLE IF NOT EXISTS hive.tpch1k.customer (
    c_custkey BIGINT NOT NULL,
    c_name VARCHAR NOT NULL,
    c_address VARCHAR NOT NULL,
    c_nationkey BIGINT NOT NULL,
    c_phone VARCHAR NOT NULL,
    c_acctbal DOUBLE NOT NULL,
    c_mktsegment VARCHAR NOT NULL,
    c_comment VARCHAR NOT NULL
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = 'file:/workspace/datasets/sf1k/customer');
CREATE TABLE IF NOT EXISTS hive.tpch1k.lineitem (
    l_orderkey BIGINT NOT NULL,
    l_partkey BIGINT NOT NULL,
    l_suppkey BIGINT NOT NULL,
    l_linenumber INTEGER NOT NULL,
    l_quantity DOUBLE NOT NULL,
    l_extendedprice DOUBLE NOT NULL,
    l_discount DOUBLE NOT NULL,
    l_tax DOUBLE NOT NULL,
    l_returnflag VARCHAR NOT NULL,
    l_linestatus VARCHAR NOT NULL,
    l_shipdate DATE NOT NULL,
    l_commitdate DATE NOT NULL,
    l_receiptdate DATE NOT NULL,
    l_shipinstruct VARCHAR NOT NULL,
    l_shipmode VARCHAR NOT NULL,
    l_comment VARCHAR NOT NULL
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = 'file:/workspace/datasets/sf1k/lineitem');
CREATE TABLE IF NOT EXISTS hive.tpch1k.part (
    p_partkey BIGINT NOT NULL,
    p_name VARCHAR NOT NULL,
    p_mfgr VARCHAR NOT NULL,
    p_brand VARCHAR NOT NULL,
    p_type VARCHAR NOT NULL,
    p_size INTEGER NOT NULL,
    p_container VARCHAR NOT NULL,
    p_retailprice DOUBLE NOT NULL,
    p_comment VARCHAR NOT NULL
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = 'file:/workspace/datasets/sf1k/part');
CREATE TABLE IF NOT EXISTS hive.tpch1k.partsupp (
    ps_partkey BIGINT NOT NULL,
    ps_suppkey BIGINT NOT NULL,
    ps_availqty INTEGER NOT NULL,
    ps_supplycost DOUBLE NOT NULL,
    ps_comment VARCHAR NOT NULL
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = 'file:/workspace/datasets/sf1k/partsupp');
CREATE TABLE IF NOT EXISTS hive.tpch1k.region (
    r_regionkey BIGINT NOT NULL,
    r_name VARCHAR NOT NULL,
    r_comment VARCHAR NOT NULL
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = 'file:/workspace/datasets/sf1k/region');
CREATE TABLE IF NOT EXISTS hive.tpch1k.supplier (
    s_suppkey BIGINT NOT NULL,
    s_name VARCHAR NOT NULL,
    s_address VARCHAR NOT NULL,
    s_nationkey BIGINT NOT NULL,
    s_phone VARCHAR NOT NULL,
    s_acctbal DOUBLE NOT NULL,
    s_comment VARCHAR NOT NULL
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = 'file:/workspace/datasets/sf1k/supplier');
SHOW TABLES FROM hive.tpch1k;
