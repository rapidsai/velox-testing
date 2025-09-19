CREATE TABLE hive.tpch_test.lineitem (
    l_orderkey BIGINT NOT NULL,
    l_partkey BIGINT NOT NULL,
    l_suppkey BIGINT NOT NULL,
    l_linenumber BIGINT NOT NULL,
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
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = 'file:{file_path}')
