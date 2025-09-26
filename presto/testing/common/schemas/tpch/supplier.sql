CREATE TABLE hive.tpch_test.supplier (
    s_suppkey BIGINT NOT NULL,
    s_name VARCHAR NOT NULL,
    s_address VARCHAR NOT NULL,
    s_nationkey INTEGER NOT NULL,
    s_phone VARCHAR NOT NULL,
    s_acctbal DOUBLE NOT NULL,
    s_comment VARCHAR NOT NULL
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = 'file:{file_path}')
