CREATE TABLE hive.{schema}.customer (
    c_custkey BIGINT NOT NULL,
    c_name VARCHAR NOT NULL,
    c_address VARCHAR NOT NULL,
    c_nationkey INTEGER NOT NULL,
    c_phone VARCHAR NOT NULL,
    c_acctbal DOUBLE NOT NULL,
    c_mktsegment VARCHAR NOT NULL,
    c_comment VARCHAR NOT NULL
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = 'file:{file_path}')
