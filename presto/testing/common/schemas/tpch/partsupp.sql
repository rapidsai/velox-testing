CREATE TABLE hive.{schema}.partsupp     (
    ps_partkey BIGINT NOT NULL,
    ps_suppkey BIGINT NOT NULL,
    ps_availqty BIGINT NOT NULL,
    ps_supplycost DOUBLE NOT NULL,
    ps_comment VARCHAR NOT NULL
)     WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = 'file:{file_path}')
