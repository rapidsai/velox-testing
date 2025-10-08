CREATE TABLE hive.{schema}.orders     (
    o_orderkey BIGINT NOT NULL,
    o_custkey BIGINT NOT NULL,
    o_orderstatus VARCHAR NOT NULL,
    o_totalprice DOUBLE NOT NULL,
    o_orderdate DATE NOT NULL,
    o_orderpriority VARCHAR NOT NULL,
    o_clerk VARCHAR NOT NULL,
    o_shippriority INTEGER NOT NULL,
    o_comment VARCHAR NOT NULL
)     WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = 'file:{file_path}')
