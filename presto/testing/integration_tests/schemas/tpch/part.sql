CREATE TABLE hive.{schema}.part     (
    p_partkey BIGINT NOT NULL,
    p_name VARCHAR NOT NULL,
    p_mfgr VARCHAR NOT NULL,
    p_brand VARCHAR NOT NULL,
    p_type VARCHAR NOT NULL,
    p_size INTEGER NOT NULL,
    p_container VARCHAR NOT NULL,
    p_retailprice DOUBLE NOT NULL,
    p_comment VARCHAR NOT NULL
)     WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = 'file:{file_path}')
