CREATE TABLE hive.{schema}.region (
    r_regionkey BIGINT NOT NULL,
    r_name VARCHAR NOT NULL,
    r_comment VARCHAR NOT NULL
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = 'file:{file_path}')
