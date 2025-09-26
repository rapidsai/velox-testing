CREATE TABLE hive.{schema}.nation     (
    n_nationkey INTEGER NOT NULL,
    n_name VARCHAR NOT NULL,
    n_regionkey INTEGER NOT NULL,
    n_comment VARCHAR NOT NULL
)     WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = 'file:{file_path}')
