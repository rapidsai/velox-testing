CREATE TABLE hive.tpcds_test.reason (
    r_reason_sk INTEGER,
    r_reason_id VARCHAR,
    r_reason_desc VARCHAR
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = 'file:{file_path}')
