CREATE TABLE hive.{schema}.reason (
    r_reason_sk BIGINT,
    r_reason_id VARCHAR,
    r_reason_desc VARCHAR
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = '{location}')
