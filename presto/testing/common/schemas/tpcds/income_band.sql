CREATE TABLE hive.{schema}.income_band (
    ib_income_band_sk BIGINT,
    ib_lower_bound BIGINT,
    ib_upper_bound INTEGER
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = '{location}')
