CREATE TABLE hive.{schema}.income_band (
    ib_income_band_sk INTEGER,
    ib_lower_bound INTEGER,
    ib_upper_bound INTEGER
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = 'file:{file_path}')
