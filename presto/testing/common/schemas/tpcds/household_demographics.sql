CREATE TABLE hive.{schema}.household_demographics (
    hd_demo_sk BIGINT,
    hd_income_band_sk BIGINT,
    hd_buy_potential VARCHAR,
    hd_dep_count BIGINT,
    hd_vehicle_count INTEGER
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = '{location}')
