CREATE TABLE hive.{schema}.household_demographics (
    hd_demo_sk INTEGER,
    hd_income_band_sk INTEGER,
    hd_buy_potential VARCHAR,
    hd_dep_count INTEGER,
    hd_vehicle_count INTEGER
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = 'file:{file_path}')
