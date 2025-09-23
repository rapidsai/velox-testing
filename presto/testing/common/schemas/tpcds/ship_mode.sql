CREATE TABLE hive.tpcds_test.ship_mode (
    sm_ship_mode_sk INTEGER,
    sm_ship_mode_id VARCHAR,
    sm_type VARCHAR,
    sm_code VARCHAR,
    sm_carrier VARCHAR,
    sm_contract VARCHAR
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = 'file:{file_path}')
