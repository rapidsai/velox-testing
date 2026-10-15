CREATE TABLE hive.{schema}.time_dim (
    t_time_sk BIGINT,
    t_time_id VARCHAR,
    t_time BIGINT,
    t_hour BIGINT,
    t_minute BIGINT,
    t_second BIGINT,
    t_am_pm VARCHAR,
    t_shift VARCHAR,
    t_sub_shift VARCHAR,
    t_meal_time VARCHAR
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = '{location}')
