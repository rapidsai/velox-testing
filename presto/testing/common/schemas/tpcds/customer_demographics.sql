CREATE TABLE hive.{schema}.customer_demographics (
    cd_demo_sk BIGINT,
    cd_gender VARCHAR,
    cd_marital_status VARCHAR,
    cd_education_status VARCHAR,
    cd_purchase_estimate BIGINT,
    cd_credit_rating VARCHAR,
    cd_dep_count BIGINT,
    cd_dep_employed_count BIGINT,
    cd_dep_college_count INTEGER
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = '{location}')
