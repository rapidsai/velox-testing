CREATE TABLE hive.{schema}.catalog_page (
    cp_catalog_page_sk BIGINT,
    cp_catalog_page_id VARCHAR,
    cp_start_date_sk BIGINT,
    cp_end_date_sk BIGINT,
    cp_department VARCHAR,
    cp_catalog_number BIGINT,
    cp_catalog_page_number BIGINT,
    cp_description VARCHAR,
    cp_type VARCHAR
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = '{location}')
