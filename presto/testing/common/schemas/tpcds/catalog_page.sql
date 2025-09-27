CREATE TABLE hive.tpcds_test.catalog_page (
    cp_catalog_page_sk INTEGER,
    cp_catalog_page_id VARCHAR,
    cp_start_date_sk INTEGER,
    cp_end_date_sk INTEGER,
    cp_department VARCHAR,
    cp_catalog_number INTEGER,
    cp_catalog_page_number INTEGER,
    cp_description VARCHAR,
    cp_type VARCHAR
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = 'file:{file_path}')
