CREATE TABLE hive.tpcds_test.customer_address (
    ca_address_sk INTEGER,
    ca_address_id VARCHAR,
    ca_street_number VARCHAR,
    ca_street_name VARCHAR,
    ca_street_type VARCHAR,
    ca_suite_number VARCHAR,
    ca_city VARCHAR,
    ca_county VARCHAR,
    ca_state VARCHAR,
    ca_zip VARCHAR,
    ca_country VARCHAR,
    ca_gmt_offset DOUBLE,
    ca_location_type VARCHAR
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = 'file:{file_path}')
