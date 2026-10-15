CREATE TABLE hive.{schema}.web_page (
    wp_web_page_sk BIGINT,
    wp_web_page_id VARCHAR,
    wp_rec_start_date DATE,
    wp_rec_end_date DATE,
    wp_creation_date_sk BIGINT,
    wp_access_date_sk BIGINT,
    wp_autogen_flag VARCHAR,
    wp_customer_sk BIGINT,
    wp_url VARCHAR,
    wp_type VARCHAR,
    wp_char_count BIGINT,
    wp_link_count BIGINT,
    wp_image_count BIGINT,
    wp_max_ad_count INTEGER
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = '{location}')
