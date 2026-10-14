CREATE TABLE hive.{schema}.inventory (
    inv_date_sk BIGINT,
    inv_item_sk BIGINT,
    inv_warehouse_sk BIGINT,
    inv_quantity_on_hand INTEGER
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = '{location}')
