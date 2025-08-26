CREATE TABLE hive.tpcds_test.inventory (
    inv_date_sk INTEGER,
    inv_item_sk INTEGER,
    inv_warehouse_sk INTEGER,
    inv_quantity_on_hand INTEGER
) WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = 'file:{file_path}')
