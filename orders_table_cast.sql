/*
SELECT
    column_name,
    data_type
FROM
    information_schema.columns
WHERE
    table_name = 'orders_table';
*/

/*
SELECT MAX(LENGTH(card_number)) AS max_length
FROM orders_table;
*/
-- SELECT pg_size_pretty(pg_total_relation_size('orders_table'));

ALTER TABLE orders_table
	ALTER COLUMN card_number TYPE TEXT;
	
ALTER TABLE orders_table
	ALTER COLUMN card_number TYPE VARCHAR(19);
	
ALTER TABLE orders_table
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;
	
ALTER TABLE orders_table
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;
	
ALTER TABLE orders_table
	ALTER COLUMN store_code TYPE VARCHAR(12);

ALTER TABLE orders_table
	ALTER COLUMN product_code TYPE VARCHAR(11);

ALTER TABLE orders_table
	ALTER COLUMN product_quantity TYPE SMALLINT;
	
