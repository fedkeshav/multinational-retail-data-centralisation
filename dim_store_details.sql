/*
SELECT
    column_name,
    data_type,
	is_nullable
FROM
    information_schema.columns
WHERE
    table_name = 'dim_store_details';
*/

-- SELECT pg_size_pretty(pg_total_relation_size('dim_store_details'));


ALTER TABLE dim_store_details
	ALTER COLUMN locality TYPE VARCHAR(255);

ALTER TABLE dim_store_details
	ALTER COLUMN continent TYPE VARCHAR(255);
	
ALTER TABLE dim_store_details
	ALTER COLUMN store_type TYPE VARCHAR(255);
	
ALTER TABLE dim_store_details
	ALTER COLUMN country_code TYPE VARCHAR(2);
	
ALTER TABLE dim_store_details
	ALTER COLUMN store_code TYPE VARCHAR(12);

ALTER TABLE dim_store_details
	ALTER COLUMN opening_date TYPE date USING opening_date::date;

ALTER TABLE dim_store_details
	ALTER COLUMN staff_numbers TYPE SMALLINT;
	
ALTER TABLE dim_store_details
	ALTER COLUMN latitude TYPE FLOAT;

ALTER TABLE dim_store_details
	ALTER COLUMN longitude TYPE FLOAT;
	
ALTER TABLE dim_store_details
	DROP COLUMN index;