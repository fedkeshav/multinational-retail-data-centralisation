/*
SELECT
    column_name,
    data_type,
	is_nullable
FROM
    information_schema.columns
WHERE
    table_name = 'dim_card_details';
*/

ALTER TABLE dim_card_details
	ALTER COLUMN expiry_date TYPE VARCHAR(5);

ALTER TABLE dim_card_details
	ALTER COLUMN card_number TYPE VARCHAR(19);
	
ALTER TABLE dim_card_details
	DROP COLUMN index;
	
/*
SELECT (MAX(LENGTH(card_number))) AS max_expiry
FROM dim_card_details;
*/
-- SELECT pg_size_pretty(pg_total_relation_size('dim_card_details'));