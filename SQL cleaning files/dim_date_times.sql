/*
SELECT
    column_name,
    data_type,
	is_nullable
FROM
    information_schema.columns
WHERE
    table_name = 'dim_date_times';
*/

/*
SELECT (MAX(LENGTH(time_period))) AS max_time
FROM dim_date_times;
*/

-- SELECT pg_size_pretty(pg_total_relation_size('dim_date_times'));

ALTER TABLE dim_date_times
	ALTER COLUMN year TYPE VARCHAR(4);

ALTER TABLE dim_date_times
	ALTER COLUMN month TYPE VARCHAR(2);
	
ALTER TABLE dim_date_times
	ALTER COLUMN day TYPE VARCHAR(2);
	
ALTER TABLE dim_date_times
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;
	
ALTER TABLE dim_date_times
	ALTER COLUMN time_period TYPE VARCHAR(10);
	
ALTER TABLE dim_date_times
	DROP COLUMN index;

