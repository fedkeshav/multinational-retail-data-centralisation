/*
SELECT
    column_name,
    data_type
FROM
    information_schema.columns
WHERE
    table_name = 'dim_users';
*/

/*
SELECT MAX(LENGTH(country_code)) AS max_length
FROM dim_users;
*/

-- SELECT pg_size_pretty(pg_total_relation_size('dim_users'));

ALTER TABLE dim_users
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;
	
ALTER TABLE dim_users
	ALTER COLUMN first_name TYPE VARCHAR(255);

ALTER TABLE dim_users
	ALTER COLUMN last_name TYPE VARCHAR(255);
	
ALTER TABLE dim_users
	ALTER COLUMN country_code TYPE VARCHAR(2);

ALTER TABLE dim_users
	ALTER COLUMN join_date TYPE date USING join_date::date;

ALTER TABLE dim_users
	ALTER COLUMN date_of_birth TYPE date USING date_of_birth::date;
	
ALTER TABLE dim_users
	DROP COLUMN index;
	
