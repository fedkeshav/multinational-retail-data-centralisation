/*
SELECT
    column_name,
    data_type,
	is_nullable
FROM
    information_schema.columns
WHERE
    table_name = 'dim_products';
*/

-- SELECT pg_size_pretty(pg_total_relation_size('dim_products'));

ALTER TABLE dim_products
	ADD weight_class VARCHAR(14);

UPDATE dim_products
SET weight_class = CASE
	WHEN weight_kg < 2 THEN 'Light'
	WHEN weight_kg >=2 AND weight_kg < 40 THEN 'Mid_sized'
	WHEN weight_kg >= 40 AND weight_kg < 140 THEN 'Heavy'
	ELSE 'Truck_Required'
END;


ALTER TABLE dim_products
	ALTER COLUMN product_code TYPE VARCHAR(11);	
ALTER TABLE dim_products
	ADD ean TEXT;
UPDATE dim_products
	SET ean = "EAN"::TEXT;
ALTER TABLE dim_products
	ALTER COLUMN ean TYPE VARCHAR(17);	
ALTER TABLE dim_products
	DROP COLUMN "EAN";
	
ALTER TABLE dim_products
	ALTER COLUMN date_added TYPE DATE USING date_added::DATE;
	
ALTER TABLE dim_products
	ALTER COLUMN uuid TYPE UUID USING uuid::UUID;

ALTER TABLE dim_products
	ADD still_available BOOL;
UPDATE dim_products
SET still_available = CASE
	WHEN availability = 'Still_avaliable'  THEN TRUE
	ELSE FALSE
END;	

ALTER TABLE dim_products
	DROP COLUMN index;

