-- TASK 1: STORES BY COUNTRY
SELECT 
	country_code,
	COUNT(store_code) AS total_stores_by_region
	
FROM
	dim_store_details

GROUP BY
	country_code

ORDER BY
	total_stores_by_region DESC;
	
-- TASK 2: STORES BY LOCALITY

SELECT 
	locality,
	COUNT(store_code) AS total_stores_by_locality
	
FROM
	dim_store_details

GROUP BY
	locality

ORDER BY
	total_stores_by_locality DESC
	
LIMIT
	10;
	
-- TASK 3: TOTAL SALES BY MONTH

SELECT 
	dim_date_times.month,
	SUM(orders_table.product_quantity * dim_products."product_price_GBP") AS monthly_sales
FROM 
	orders_table
LEFT JOIN
	dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
LEFT JOIN
	dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY
	dim_date_times.month
ORDER BY
	dim_date_times.month DESC;

-- TASK 4: TOTAL SALES ONLINE VS OFFLINE
SELECT 
	dim_store_details.store_type,
	SUM(orders_table.product_quantity) AS product_quantity_sum,
	COUNT(orders_table.product_quantity) AS number_of_sales
FROM 
	orders_table
LEFT JOIN
	dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY
	store_type;
	
-- TASK 5: TOTAL SALES BY STORE TYPE
WITH sales_info AS (
	SELECT 
		dim_store_details.store_type,
		SUM(orders_table.product_quantity * dim_products."product_price_GBP") AS product_sales_total
	FROM 
		orders_table
	LEFT JOIN
		dim_store_details ON orders_table.store_code = dim_store_details.store_code
	LEFT JOIN
		dim_products ON orders_table.product_code = dim_products.product_code
	GROUP BY
		store_type
)
SELECT 
	store_type,
	product_sales_total,
	(product_sales_total*100 / SUM(product_sales_total) OVER ()) AS percent_of_total
FROM 
	sales_info
ORDER BY
	percent_of_total DESC;
	
-- TASK 6: PEAK MONTH EVERY YEAR
WITH monthly_sales AS (
	SELECT 
		dim_date_times.month,
		dim_date_times.year,
		SUM(orders_table.product_quantity * dim_products."product_price_GBP") AS product_sales_total
	FROM 
		orders_table
	LEFT JOIN
		dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
	LEFT JOIN
		dim_products ON orders_table.product_code = dim_products.product_code
	GROUP BY
		dim_date_times.month,
		dim_date_times.year
),
max_sales_per_year AS (
SELECT
	year,
	MAX(product_sales_total) AS max_sales
FROM 
	monthly_sales
GROUP BY
	year
)
SELECT
	monthly_sales.month,
	monthly_sales.year,
	max_sales_per_year.max_sales
FROM
	max_sales_per_year
LEFT JOIN
	monthly_sales ON max_sales_per_year.year = monthly_sales.year
	AND monthly_sales.product_sales_total = max_sales_per_year.max_sales
ORDER BY
	year DESC;


-- TASK 7: STAFF NUMBER BY LOCATION
SELECT
	country_code,
	SUM(staff_numbers)
FROM
	dim_store_details
GROUP BY
	country_code;
	
-- TASK 8: SALES BY STORE TYPE IN GERMANY
WITH sales_info AS (
	SELECT 
		dim_store_details.store_type,
		dim_store_details.country_code,
		SUM(orders_table.product_quantity * dim_products."product_price_GBP") AS product_sales_total
	FROM 
		orders_table
	LEFT JOIN
		dim_store_details ON orders_table.store_code = dim_store_details.store_code
	LEFT JOIN
		dim_products ON orders_table.product_code = dim_products.product_code
	GROUP BY
		store_type,
		country_code
)
SELECT 
	store_type,
	country_code,
	product_sales_total,
	(product_sales_total*100 / SUM(product_sales_total) OVER ()) AS percent_of_total
FROM 
	sales_info
WHERE
	country_code = 'DE'
ORDER BY
	percent_of_total DESC;	
	
-- TASK 9: HOW QUICKLY IS COMPANY MAKING SALES?

WITH order_dates AS (
	SELECT
		orders_table.date_uuid,
		orders_table.product_quantity,
		dim_date_times.year,
		CONCAT(	
		dim_date_times.year,
        '-',
        LPAD(dim_date_times.month, 2, '0'), -- Ensure a leading zero for single-digit months
        '-',
        LPAD(dim_date_times.day, 2, '0'),   -- Ensure a leading zero for single-digit days
        ' ',
        dim_date_times.timestamp) AS datetime
	FROM
		orders_table
	LEFT JOIN
		dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
),

order_dates_formatted AS (
	SELECT
		date_uuid,
		year,
		product_quantity,
		TO_TIMESTAMP(datetime, 'YYYY-MM-DD HH24:MI:SS')
	FROM 
		order_dates
	ORDER BY
		datetime
),

order_dates_final AS (
	SELECT
		date_uuid,
		year,
		product_quantity,
		to_timestamp,
		LEAD(to_timestamp) OVER (PARTITION BY year) AS next_sales_date,
		(LEAD(to_timestamp) OVER (PARTITION BY year) - to_timestamp) AS sales_time_diff
	FROM
		order_dates_formatted
)

SELECT
	year,
	AVG(sales_time_diff) AS avg_sales_time_diff
FROM
	order_dates_final
GROUP BY
	year;