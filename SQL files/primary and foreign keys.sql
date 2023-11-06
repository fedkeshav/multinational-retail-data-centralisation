-- ADDING PRIMARU KEY CONSTRAINTS TO ALL TABLES STARTING WITH DIM

ALTER TABLE dim_card_details
	ADD CONSTRAINT pk_dim_card_details PRIMARY KEY (card_number);

ALTER TABLE dim_date_times
	ADD CONSTRAINT pk_dim_date_times PRIMARY KEY (date_uuid);
	
ALTER TABLE dim_products
	ADD CONSTRAINT pk_dim_products PRIMARY KEY (product_code);
	
ALTER TABLE dim_store_details
	ADD CONSTRAINT pk_dim_store_details PRIMARY KEY (store_code);
	
ALTER TABLE dim_users
	ADD CONSTRAINT pk_dim_users PRIMARY KEY (user_uuid);
	
-- ADDING FOREIGN KEY CONSTRAINTS TO ORDERS TABLE WHICH IS SINGLE SOURCE OF TRUTH ABOUT ORDERS

ALTER TABLE orders_table
	ADD CONSTRAINT fk_card_orders_table FOREIGN KEY (card_number) REFERENCES dim_card_details (card_number);

ALTER TABLE orders_table
	ADD CONSTRAINT fk_date_orders_table FOREIGN KEY (date_uuid) REFERENCES dim_date_times (date_uuid);
	
ALTER TABLE orders_table
	ADD CONSTRAINT fk_product_orders_table FOREIGN KEY (product_code) REFERENCES dim_products (product_code);
	
ALTER TABLE orders_table
	ADD CONSTRAINT fk_store_orders_table FOREIGN KEY (store_code) REFERENCES dim_store_details (store_code);
	
ALTER TABLE orders_table
	ADD CONSTRAINT fk_user_orders_table FOREIGN KEY (user_uuid) REFERENCES dim_users (user_uuid);
	

