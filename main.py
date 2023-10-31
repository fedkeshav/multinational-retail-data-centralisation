#%%
import database_utils
import data_cleaning
import data_extraction
import pandas as pd
import tabula
import yaml
import requests

# Creating instances of each class
db_conn = database_utils.DatabaseConnector()
db_extract = data_extraction.DataExtractor()
db_cleaning = data_cleaning.DataCleaning()


# %%
''' 1. CLEANING AND UPLOADING USER DATA FROM DATABASE'''
# Starting engine
engine = db_conn.init_db_engine()
connection = engine.connect()

# Reading data from table called legacy_users into pandas df
user_data_df = db_extract.read_rds_tables(engine,'legacy_users')
user_data_df.set_index('index', inplace=True)

# Cleaning user data
user_clean_df = db_cleaning.clean_user_data(user_data_df)
user_clean_df.head(20)

# Uploading clean data back to SQL database
db_conn.upload_to_db(user_clean_df,'dim_users')

# %%
'''2. CLEANING AND UPLOADING CARD DATA FROM PDF ON AWS'''

# Reading card data from PDF
url = "card_details.pdf"
card_df = db_extract.retrieve_pdf_data(url)

# Cleaning card data
card_clean_df = db_cleaning.clean_card_data(card_df)

'''
??? Should you store date as date object or datetime object here? Date object just shows as object 
'''

# Uploading data
db_conn.upload_to_db(card_clean_df,'dim_card_details')


# %%
'''3. CLEANING AND UPLOADING STORE DATA VIA APIs'''

# Reading store data through APIs
with open('api_creds.yaml','r') as creds:
    api_creds = yaml.safe_load(creds)
endpoint = api_creds['NUMBER_STORES_EP']
headers = {'x-api-key': api_creds['API_KEY']}
number_stores = db_extract.list_number_of_stores(endpoint, headers)

retrieve_endpoint = api_creds['RETRIEVE_STORE_EP']
store_df = db_extract.retrieve_store_info(retrieve_endpoint, headers, number_stores)

# Cleaning store data
store_clean_df = db_cleaning.clean_store_data(store_df)

# Uploading data
db_conn.upload_to_db(store_clean_df,'dim_store_details')



# %%
'''4. CLEANING AND UPLOADING PRODUCT DETAILS DATA FROM AWS S3 BUCKET'''

# Reading product details data through S3 Boto
with open('s3_creds.yaml','r') as creds:
    s3_creds = yaml.safe_load(creds)
bucket = s3_creds['BUCKET']
file = s3_creds['FILE1']
local_path = '/Users/keshavparthasarathy/Documents/AICore_projects/multinational-retail-data-centralisation/products.csv'

df = db_extract.extract_from_s3(bucket,file,local_path)

# Cleaning product details data
product_df = db_cleaning.convert_product_weights(df)
product_clean_df = db_cleaning.clean_products_data(product_df)

# Uploading data
db_conn.upload_to_db(product_clean_df,'dim_products')

# %%
'''5. CLEANING AND UPLOADING PRODUCT ORDERS DATA FROM DATABASE'''

# Starting engine
engine = db_conn.init_db_engine()
connection = engine.connect()
tables = db_extract.list_db_tables(engine)
print(tables)

# Reading data from table called legacy_users into pandas df
orders_df = db_extract.read_rds_tables(engine,'orders_table')

# Cleaning data
orders_clean_df = db_cleaning.clean_orders_data(orders_df)

# Uploading data
db_conn.upload_to_db(orders_clean_df,'orders_table')

# %%
'''6. CLEANING AND UPLOADING SALES TIMES DATA FROM S3'''

# Reading sales details data through S3 Boto
with open('s3_creds.yaml','r') as creds:
    s3_creds = yaml.safe_load(creds)
bucket = s3_creds['BUCKET']
file = s3_creds['FILE2']
local_path = '/Users/keshavparthasarathy/Documents/AICore_projects/multinational-retail-data-centralisation/products.csv'

df = db_extract.extract_from_s3_json(bucket,file,local_path)

# Cleaning data
orderdates_clean_df = db_cleaning.clean_order_dates(df)

# Uploading data
db_conn.upload_to_db(orderdates_clean_df,'dim_date_times')

'''NEXT STEPS
1. Add docstrings
2. Clean code
3. Add to github

'''