
import sqlalchemy as db
import database_utils
import pandas as pd
import tabula
import requests
import yaml
import boto3

class DataExtractor():
    ''' 
    This is a class created primarily to define methods to extract data from datatables, S3 buckets etc 
    '''    
    
    def list_db_tables(self,engine):
        '''
        Shows list of table names stored in database

        Inputs:
            Engine connected to database

        Returns:
            Dictionary with list of tables in database        
        '''
        inspector = db.inspect(engine)
        x = inspector.get_table_names()
        return x


    def read_rds_tables(self,engine,table_name):
        '''
        Reads table from database into a dataframe

        Inputs:
            Engine connected to database and the table name to read

        Returns:
            A dataframe
        '''

        df = pd.read_sql_table(table_name, engine)
        return df
    
    def retrieve_pdf_data(self, url):
        '''
        Converts data from pdf into a dataframe

        Inputs:
            URL of the pdf data

        Returns:
            A dataframe
        '''

        table = tabula.read_pdf(url, stream=False, pages='all')
        dataframes = []
        for i in range(0,len(table)):
            df = table[i]
            dataframes.append(df)
            result_df = pd.concat(dataframes, ignore_index=True)
        return result_df
    
    def list_number_of_stores(self, endpoint, headers):
        '''
        Identifies the number of stores for which there is data through API

        Inputs:
            Endpoint for the API, and headers for the API

        Returns:
            An int for the number of stores
        '''
        response = requests.get(endpoint, headers = headers)
        data = response.json()
        stores = data['number_stores']
        return stores
    
    def retrieve_store_info(self, endpoint, headers, stores):
        '''
        Collects data on all stores and returns them as a consolidated dataframe

        Inputs:
            Endpoint for the API, and headers for the API and the number of stores

        Returns:
            A consolidated dataframe with details of the stores
        '''
        dataframes = []
        for i in range(1,stores+1):
            url = endpoint + str(i)
            response = requests.get(url, headers = headers)
            data = response.json()
            df = pd.DataFrame([data])
            dataframes.append(df)
            result_df = pd.concat(dataframes,ignore_index=True)
        return result_df
    
    def extract_from_s3(self,bucket, file, local_storage):
        '''
        Reads and converts data from S3 bucket into a dataframe

        Inputs:
            Bucket and file name for S3 item, as well as path for locally storing the file

        Returns:
            A dataframe
        '''
        s3 = boto3.client('s3')
        s3.download_file(bucket,file, local_storage)
        df = pd.read_csv(local_storage)
        return df

    def extract_from_s3_json(self,bucket, file, local_storage):
        '''
        Reads and converts data from JSON in S3 bucket into a dataframe

        Inputs:
            Bucket and file name for S3 item, as well as path for locally storing the file

        Returns:
            A dataframe
        '''
        s3 = boto3.client('s3')
        s3.download_file(bucket,file, local_storage)
        df = pd.read_json(local_storage)
        return df


