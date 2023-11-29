import boto3
import pandas as pd
import requests
import sqlalchemy as db
from sqlalchemy.engine import Connection
import tabula


class DataExtractor():
    ''' 
    This is a class created primarily to define methods to extract data from datatables, S3 buckets etc 
    '''    
    def __init__(self):
        pass
    
    def list_db_tables(self,engine: Connection) -> dict:
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

    def read_rds_tables(self,engine: Connection,table_name: str) -> pd.DataFrame:
        '''
        Reads table from database into a dataframe

        Inputs:
            Engine connected to database and the table name to read

        Returns:
            A dataframe
        '''
        df = pd.read_sql_table(table_name, engine)
        return df
    
    def retrieve_pdf_data(self, url: str) -> pd.DataFrame:
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
    
    def list_number_of_stores(self, endpoint: str, headers: str) -> int:
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
    
    def retrieve_store_info(self, endpoint: str, headers: str, stores: int) -> pd.DataFrame:
        '''
        Collects data on all stores and returns them as a consolidated dataframe

        Inputs:
            Endpoint for the API, and headers for the API and the number of stores

        Returns:
            A consolidated dataframe with details of the stores
        '''
        dataframes = []
        for i in range(0,stores):
            url = endpoint + str(i) #Retrives url for each store number
            response = requests.get(url, headers = headers) #Sends get request to each url
            data = response.json() #Stores the response as json
            df = pd.DataFrame([data]) #Converts the json data into dataframe
            dataframes.append(df) #Appends each store data to master dataframe list
            result_df = pd.concat(dataframes,ignore_index=True) #Concats the list to the aggregate dataframe
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

    def extract_from_s3_json(self,bucket: str, file: str, local_storage: str) -> pd.DataFrame:
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