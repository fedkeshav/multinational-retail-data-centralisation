import sqlalchemy as db
from sqlalchemy.engine import Connection
import yaml
import pandas as pd


class DatabaseConnector():
    '''
    This is a class created primarily to define methods to connect with and upload to the database
    '''
    def __init__(self):
        pass

    def read_db_creds(self) -> dict:
        '''
        Reads and returns database credentials

        Inputs:
            None

        Returns:
            Database credentials as a dictionary
        '''
        with open('db_creds.yaml','r') as creds:
            data_creds = yaml.safe_load(creds)
        return data_creds
    
    def init_db_engine(self) -> Connection:
        '''
        Starts the engine to the database

        Inputs:
            None

        Returns:
            Engine to the database
        '''
        creds_dict = self.read_db_creds()
        creds = list(creds_dict.values())
        url = (f"{creds[0]}+{creds[1]}://{creds[2]}:{creds[3]}@{creds[4]}:{creds[5]}/{creds[6]}")
        engine = db.create_engine(url)
        return engine
    
    def upload_to_db(self, df: pd.DataFrame, table_name: str):
        '''
        Uploads dataframe as a table to database

        Inputs:
            Dataframe to be uploaded along with desired table name

        Returns:
            Nothing
        '''
        with open('db_creds.yaml','r') as creds:
            data_creds = yaml.safe_load(creds)
        creds_list = data_creds.values()
        url = (f"{creds_list[0]}+{creds_list[1]}://{creds_list[2]}:{creds_list[3]}@{creds_list[4]}:{creds_list[5]}/{creds_list[6]}")
        engine = db.create_engine(url)
        engine.connect()
        df.to_sql(table_name, engine, if_exists='replace')