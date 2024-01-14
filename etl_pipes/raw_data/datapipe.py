import datetime,pytz,os,json,pyodbc
import pandas as pd
import numpy as np
from utils.etl_ops import ETLOps 
from utils.db_ops import DBOps
from utils.log_ops import LogOps

""" ADD RefSourceId COLUMN TO RawLeads TABLE"""


class PullToRawLeads(object):
    SAMSUNG_SRC_TABLE = '[Promotions].[dbo].[LMSUserRegistration]'
    DELTAX_SRC_TABLE = '[dbo].[LMSUserRegistration]'
    DEST_TABLE = '[dbo].[RawLeads]'
    CREDENTIAL_FILE = os.path.join(os.path.join('configs', 'db_configs'), 
                                    'credentials.json')
    ENV_CONFIG_FILE = os.path.join(os.path.join('configs' , 'env_config' ,'coding_environment.json'))
    


    def __init__(self):
        self.db_env = self.read_json(DBOps.ENV_CONFIG)['db_env']
        self.IST = pytz.timezone('Asia/Kolkata')
        self.etl_ops = ETLOps()
        self.log_ops = LogOps()
        self.db_ops = DBOps()
        self.connection = self.db_ops.create_connection()
        self.credential_Promotions_db = self.read_json(PullToRawLeads.CREDENTIAL_FILE)[ self.db_env]   #to fetch Samsung_com leads from another db(Promotions) on same server
        self.credential_Promotions_db['database'] = 'Promotions'
        self.connection_promotions_db = self.db_ops.create_connection()
        pass


    def create_connection(self):
        connection = pyodbc.connect(
        driver = self.credential_promotions_db["driver"], 
        server = self.credential_promotions_db["server"], 
        database = self.credential_promotions_db["database"],               
        UID = self.credential_promotions_db["username"],
        PWD = self.credential_promotions_db["password"]
        )
        return connection

    

    def read_json(self, filepath):
        if not os.path.exists(filepath):
            raise Exception(f"File is Missing at {filepath}")
        with open(filepath, "r") as rj:
            di = json.load(rj)
        return di


    def fetch_samsung_com(self):
        query = f"SELECT UserID as RefSourceId, AppName as Campaign, LeadId as LeadID , InsertedOn as LeadCreationTimestamp, Name as FullName,EmailId as Email, \
                    Mobile as PhoneNumber,CityName as City, PinCode , Consent, DMSCode as SelectedStore, Requirement as ShopperCategorySelected \
                    FROM {PullToRawLeads.SAMSUNG_SRC_TABLE} WITH (NOLOCK) \
                    WHERE RefSourceId > (SELECT MAX(RefSourceId) FROM {PullToRawLeads.DEST_TABLE}) WHERE Platform = 'SAMSUNG' "
        
        df = self.db_ops.execute_dataframe_query(self.connection_promotions_db ,query)

        return df
    

    def fetch_webhook_table(self):
        query = f"SELECT UserID as RefSourceId, AppName as Campaign, LeadId as LeadID , InsertedOn as LeadCreationTimestamp, Name as FullName,EmailId as Email, \
                    Mobile as PhoneNumber,CityName as City, PinCode , Consent, DMSCode as SelectedStore, Requirement as ShopperCategorySelected \
                    FROM {PullToRawLeads.DELTAX_SRC_TABLE} WITH (NOLOCK)\
                    WHERE RefSourceId > (SELECT MAX(RefSourceId) FROM {PullToRawLeads.DEST_TABLE}) WHERE Platform != 'SAMSUNG' "
        
        df = self.db_ops.execute_dataframe_query(self.connection_promotions_db ,query)

        return df





    def run(self):
        pass

