import pandas as pd
import numpy as np
import requests ,pytz,datetime,time, json,pyodbc
import os
from utils.db_ops import DBOps


#update dasbpard onm cheil server

class DasboardTableCheil(object):
    CREDENTIAL_FILE = os.path.join(os.path.join('configs', 'db_configs'), 
                                    'credentials.json')
    
    DASHBOARD_TABLE = 'dbo.DashboardTable'
    def __init__(self):
        self.IST = pytz.timezone('Asia/Kolkata')
        self.db_ops = DBOps()
        self.connection = self.db_ops.create_connection()
        self.table_name = "DashboardTable"  
        self.credentials_leadmanagement_di = self.read_json(DasboardTableCheil.CREDENTIAL_FILE)['LeadManagementCheilServer']
        self.leadmanagement_connection = self.create_connection()
    def create_connection(self):
        connection = pyodbc.connect(
                    driver = self.credentials_leadmanagement_di["driver"], 
                    server = self.credentials_leadmanagement_di["server"], 
                    database = self.credentials_leadmanagement_di["database"],               
                    UID = self.credentials_leadmanagement_di["username"],
                    PWD = self.credentials_leadmanagement_di["password"]
                    )
        return connection
    
    def read_json(self, filepath):
        if not os.path.exists(filepath):
            raise Exception(f"File is Missing at {filepath}")
        with open(filepath, "r") as rj:
            di = json.load(rj)
        return di
    


    def run(self):
        import pdb ;pdb.set_trace()
        tuncate_query = f'TRUNCATE TABLE {DasboardTableCheil.DASHBOARD_TABLE}'
        flag = self.db_ops.execute_query(self.leadmanagement_connection ,tuncate_query)

        query_fetch = f'SELECT * FROM {DasboardTableCheil.DASHBOARD_TABLE}'
        df = self.db_ops.execute_dataframe_query(self.connection ,query_fetch)
        df['LeadCollectionDateTime']  = pd.to_datetime(df['LeadCollectionDateTime'],format='%Y/%m/%d %H:%M:%S %z' , errors='coerce').dt.tz_localize(None).replace({np.NaN: '1990/12/19 00:00:00'}) 
        df['LeadAllocationDateTime']  = pd.to_datetime(df['LeadAllocationDateTime'],format='%Y/%m/%d %H:%M:%S %z' , errors='coerce').dt.tz_localize(None).replace({np.NaN: '1990/12/19 00:00:00'}) 
        df['LeadFirstContactDateTime']  = pd.to_datetime(df['LeadFirstContactDateTime'],format='%Y/%m/%d %H:%M:%S %z' , errors='coerce').dt.tz_localize(None).replace({np.NaN: '1990/12/19 00:00:00'}) 
        df['UUID'] = df['UUID'].astype(str)
        df['MaxCapacity'] = df['MaxCapacity'].astype(str)

        split_count = df.shape[0]//5000
        df_split  = np.array_split(df,split_count)
        for i in range(0,split_count):
            df = df_split[i]
            self.db_ops.ingest_dataframe_to_sql(self.leadmanagement_connection ,"DashboardTable",df)



if __name__ == '__main__':
    obj = DasboardTableCheil()
    obj.run()
