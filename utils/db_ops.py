import os, json, time, datetime, pytz
import pandas as pd
import pyodbc
import sqlalchemy


class DBOps(object):

    CREDENTIAL_FILE = os.path.join(os.path.join('configs', 'db_configs'), 
                                    'credentials.json')
    ENV_CONFIG = os.path.join(os.path.join('configs' , 'env_config' ,'coding_environment.json'))
     

    def __init__(self):
        self.env_set_db = self.read_json(DBOps.ENV_CONFIG)['db_env']
        self.credentials_di = self.read_json(DBOps.CREDENTIAL_FILE)[self.env_set_db]
        
        

    def read_json(self, filepath):
        if not os.path.exists(filepath):
            raise Exception(f"File is Missing at {filepath}")
        with open(filepath, "r") as rj:
            di = json.load(rj)
        return di
    
    def create_engine(self):
        connection_uri = sqlalchemy.engine.URL.create(
                        "mssql",
                        username="sa",
                        password="Office@23",
                        host="CHEIL-D-XMMC",
                        database="LMS_Test",
                        query={"driver": "SQL Server"},
                        )
        print(connection_uri)
        engine = sqlalchemy.create_engine(connection_uri)

        #engine = sqlalchemy.create_engine(f"mssql://sa:Office@23:@CHEIL-D-XMMC:/LMS_Test?DRIVER=SQL Server")
        return engine

    def create_connection(self):
        connection = pyodbc.connect(
                    driver = self.credentials_di["driver"], 
                    server = self.credentials_di["server"], 
                    database = self.credentials_di["database"],               
                    UID = self.credentials_di["username"],
                    PWD = self.credentials_di["password"]
                    )
        return connection


    def execute_dataframe_query(self, connection, query):
        #connection = self.create_connection()
        df = pd.read_sql(query, connection)
        return df

    def execute_query(self, connection, query):
        flag = 0
        try:
            #connection = self.create_connection()
            connection.cursor().execute(query)
            connection.commit()
            flag = 1
        except Exception as e:
            raise Exception()
        return flag
    

    def ingest_dataframe_to_sql(self, connection, table_name, df):
        try:
            cols = str(tuple(df.columns)).replace("'", "") 
            vals = str(tuple([ "?" for col in df.columns ])).replace("'", "")
            query = f"INSERT INTO {table_name} {cols} VALUES {vals}"  
            #import pdb;pdb.set_trace()
            #print(df.shape)
            #df.to_sql(table_name, engine, if_exists='append')
            #result = df.to_dict('records')   
            connection.cursor().executemany(query, df.itertuples(index=False))    
            connection.commit()
        except Exception as e:
            raise Exception(f"Error in ingest {e}")
        

    def update_table(self, connection, query, values):
        try:
            flag = False
            connection.cursor().executemany(query, values)
            connection.commit()
            flag = True
        except Exception as e:
            raise Exception(f"Error in update table {e}")
        return flag
        
    



    


if __name__ == "__main__":
    obj = DBOps()
    