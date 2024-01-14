import pandas as pd
import regex as re
import requests ,pytz,datetime,time, json,pyodbc
import os
from utils.db_ops import DBOps

class LogOps(object):
    CREDENTIAL_FILE = os.path.join(os.path.join('configs', 'db_configs'), 
                                    'credentials.json')

    def __init__(self):
        self.IST = pytz.timezone('Asia/Kolkata')
        self.db_ops = DBOps()
        self.connection = self.db_ops.create_connection()
        self.table_name = "Logs" 
        self.save_logs_lead_management = self.read_json(DBOps.ENV_CONFIG)['SaveLogsToLeadManagement']
        #if self.save_logs_lead_management == "YES":
        self.credentials_leadmanagement_di = self.read_json(LogOps.CREDENTIAL_FILE)['LeadManagementCheilServer']
        if self.save_logs_lead_management == "YES":
            try:
                self.leadmanagement_connection = self.create_connection()
            except Exception as e:
                log_data = [{"LogDetails" :f'Error in Connecting to LeadManagement DB {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T7", "Status" : "ERROR"}]
                self.save_logs(log_data)
                #raise(f"Error in Connecting to LeadManagement {e}")
                pass
        
    def read_json(self, filepath):
        if not os.path.exists(filepath):
            raise Exception(f"File is Missing at {filepath}")
        with open(filepath, "r") as rj:
            di = json.load(rj)
        return di
    
    def create_connection(self):
        connection = pyodbc.connect(
                    driver = self.credentials_leadmanagement_di["driver"], 
                    server = self.credentials_leadmanagement_di["server"], 
                    database = self.credentials_leadmanagement_di["database"],               
                    UID = self.credentials_leadmanagement_di["username"],
                    PWD = self.credentials_leadmanagement_di["password"]
                    )
        return connection


    def save_logs(self, error_data):
        try:
            df = pd.DataFrame(error_data)
           # df['ScriptName'] = df['ScriptName'].apply(lambda x : x.split('samsung-core',2)[-1])
            
            try:
                df['ScriptName'] = df['ScriptName'].apply(lambda x : x.split('\\')[-1])
                df['LogDetails'] = df['LogDetails'].apply(lambda x : re.sub('[\S]+(\.com|\.in|.php)','Apidomain',x)  )
                df['LogDetails'] = df['LogDetails'].apply(lambda x :  re.sub(r'\bport=443\b','',x) )
            
            except Exception as e:
                pass
            
            try:
                self.db_ops.ingest_dataframe_to_sql(self.connection, self.table_name, df)
            except Exception as e:
                pass
            if self.save_logs_lead_management == 'YES':
                try:
                    self.db_ops.ingest_dataframe_to_sql(self.leadmanagement_connection,self.table_name,df)
                except Exception as e:
                    raise(f"Error in Ingesting Logs to LeadManagement db {str(e)}")
        except Exception as e:
            print(f"Error in {e}")
       
        # finally:
        #     self.connection.close()

if __name__ == "__main__":
    obj = ErrorLogging()
    data = [{"ErrorString" : "Test", "ScriptName" : "test_script.py", "ModuleName" : "Testing", "SeverityLevel" : 1, "Stage" : "T1"}]
    obj.save_error(data)




#    re.sub('[\S]+(\.com|\.in|\.php)','Apidomain.com' , str)
#    re.sub(r'\bport=443\b','' , str) 