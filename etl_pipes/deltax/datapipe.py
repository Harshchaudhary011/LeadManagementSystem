import os, json, boto3, time, datetime, pytz
import pandas as pd
import numpy as np
from utils.etl_ops import ETLOps 
from utils.db_ops import DBOps
from utils.log_ops import LogOps

class PullData(object):
    '''
        TO DO : 
        1. Add data ingestion logic - DONE
        2. Add dedupe logic     - DONE
        3. Add logic to get max date from db - DONE
        4. Populate campaign fields - NR
        5. Change select max date logic - PARTIAL DONE 
        6. Add data cleaning and validation logic - PARTIAL DONE
        7. Add lead quality flag - PARTIAL DONE
        8. Add consent flag - PARTIAL DONE 
        9. fill na by empty string - DONE
        10. min date filter  - DONE
        11. Add cleaning logic on joining fields - dedupe func - DONE
        
    '''
    AWS_ACCESS_KEY_ID = 'AKIA5UMB7FLMPJBWUT4X'
    AWS_SECRET_ACCESS_KEY ='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'
    BUCKET_NAME = 'deltax' 
    S3FILE_PATH = 'BPs/2403/datasets/1/Q171-a3386b16-274e-45f8-90f0-210e7a5b5ee6.csv'
    DELTAX_ALLLEADS_MAPPER_CONF = os.path.join(os.path.join('configs', 'etl_configs'), 
                                    'column_name_mappings.json') 
    ALL_LEADS_DATATYPE_MAPPER_CONF = os.path.join(os.path.join('configs', 'db_configs'), 
                                'all_leads_datatype_mapping.json')
    ADD_ON_COLS = ['Division','Product','Category','ShopperCategorySelected','ShopperChannelPreference','SelectedStore']
    
    

    def __init__(self):
        self.s3_client = boto3.client(
            service_name = 's3',
            aws_access_key_id = PullData.AWS_ACCESS_KEY_ID,
            aws_secret_access_key = PullData.AWS_SECRET_ACCESS_KEY
            )
        self.IST = pytz.timezone('Asia/Kolkata')
        self.timestr = datetime.datetime.now(self.IST).strftime('%Y-%m-%d_%H_%M_%S')
        self.tmp_dir = "./etl_pipes/temp_data"
        self.tmp_file_name = os.path.join(self.tmp_dir, 
           f'deltax_data_file_{self.timestr}.csv')
        self.etl_ops = ETLOps()
        self.log_ops = LogOps()
        
        self.db_ops = DBOps()
        self.connection = self.db_ops.create_connection()

    def delete_files(self, folderpath, prefix):
        files_to_delete = [ filename
                            for filename in os.listdir(folderpath) 
                            if filename.startswith(prefix)
                            ]
        for filename in files_to_delete:
            os.remove(os.path.join(folderpath, filename))
        


    def download_file(self):
        try:
            # print(r)
            self.s3_client.download_file(
                Filename=self.tmp_file_name,
                Bucket=PullData.BUCKET_NAME, 
                Key=PullData.S3FILE_PATH
                )
        except Exception as e:
            log_data = [{"LogDetails" : f"Error in download file : {str(e)}", 
                        "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                        "SeverityLevel" : 4, "Stage" : "T1", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f"Error in download file {e}")

    def read_dataframe(self, filename):
        df = pd.read_csv(filename)
        return df 

    def filter_records(self, df, min_date_str):
        df = df[pd.to_datetime(df["LeadCreationTimestamp"]) > min_date_str]
        return df

    def read_json(self, filepath):
        if not os.path.exists(filepath):
            log_data = [{"LogDetails" : f"{filepath} does not exists..", 
            "ScriptName" : str(__file__), 
            "ModuleName" : str(self.__class__.__name__), 
            "SeverityLevel" : 4, "Stage" : "T1", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f"File is Missing at {filepath}")
        with open(filepath, "r") as rj:
            di = json.load(rj)
        return di


    def rename_columns(self, df):
        di = self.read_json(PullData.DELTAX_ALLLEADS_MAPPER_CONF)
        df.rename(columns = di, inplace = True)
        df = df[[di[key] for key in di]] 
        return df

    def validate_excel(self, df):
        di = self.read_json(PullData.DELTAX_ALLLEADS_MAPPER_CONF)
        missing_columns = [key for key in di if key not in df.columns]
        if missing_columns:
            log_data = [{"LogDetails" : f"Invalid Excel... Missing columns {str(missing_columns)}", 
            "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
            "SeverityLevel" : 4, "Stage" : "T1", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f"Invalid Excel... Missing columns {missing_columns}") 

    def select_db_maxdate(self):
        max_date = ""
        query = "SELECT MAX(LeadCreationTimestamp) AS MAX_DATE FROM dbo.RawLeads  WITH (NOLOCK) WHERE Platform != 'SAMSUNG'"
        df = self.db_ops.execute_dataframe_query(self.connection, query)
        if df["MAX_DATE"][0]:
            max_date = df["MAX_DATE"][0]
        return max_date
    
    def calculate_consent(self, x):
        consent = ""
        if x is True:
            consent = "YES"
        elif x is False:
            consent = "NO"
        else:
            consent = "NA"
        return consent 
                  

    def run(self):
        try:
            if not os.path.exists(self.tmp_dir):
                os.mkdir(self.tmp_dir)
            
            self.delete_files(self.tmp_dir, 'deltax_data_file_')
            self.download_file()
            
            if not os.path.exists(self.tmp_file_name):
                log_data = [{"LogDetails" : f"File Does not exists at {self.tmp_file_name}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T1", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                raise Exception(f"File Does not exists at {self.tmp_file_name}")
            df = self.read_dataframe(self.tmp_file_name)

            if df.empty:
                print("No Leads found in the file")

            df["Consent"] = df["yes"].apply(lambda x : self.calculate_consent(x))
            
            self.validate_excel(df)
            
            df = self.rename_columns(df)
            df["LeadCreationTimestamp"] = pd.to_datetime(df["LeadCreationTimestamp"],format='%d-%m-%Y %H:%M',errors='coerce').dt.tz_localize(None).replace({np.NaN: '1990/12/19 00:00:00'})   #format='%d-%m-%Y %H:%M'
            df["LeadCreationTimestamp"] = df["LeadCreationTimestamp"] + datetime.timedelta(hours=5, minutes=30)
            print(f"Total Rows : {df.shape}")

            min_date_str = self.select_db_maxdate()
            if min_date_str:
               df = self.filter_records(df, min_date_str)
               
            
            for col in df.columns:
                if col == "LeadCreationTimestamp":
                    continue
                df[col] = df[col].astype('str')
                df[col] = df[col].fillna("NA")
            if df.empty:
                print(f'No new leads after {str(min_date_str)}')
                log_data = [{"LogDetails" : f"No new leads after {str(min_date_str)}", 
                "ScriptName" : str(__file__), 
                "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 1, "Stage" : "T1", "Status" : "INFO"}]
                self.log_ops.save_logs(log_data)
                raise Exception(f"No new leads after {min_date_str}")
            else:
                df[PullData.ADD_ON_COLS] = 'NA'
                df['IngestionTimestamp'] = datetime.datetime.now(self.IST).strftime("%Y-%m-%d %H:%M:%S")
                

                self.db_ops.ingest_dataframe_to_sql(self.connection, "RawLeads", df)
                print('Successfully Ingested to RawLeads')
            log_data = [{"LogDetails" : f"Successfully ingested {df.shape[0]} deltax leads", 
                "ScriptName" : str(__file__), 
                "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T1", "Status" : "SUCCESS"}]
            self.log_ops.save_logs(log_data)
        except Exception as e:
            log_data = [{"LogDetails" : f"Error in deltax pipe {str(e)}", 
            "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
            "SeverityLevel" : 5, "Stage" : "T1", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f"Error in deltax pipe {e}")
        finally:
            self.connection.close()


if __name__ == "__main__":
    obj = PullData()
    obj.run()


