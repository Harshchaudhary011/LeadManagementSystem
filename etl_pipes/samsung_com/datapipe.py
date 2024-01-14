import requests , base64 , hashlib, os, json, datetime, pytz, pyDes, chardet,eventlet
import pandas as pd
import subprocess
from utils.etl_ops import ETLOps 
from utils.db_ops import DBOps
from utils.log_ops import LogOps

'''
TODO:
1. MAKE DYNAMIC START DATE & END DATE
2. FETCH LAST LeadID INGESTED FROM DB FOR SAMSUNG
3. add on fields are mentioned in self


'''

class PullData(object):

    SAMSUNG_ALLLEADS_MAPPER_CONF = os.path.join(os.path.join('configs', 'etl_configs'), 
                                    'samsung_columns_name_mapping.json')
    ENV_CONFIG = os.path.join(os.path.join('configs' , 'env_config' ,'coding_environment.json'))
     
    API_DI = {"UAT" : "https://smartdost.samsungmarketing.in" ,"TEST" : "http://test01.samsungindiamarketing.com" ,"PROD" :"https://www.samsungindiamarketing.com"}
    API_HOST_DI = {"UAT" : "www.smartdost.samsungmarketing.in" ,"TEST" : "www.test01.samsungindiamarketing.com" ,"PROD" :"www.samsungindiamarketing.com"}
    UAT = 0
    API_USERNAME = 'samsungwebapi'
    API_PASSWORD = '4436612f0e11c5f0be7b70e6efe8d13c41af183f31435819c058edddc2b806d5'
    BATCH_SIZE = 500
    COLS_TO_DECRYPT = ["Email", "PhoneNumber"] 
    KEY = "!*&@9876543210"
    IV = [18, 52, 86, 120, 144, 171, 205, 239]
    TIME_OUT = 15


    def __init__(self):  
                
        self.fields_add_on_dict = {
            'Platform':'SAMSUNG', 'Division':'NA',
            'Product':'NA','Category' :'NA','ShopperChannelPreference' :'NA'
            }
        self.db_ops = DBOps()
        self.etl_ops = ETLOps()
        self.log_ops = LogOps()
        self.connection = self.db_ops.create_connection()
        self.IST = pytz.timezone('Asia/Kolkata')
        self.env_set = self.read_json(PullData.ENV_CONFIG)['code_env']
    
    
    
    
    def get_lead_id_hold(self, start_date, end_date, cnt):
        try:
            resp = {}
            cnt += 1
            #import pdb;pdb.set_trace()
            # get_lead_min_max_id_api_TEST = f"http://test01.samsungindiamarketing.com/LMSWebAPI/Api/LMSLeadCountMinAndMaxId?StartDate={start_date}&EndDate={end_date}"
            # get_lead_min_max_id_api_PROD = f"https://www.samsungindiamarketing.com/LMSWebAPI/Api/LMSLeadCountMinAndMaxId?StartDate={start_date}&EndDate={end_date}"
            #import pdb;pdb.set_trace()
            get_lead_min_max_id_api =  PullData.API_DI[self.env_set] + f"/LMSWebAPI/Api/LMSLeadCountMinAndMaxId?StartDate={start_date}&EndDate={end_date}"
            payload={}
            headers_di = {
                'Authorization': 'Basic c2Ftc3VuZ3dlYmFwaTo0NDM2NjEyZjBlMTFjNWYwYmU3YjcwZTZlZmU4ZDEzYzQxYWYxODNmMzE0MzU4MTljMDU4ZWRkZGMyYjgwNmQ1',  
                'Host': PullData.API_HOST_DI[self.env_set],#'www.samsungindiamarketing.com',
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/46.0.2490.80',
                }
            try:
                with eventlet.Timeout(PullData.TIME_OUT) :
                    resp = requests.get(get_lead_min_max_id_api,
                                headers = headers_di
                                )

            except eventlet.Timeout as e:
                log_data = [{"LogDetails" : f'Samsung.com TIMEOUT Error {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T1", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                raise TimeoutError('Samsung.com Timeout Error')
            if resp.status_code != 200:
                log_data = [{"LogDetails" : f"Error in lead id min max api response status code : {str(resp.status_code)}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T1", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                raise Exception(f"Api response other than 200 {resp.status_code} retry number {cnt}")
            
        except Exception as e:
            #import pdb;pdb.set_trace()
            print(f"Error in GetLeadID API {e}")
            if cnt < 3:
                resp = self.get_lead_id_hold(start_date, end_date, cnt)
            else:
                log_data = [{"LogDetails" : f"last attempt of api hit Error in lead id min max api response status code...{str(resp.json())}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T1", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
        return resp



    def parse_lead_id(self, resp):
        try:
            resp_json_dict = (resp.json())[0]
            lead_count = int(resp_json_dict['Count']) if resp_json_dict['Count'] else ""
            lead_min_id = int(resp_json_dict['MinId']) if resp_json_dict['MinId'] else ""
            lead_max_id = int(resp_json_dict['MaxId']) if resp_json_dict['MaxId'] else ""
        except Exception as e:
            log_data = [{"LogDetails" : f"Error in lead parsing from Get LeadId API {str(e)}", 
            "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
            "SeverityLevel" : 4, "Stage" : "T1", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f"Error in Parsing LeadId from GetLeadId API {e}")
        return lead_count, lead_min_id, lead_max_id

    

    def get_lead_details_hold(self, min_id, max_id, cnt):
        #import pdb;pdb.set_trace()
        try:
            cnt += 1
            lead_results = []
            resp = {}
            buckets = list(range(min_id, max_id+PullData.BATCH_SIZE, PullData.BATCH_SIZE))
            buckets = [buckets[i:i+2] for i,e in enumerate(buckets) if len(buckets[i:i+2]) == 2]
            headers_di = {
                'Authorization': 'Basic c2Ftc3VuZ3dlYmFwaTo0NDM2NjEyZjBlMTFjNWYwYmU3YjcwZTZlZmU4ZDEzYzQxYWYxODNmMzE0MzU4MTljMDU4ZWRkZGMyYjgwNmQ1',  
                'Host': PullData.API_HOST_DI[self.env_set] ,#'www.samsungindiamarketing.com',
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/46.0.2490.80',
                } 
            for buck in buckets:
                
                get_lead_details_api = PullData.API_DI[self.env_set] + f"/LMSWebAPI/Api/LMSLeadDetailsById?MinId={buck[0]}&MaxId={buck[1]}"
                try:
                    try:
                        with eventlet.Timeout(PullData.TIME_OUT) :
                            resp = requests.get(get_lead_details_api,
                                headers = headers_di
                                )
                    except eventlet.TimeoutError as e:
                        log_data = [{"LogDetails" : f'Samsung.com TIMEOUT Error {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T1", "Status" : "ERROR"}]
                        self.log_ops.save_logs(log_data)
                        raise TimeoutError('Samsung.com Timeout Error')

                    if resp.status_code == 200:
                        lead_results.extend(resp.json())
                    else:
                        break
                except Exception as e:
                    break                
        except Exception as e:
            print(f"Error in First batch of GetLeadDetails API {e}")
            if cnt < 3:
                lead_results = self.get_lead_details_hold(min_id, max_id, cnt)
            else:
                log_data = [{"LogDetails" : f"Error in Get lead details api {str(resp)} Error {str(e)} Attempt number {str(cnt)}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T1", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
        return lead_results



    def read_json(self, filepath):
        if not os.path.exists(filepath):
            log_data = [{"LogDetails" : f"File is Missing at {filepath}", 
            "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
            "SeverityLevel" : 4, "Stage" : "T1", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f"File is Missing at {filepath}")
            
        with open(filepath, "r") as rj:
            di = json.load(rj)
        return di


    def rename_columns(self,df):
        di = self.read_json(PullData.SAMSUNG_ALLLEADS_MAPPER_CONF)
        df = df[di.keys()]
        df.rename(columns=di,inplace=True)
        return df

    def make_final_df(self, df):
        
        for col in self.fields_add_on_dict:
            df[col] = self.fields_add_on_dict[col]   
        return df
    

    def fetch_db_max_id(self):
        '''
        fetch max Lead_Id query
        '''
        try:
            max_id = 0
            min_date = (datetime.datetime.now(self.IST) - datetime.timedelta(days = 2)).strftime("%Y-%m-%d")
            #query = f"SELECT MAX(LeadID) AS MAX_ID FROM dbo.RawLeads WITH (NOLOCK) WHERE Platform ='SAMSUNG'  and LeadCreationTimestamp > '{min_date}'"
            query = f"EXEC dbo.spEtlSasmsungFetchDbMaxId @pMinDate = '{min_date}'"
            df = self.db_ops.execute_dataframe_query(self.connection, query)
            if df["MAX_ID"][0]:
                max_id = int(df["MAX_ID"][0])
            else:
                max_id = 0
        except Exception as e:
            log_data = [{"LogDetails" : f"Error in finding max lead id {str(e)}", 
            "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
            "SeverityLevel" : 4, "Stage" : "T1", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f"Error in finding max lead id {e}")
        return max_id


    def decrypt_des(self, col, row):
        try:
            data = row[col]
            while (len(data) % 4 != 0):
                data += "="

            key = bytes(PullData.KEY[:8], 'utf-8')
            data_byte = bytes(data, 'utf-8')
            data_byte_array = base64.b64decode(data_byte)
            k = pyDes.des(key, pyDes.CBC, IV = bytes(PullData.IV))
            decrypted = k.decrypt(data_byte_array)
            pad_remove = decrypted[-1]
            decrypted = decrypted[:-pad_remove]
            decrypted = decrypted.decode('utf-8')
        except Exception as e:
            log_data = [{"LogDetails" : f"Error in decrypt des {str(e)} ", 
            "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
            "SeverityLevel" : 4, "Stage" : "T1", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f"Error in decrypt des {e} ")
        return decrypted 

    def decrypt_columns(self, leads_df):
        for col in PullData.COLS_TO_DECRYPT:
            leads_df[col] = leads_df.apply(lambda x : self.decrypt_des(col, x), axis=1)
        return leads_df


    def run(self):
        #import pdb;pdb.set_trace()
        try:

            yesterday_date = (datetime.datetime.now(self.IST) -  datetime.timedelta(1)).strftime("%Y%m%d") 
            today_date =  (datetime.datetime.now(self.IST)).strftime("%Y%m%d") 
            startdate = '20230426' if PullData.UAT == 1 else yesterday_date  
            enddate = today_date
            resp = self.get_lead_id_hold(startdate, enddate, 0)
            if not resp:
                log_data = [{"LogDetails" : f"Error in get lead api id blank resp {str(resp)}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T1", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                raise Exception(f"Error in get lead api id blank resp")
            lead_id_tup = self.parse_lead_id(resp)
            if not lead_id_tup[0]:
                log_data = [{"LogDetails" : f"No leads found for date range : {str(startdate)} - {str(enddate)} count : {str(lead_id_tup[0])}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 1, "Stage" : "T1", "Status" : "INFO"}]
                self.log_ops.save_logs(log_data)
                raise Exception(f"No leads found for date range : {startdate} - {enddate} count : {lead_id_tup[0]}")
            ingested_max_id = self.fetch_db_max_id()
            min_id = lead_id_tup[1] if lead_id_tup[1] > ingested_max_id else (ingested_max_id +1)
              
            leads_li = self.get_lead_details_hold(min_id, lead_id_tup[2], 0)
            if not leads_li:
                log_data = [{"LogDetails" : f"No leads found for min_id {str(min_id)}, max_id {str(lead_id_tup[2])} for the date : {str(today_date)}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 1, "Stage" : "T1", "Status" : "INFO"}]
                self.log_ops.save_logs(log_data)
                raise Exception(f"No leads found for min_id {min_id}, max_id {lead_id_tup[2]} for the date : {today_date}")
            leads_df = pd.DataFrame(leads_li)
            leads_df["Consent"] = leads_df["NeedReminderMail"].apply(lambda x : "YES" if x.upper().strip() == "Y" else "NO")
            leads_df = self.rename_columns(leads_df)

           

            leads_df = self.make_final_df(leads_df)
            leads_df = self.decrypt_columns(leads_df)

            for col in leads_df.columns:
                if col == "LeadCreationTimestamp":
                    leads_df[col] = pd.to_datetime(leads_df[col],format = '%m/%d/%Y %H:%M:%S %p')
                    continue
                leads_df[col] = leads_df[col].astype('str')
                leads_df[col] = leads_df[col].fillna("NA")
                leads_df[col] = leads_df[col].replace(r'^\s*$', 'NA', regex=True)
            leads_df['IngestionTimestamp'] = datetime.datetime.now(self.IST).strftime("%Y-%m-%d %H:%M:%S")
        
            print(f'fetched {len(leads_df)} leads for {startdate} to {enddate}')
            #import pdb; pdb.set_trace()
            
            self.db_ops.ingest_dataframe_to_sql(self.connection, "RawLeads", leads_df)
            print('successfully ingested to RawLeads')
            log_data = [{"LogDetails" : f"Successfully ingested {str(leads_df.shape[0])} to RawLeads from samsung.com pipe", 
            "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
            "SeverityLevel" : 5, "Stage" : "T1", "Status" : "SUCCESS"}]
            self.log_ops.save_logs(log_data)
        except Exception as e:
            log_data = [{"LogDetails" : f"Error in fetching Samsung.com Leads {str(e)}", 
            "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
            "SeverityLevel" : 5, "Stage" : "T1", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f"Error in fetching Samsung.com Leads {e}")
        finally:
            self.connection.close()
        



if __name__ =='__main__':
    obj = PullData()
    obj.run()
    
    
    




