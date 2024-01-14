import pandas as pd
import numpy as np
import pytz ,requests , datetime ,os,json,eventlet
from utils.db_ops import DBOps
from utils.log_ops import LogOps

"""ADD PRODUCTION API DETAILS IN CODE AND CHANGE UAT FLAG TO 0 FOR RUNNING"""

class LeadStatus():
    DEST_TABLE = "dbo.LeadStatus"
    SRC_TABLE = 'dbo.LeadsAssignment'
    Capacity_Master_TABLE = 'dbo.StoreDetailsMaster'
    STATUS_MAPPER_TABLE = 'dbo.LeadStatusMappingSD'
    #STORE_MASTER_TABLE = 'dbo.StoreDetailsMaster'
    ENV_CONFIG = os.path.join(os.path.join('configs' , 'env_config' ,'coding_environment.json'))
    FINAL_COLS = ['SystemDisposition','Level1','Level2','Level3','FinalDisposition',
         'Status','IngestionTimestamp','CallStart','UUID','Division']
    IST = pytz.timezone('Asia/Kolkata')
    UAT = 1
    API_DI = {"UAT" : "https://smartdost.samsungmarketing.in" ,"TEST" : "http://test01.samsungindiamarketing.com" ,"PROD" :"https://www.samsungindiamarketing.com"}
    API_HOST_DI = {"UAT" : "www.smartdost.samsungmarketing.in" ,"TEST" : "www.test01.samsungindiamarketing.com" ,"PROD" :"www.samsungindiamarketing.com"}
    TIMEOUT = 10



    def __init__(self):
        self.db_ops = DBOps()
        self.log_ops = LogOps()
        self.connection = self.db_ops.create_connection()
        self.env_set = self.read_json(LeadStatus.ENV_CONFIG)['code_env']
        query = f"SELECT Division ,Response,Level1,Level2,Level3,FinalStatus FROM {LeadStatus.STATUS_MAPPER_TABLE}"
        self.status_mapper = self.db_ops.execute_dataframe_query(self.connection , query)
        self.status_mapper['Division'].replace("MX" ,"IM" ,inplace = True)


    def call_status_api(self ,uuid ,division,cnt):
        try:
            #import pdb;pdb.set_trace()
            cnt += 1
            # get_lead_status_sd_api_uat = f'https://smartdost.samsungmarketing.in/LMSWebApi/Api/SmartDostLeadDetailsByUUID?UUID={uuid}'
            # get_lead_status_sd_api_prod = f'https://www.samsungindiamarketing.com/LMSWebApi/Api/SmartDostLeadDetailsByUUID?UUId={uuid}'
           
            api_path_division_di = {"CE":f"/LMSWebApi/Api/SmartDostLeadDetailsByUUID?UUID={uuid}" , "IM" :f"/LMSWebApi/Api/SmartDostMXLeadDetailsByUUID?UUID={uuid}"}
            get_lead_status_sd_api = LeadStatus.API_DI[self.env_set] +  api_path_division_di[division]   #f"/LMSWebApi/Api/SmartDostLeadDetailsByUUID?UUID={uuid}"
            #get_lead_status_sd_api = get_lead_status_sd_api_uat if LeadStatus.UAT == 1 else get_lead_status_sd_api_prod
            headers_di = {
                    'Authorization': 'Basic c2Ftc3VuZ3dlYmFwaTo0NDM2NjEyZjBlMTFjNWYwYmU3YjcwZTZlZmU4ZDEzYzQxYWYxODNmMzE0MzU4MTljMDU4ZWRkZGMyYjgwNmQ1',  
                    'Host': LeadStatus.API_HOST_DI[self.env_set],#'www.samsungindiamarketing.com',
                    'Accept': '*/*',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive',
                    'Content-Type': 'application/json',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/46.0.2490.80',
                    }
            try:
                with eventlet.Timeout(LeadStatus.TIMEOUT):
                    resp = requests.get(get_lead_status_sd_api , headers= headers_di)
          
            except TimeoutError as e:
                log_data = [{"LogDetails" : f'Smartdost Lead Status TIMEOUT Error {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T5", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                raise TimeoutError("Timeout Error Smartdost Lead Status API")

            if resp.status_code != 200:
                log_data = [{"LogDetails" : f"Error in GetLeadStatus Smartdost API :{str(resp.status_code)}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T5", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                raise Exception(f"Api response other than 200 {resp.status_code} retry number {cnt}")
            
            result = resp.json()
            #print(result)

            if len(result) == 0:
                log_data = [{"LogDetails" : f"No response found in LeadStatus for UUID:{str(uuid)}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T5", "Status" : "ERROR"}]
            
        except Exception as e:
            if cnt < 3:
                resp = self.call_status_api(uuid ,division, cnt)
            else:
                log_data = [{"LogDetails" : f"Error in Smartdost call_lead_status_api {str(e)} retry count {str(cnt)}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T5", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
        return result

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

    def fetch_lead_status(self ,row):
        #import pdb;pdb.set_trace()
        #saving raw resp in LeadStatus
        status_di = {"Response":"NA","Level1" : "NA" , "Level2" : "NA" , "Level3" : "NA" , "FinalStatus" :"NA","CallStart":row['CallStart']}
        try:
            resp = self.call_status_api(row['UUID'] ,row['Division'], 0)
            #print(resp)
            
            if (resp and len(resp)>0) and len(resp[0]['LeadStatus'])>0: 
                api_resp = resp[0]
                #status_di['LeadStatus'] = resp['LeadStatus']
                try:
                    resp = self.status_mapper[(self.status_mapper['Division'] == row['Division']) & (self.status_mapper['Response'] == api_resp['LeadStatus'])].iloc[0]
                    resp = resp[[ "Response" ,"Level1" , "Level2" , "Level3" , "FinalStatus"]]
                    resp_di = resp.to_dict()

                    if api_resp['ModifiedDate']:
                        resp_di['CallStart'] = api_resp['ModifiedDate']

                    for key in status_di:
                        if key == 'CallStart':
                            if status_di[key] == pd.to_datetime('1990-12-19 00:00:00.000') or "NA":
                                status_di[key] = api_resp['ModifiedDate']
                        else:
                            status_di[key] = resp_di[key]
                    
                    
                        # pd.to_datetime().strftime(format = '%Y/%m/%d %H:%M:%S')
                except Exception as e:
                    print(f"No Status Mapping found for - {resp['LeadStatus']}")
                    log_data = [{"LogDetails" : f"No Status Mapping fouund for {str(e)} , {resp['LeadStatus']}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T5", "Status" : "ERROR"}]
                    self.log_ops.save_logs(log_data)
                    raise Exception()
               
        except Exception as e:
            print(f'Error in Fetching LeadStatus {e}')
            pass
        
        return status_di
    
    def make_final_df(self ,df):
        try:
           # import pdb;pdb.set_trace()
            fresh_df = df
            fresh_df['TEMP_DI'] = {"Level1" : "NA" , "Level2" : "NA" , "Level3" : "NA" , "FinalDisposition" :"NA","CallStart":""}
            fresh_df["TEMP_DI"] = fresh_df.apply(lambda x : self.fetch_lead_status(x),axis = 1)
            #import pdb;pdb.set_trace()
            fresh_df["Level1"] = fresh_df["TEMP_DI"].apply(lambda x : x["Level1"])
            fresh_df["Level2"] = fresh_df["TEMP_DI"].apply(lambda x : x["Level2"])
            fresh_df["Level3"] = fresh_df["TEMP_DI"].apply(lambda x : x["Level3"])
            fresh_df['SystemDisposition'] = fresh_df["TEMP_DI"].apply(lambda x : x["Response"])
            fresh_df["FinalDisposition"] = fresh_df["TEMP_DI"].apply(lambda x : x["FinalStatus"])
            fresh_df["Status"] = fresh_df["TEMP_DI"].apply(lambda x : x["FinalStatus"])
            fresh_df["CallStart"] = fresh_df["TEMP_DI"].apply(lambda x : x["CallStart"])
            fresh_df['CallStart'] = fresh_df['CallStart'].apply(lambda x :  pd.to_datetime(x).strftime(format = '%Y/%m/%d %H:%M:%S'))
            fresh_df.drop(["TEMP_DI"], axis=1, inplace=True)  
            fresh_df["IngestionTimestamp"] = datetime.datetime.now(LeadStatus.IST).strftime("%Y-%m-%d %H:%M:%S")
            #fresh_df["CallStart"]= pd.to_datetime('1990/12/19 00:00:00',format='%Y/%m/%d %H:%M:%S')
        except Exception as e:
            raise Exception(f"Error in making Final df {e}")
        return fresh_df
    
    def calculate_lead_balance(self,df):
        df = df[['Status','UUID', 'Division' ,"StoreCode"]]
        df = df.groupby(['Division','Status',"StoreCode"]).size().to_frame('count').reset_index()
        df_closed = df[df['Status'] =='CLOSED']
        df_closed['LeadModifiedTimestamp'] = datetime.datetime.now(LeadStatus.IST).strftime("%Y-%m-%d %H:%M:%S")
        df_closed = df_closed[['count','LeadModifiedTimestamp','StoreCode','Division']]
        return df_closed
   
    def update_capacity(self,df):
        values ,buckets= self.batch_update(df)
        for batch in buckets:
            b_vals = values[batch[0] : batch[1]]
            query_upd = f"UPDATE {LeadStatus.Capacity_Master_TABLE} SET CurrentCapacity = CurrentCapacity + ? , DateModified = ? \
                WHERE StoreCode = ? AND Division = ? " 
            flag = self.db_ops.update_table(self.connection, query_upd, b_vals)
            print(f'Successfully Updated Capacity for {len(b_vals)} entries in {LeadStatus.Capacity_Master_TABLE}')
            return flag

    def batch_update(self,df):
        try:
            values = list(df.itertuples(index=False, name=None))
            batch_size = 10000
            buckets = list(range(0, len(values) + batch_size, batch_size))
            buckets = [buckets[i:i+2] for i,e in enumerate(buckets) if len(buckets[i:i+2]) == 2]

            batch_size = 10000
            buckets = list(range(0, len(values) + batch_size, batch_size))
            buckets = [buckets[i:i+2] for i,e in enumerate(buckets) if len(buckets[i:i+2]) == 2]
        except Exception as e:
            print(f'Error in Making Ingestion Batches')
        
        return values , buckets


    def run(self):
        try:
            #import pdb;pdb.set_trace()
            today_date = (datetime.datetime.now(LeadStatus.IST) - datetime.timedelta(2)).strftime("%Y-%m-%d")
            yesterday_date = (datetime.datetime.now(LeadStatus.IST) - datetime.timedelta(4)).strftime("%Y-%m-%d")
            
            old_df_query = f"SELECT A.UUID,A.Product,A.Division,B.StoreCode,A.CallStart FROM {LeadStatus.DEST_TABLE} A WITH (NOLOCK)\
                inner join  {LeadStatus.SRC_TABLE} B WITH (NOLOCK) ON A.UUID = B.UUID WHERE A.AllocatedTo = 'SD' and  A.Status != 'CLOSED' " #A.Division = 'CE' and and A.Status = 'OPEN'

        #LeadPushed = 1 AND  AND MasterCampaign = 'NeoQLED98-Q2-2023-CE-Television' #and Division = 'CE'
            fresh_df_query = f"SELECT UUID ,Product,Division ,StoreCode FROM {LeadStatus.SRC_TABLE} WITH (NOLOCK) WHERE  StoreCode != 'EP' \
             and LeadPushed = 1\
            AND CAST(LeadModifiedTimestamp as Date) > '{today_date}' AND UUID NOT IN (SELECT UUID FROM {LeadStatus.DEST_TABLE} WITH (NOLOCK) WHERE \
                CAST(IngestionTimestamp AS DATE) > '{yesterday_date}') "
            
            store_master_df = f"SELECT Distinct(StoreCode) FROM {LeadStatus.Capacity_Master_TABLE} WITH (NOLOCK)"
            
            old_leads_df =   self.db_ops.execute_dataframe_query(self.connection , old_df_query)
            fresh_leads_df = self.db_ops.execute_dataframe_query(self.connection , fresh_df_query)
            store_master_df =  self.db_ops.execute_dataframe_query(self.connection , store_master_df)

            #fresh_leads_df = fresh_leads_df.head(10)
            #old_leads_df = old_leads_df.head(5)
            print(f'Found {fresh_leads_df.shape[0]} fresh leads')
                
            if not fresh_leads_df.empty:
                fresh_leads_df["CallStart"]= pd.to_datetime('1990/12/19 00:00:00',format='%Y/%m/%d %H:%M:%S')
                final_df = self.make_final_df(fresh_leads_df)
                update_capacity_df = pd.merge(store_master_df , final_df , how="inner" , on = "StoreCode")
                final_df["AllocatedTo"] = "SD"
                final_df.drop(columns = ['StoreCode'] ,inplace = True)
                #import pdb ;pdb.set_trace()
                flag = self.db_ops.ingest_dataframe_to_sql(self.connection,LeadStatus.DEST_TABLE,final_df)
                print(f'succesfully ingested status for {len(final_df)} to LeadStatus')

                if not update_capacity_df.empty:
                    update_capacity_balance = self.calculate_lead_balance(update_capacity_df)
            
                    if not update_capacity_balance.empty:
                        ingest_flag = self.update_capacity(update_capacity_balance)
                        self.update_capacity(update_capacity_balance)
                        log_data = [{"LogDetails" : f"Successfully Updated Capacity for {len(update_capacity_balance)} stores", 
                    "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                    "SeverityLevel" : 5, "Stage" : "T5", "Status" : "SUCCESS"}]
                        self.log_ops.save_logs(log_data)
                    else:
                        print("No Leads Found with CLOSED status")
                        log_data = [{"LogDetails" : f"No Leads Found With CLOSED Status For Smartdost", 
                    "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                    "SeverityLevel" : 5, "Stage" : "T5", "Status" : "SUCCESS"}]
                        self.log_ops.save_logs(log_data)
                
                del update_capacity_df
                
            print(f'Found {old_leads_df.shape[0]} old leads')

            if not old_leads_df.empty:
                final_df = self.make_final_df(old_leads_df)
                update_capacity_df = pd.merge(store_master_df , final_df , how="inner" , on = "StoreCode")
                final_df = final_df[LeadStatus.FINAL_COLS]
                values,buckets = self.batch_update(final_df)
                for batch in buckets:
                        b_vals = values[batch[0] : batch[1]]
                        query_upd = f"UPDATE {LeadStatus.DEST_TABLE} SET \
                        SystemDisposition= ?,Level1 = ? , Level2 = ? , Level3 = ? , FinalDisposition = ?, Status = ? \
                        ,IngestionTimestamp = ? ,CallStart = ?\
                        WHERE UUID = ? and Division = ?"
                        flag = self.db_ops.update_table(self.connection, query_upd, b_vals)
                        print(f'Successfully Updated Status for {len(b_vals)} in {LeadStatus.DEST_TABLE}')
                
                
                if not update_capacity_df.empty: 
                    update_capacity_balance = self.calculate_lead_balance(update_capacity_df)
                
                    if not update_capacity_balance.empty:
                        self.update_capacity(update_capacity_balance)
                        log_data = [{"LogDetails" : f"Successfully Updated Capacity for {len(update_capacity_balance)} stores", 
                    "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                    "SeverityLevel" : 5, "Stage" : "T5", "Status" : "SUCCESS"}]
                        self.log_ops.save_logs(log_data)
                        
                    else:
                        print("No Leads Found with CLOSED status")
                        log_data = [{"LogDetails" : f"No Leads Found With CLOSED Status For Smartdost", 
                    "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                    "SeverityLevel" : 5, "Stage" : "T5", "Status" : "SUCCESS"}]
                        self.log_ops.save_logs(log_data)
            
            log_data = [{"LogDetails" : f"Successfully pulled lead status from SmartDost", 
                    "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                    "SeverityLevel" : 5, "Stage" : "T5", "Status" : "SUCCESS"}]
            self.log_ops.save_logs(log_data)
        
        except Exception as e:
            log_data = [{"LogDetails" : f"Error in lead Status pipe run SmartDost : {str(e)}", 
                    "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                    "SeverityLevel" : 5, "Stage" : "T5", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f"Error in lead Status pipe run : {e}")
        
        finally:
            self.connection.close()
            


if __name__ == "__main__":
    obj = LeadStatus()
    obj.run()
