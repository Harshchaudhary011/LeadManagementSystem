import datetime, pytz, timedelta, os, json, requests ,time ,eventlet
import pandas as pd
import numpy as np
from utils.db_ops import DBOps
from utils.log_ops import LogOps

class LeadStatus(object):
    
    """
        TODO: 
        1. 
     
    """
    DEST_TABLE = "dbo.LeadStatus"
    SRC_TABLE = "dbo.LeadsAssignment"
    Campaign_Master_TABLE = "dbo.CampaignDetails"
    Capacity_Master_TABLE = "dbo.StoreDetailsMaster"
    API_CREDS = {
        "username":"crm_sam",
        "password":"Sam$123456"
        }
    # {
    #     "username":"crm_test",
    #     "password":"Sam$123456"
    #     }
    
    DEST_TABLE_COLS = ['SystemDisposition','Level1','Level2','Level3','FinalDisposition',
        'Attempt', 'Status','IngestionTimestamp','CallStart','UUID','Division']
    LEAD_STATUS_MAPPER_CONF = os.path.join(os.path.join('configs', 'lead_status_config'), 
                                    'disposition_mapping_eprom.json')
    IST = pytz.timezone('Asia/Kolkata')
    LEVEL2_MAPPER = { "No Further Action Needed" : "CLOSED" }
    TIMEOUT = 15

    #Level2 Mapper for Non-Engagement & Unable To Buy
    DF_BATCH_SIZE = 1500



    def __init__(self):
        self.db_ops = DBOps()
        self.log_ops = LogOps()
        self.connection = self.db_ops.create_connection()


    def get_token(self, cnt):
        try:
            token = ""
            cnt += 1
            url = "https://crm.maxicus.com/samsung_siel/client/res/templates/record/custom_files/get_token.php"
            
            try:
                with eventlet.Timeout(LeadStatus.TIMEOUT):
                    resp = requests.post(url, json = LeadStatus.API_CREDS)
            
            except TimeoutError as e:
                log_data = [{"LogDetails" : f'Epromoter Token API - Pipe TIMEOUT Error {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T5", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                raise TimeoutError("Epromoter Get Token API - Timeout Error")
            
            if resp.status_code != 200:
                raise Exception(f"Error in get token {resp}")
            result = resp.json()
            if result["status"] != '200':
                raise Exception(f"Error in get token  {result}")
            token = result.get("token", "")
            expiry = result.get("token_expiry","")
            expiry = datetime.datetime.strptime(expiry,"%Y-%m-%d %H:%M:%S").replace(tzinfo = LeadStatus.IST)

        except Exception as e:
            if cnt <= 3:
                token = self.get_token(cnt)
            else:
                log_data = [{"LogDetails" : f"Error in get token api {str(e)} retry count {cnt}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T5", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
        return token ,expiry


    def call_lead_status_api(self, row,token, cnt):
        #import pdb;pdb.set_trace()
        try:
            cnt += 1
            current_time = datetime.datetime.now(LeadStatus.IST)  # make it a function

            if current_time >= self.expiry:
                self.token, self.expiry = self.get_token(0)
                token = self.token
            result = {}
            
            if not token:
                return result
            uuid = row["UUID"]
            url = f"https://crm.maxicus.com/samsung_siel/client/res/templates/record/custom_files/get_user.php?uuid={uuid}"

            headers_di = {"Authorization" : self.token}

            try:
                with eventlet.Timeout(LeadStatus.TIMEOUT):
                    resp = requests.get(url, headers = headers_di)

            except TimeoutError as e:
                log_data = [{"LogDetails" : f'Epromoter GetLeadStatus API- Pipe TIMEOUT Error {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T5", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                raise TimeoutError("Epromoter Get Lead Status - Timeout Error")       
             
            if resp.status_code != 200:
                raise Exception(f"Error in get lead status {resp}")
            result = resp.json()
          
            if result["status"] != '200':
                if result['message'] == 'token expired':
                    #import pdb;pdb.set_trace()
                    log_data = [{"LogDetails" : f'Token Expired For Epromoter LeadStatus API {str(result)}', 
                    "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                    "SeverityLevel" : 4, "Stage" : "T5", "Status" : "ERROR"}]
                    self.log_ops.save_logs(log_data)
                    self.token, self.expiry = self.get_token(0)
                    token = self.token
                    raise Exception(f"Error in call_lead_status_api {result}")
                
        except Exception as e:
            if cnt < 3:
                result = self.call_lead_status_api(row,token, cnt)
            else:
                log_data = [{"LogDetails" : f"Error in call_lead_status_api for UUID = {str(uuid)} and {str(e)} and resp ={str(result)} retry count {str(cnt)}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T5", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
        return result

    def get_lead_status(self,token, row):
        #import pdb;pdb.set_trace()
        try:
            lead_status_di = {"l1" : "NA", "l2" : "NA", "l3" : "NA", "final_disposition" : "NA", 
                              "system_disposition" : "NA", "call_attempt" : '0',"first_connected_call":"NA"}
            lead_status_dialer = {"system_disposition" : "NA", "call_attempt" : "0","first_connected_call":"NA"}
            
            resp = self.call_lead_status_api(row,self.token, 0)
            time.sleep(0.5)
            
            if resp['status'] == '200' and len(resp['data'])>0:
                dialer_data = resp.get("data", {}).get("dialer", [])
                ticket_data = resp.get("data", {}).get("ticket", [])
                dialer_data = dialer_data[0] if dialer_data else {}
                ticket_data = ticket_data[0] if ticket_data else {}
                
                if ticket_data: 
                    for key in lead_status_di:
                        lead_status_di[key] = ticket_data.get(key, "NA")
                    
                if dialer_data:
                    for key in lead_status_dialer:
                        lead_status_dialer[key] = dialer_data.get(key, "NA")
                    
                    lead_status_di.update(lead_status_dialer)
                
                lead_status_di['call_attempt'] = int(lead_status_di['call_attempt']) if str(lead_status_di['call_attempt']).isdigit() else 0
                # if lead_status_di['call_attempt'] == None or lead_status_di['call_attempt'] =='NA' :
                #     lead_status_di['call_attempt'] = '0'

                #if lead_status_di['system_disposition'] != 'CONNECTED':
                if int(lead_status_di['call_attempt']) >= 7:
                    lead_status_di['final_disposition'] = 'NC Threshold Cross'
            
            else:
                print(f"Error in GetLeadDetails for UUID= {row['UUID']} & response={resp['status'],resp}") 
                lead_status_di['final_disposition'] = 'NA'

        except Exception as e:
            print(f"Error in get lead status parsing {e}")
            log_data = [{"LogDetails" : 'Error in LeadStatus parsing  For Epromoter', 
                    "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                    "SeverityLevel" : 4, "Stage" : "T5", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
        return lead_status_di
    


    def read_json(self, filepath):
        if not os.path.exists(filepath):
            log_data = [{"LogDetails" : f"File is Missing at {filepath}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T5", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f"File is Missing at {filepath}")
        with open(filepath, "r") as rj:
            di = json.load(rj)
        return di



    def calculate_final_status(self, row):
        di_final_status = self.read_json(LeadStatus.LEAD_STATUS_MAPPER_CONF)
        try:
            final_disposition = row['FinalDisposition']
            system_disposition = row['SystemDisposition']

            if final_disposition :
                STATUS = di_final_status.get(final_disposition,'NA')

            if final_disposition == "Non-Engagement" or final_disposition == "Unable To Buy": 
                STATUS = LeadStatus.LEVEL2_MAPPER.get(row['Level2'],"OPEN")
            if STATUS == 'NA':                                 #FINAL DISPOSITION is empty for NC leads
                STATUS = di_final_status.get(system_disposition,"NA")
                if STATUS == "NA" and system_disposition == "CONNECTED":
                    STATUS = "CLOSED"
           
        except Exception as e:
            Exception(f"Error in calculate status {e}")
        return STATUS
   

    def make_final_df(self,token,final_df):
        try:
            final_df["TEMP_DI"] = {"l1" : "NA", "l2" : "NA", "l3" : "NA", "final_disposition" : "NA", 
                            "system_disposition" : "NA", "call_attempt" : 0,"first_connected_call":"NA"}
            final_df["TEMP_DI"] = final_df.apply(lambda x : self.get_lead_status(self.token,x),axis = 1)
            final_df["Level1"] = final_df["TEMP_DI"].apply(lambda x : x["l1"])
            final_df["Level2"] = final_df["TEMP_DI"].apply(lambda x : x["l2"])
            final_df["Level3"] = final_df["TEMP_DI"].apply(lambda x : x["l3"])
            final_df["FinalDisposition"] = final_df["TEMP_DI"].apply(lambda x : x["final_disposition"])
            final_df["SystemDisposition"] = final_df["TEMP_DI"].apply(lambda x : x["system_disposition"])
            final_df["Attempt"] = final_df["TEMP_DI"].apply(lambda x : x["call_attempt"])
            final_df["CallStart"] = final_df["TEMP_DI"].apply(lambda x : x["first_connected_call"])
            final_df.drop(["TEMP_DI"], axis=1, inplace=True)
            final_df["Status"] = final_df.apply(lambda x : self.calculate_final_status(x),axis =1)
            final_df["IngestionTimestamp"] = datetime.datetime.now(LeadStatus.IST).strftime("%Y-%m-%d %H:%M:%S")
            final_df['CallStart']  = pd.to_datetime(final_df['CallStart'],format='%Y/%m/%d %H:%M:%S %z' , errors='coerce').dt.tz_localize(None).replace({np.NaN: '1990/12/19 00:00:00'}) 
 
        except Exception as e:
            print(f'Error in making Final Df {e}')
        return final_df
    


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

    
    def update_capacity(self,df):
        values ,buckets= self.batch_update(df)
        for batch in buckets:
            b_vals = values[batch[0] : batch[1]]
            query_upd = f"UPDATE {LeadStatus.Capacity_Master_TABLE} SET CurrentCapacity = CurrentCapacity + ? , DateModified = ? \
                WHERE StoreCode = 'EP' AND Division = ? " 
            flag = self.db_ops.update_table(self.connection, query_upd, b_vals)
            print(f'Successfully Updated Capacity for {len(b_vals)} entries in {LeadStatus.Capacity_Master_TABLE}')
            return flag



    def calculate_lead_balance(self,df):
        df = df[['Status','UUID', 'Division']]
        df = df.groupby(['Division','Status']).size().to_frame('count').reset_index()
        df_closed = df[df['Status'] =='CLOSED']
        df_closed['LeadModifiedTimestamp'] = datetime.datetime.now(LeadStatus.IST).strftime("%Y-%m-%d %H:%M:%S")
        df_closed = df_closed[['count','LeadModifiedTimestamp','Division']]
        return df_closed
   


    def run(self):
        try:
            today_date = (datetime.datetime.now(self.IST) - datetime.timedelta(5)).strftime("%Y-%m-%d")

            query = f"SELECT UUID, Product, Division, SystemDisposition, Level1, Level2, Level3,\
                    FinalDisposition, Attempt, Status, IngestionTimestamp, CallStart\
                    FROM {LeadStatus.DEST_TABLE} WITH (NOLOCK) WHERE AllocatedTo = 'EP'  and Status != 'CLOSED' "  
                        #and SystemDisposition = 'NA'
            old_lead_df = self.db_ops.execute_dataframe_query(self.connection, query)
            #old_lead_df = old_lead_df.head(5000)
            

            query = f"SELECT A.LeadID, A.UUID, A.LeadCreationTimestamp, A.LeadModifiedTimestamp,\
            A.FullName, A.PhoneNumber, A.Email, A.City, A.PinCode, A.Division, A.Category,\
            A.Product, A.StoreCode, A.AllocationType, A.LeadScore, A.CustomerTier, A.MasterCampaign,\
            A.LeadPushed\
            FROM {LeadStatus.SRC_TABLE} A WITH (NOLOCK)\
            WHERE A.LeadPushed = 1 AND CAST(LeadModifiedTimestamp as Date) > '{today_date}' \
            AND A.StoreCode = 'EP' AND A.UUID NOT IN (SELECT UUID FROM {LeadStatus.DEST_TABLE}\
            WHERE CAST(IngestionTimestamp as Date)>'{today_date}') order by A.LeadModifiedTimestamp asc"

            # query = f"SELECT A.LeadID, A.UUID, A.LeadCreationTimestamp, A.LeadModifiedTimestamp,\
            # A.FullName, A.PhoneNumber, A.Email, A.City, A.PinCode, A.Division, A.Category,\
            # A.Product, A.StoreCode, A.AllocationType, A.LeadScore, A.CustomerTier, A.MasterCampaign,\
            # A.LeadPushed\
            # FROM {LeadStatus.SRC_TABLE} A\
            # join {LeadStatus.Campaign_Master_TABLE} B ON A.MasterCampaign = B.CampaignName \
            # WHERE A.LeadPushed = 1 AND CAST(LeadModifiedTimestamp as Date) > '{today_date}' \
            # AND A.StoreCode = 'EP' AND A.UUID NOT IN (SELECT UUID FROM {LeadStatus.DEST_TABLE}\
            # WHERE CAST(IngestionTimestamp as Date)>'{today_date}') order by A.LeadModifiedTimestamp asc"
            

            #import pdb;pdb.set_trace()
            fresh_leads_df = self.db_ops.execute_dataframe_query(self.connection, query)
            #fresh_leads_df = fresh_leads_df.head(5)
            self.token,self.expiry = self.get_token(0)
            
            print(f'Fetching Status for {len(fresh_leads_df)} fresh leads')
            if not fresh_leads_df.empty :
                df_fresh_leads = pd.DataFrame({"UUID" : fresh_leads_df["UUID"].tolist(),'Division' : fresh_leads_df["Division"].tolist(),'Product' : fresh_leads_df["Product"].tolist()})
                del fresh_leads_df
                #df_fresh_leads = df_fresh_leads.head(2000)
                
                for  k, sub_df in df_fresh_leads.groupby(np.arange(len(df_fresh_leads)) // LeadStatus.DF_BATCH_SIZE):
                    if not self.token:
                        log_data = [{"LogDetails" : 'Error in GetToken API For Epromoter', 
                        "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                        "SeverityLevel" : 4, "Stage" : "T5", "Status" : "ERROR"}]
                        self.log_ops.save_logs(log_data)
                        raise Exception('Error in GetToken API For Eporomoter')
                    #final_df_fresh = self.make_final_df(self.token, df_fresh_leads)
                    final_df_fresh = self.make_final_df(self.token, sub_df)   
                    final_df_fresh['AllocatedTo'] = "EP"
                    #import pdb;pdb.set_trace()
                    self.db_ops.ingest_dataframe_to_sql(self.connection,LeadStatus.DEST_TABLE,final_df_fresh)
                    final_count_closed_fresh = self.calculate_lead_balance(final_df_fresh)
                    if not final_count_closed_fresh.empty:
                        ingest_flag = self.update_capacity(final_count_closed_fresh)

                print(f'succesfully ingested status for {len(df_fresh_leads)} to LeadStatus')
                


            #old leads df
            if not old_lead_df.empty:
                df_old_leads = pd.DataFrame({"UUID" : old_lead_df["UUID"].tolist(),'Division':old_lead_df['Division'].tolist(),'Product' : old_lead_df["Product"].tolist()})
                del old_lead_df
                #df_old_leads = df_old_leads.head(5)
                print(f'Fetching Status for {len(df_old_leads)} old leads')
                

                for  k, sub_df in df_old_leads.groupby(np.arange(len(df_old_leads)) // LeadStatus.DF_BATCH_SIZE):
                    if not self.token:
                        log_data = [{"LogDetails" : 'Error in GetToken API For Eporomoter', 
                        "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                        "SeverityLevel" : 4, "Stage" : "T5", "Status" : "ERROR"}]
                        self.log_ops.save_logs(log_data)
                        raise Exception('Error in GetToken API For Eporomoter')
                    
                    final_df_old = self.make_final_df(self.token,sub_df)
                    tmp_ingest_df = final_df_old[LeadStatus.DEST_TABLE_COLS]
                    values,buckets = self.batch_update(tmp_ingest_df)
                    for batch in buckets:
                        b_vals = values[batch[0] : batch[1]]

                        query_upd = f"UPDATE {LeadStatus.DEST_TABLE} SET SystemDisposition = ?, \
                        Level1 = ? , Level2 = ? , Level3 = ? , FinalDisposition = ?, Attempt = ?, Status = ? \
                        ,IngestionTimestamp = ? ,CallStart = ?\
                        WHERE UUID = ? and Division = ?"
                        flag = self.db_ops.update_table(self.connection, query_upd, b_vals)
                        print(f'Successfully Updated Status for {len(b_vals)} in {LeadStatus.DEST_TABLE}')
                    
                    final_count_closed_old = self.calculate_lead_balance(final_df_old)

                    if not final_count_closed_old.empty:
                        ingest_flag = self.update_capacity(final_count_closed_old)
                    
            log_data = [{"LogDetails" : f"Successfully pulled lead status from epromoter....", 
                    "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                    "SeverityLevel" : 5, "Stage" : "T5", "Status" : "SUCCESS"}]
            self.log_ops.save_logs(log_data)
        except Exception as e:
            log_data = [{"LogDetails" : f"Error in lead Status pipe run : {str(e)}", 
                    "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                    "SeverityLevel" : 5, "Stage" : "T5", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f"Error in lead Status pipe run : {e}")

        finally:
            self.connection.close()
   



if __name__ == "__main__":
    obj = LeadStatus()
    obj.run()
