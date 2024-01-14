import pandas as pd
import requests ,pytz,datetime,time,eventlet
from utils.db_ops import DBOps
import pytz
from utils.log_ops import LogOps

"""
[Todo]
1. Sort by Propensity Score Mapping
"""



class PushDataEprom(object):
    
    API_USERNAME = "crm_test"
    API_PASSWORD = "Sam$123456"
    GET_TOKEN_API = 'https://crm.maxicus.com/samsung_siel/client/res/templates/record/custom_files/get_token.php'
    LEAD_PUSH_API = 'https://crm.maxicus.com/samsung_siel/client/res/templates/record/custom_files/post_user.php'
    IST = pytz.timezone('Asia/Kolkata')
    CAMPAIGN_DETAILS_TABLE = '[dbo].[CampaignDetails]'
    TIME_OUT = 10
    TIME_OUT_RUN_FUNC =7200
 
 
    def __init__(self):
        self.IST = pytz.timezone('Asia/Kolkata')
        self.dbops = DBOps()
        self.log_ops = LogOps()
        self.connection = self.dbops.create_connection()
        self.filter_db_fields = ['UUID','FullName','PhoneNumber','Email','PinCode','MasterCampaign','LeadCreationTimestamp','Division','Category','LeadScore','CustomerTier','Product','Platform']
        


    def get_api_token(self):
        try:
            token, expiry = "", ""

            try:
                with eventlet.Timeout(PushDataEprom.TIME_OUT):
                    resp = requests.post(PushDataEprom.GET_TOKEN_API, json = {'username':PushDataEprom.API_USERNAME,
                                                                     'password':PushDataEprom.API_PASSWORD})
            
            except Exception as e:
                log_data = [{"LogDetails" : f'Epromoter  GetToken API TIMEOUT Error {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T4", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                raise TimeoutError('Epromoter LeadPush GetToken API-Timeout Error')

            if resp.status_code != 200:
                log_data = [{"LogDetails" : f"Error in EP push get token api status code {str(resp.status_code)}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T4", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                raise Exception(f'Eprom GetTokenAPI Resp is {resp}')
            result = resp.json()
            if (result["status"] != "200") and (not result["isSuccess"]):
                log_data = [{"LogDetails" : f'Eprom GetTokenAPI Resp is {str(result)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T4", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                raise Exception(f'Eprom GetTokenAPI Resp is {result}')
            token, expiry = result["token"], result["token_expiry"]
            expiry = datetime.datetime.strptime(expiry,"%Y-%m-%d %H:%M:%S").replace(tzinfo = PushDataEprom.IST)
        except Exception as e:
            log_data = [{"LogDetails" : f'Eprom GetTokenAPI Resp is {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T4", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            print(f'Error in GetTokenEprom API {e}')
            raise Exception(f'Eprom GetTokenAPI')
        return token, expiry
        

    def parse_token_resp(self,resp):
        try:
            resp_json = resp.json()
            token_fetched = resp_json['token']
            token_expiry = resp_json['token_expiry']
            token_expiry = datetime.datetime.strptime(token_expiry,"%Y-%m-%d %H:%M:%S")


        except Exception as e:
            log_data = [{"LogDetails" : f'Error in Parsing EpromApiToken Details {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T4", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f'Error in Parsing EpromApiToken Details {str(e)}')    
        return {'token':token_fetched,'token_expiry':token_expiry}
    

    def fetch_leads_db(self):
        try:

            query = f"SELECT A.LeadID, A.UUID,C.Platform, A.LeadCreationTimestamp, A.LeadModifiedTimestamp, A.FullName, \
            A.PhoneNumber, A.Email, A.City, A.PinCode, A.Division, A.Category, A.Product, A.StoreCode ,\
            A.LeadScore, A.CustomerTier, A.MasterCampaign \
            FROM dbo.LeadsAssignment A WITH (NOLOCK)\
            JOIN {PushDataEprom.CAMPAIGN_DETAILS_TABLE} B WITH (NOLOCK) ON A.MasterCampaign = B.CampaignName \
            JOIN [dbo].[AllLeads] C  WITH (NOLOCK) ON A.UUID = C.UUID \
            WHERE A.LeadPushed = 0  and B.IsActive = 'Y' and B.LeadDistributionFlag= 'Y'  and A.StoreCode = 'EP' ORDER BY A.LeadCreationTimestamp asc"
            
        #    query =  f"SELECT LeadID, UUID, LeadCreationTimestamp, LeadModifiedTimestamp, FullName,\
        #     PhoneNumber, Email, City, PinCode, Division, Category, Product, StoreCode, AllocationType,\
        #     LeadScore, CustomerTier, MasterCampaign,LeadPushed\
        #     FROM dbo.LeadsAssignment A WITH (NOLOCK)\
        #     JOIN {PushDataEprom.CAMPAIGN_DETAILS_TABLE} B WITH (NOLOCK) ON A.MasterCampaign = B.CampaignName \
        #     WHERE A.LeadPushed = 0  and B.IsActive = 'Y' and B.LeadDistributionFlag= 'Y'  and A.StoreCode = 'EP' ORDER BY A.LeadCreationTimestamp desc "
            # WHERE LeadPushed = 0 and StoreCode = 'EP' and MasterCampaign = 'GalaxyF54-Q2-2023-MX-Smartphones   #AND B.IsActive = 'Y'
            # ORDER BY LeadCreationTimestamp asc
            df = self.dbops.execute_dataframe_query(self.connection, query)
        except Exception as e:
            log_data = [{"LogDetails" : f'Error in fetching Leads from db {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T4", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f'Error in fetching Leads from db {e}')
        return df

    
    def dynamic_payload(self,row):
        payload_json = {
                    "uuid" : f"{row['UUID'].strip()}",
                    "name" : f"{row['FullName'].strip()}",
                    "mobileno" : f"{row['PhoneNumber'].strip()}",
                    "email" : f"{row['Email'].strip()}",
                    "CampaignName":f"{str(row['Platform'] + '-' + row['MasterCampaign'])}",#f"{row['MasterCampaign'].strip()}",
                    "LeadCreationDate": f"{row['LeadCreationTimestamp']}",
                    "LeadDivison" : f"{row['Division'].strip()}",
                    "LeadCategory" :f"{row['Category'].strip()}",
                    "PinCode" : f"{row['PinCode'].strip()}",
                    "Tier" : f"{row['CustomerTier'].strip()}",
                    "Score" : f"{row['LeadScore'].strip()}",
                    "Product" : f"{row['Product'].strip()}"
                    }
        return payload_json

    
    def push_leads(self, api_token, data, cnt):
        try:
            resp = {}
            flag = False
            cnt += 1
            headers_di = {'Authorization': api_token,'Content-Type':'application/json'}
            try:
                with eventlet.Timeout(PushDataEprom.TIME_OUT):
                    resp = requests.post(PushDataEprom.LEAD_PUSH_API, headers = headers_di,
                          json = data)
            
            except eventlet.Timeout as e:
                log_data = [{"LogDetails" : f'Epromoter  LeadPush API TIMEOUT Error {str(e)} for UUID = {str(data["uuid"])}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T4", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                raise TimeoutError('Epromoter LeadPush API-Timeout Error')
            
            if resp.status_code != 200:
                log_data = [{"LogDetails" : f'Eprom LeadPushAPI Resp is {str(resp)} for UUID {str(data["uuid"])}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T4", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                raise Exception(f'Eprom LeadPushAPI Resp is {resp}')
            result = resp.json()
            
            if (result["status"] != '200') and (not result["isSuccess"]):
                log_data = [{"LogDetails" : f'Eprom LeadPushAPI Resp is {str(result)} for UUID = {str(data["uuid"])}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T4", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                raise Exception(f'Eprom LeadPushAPI Resp is {result}')
            
            if (result["status"] != '200') and  (result['message'] == 'token expired'):
                log_data = [{"LogDetails" : f'Token Expired For Epromoter LeadPush API {str(result)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T4", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                self.token, self.expiry = self.get_token(0)
                
                raise Exception(f"Error in EpromLeadPushAPI Resp  {result}")
            flag = True
        except Exception as e:
            if cnt < 3:
                flag = self.push_leads(self.token, data, cnt)  #api_token
            else:
                log_data = [{"LogDetails" : f'Eprom LeadPushAPI Error retry count {str(cnt)} for UUID = {str(data["uuid"])}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T4", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
        return flag
    

    def update_status_flag(self,uuid):
        try:
            query = f"UPDATE dbo.LeadsAssignment SET LeadPushed = 1 WHERE UUID = {uuid}"
            df = self.dbops.execute_query(self.connection, query)
        
        except Exception as e:
            log_data = [{"LogDetails" : f'Error in updating the FlagStoreCallCenter Flag {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T4", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f'Error in updating the FlagStoreCallCenter Flag{e}')


    def clean_phone_number(self,phone_number):
        if len(phone_number) == 13:
            try:
               phone_number = phone_number.replace('+91','')
            except Exception as e:
                log_data = [{"LogDetails" : f'PhoneNumber Validation Error {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T4", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                raise Exception(f'PhoneNumber Validation Error {e}')
            
            return phone_number



    def run(self):
        #import pdb;pdb.set_trace()
        try:
            df_leads = self.fetch_leads_db()
            #df_leads = df_leads[:15]
            df_leads = df_leads[self.filter_db_fields]
            success_uuid_li = []
            if df_leads.empty:
                log_data = [{"LogDetails" : f'No New Leads Found for Epromoter....', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 1, "Stage" : "T4", "Status" : "INFO"}]
                self.log_ops.save_logs(log_data)
                raise Exception(f'No New Leads Found for Epromoter....')

            print(f'Found {len(df_leads)} for Epromoter')            
            self.token, token_expiry = self.get_api_token()
            
            df_leads["LeadSuccess"] = 0
            df_leads.loc[df_leads['PinCode'] == '0' ,"PinCode"] ="NA"
            
            for index, rows in df_leads.iterrows():

                current_time = datetime.datetime.now(PushDataEprom.IST)

                if token_expiry <= current_time:
                    self.token, token_expiry = self.get_api_token()

                try:
                    rows['PhoneNumber'] = self.clean_phone_number(rows['PhoneNumber'])
                    json_payload = self.dynamic_payload(rows)
                    #import pdb;pdb.set_trace()
                    success = self.push_leads(self.token, json_payload, 0)
                   
                    if success:
                        success_uuid_li.append(rows["UUID"])
                    
                    if ((index+1) % 5) == 0:
                        
                        if success_uuid_li:
                            #import pdb;pdb.set_trace()
                            LeadPushTime = current_time.strftime("%Y-%m-%d %H:%M:%S")
                            tmp_uuid_tup = f'({success_uuid_li[0]})' if len(success_uuid_li) == 1 else tuple(success_uuid_li)
                            query = f"UPDATE LeadsAssignment SET LeadPushed = 1 , LeadPushedTimestamp = '{LeadPushTime}' WHERE UUID IN {tmp_uuid_tup}"
                            self.dbops.execute_query(self.connection, query)
                            print(f"Pushed {len(success_uuid_li)} uuids successfully at index {index+1}")
                        else:
                            print(f"Pushed {len(success_uuid_li)} uuids successfully at index {index+1}")
                        success_uuid_li = []

                    time.sleep(0.1)


                except Exception as e:
                    print(f'Error in Hit for UUID = {rows["UUID"]}')

            print(f'successfully Pushed to Epromoter')
            log_data = [{"LogDetails" : f'Successfully Pushed  Eprom leads...', 
                    "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                    "SeverityLevel" : 5, "Stage" : "T4", "Status" : "SUCCESS"}]
            self.log_ops.save_logs(log_data)
       
        except Exception as e:
            log_data = [{"LogDetails" :f'Error in Eprom PushPipe {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T4", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception (f'Error in Eprom PushPipe {e}')
        
        finally:
            if success_uuid_li:
                #import pdb;pdb.set_trace()
                LeadPushTime = current_time.strftime("%Y-%m-%d %H:%M:%S")
                tmp_uuid_tup = f'({success_uuid_li[0]})' if len(success_uuid_li) == 1 else tuple(success_uuid_li)
                query = f"UPDATE dbo.LeadsAssignment SET LeadPushed = 1  , LeadPushedTimestamp = '{LeadPushTime}' WHERE UUID IN {tmp_uuid_tup}"
                try:
                    self.dbops.execute_query(self.connection, query)
                except Exception as e:
                     log_data = [{"LogDetails" :f'Error in Updating flag in AllLeads {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T4", "Status" : "ERROR"}]
                     self.log_ops.save_logs(log_data)

                print(f"Pushed {len(success_uuid_li)} uuids successfully at index {index+1}.....")
            
            self.connection.close()
                    
        
                    


    
if __name__ == "__main__":
    obj = PushDataEprom()
    obj.run()



        





