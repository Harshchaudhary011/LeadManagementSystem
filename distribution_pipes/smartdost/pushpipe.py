import pandas as pd
import requests ,time,pytz,datetime,time, json ,re ,eventlet
import os
from utils.db_ops import DBOps
from utils.log_ops import LogOps
from copy import deepcopy



class PushDataSmartDost(object):
    #API = "PROD"
    TIME_OUT = 30
    REGEX = re.compile('[^a-zA-Z]')
    SD_TIERING_TABLE = "dbo.SdTierMapping"
    CAMPAIGN_DETAILS_TABLE = '[dbo].[CampaignDetails]'
    ENV_CONFIG = os.path.join(os.path.join('configs' , 'env_config' ,'coding_environment.json'))
    API_HOST_DI_CE = {"UAT" : "www.smartdost.samsungmarketing.in" ,"TEST" : "www.test01.samsungindiamarketing.com" ,"PROD" :"www.smartdost-siel.com"}
    API_HOST_DI_IM =  {"UAT" : "www.smartdost.samsungmarketing.in" ,"TEST" : "www.test01.samsungindiamarketing.com" ,"PROD" :"www.smartdost-siel.com"}
    headers = {"Content-Type" : "application/json",
                'Host': '' ,   
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/46.0.2490.80'
                            }
    ''''not accepting email as null or NA
    WE ARE CLEANIGN NAME DROPPPING NON ALPHABETIC DIGITS AND SENDING UNKNOWN FOR INVALID NAMES'''


    def __init__(self):    
        self.IST = pytz.timezone('Asia/Kolkata')
        self.dbops = DBOps()
        self.log_ops = LogOps()
        self.connection = self.dbops.create_connection()
        self.env_set = self.read_json(PushDataSmartDost.ENV_CONFIG)['code_env']
        self.filter_db_fields = ['UUID','FullName','PhoneNumber','Email','PinCode','MasterCampaign','LeadCreationTimestamp','Division','Category','LeadScore','CustomerTier','Product']
        self.api_di_ce = self.read_json("distribution_pipes/smartdost/api_conf.json")["CE"][self.env_set] #[PushDataSmartDost.API]
        self.api_di_im = self.read_json("distribution_pipes/smartdost/api_conf.json")["IM"][self.env_set]   #[PushDataSmartDost.API]
        # self.ce_headers_PROD = {"Content-Type" : "application/json"}
        # self.ce_headers_UAT = {"Content-Type" : "application/json",
        #         'Host': 'www.smartdost.samsungmarketing.in',
        #         'Accept': '*/*',
        #         'Accept-Encoding': 'gzip, deflate',
        #         'Connection': 'keep-alive',
        #         'Content-Type': 'application/json',
        #         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/46.0.2490.80'}
        
        #import pdb;pdb.set_trace()
        #self.im_headers = {"Content-Type" : "application/json"}
        #self.ce_headers = self.ce_headers_PROD if PushDataSmartDost.API == "PROD" else self.ce_headers_UAT
        self.ce_headers = deepcopy(PushDataSmartDost.headers)
        self.im_headers = deepcopy(PushDataSmartDost.headers)

        self.ce_headers['Host'] = PushDataSmartDost.API_HOST_DI_CE[self.env_set]
        self.im_headers['Host'] = PushDataSmartDost.API_HOST_DI_IM[self.env_set]

        for var in self.api_di_ce:
            if var not in ["URL"]:
                self.ce_headers[var] = self.api_di_ce[var]
        
        for key in self.api_di_im:
            if key not in ["URL"]:
                self.im_headers[key] = self.api_di_im[key]
        

    def read_json(self, filepath):
        if not os.path.exists(filepath):
            log_data = [{"LogDetails" : f"{filepath} does not exists", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T4", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f"{filepath} does not exists")

        with open(filepath, "r") as rj:
            di = json.load(rj)
        return di

    def fetch_leads_db(self):
        try:
            query = f"SELECT LeadID, UUID, LeadCreationTimestamp, LeadModifiedTimestamp, FullName,\
            PhoneNumber, Email, City, PinCode, Division, Category, Product, StoreCode, AllocationType,\
            LeadScore, CustomerTier, MasterCampaign, LeadPushed \
            FROM dbo.LeadsAssignment A WITH (NOLOCK) join {PushDataSmartDost.CAMPAIGN_DETAILS_TABLE} B WITH (NOLOCK) ON \
            A.MasterCampaign = B.CampaignName\
            WHERE A.LeadPushed = 0 and A.StoreCode != 'EP' and B.IsActive =  'Y' and B.LeadDistributionFlag= 'Y' ORDER BY LeadCreationTimestamp asc"
            df = self.dbops.execute_dataframe_query(self.connection, query)
        except Exception as e:
            log_data = [{"LogDetails" : f'Error in fetching Leads from db {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T4", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f'Error in fetching Leads from db {e}')
        return df
    
    
    def apply_tiering(self,row):
        try:
            if row["Division"] == "CE":


                if row['LeadScore'] != "UNKNOWN":
                    row['CustomerTier']  = self.tier_mapping_df[(self.tier_mapping_df["Division"] == row["Division"]) & (self.tier_mapping_df["Score"] == row["LeadScore"]) ]["SdTier"].iloc[0]
                    row['LeadScore'] = int(row['LeadScore'])

                    if row['CustomerTier'] == "New":
                        row['CustomerTier'] = None
                    
                    else:
                        row['CustomerTier'] = int(row['CustomerTier'])
                else:
                    #row['CustomerTier']
                    #row['CutomerTier'] = self.tier_mapping_df[(self.tier_mapping_df["Division"] == row["Division"]) & (self.tier_mapping_df["Score"] == row["CustomerTier"])]["SdTier"].iloc[0]
                    row['CustomerTier'] = self.tier_mapping_df[(self.tier_mapping_df["Division"] == row["Division"]) & (self.tier_mapping_df["Score"] == row["CustomerTier"])]["SdTier"].iloc[0]
                    row['LeadScore'] = None

                    if row['CustomerTier'] == "New":
                        row['CustomerTier'] = None                        
                    else:
                        row['CustomerTier'] = int(row['CustomerTier'])
                        

            elif row["Division"] == "IM":
                if row["LeadScore"] != "UNKNOWN":
                    score_mapping = self.tier_mapping_df[(self.tier_mapping_df["Division"] == row["Division"]) & (self.tier_mapping_df["Score"] == row["LeadScore"]) ]["SdTier"].iloc[0]
                
                else:        
                    score_mapping = self.tier_mapping_df[(self.tier_mapping_df["Division"] == row["Division"]) & (self.tier_mapping_df["Score"] == row["CustomerTier"]) ]["SdTier"].iloc[0]
                row['FullName'] = str(score_mapping + '-' + row['FullName'])

        except Exception as e:
            log_data =  [{"LogDetails" : f'Error in applying Tiering Logic {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T4", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f'Error in applying Tiering Logic {e}')
        return row

    def push_lead_im(self , row, cnt):
        try:
            cnt += 1
            lead_pushed = 0
            url = self.api_di_im["URL"]
            payload = {
                "userId" : int(self.im_headers["APIUser"]),
                "InputOfCustomer" : {
                    "LeadID": str(row["LeadID"]),
                    "CustomerName": str(row["FullName"]),
                    "CustomerMobileNumber": str(row["PhoneNumber"]),
                    "CustomerEmailID": str(row["Email"]),
                    "StoreCode": str(row["StoreCode"]),
                    "CampaignName": str(row["MasterCampaign"]),
                    "LeadCreationDate": str(row["LeadCreationTimestamp"]),  
                    "Uuid": str(row["UUID"])
                    }
                }
            #"CampaignId" :1,  "GooglePlaceId" :"0", ---NOT SENDING THESE PARAMS
             #"RequestType": 2,  "PinCode": str(row["PinCode"]),
                   #"LeadDivison": str(row["Division"])
                    # "Score": str(row["LeadScore"]), 
                    # "Tier" : str(row["CustomerTier"])
            #import pdb;pdb.set_trace()
            
            try: 
                with eventlet.Timeout(PushDataSmartDost.TIME_OUT):
                    response = requests.request("POST", url, headers=self.im_headers, data= json.dumps(payload))
            
            except TimeoutError as e: #eventlet.Timeout as t:
                    log_data = [{"LogDetails" : f'Smartdost MX API TIMEOUT Error  for UUID = {str(row["UUID"])}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T4", "Status" : "ERROR"}]
                    self.log_ops.save_logs(log_data)
                    raise TimeoutError('Smartdost MX API - Timeout Error')
                    
            

            if response.status_code == 200 and response.json()["IsSuccess"] == True:
                result = response.json()
                print(f'lead pushed api pushed IsSuccess status {result["IsSuccess"]}')
                if result["IsSuccess"]:
                    lead_pushed = 1
                else:
                    lead_pushed = 0
            else:
                raise Exception(1)
            print(f"lead pushed api connected status {response.status_code}")
        except Exception as e:
            if cnt <= 3:
                row['Email'] = 'dummy@dummy.in'
                lead_pushed = self.push_lead_im(row, cnt)
            else:
                log_data = [{"LogDetails" : f'Smartdost MX API Error with {str(e)} for UUID = {str(row["UUID"])}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T4", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)

        return lead_pushed


    def push_lead_ce(self, row, cnt):
        try:
            cnt += 1
            lead_pushed = 0
            #import pdb;pdb.set_trace()
            url = self.api_di_ce["URL"]
            payload = {
                "userID" : int(self.ce_headers["UserID"]),
                "InputOfCustomer" : {
                    "LeadID": str(row["LeadID"]),
                    "CustomerName": str(row["FullName"]),
                    "CustomerMobileNumber": str(row["PhoneNumber"]),
                    "CustomerEmailID": str(row["Email"]),
                    "StoreCode": str(row["StoreCode"]),
                    "CampaignName": str(row["MasterCampaign"]),
                    "LeadCreationDate": str(row["LeadCreationTimestamp"]),
                    "LeadCategory": str(row["Category"]),
                    "RequestType": 2, 
                    "PinCode": str(row["PinCode"]), 
                    "Uuid": str(row["UUID"]),
                    "LeadDivison": str(row["Division"]),
                    "Score": row["LeadScore"],
                    "Tier" : row["CustomerTier"]
                    }
                }
            
            try:
                with eventlet.Timeout(PushDataSmartDost.TIME_OUT):
                    response = requests.request("POST", url, headers=self.ce_headers, data= json.dumps(payload))
            
            except TimeoutError as e:
                log_data = [{"LogDetails" : f'Smartdost CE PUSH API - Pipe  TIMEOUT Error {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T4", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                raise TimeoutError("Timeout Error - Smartdost - CE Push API")
            
            if response.status_code == 200 and response.json()["IsSuccess"] == True :
                result = response.json()
                print(f'lead pushed api pushed IsSuccess status {result["IsSuccess"]}')
                if result["IsSuccess"]:
                    lead_pushed = 1
                else:
                    lead_pushed = 0
            else:
                raise Exception(f"Error in API for {response.json()}")
            print(f"lead pushed api connected status {response.status_code}")
        except Exception as e:
            if cnt <= 3:
                row['Email'] = 'dummy@dummy.in'
                lead_pushed = self.push_lead_ce(row, cnt)
            else:
                log_data = [{"LogDetails" : f'Smartdost CE API Error with {str(e)} for UUID= {str(row["UUID"])}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T4", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
        return lead_pushed
        
            

    def run(self):
        #import pdb;pdb.set_trace()
        try:
            df_leads = self.fetch_leads_db()
            success_uuid_li = []
            if df_leads.empty:
                log_data = [{"LogDetails" : f'No New Leads Found for Smartdost.......', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T4", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                raise Exception(f'No New Leads Found for Smartdost.....')

            print(f'Found {len(df_leads)} for SmartDost')

            tiering_query = f"SELECT Division, Type, Score, SdTier FROM {PushDataSmartDost.SD_TIERING_TABLE} WITH (NOLOCK)"
            self.tier_mapping_df = self.dbops.execute_dataframe_query(self.connection,tiering_query)

            if self.tier_mapping_df.empty:
                raise Exception("Error in Fetching SDTierTable")


            df_leads["LeadSuccess"] = 0
            df_leads.loc[df_leads['PinCode'] == 'NA' ,"PinCode"] = '0'
            df_leads.loc[df_leads['Email'] == 'NA' , 'Email'] = 'dummy@dummy.in'
            
            for index, row in df_leads.iterrows():

                row['FullName'] = PushDataSmartDost.REGEX.sub(' ', row['FullName']).strip()
                if len(row['FullName']) == 0:
                    row['FullName'] = "UNKNOWN"


                row = self.apply_tiering(row)

                if row["Division"] == 'CE':
                    success = self.push_lead_ce(row, 0)

                elif row["Division"] == 'IM':
                    success = self.push_lead_im(row, 0)
     
                    
                if success:
                    success_uuid_li.append(row["UUID"])

                if ((index+1) % 5) == 0:
                    if success_uuid_li:
                        #import pdb;pdb.set_trace()
                        LeadPushedTime = datetime.datetime.now(self.IST).strftime("%Y-%m-%d %H:%M:%S")
                        tmp_uuid_tup = f'({success_uuid_li[0]})' if len(success_uuid_li) == 1 else tuple(success_uuid_li)
                        query = f"UPDATE dbo.LeadsAssignment SET LeadPushed = 1 , LeadPushedTimestamp = '{LeadPushedTime}' WHERE UUID IN {tmp_uuid_tup}"
                        self.dbops.execute_query(self.connection, query)
                        print(f"Pushed {len(success_uuid_li)} uuids successfully at index {index+1}")
                    else:
                        print(f"Pushed {len(success_uuid_li)} uuids successfully at index {index+1}")
                    success_uuid_li = []
               
                time.sleep(0.7)
            log_data = [{"LogDetails" : f'Successfully Pushed to Smartdost', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T4", "Status" : "SUCCESS"}]
            self.log_ops.save_logs(log_data)
        except Exception as e:
            log_data = [{"LogDetails" : f'Error in Smartdost Push {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T4", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f"Error in smartdost push pipe run {str(e)}")
       
        finally:
            if success_uuid_li:
                LeadPushedTime = datetime.datetime.now(self.IST).strftime("%Y-%m-%d %H:%M:%S")
                tmp_uuid_tup = f'({success_uuid_li[0]})' if len(success_uuid_li) == 1 else tuple(success_uuid_li)
                query = f"UPDATE dbo.LeadsAssignment SET LeadPushed = 1 , LeadPushedTimestamp = '{LeadPushedTime}' WHERE UUID IN {tmp_uuid_tup}"
                try:
                    self.dbops.execute_query(self.connection, query)
                    print(f"Pushed {len(success_uuid_li)} uuids successfully at index {index+1}.....")
                except Exception as e:
                    log_data = [{"LogDetails" :f'Error in Updating flag in LeadsAssignment {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T4", "Status" : "ERROR"}]
                    self.log_ops.save_logs(log_data)

            self.connection.close()


if __name__ == "__main__":
    obj = PushDataSmartDost()
    obj.run()