import requests ,eventlet
import json
import pandas as pd
import numpy as np 
from utils.db_ops import DBOps
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import hashlib
from Crypto.Util import Padding
import base64
import pyDes
import binascii
import pandas as pd
import time
import os ,pytz, json, datetime
from utils.db_ops import DBOps
from utils.log_ops import LogOps


class GCDMData(object):
    ALL_LEADS_TABLE  = "dbo.AllLeads"
    CAMPAIGN_DETAILS = "dbo.CampaignDetails"
    COLS_LI = ['LeadID','UUID','LeadCreationTimestamp','IngestionTimestamp','ModifiedTimestamp',
    'PhoneNumber','Email','Division','Category','Product','LeadScore', 'CustomerTier', 'PreviousBrandName', 'PendingForAction']
    IST = pytz.timezone('Asia/Kolkata')
    API_BATCH_SIZE = 10 #290
    TIMEOUT = 15

    def __init__(self):
        self.db_ops = DBOps()
        self.connection = self.db_ops.create_connection()
        api_creds_di = self.read_json("lead_scoring/api_credentials.json")
        profile = "USER"  
        self.username = api_creds_di[profile]["username"]
        self.password = api_creds_di[profile]["password"]
        self.log_ops = LogOps()

    def read_json(self, filepath):
        if not os.path.exists(filepath):
            log_data = [{"LogDetails" : f'File Does not exists at {filepath}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T2", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f"File Does not exists at {filepath}")

        with open(filepath, "r") as rj:
            di = json.load(rj)
        return di

    def pad(self,st , block_size):
        padding = ((block_size-len(st)) % block_size)
    
        return st + bytes([padding]*padding)

    def encrypt_data(self,data_to_encrypt):
        try:
            iv = "CCE@GG@dm1n99887"
            key = 'CCE@MG@dm1n99887'
            key_en = bytes(key,'utf-8')
            iv_en = bytes(iv,'utf-8')
            cipher = AES.new(key_en,AES.MODE_CBC,iv_en)
            encrypt_data = cipher.encrypt(self.pad(data_to_encrypt.encode(), AES.block_size))
            cipher_text_en = base64.b64encode(encrypt_data).decode()
            hex_string = cipher_text_en.encode('utf-16le').hex().upper()
        except Exception as e:
            log_data = [{"LogDetails" : f"Error in encrypt data {str(e)}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T2", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f"Error in encrypt data {e}")
        return hex_string

    def get_token(self):
        try:
            token = ""
            url = "https://mygalaxy-samsung.com/CustomerTierAPI/token"
            payload = f'username={self.username}&password={self.password}&grant_type=password'
            headers = {
                    'Content-Type': 'application/x-www-form-urlencoded'
                    }
        
            try:
                with eventlet.Timeout(GCDMData.TIMEOUT):
                    response = requests.request("POST", url, headers=headers, data=payload)
            #print(response)
            except TimeoutError as e:
                log_data = [{"LogDetails" : f'GCDM API - Pipe - GET TOKEN API - TIMEOUT Error {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T2", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                raise TimeoutError(f"Timeout Error in Fetching GCDM -TOKEN")
            if response.status_code == 200:
                try:
                    jsn = response.json()
                    with open('lead_scoring/gcdm_token.json','w') as wj:
                        json.dump(jsn,wj) 
                    token = jsn['access_token']
                    #print(token,"token is")
                    
                except Exception as e:
                    log_data = [{"LogDetails" : f'Error in token generation {str(e)} status code {response.status_code}', 
                    "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                    "SeverityLevel" : 4, "Stage" : "T2", "Status" : "ERROR"}]
                    self.log_ops.save_logs(log_data)
                    raise Exception(f"exception in jsn  response {e}")
            else:
                
                filepath = 'lead_scoring/gcdm_token.json'
                if not os.path.exists(filepath):
                    log_data = [{"LogDetails" : f"File Does not exists at {filepath}", 
                    "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                    "SeverityLevel" : 4, "Stage" : "T2", "Status" : "ERROR"}]
                    self.log_ops.save_logs(log_data)

                    raise Exception(f"File Does not exists at {filepath}")
                with open(filepath,'r') as rj:
                    jsn=json.load(rj) 
                    token = jsn["access_token"]
        except Exception as e:
            log_data = [{"LogDetails" : f"Error in GCDM get token API {str(e)}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T2", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f"Error in GCDM get token API {e}")
        return token
            



    def get_data(self, email, phone, division, uuid):
        try:
            url = "https://mygalaxy-samsung.com/CustomerTierAPI/api/GetCustRecommendationTieringDetails"
            
            headers = {
                    'Authorization': 'Bearer' + ' ' + self.token,
                    'Content-Type': 'application/json'
                    }
            payload = json.dumps([{
                "email": str(email),
                "mobileno": str(phone),  
                "division": division,
                "uuid": str(uuid)
            }])
            self.count += 1
            try:
                with eventlet.Timeout(GCDMData.TIMEOUT):

                    response = requests.request("POST", url, headers=headers, data=payload)
            #print(response)
            except TimeoutError as e:
                log_data = [{"LogDetails" : f'GCDM Get Score API - Pipe TIMEOUT Error {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T2", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                raise TimeoutError("Timeout Error - Get Score API -GCDM")
            
            if response.status_code == 200:
                jsn = response.json()
                if jsn["status"] == 200:
                    score = jsn['result']['Score']
                    tier = jsn['result']['Tier']
                    if score == 'null':
                        score = 'UNKNOWN'
                    if tier == 'null':
                        tier = 'UNKNOWN'
                else:
                    score = 'NA'
                    tier = 'NA'
            elif response.status_code == 401 and self.count <= 3:
                self.token  = self.get_token()
                score, tier = self.get_data(email, phone, division,uuid)
            elif self.count > 3:
                log_data = [{"LogDetails" : f"Error in gcdm get data api {str(e)} retry count {str(self.count)}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T2", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                score = 'NA'
                tier = 'NA'
            else:
                log_data = [{"LogDetails" : f"Error in gcdm get data api status code {str(response.status_code)}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T2", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                score = 'NA'
                tier = 'NA'
        except Exception as e:
            log_data = [{"LogDetails" : f"Error in gcdm get data api {str(e)}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T2", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f"Error in gcdm get data api {e}")
            
        return score,tier


    def read_data_from_sql(self):
        try:
            query = f"SELECT A.Platform, A.LeadID, A.RowID, A.UUID, A.FullName, A.Email, A.PhoneNumber,\
                        A.City, A.PinCode, A.CampaignID, A.MasterCampaign, A.Campaign, A.Division, A.Category,\
                        A.Product, A.Consent, A.LeadQuality, A.LeadCreationTimestamp, A.LeadModifiedTimestamp,\
                        A.PendingForAction, A.IngestionTimestamp, A.ShopperCategorySelected,\
                        A.ShopperChannelPreference, A.SelectedStore,\
                        A.GCDMSystemToBeFollow \
                        FROM {GCDMData.ALL_LEADS_TABLE} AS A  WITH (NOLOCK)\
                        JOIN {GCDMData.CAMPAIGN_DETAILS} AS B  WITH (NOLOCK)\
                        ON A.MasterCampaign = B.CampaignName\
                        WHERE B.isActive = 'Y' AND B.LeadDistributionFlag = 'Y' AND  A.PendingForAction = 1 AND A.LeadQuality = 1 order by A.IngestionTimestamp asc" #added Leaddistribution flag here
                        #removed GCDMSystemToBeFollow fetchiung from CampaignDetails
            df = self.db_ops.execute_dataframe_query(self.connection, query)
        except Exception as e:
            log_data = [{"LogDetails" : f"Error in read_data_from_sql {str(e)}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T2", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f"Error in read_data_from_sql {e}")
        return df

    def get_batch_data(self, df):
        try:
            skip_cols = ["Score", "Tier", "email", "mobileno", "uuid", "division","ApiDivision"]
            url = "https://mygalaxy-samsung.com/CustomerTierAPI/api/GetCustRecommendationTieringDetails"
            headers = {
                    'Authorization': 'Bearer' + ' ' + self.token,
                    'Content-Type': 'application/json'
                    }
            api_df = df[["email", "mobileno", "division", "uuid"]]
            payload = api_df.to_dict("records")
            self.count += 1
            
            response = requests.request("POST", url, headers=headers, data= json.dumps(payload))
            print(f"API resp status {response.status_code }")  #response.json()
            
            if response.status_code == 200:
                
                jsn = response.json()
               # print(f"API result {jsn}")
                if jsn["status"] == '200' and jsn["result"]:
                    resp_df = pd.DataFrame(jsn['result'])
                    resp_df["UUID"] = resp_df["UUID"].astype('int')
                    resp_df = resp_df[["UUID", "Score", "Tier"]]
                    resp_df = resp_df.replace(r'^\s*$', np.nan, regex=True)
                    
                    resp_df["Score"] = resp_df["Score"].fillna("UNKNOWN") 
                    resp_df["Tier"] = resp_df["Tier"].fillna("UNKNOWN")
                    
                    df = pd.merge(df, resp_df, on = 'UUID', how = 'inner')
                    
                    df["LeadScore"] = df["Score"]
                    df["CustomerTier"] = df["Tier"]
                    df = df[[col for col in df.columns if col not in skip_cols]]
                else:
                    df["LeadScore"] = "NA"
                    df["CustomerTier"] = 'NA'
            elif response.status_code == 401 and self.count <= 3:
                self.token  = self.get_token()
                df = self.get_batch_data(df)
            elif self.count > 3:
                log_data = [{"LogDetails" : f"Error in gcdm GetDataAPI  retry count {str(self.count)} and status {str(response)} and Response {str(response.json())}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T2", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                df["LeadScore"] = "NA"
                df["CustomerTier"] = 'NA'
                raise Exception(f"Error in GCDM get batch data api retry count {str(self.count)}")
            else:
                log_data = [{"LogDetails" : f"Error in gcdm get data api status code {str(response.status_code)}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T2", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                df["LeadScore"] = "NA"
                df["CustomerTier"] = 'NA'
                raise Exception("Error in GCDM get batch data api")
        except Exception as e:
            df["LeadScore"] = "NA"
            df["CustomerTier"] = 'NA'
            df = df[[col for col in df.columns if col not in skip_cols]]
            log_data = [{"LogDetails" : f"Error in GCDM get batch data api {str(e)}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T2", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
        return df 

    def get_score(self, df):
        try:

            df["LeadScore"] = 'NA'
            df["CustomerTier"] = 'NA'
            df['PreviousBrandName'] = 'NA'
            #gcdm_df = df[df["GCDMSystemToBeFollow"] != 'No']
            #non_gcdm_df = df[df["GCDMSystemToBeFollow"] == 'No']

            gcdm_df = df[df["GCDMSystemToBeFollow"] != "No"]
            non_gcdm_df = df[df["GCDMSystemToBeFollow"] == "No"]
            
            #gcdm_df["ApiDivision"] = gcdm_df["Division"]
            # for index,row in gcdm_df.iterrows():
            #     if row["ApiDivision"] == 'Others':
            #         gcdm_df.loc[index , "ApiDivision"] = row["GCDMSystemToBeFollow"]
            #gcdm_df.loc[gcdm_df['ApiDivision'] == 'MX' ,'ApiDivision'] = 'IM'
            # gcdm_df = df[(df["IsLeadPropensityAvailable"] == "Y") | (df["IsLeadTieringAvailable"] == "Y")]
            # non_gcdm_df = df[(df["IsLeadPropensityAvailable"] == "N") & (df["IsLeadTieringAvailable"] == "N")]
            del df 
            non_gcdm_df["LeadScore"] = "UNKNOWN"
            non_gcdm_df["CustomerTier"] = "UNKNOWN"
            non_gcdm_df["PreviousBrandName"] = "NA"

            if gcdm_df.empty():
                self.token = self.get_token()
                gcdm_df["email"] = gcdm_df["Email"].apply(lambda x : self.encrypt_data(x))
                gcdm_df["mobileno"] = gcdm_df["PhoneNumber"].apply(lambda x : self.encrypt_data(x))
                gcdm_df["division"] = gcdm_df["GCDMSystemToBeFollow"]
                gcdm_df["uuid"] = gcdm_df["UUID"].astype('str')
                #gcdm_df = gcdm_df.head(2000)
                df_li = []

                for  k, g_df in gcdm_df.groupby(np.arange(len(gcdm_df)) // GCDMData.API_BATCH_SIZE):
                    self.count = 0
                    time.sleep(2)
                    g_df = self.get_batch_data(g_df)
                    df_li.append(g_df)
                df_li.append(non_gcdm_df)
                df = pd.concat(df_li)
        except Exception as e:
            log_data = [{"LogDetails" : f"Error in GCDM get Score func {str(e)}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T2", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
        return df

    def runner(self):
        try:
            #import pdb;pdb.set_trace()
            df = self.read_data_from_sql()

            if df.empty:
                raise Exception("No New Leads Found for Fetching GCDM Score")
        
            print(f'Fetched {len(df)} leads for scoring from ALl Leads')
            df.loc[df['Division'] == 'MX' ,'Division'] = 'IM'
            
            df = self.get_score(df)
            df = df.drop_duplicates(subset=['UUID'])
   
            df = df[(df['LeadScore'] != 'NA') & (df["CustomerTier"] != 'NA')]
            df["PendingForAction"] = 1
            df["ModifiedTimestamp"] = datetime.datetime.now(GCDMData.IST).strftime("%Y-%m-%d %H:%M:%S")
            df = df[GCDMData.COLS_LI]
            self.db_ops.ingest_dataframe_to_sql(self.connection, "CustomerScoreGCDM", df)
            uuid_li = df["UUID"].tolist()
                
            batch_size = 10000
            buckets = list(range(0, len(uuid_li) + batch_size, batch_size))
            buckets = [buckets[i:i+2] for i,e in enumerate(buckets) if len(buckets[i:i+2]) == 2]

            for batch in buckets:
                tmp_uuid_li = uuid_li[batch[0] : batch[1]]
                tmp_uuid_tup = f'({tmp_uuid_li[0]})' if len(tmp_uuid_li) == 1 else tuple(tmp_uuid_li)
                query = f"UPDATE {GCDMData.ALL_LEADS_TABLE} SET PendingForAction = 0 WHERE UUID IN {tmp_uuid_tup}"
                self.db_ops.execute_query(self.connection, query)
            
            log_data = [{"LogDetails" : f'GCDM Ingestion Step Completed for {len(uuid_li)} leads...', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T2", "Status" : "SUCCESS"}]
            self.log_ops.save_logs(log_data)
        except Exception as e:
            log_data = [{"LogDetails" : f"GCDM Ingestion Step Error {str(e)}...", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T2", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            print(f"Error in run - {e}")
        
        finally:
            self.connection.close()
   


if __name__ == "__main__":
    obj = GCDMData()
    obj.runner()


