import pandas as pd
import os ,pytz, json,datetime
from utils.etl_ops import ETLOps
from utils.db_ops import DBOps
import requests , base64 , hashlib, os, json, datetime, pytz, pyDes, chardet






class PullData(object):
    SAMSUNG_ALLLEADS_MAPPER_CONF = os.path.join(os.path.join('configs', 'etl_configs'), 
                                   'samsung_columns_name_mapping.json') 
    KEY = "!*&@9876543210"
    IV = [18, 52, 86, 120, 144, 171, 205, 239]
    # COL_MAPPER_DI = {
    #                     "Campaign" : "Campaign",
    #                     "LeadID" : "LeadID",
    #                     "LeadCreationTimestamp" : "LeadCreationTimestamp",
    #                     "FullName" : "FullName",
    #                     "Email" : "Email",
    #                     "PhoneNumber" : "PhoneNumber",
    #                     "City" : "City",
    #                     "PinCode" : "PinCode",
    #                     "CampaignID" : "CampaignID",
    #                     "Platform" : "Platform",
    #                     "Product" : "Product",
    #                     "Division" : "Division",
    #                     "Category" : "Category",
    #                     "Consent" : "Consent",
    #                     "Pending_For_Action" : "Pending_For_Action"
    #                 }
    
    COL_MAPPER_DI = {    "AppName" : "Campaign",
    "LeadId" : "LeadID",
    "InsertedOn" : "LeadCreationTimestamp",
    "Name" : "FullName",
    "EmailId" : "Email",
    "Mobile" : "PhoneNumber",
    "CityName" : "City",
    "PinCode" : "PinCode",
    "Consent" : "Consent",
    "DMSCode" : "SelectedStore",
    "Requirement" :"ShopperCategorySelected",
    "FormId":"FormID"}
    TABLE_NAME = "dbo.LMSUserRegistration"
    SEC_DETAILS_TABLE = 'dbo.SECDealerMapping'
    FORM_ID = '1380405055867708'
    COLS_TO_DECRYPT = ["Email", "PhoneNumber"] 
   # DEDUPE_PHONE_FIELDS = ['PhoneNumber_DUMMY']
   # DEDUPE_DAYS = 7

    
    def __init__(self):
        self.IST = pytz.timezone('Asia/Kolkata')
        self.fields_add_on_dict = {}
        self.etl_ops = ETLOps()
        self.db_ops = DBOps()
        self.connection = self.db_ops.create_connection()
        self.fields_add_on_dict = {
             'Division':'NA',
            'Product':'NA','Category' :'NA','ShopperChannelPreference' :'NA'
            }


    def read_json(self, filepath):
        if not os.path.exists(filepath):
            raise Exception(f"File is Missing at {filepath}")
        with open(filepath, "r") as rj:
            di = json.load(rj)
        return di
   
    def make_final_df(self, df):
        
        for col in self.fields_add_on_dict:
            df[col] = self.fields_add_on_dict[col]   
        return df
    
    
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
            #self.log_ops.save_logs(log_data)
            raise Exception(f"Error in decrypt des {e} ")
        return decrypted 
    

    def decrypt_columns(self, leads_df):
        for col in PullData.COLS_TO_DECRYPT:
            leads_df[col] = leads_df.apply(lambda x : self.decrypt_des(col, x), axis=1)
        return leads_df

    def rename_columns(self,df):
        #import pdb;pdb.set_trace()
        di = self.read_json(PullData.SAMSUNG_ALLLEADS_MAPPER_CONF)
        di['Source'] = 'Platform'
        di['FormId'] = 'FormID'
        
        df = df[di.keys()]
        df.rename(columns=di,inplace=True)
        return df
            
    def read_table_data(self):
        query = f"SELECT * FROM {PullData.TABLE_NAME}"
        # query = f"SELECT Platform, LeadID, RowID, UUID, FullName, Email, PhoneNumber, City,\
        #         PinCode, CampaignID, MasterCampaign, Campaign, Division, Category,\
        #         Product, Consent, LeadQuality, LeadCreationTimestamp, LeadModifiedTimestamp,\
        #         Pending_For_Action, IngestionTimestamp, ShopperCategorySelected,\
        #         ShopperChannelPreference, SelectedStore, Pending_For_Action FROM {PullData.TABLE_NAME}"

        df = self.db_ops.execute_dataframe_query(self.connection, query)
        return df

    def make_final_df(self, df):
        for col in self.fields_add_on_dict:
            df[col] = self.fields_add_on_dict[col]   
        return df 
    
    def set_sec_flag(self,df):
        query = f"SELECT DISTINCT(SECMobile) as PhoneNumber FROM {PullData.SEC_DETAILS_TABLE}  WITH (NOLOCK) WHERE SECMobile IS NOT NULL and  \
            StoreStatus = 'Active' "
        sec_details_df = self.db_ops.execute_dataframe_query(self.connection ,query)
        sec_details_df = self.etl_ops.data_cleaning(sec_details_df)[['PhoneNumber']]
        df = df.merge(sec_details_df,on = 'PhoneNumber',how = 'left',indicator = True )
        df.loc[df['_merge'] == 'both', "LeadQuality"] = 2 
        return df

    def run(self):
        try:
            import pdb;pdb.set_trace()
            
            df = self.read_table_data()
            
            df['LeadId'] = 'NA'
            df['FormId'] = PullData.FORM_ID
            df["Consent"] = df["NeedReminderMail"].apply(lambda x : "YES" if x == "1" else "NO")      
            df = self.rename_columns(df)     
            df = self.decrypt_columns(df)
           
            print(f"Total Rows : {df.shape}")
            
            for col in df.columns:
                if col == "LeadCreationTimestamp":
                    df[col] = pd.to_datetime(df[col],format = '%m/%d/%Y %H:%M:%S %p')
                    continue
                df[col] = df[col].astype('str')
                df[col] = df[col].fillna("NA")
                df[col] = df[col].replace(r'^\s*$', 'NA', regex=True)
            df['IngestionTimestamp'] = datetime.datetime.now(self.IST).strftime("%Y-%m-%d %H:%M:%S")

            #df = self.etl_ops.data_cleaning(df)
            #df = self.etl_ops.dedupe_dataframe(self.connection, df, PullData.DEDUPE_PHONE_FIELDS,PullData.DEDUPE_DAYS)
            #df = self.etl_ops.populate_campaign_field(df)
            df = self.make_final_df(df)
           # df = self.etl_ops.convert_datatypes(df, date_format='%Y-%m-%d %H:%M:%S')
            #df['IngestionTimestamp'] = datetime.datetime.now(self.IST).strftime("%Y-%m-%d %H:%M:%S")
            import pdb;pdb.set_trace()
            self.db_ops.ingest_dataframe_to_sql(self.connection, "dbo.RawLeads", df)
        except Exception as e:
            raise Exception(f"Error in manual pipe {e}")
        finally:
            self.connection.close()



if __name__ =="__main__":
    obj = PullData()
    obj.run()
