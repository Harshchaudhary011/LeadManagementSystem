import datetime, pytz, timedelta, os, json
import pandas as pd
import numpy as np
from utils.db_ops import DBOps


class ETLOps(object):

    DEDUPE_CONF_FILE = os.path.join(os.path.join('configs', 'etl_configs'), 
                                    'dedupe_conf.json')
    ALL_LEADS_DATATYPE_MAPPER_CONF = os.path.join(os.path.join('configs', 'db_configs'), 
                                'all_leads_datatype_mapping.json')
    CAMPAIGN_MASTER_TABLE = "Campaign_Master"

    def __init__(self):
        self.db_ops = DBOps()
        self.connection = self.db_ops.create_connection()
        self.connection.close()
        di = self.read_json(ETLOps.ALL_LEADS_DATATYPE_MAPPER_CONF)
        
        self.rename_all_leads_fields = {f"{key}_y" : key for key in di}

    def read_json(self, filepath):
        if not os.path.exists(filepath):
            raise Exception(f"File is Missing at {filepath}")
        with open(filepath, "r") as rj:
            di = json.load(rj)
        return di

    

    def populate_campaign_field(self,campaign_mapper ,df):
        #import pdb;pdb.set_trace()
        #campaign_mapper = self.campaign_master.set_index("Campaign").to_dict("index")
        for i, row in df.iterrows():
            tmp_di = campaign_mapper#.get(row["Campaign"], {})
            if df.loc[i, "Category"] == 'NA':
                df.loc[i, "Category"] = tmp_di.get("Category", "NA")
            if df.loc[i,"Product"] == 'NA':
                df.loc[i, "Product"] = tmp_di.get("Product", "NA") 
            if df.loc[i,"Division"] == 'NA':
                df.loc[i, "Division"] = tmp_di.get("Division", "NA")

        df["Pending_For_Action"] = 1
        return df 

    # def dedupe_dataframe(self)
        
    def dedupe_dataframe(self, connection, df, dedupe_fields , days):
        try:
            #dedupe_conf_di = self.read_json(ETLOps.DEDUPE_CONF_FILE)
            #import pdb;pdb.set_trace()
            for fi in dedupe_fields:
                df[fi.replace("_DUMMY", "")] = df[fi.replace("_DUMMY", "")].str.strip()
                df[fi] = df[fi.replace("_DUMMY", "")].str.lower().str.strip()
            
            dedupe_on_days = days
            IST = pytz.timezone('Asia/Kolkata')
            df = df[~(df[dedupe_fields].isna().all(1))]
            
            fill_na_val_di = { fi : "" for fi in dedupe_fields}
            df = df.fillna(value = fill_na_val_di)
            
            df = df.drop_duplicates(subset = dedupe_fields)
            print(f"Within df dedupe {df.shape}")
            
            dedupe_start_date = (datetime.datetime.now(IST) - datetime.timedelta(days=dedupe_on_days)).strftime("%Y-%m-%d")
            #query = f"SELECT * FROM dbo.AllLeads WITH (NOLOCK) WHERE LeadCreationTimestamp >= '{dedupe_start_date}'"
            query = f"SELECT A.Platform, A.LeadID, A.RowID, A.UUID, A.FullName, A.Email, A.PhoneNumber,\
                        A.City, A.PinCode, A.CampaignID, A.MasterCampaign, A.Campaign, A.Division, A.Category,\
                        A.Product, A.Consent, A.LeadQuality, A.LeadCreationTimestamp, A.LeadModifiedTimestamp,\
                        A.PendingForAction, A.IngestionTimestamp, A.ShopperCategorySelected,\
                        A.ShopperChannelPreference, A.SelectedStore,\
                        A.GCDMSystemToBeFollow \
                        FROM  dbo.AllLeads as A WITH (NOLOCK) \
                        WHERE LeadCreationTimestamp >= '{dedupe_start_date}'"
            
            lead_df = self.db_ops.execute_dataframe_query(connection, query)
            for fi in dedupe_fields:
                lead_df[fi] = lead_df[fi.replace("_DUMMY", "")].str.lower().str.strip()
            #import pdb;pdb.set_trace()
            lead_df = lead_df[dedupe_fields]
            dedupe_df = lead_df.merge(df, on = dedupe_fields, how="outer", indicator=True)\
                            .query('_merge == "right_only"')
            #drop_all_leads_fields = [f"{col}_x" for col in lead_df ]
            dedupe_df = dedupe_df.drop(["_merge"], axis = 1)
            cols_to_select = [col for col in dedupe_df.columns if col not in dedupe_fields]
            print(f"Table df dedupe {dedupe_df.shape}")
            dedupe_df = dedupe_df[cols_to_select]
            dedupe_df.rename(columns = self.rename_all_leads_fields, inplace=True)
        except Exception as e:
            raise Exception(f"Error in dedupe data for source {e} ")
        return dedupe_df
    
    def convert_datatypes(self, df, date_format):
        try:
            di = self.read_json(ETLOps.ALL_LEADS_DATATYPE_MAPPER_CONF)
            for column_name in di:
                if di[column_name] == "datetime":
                    df[column_name] = pd.to_datetime(df[column_name], format=date_format)
                else:
                    df[column_name] = df[column_name].astype(di[column_name])
        except Exception as e:
            raise Exception(f"Error in data type conversion {e}")
        return df 
    
    
    def phone_correction(self, val):
        if len(val) == 10:
            val = "+91" + val 
        elif len(val) == 13 and (not val.startswith("+91")):
            val = ""
        elif len(val) == 12 and val.startswith("91"):
            val = "+" + val
        return val

    def data_cleaning(self, df):
        df["PhoneNumber"] = df["PhoneNumber"].fillna("")
        df["PhoneNumber"] = df["PhoneNumber"].astype('str').str.replace("+", "").str.replace(".0", "").astype('str')
        df["PhoneNumber"]  = df["PhoneNumber"].apply(lambda x : self.phone_correction(x))
        df["LeadQuality"] = np.where(df["PhoneNumber"].str.len() == 13, 1, 0)
        
        #df[df["LeadQuality"] == 0]["PhoneNumber"].value_counts() #VALIDATE LEAD QUALITY LOGIC
        return df 
        
