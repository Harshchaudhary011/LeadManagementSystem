import datetime,pytz
import pandas as pd
import numpy as np
from utils.etl_ops import ETLOps 
from utils.db_ops import DBOps
from utils.log_ops import LogOps


class ProcessRaw(object):

    SRC_TABLE = 'dbo.RawLeads'
    DEST_TABLE = 'dbo.AllLeads'
    CAMPAIGN_MASTER_TABLE = 'dbo.CampaignDetails'
    Sub_Campaign_Mapping = 'dbo.SubCampaignMaster'
    SEC_DETAILS_TABLE = 'dbo.SECDealerMapping'
    DEDUPE_PHONE_FIELDS = ['PhoneNumber_DUMMY' ,'Division_DUMMY']
    Dedupe_Field_Camp = ['PhoneNumber_DUMMY','MasterCampaign_DUMMY']

    FINAL_COLS_DEST_TABLE = ['Platform','RowID','LeadID','FullName','Email','PhoneNumber','City','PinCode','Campaign','CampaignID','Division','GCDMSystemToBeFollow','Category','Product','MasterCampaign','Consent',
                             'ShopperCategorySelected','ShopperChannelPreference','SelectedStore','LeadCreationTimestamp','IngestionTimestamp','LeadQuality','PendingForAction']


    def __init__(self):
        self.db_ops = DBOps()
        self.connection = self.db_ops.create_connection()
        self.etl_ops = ETLOps()
        self.log_ops = LogOps()
        self.IST = pytz.timezone('Asia/Kolkata')

    def dedupe_phone_days(self):
        dedupe_days_phone_query = f"SELECT TOP(1) DedupeNoOfDaysPhoneCategory FROM {ProcessRaw.CAMPAIGN_MASTER_TABLE}  WITH (NOLOCK) WHERE isActive = 'Y' ORDER BY CreatedDate DESC"
        dedupe_phone_df =   self.db_ops.execute_dataframe_query(self.connection ,dedupe_days_phone_query)
        dedupe_phone_days = dedupe_phone_df['DedupeNoOfDaysPhoneCategory'].iloc[0]
        return dedupe_phone_days
      
    def populate_campaign_field(self ,df):
        query = f"SELECT CampaignName,Business,CategoryName,ProductName,GCDMSystemToBeFollow FROM \
             {ProcessRaw.CAMPAIGN_MASTER_TABLE} WITH (NOLOCK) WHERE IsActive = 'Y' "
        campaign_config_df = self.db_ops.execute_dataframe_query(self.connection,query)
        campaign_config_df.replace("MX" , "IM" , inplace = True)
        #campaign_config_df.reset_index(inplace=True ,drop=True)
        campaign_mapper = campaign_config_df.set_index("CampaignName").to_dict("index")

        for i, row in df.iterrows():
            tmp_di = campaign_mapper.get(row["MasterCampaign"], {})
            if df.loc[i, "Category"] == 'NA' or '':
                df.loc[i, "Category"] = tmp_di.get("CategoryName", "NA")
            if df.loc[i,"Product"] == 'NA' or '':
                df.loc[i, "Product"] = tmp_di.get("ProductName", "NA") 
            if df.loc[i,"Division"] == 'NA' or '':
                df.loc[i, "Division"] = tmp_di.get("Business", "NA")
            if df.loc[i , "GCDMSystemToBeFollow"] == 'NA' or '':
                df.loc[i , "GCDMSystemToBeFollow"] = tmp_di.get("GCDMSystemToBeFollow")
            
        df["PendingForAction"] = 1
        return df 


    def set_sec_flag(self,df):
        query = f"SELECT DISTINCT(SECMobile) as PhoneNumber FROM {ProcessRaw.SEC_DETAILS_TABLE}  WITH (NOLOCK) WHERE SECMobile IS NOT NULL and  \
            StoreStatus = 'Active' "
        sec_details_df = self.db_ops.execute_dataframe_query(self.connection ,query)
        sec_details_df = self.etl_ops.data_cleaning(sec_details_df)[['PhoneNumber']]
        df = df.merge(sec_details_df,on = 'PhoneNumber',how = 'left',indicator = True )
        df.loc[df['_merge'] == 'both', "LeadQuality"] = 2 
        return df



    def run(self):
        try:
            #import pdb;pdb.set_trace()
            query = f"SELECT RowID, Platform, LeadID, FullName, Email, PhoneNumber, City, PinCode, FormID,\
            Campaign, Division, Category, Product, Consent, ShopperCategorySelected,\
            ShopperChannelPreference, SelectedStore, LeadCreationTimestamp, IngestionTimestamp\
            FROM {ProcessRaw.SRC_TABLE}  WITH (NOLOCK) WHERE IngestionTimestamp > (SELECT \
            CASE WHEN MAX(IngestionTimestamp) IS NULL THEN  cast('01-01-2000 00:00' as date) ELSE MAX(IngestionTimestamp) END from \
            {ProcessRaw.DEST_TABLE}) and FormID in \
            (SELECT B.FormID as Campaign \
            FROM {ProcessRaw.CAMPAIGN_MASTER_TABLE} A WITH (NOLOCK) \
            join {ProcessRaw.Sub_Campaign_Mapping} B  WITH (NOLOCK) on \
            A.CampaignName = B.CampaignName where A.IsActive = 'Y' and B.IsActive = 'Y')"   #earlier was joining on campaign id

            # query = f"SELECT RowID, Platform, LeadID, FullName, Email, PhoneNumber, City, PinCode, FormID,\
            # Campaign, Division, Category, Product, Consent, ShopperCategorySelected,\
            # ShopperChannelPreference, SelectedStore, LeadCreationTimestamp, IngestionTimestamp\
            # FROM {ProcessRaw.SRC_TABLE} WHERE Campaign = 'Neo QLED Launch 2023' and \
            # LeadID not in (SELECT LeadID from \
            # {ProcessRaw.DEST_TABLE})"
            # and FormID in \
            # (SELECT B.FormID as Campaign \
            # FROM {ProcessRaw.CAMPAIGN_MASTER_TABLE} A \
            # join {ProcessRaw.Sub_Campaign_Mapping} B on \
            # A.CampaignName = B.CampaignName where A.isActive = 'Y')"  

            raw_leads_df = self.db_ops.execute_dataframe_query(self.connection ,query)
            df = self.etl_ops.data_cleaning(raw_leads_df)
            
            #import pdb;pdb.set_trace()          
            
            
            if df.empty:
                log_data = [{"LogDetails" : f'No New leads found in {str(ProcessRaw.SRC_TABLE)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 1, "Stage" : "T1", "Status" : "INFO"}]
                self.log_ops.save_logs(log_data)
                raise Exception(f'No New leads found in {ProcessRaw.SRC_TABLE}')
            
            query = f"SELECT A.CampaignName as MasterCampaign ,B.FormId as FormID ,A.DedupeNoOfDaysPhoneCampaign \
                    FROM {ProcessRaw.CAMPAIGN_MASTER_TABLE} A WITH (NOLOCK) \
                    join {ProcessRaw.Sub_Campaign_Mapping} B WITH (NOLOCK) on \
                    A.CampaignName = B.CampaignName where A.IsActive = 'Y' and B.IsActive = 'Y' "    #and B.isActive = 'Y'
            
            campaign_master_df = self.db_ops.execute_dataframe_query(self.connection,query)

            df_temp = pd.merge(df,campaign_master_df,on = 'FormID',how = 'inner').reset_index()
            df_temp['CampaignID'] = 'NA'              
            df_temp['GCDMSystemToBeFollow'] = df_temp['Division'] #added new field in AllLeads for handling different division for gcdm
            
            df_temp = self.populate_campaign_field(df_temp)    #keeping division as im for mx when populating
            #df_temp.loc[df_temp['Division'] == 'MX' ,'Division'] = 'IM'  

            grouped_df =  df_temp.groupby(['MasterCampaign']) 
            final_df_lst = []

            for i ,group in grouped_df:
                dedupe_days = int(group['DedupeNoOfDaysPhoneCampaign'].iloc[0])
                group = self.etl_ops.dedupe_dataframe(self.connection, group,ProcessRaw.Dedupe_Field_Camp,dedupe_days).reset_index(drop=True)
                final_df_lst.append(group)

            df = pd.concat(final_df_lst).reset_index(drop=True)
            
            if not df.empty:
                #Second dedupe within Same Division CE IM Others
                dedupe_field_phone = ProcessRaw.DEDUPE_PHONE_FIELDS 
                dedupe_days = int(self.dedupe_phone_days())
                df = self.etl_ops.dedupe_dataframe(self.connection, df,dedupe_field_phone,dedupe_days)
            
            df = self.set_sec_flag(df)
            df["PinCode"] = df['PinCode'].apply(lambda x : '0' if len(x) > 10 else x)
            df["PinCode"] =  df["PinCode"].apply(lambda x :int(x.replace(" ","")) if x.replace(" ","").strip().isdigit() else 0)
            

            df = df[ProcessRaw.FINAL_COLS_DEST_TABLE]
            df['LeadModifiedTimestamp'] = datetime.datetime.now(self.IST).strftime("%Y-%m-%d %H:%M:%S")
            self.db_ops.ingest_dataframe_to_sql(self.connection, "AllLeads", df)

            log_data = [{"LogDetails" : f"Process raw pipe ingested {str(df.shape[0])} leads successfully...", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T1", "Status" : "SUCCESS"}]
            self.log_ops.save_logs(log_data)
        except Exception as e:
            log_data = [{"LogDetails" : f"Error in process raw {str(e)}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T1", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f"Error in process raw pipe {e}")

        
        finally:
            self.connection.close()




if __name__ == "__main__":
    obj = ProcessRaw()
    obj.run()
