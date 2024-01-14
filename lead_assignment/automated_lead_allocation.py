import pandas as pd
import os ,pytz, json, datetime
from utils.etl_ops import ETLOps
from utils.db_ops import DBOps
from utils.log_ops import LogOps


'''
ShopperChannelPreference = 'ONLINE' will go to EP Only
Add UserSelectedStore condition if not found in StoreMaster dump 
Assign Division For User Selected Store with others as division
'''

class LeadAssignment(object):

    GCDM_TABLE = "dbo.CustomerScoreGCDM" 
    ALL_LEADS_TABLE  = "dbo.AllLeads"
    STORE_MASTER_TABLE = "dbo.StorePinMaster"
    STORE_CAPACITY_MASTER_TABLE = "dbo.StoreDetailsMaster"
    #MDM_TABLE = "dbo.SECDealerMapping"
    DEDUPE_CONF_FILE = os.path.join(os.path.join('configs', 'etl_configs'), 
                                    'dedupe_conf.json')
    COLS_LI = ["LeadID", "UUID", "LeadCreationTimestamp", "LeadModifiedTimestamp", "FullName", 
               "PhoneNumber", "Email", "City", "PinCode", "Division", "Category", "Product", 
               "StoreCode","AllocationType","LeadScore", "CustomerTier", "MasterCampaign",
                "LeadPushed" ]
    IST = pytz.timezone('Asia/Kolkata')
    CAMPAIGN_DETAILS_TABLE = '[dbo].[CampaignDetails]'
    RADIUS_DIR = {0:["T0"],5:["T0" ,"T1"] ,10 : ["T0","T1","T2"], 15: ["T0","T1","T2","T3"]}
    STORE_TIER_MAPPING = {"T0":"Servicable Pin Code","T1": "5 KM","T2" : "10 KM","T3":"15 KM"}



    def __init__(self):
        self.etl_ops = ETLOps()
        self.db_ops = DBOps()
        self.log_ops = LogOps()
        self.current_date = datetime.datetime.today().strftime("%Y-%m-%d")
        self.connection = self.db_ops.create_connection()
        query = f"SELECT Division, StoreCode, ServicablePinCode,Tier FROM {LeadAssignment.STORE_MASTER_TABLE} WITH (NOLOCK) WHERE \
            StoreCode in (SELECT DISTINCT(StoreCode) FROM {LeadAssignment.STORE_CAPACITY_MASTER_TABLE} WITH (NOLOCK) WHERE StoreStatus = 'Y' )"
        
        self.store_master_df = self.db_ops.execute_dataframe_query(self.connection, query)
        self.capacity_date = datetime.datetime.now(LeadAssignment.IST).strftime("%Y-%m-%d")
        query = f"SELECT DateCreated, DateModified, StoreType, StoreCode, City, PinCode, Division,\
                StoreLat, StoreLong, StoreStatus, MaxCapacity, CurrentCapacity, CurrentWorkload, SecCount\
                FROM {LeadAssignment.STORE_CAPACITY_MASTER_TABLE} WITH (NOLOCK) WHERE StoreStatus = 'Y' " # WHERE CAST(DateModified AS DATE) = '{self.today}
        self.store_capacity_master_df = self.db_ops.execute_dataframe_query(self.connection, query)
        self.campaign_config_df = self.get_campaign_config()
        self.campaign_config_df['StoreList'] = (self.campaign_config_df['ApplicableStore']).str.split(',') 
       #query_mdm = f"SELECT DISTINCT(StoreCode) as StoreCode , Division FROM {LeadAssignment.MDM_TABLE} where StoreStatus = 'Active' GROUP BY StoreCode , Division"
        #self.mdm_table = self.db_ops.execute_dataframe_query(self.connection,query_mdm)
       
        if self.store_capacity_master_df.empty:
            self.capacity_date = (datetime.datetime.now(LeadAssignment.IST) - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
            self.store_capacity_master_df = self.db_ops.execute_dataframe_query(self.connection, query)
         
        
    def read_json(self, filepath):
        if not os.path.exists(filepath):
            log_data = [{"LogDetails" : f"File is Missing at {filepath}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T3", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f"File is Missing at {filepath}")
        with open(filepath, "r") as rj:
            di = json.load(rj)
        return di
    

    def filter_store(self,tier,campaign_config,store_capacity_master_df,store_master_df,division,pincode):
        
        if campaign_config['RadiusDistance'] is not None:
            store_master_df = store_master_df[store_master_df["Tier"].isin(LeadAssignment.RADIUS_DIR[campaign_config['RadiusDistance']])]
           

        if campaign_config['TakeAStoreListForEachCampaign'] == 'Y':
            store_capacity_master_df = store_capacity_master_df[store_capacity_master_df["StoreCode"].isin(campaign_config['StoreList'])]
            store_master_df = store_master_df[store_master_df['StoreCode'].isin(campaign_config['StoreList'])]
        
        if division != "Others":
            temp_store_capacity_table = store_capacity_master_df[
                (store_capacity_master_df["CurrentCapacity"] > 0) & 
                (store_capacity_master_df["Division"] == division)]
            
            filtered_store_master = store_master_df[(store_master_df["ServicablePinCode"] == pincode)\
                                                        & (store_master_df["Division"] == division)]
            if filtered_store_master.empty:
                tier = 'Non Servicable Pincode'
            else:
                filtered_store_master = pd.merge(temp_store_capacity_table, filtered_store_master, 
                                            on = ["StoreCode", "Division"], how = 'inner')
                if filtered_store_master.empty:
                    tier = 'Threshold Overflow'

        else:
            temp_store_capacity_table = store_capacity_master_df[(store_capacity_master_df["CurrentCapacity"] > 0)]
            filtered_store_master = store_master_df[(store_master_df["ServicablePinCode"] == pincode)]
            
            if filtered_store_master.empty:
                tier = 'Non Servicable Pincode'
            else:
                filtered_store_master = pd.merge(temp_store_capacity_table, filtered_store_master, 
                                            on = ["StoreCode", "Division"], how = 'inner')
                if filtered_store_master.empty:
                    tier = 'Threshold Overflow'          
                    
        filtered_store_master = filtered_store_master.sort_values("Tier")
         # Add filter for capacity
        return filtered_store_master, temp_store_capacity_table ,tier


    def check_capacity(self,store_code):
        selected_store = self.store_capacity_master_df[self.store_capacity_master_df['StoreCode'] == store_code]

        if not selected_store.empty:
            current_capacity = selected_store['CurrentCapacity'].iloc[0]
        else:
            current_capacity = 0
            
        return current_capacity


    def update_capacity(self,store_code,division):
        sel_index = self.store_capacity_master_df.loc[(self.store_capacity_master_df["StoreCode"] == store_code) & (self.store_capacity_master_df["Division"] == division)].index[0]
        self.store_capacity_master_df.loc[sel_index, "CurrentCapacity"] = (self.store_capacity_master_df.iloc[sel_index]["CurrentCapacity"] - 1)


    def select_store(self,row):
        try:
            campaign_config = self.campaign_config_df[self.campaign_config_df["CampaignName"] == row["MasterCampaign"]].iloc[0]
        except:
            log_data = [{"LogDetails" : f'No Active Campaign found for {str(row["MasterCamapign"])}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 1, "Stage" : "T3", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f'No Active Campaign found for {row["MasterCampaign"]}')
        
        pincode = row["PinCode"]
        division = row["Division"]

        store_code = "NA"
        tier = "NA"
        
        if row["SelectedStore"] !="NA" and row["SelectedStore"] != "":
            
            if not self.store_capacity_master_df[self.store_capacity_master_df['StoreCode'] == row["SelectedStore"]].empty :
                curr_capacity = self.check_capacity(row["SelectedStore"])
                if curr_capacity > 0:
                    store_code = row["SelectedStore"]
                    tier = "User Selected Store"
                    if division == 'Others' :
                        division = self.store_capacity_master_df[self.store_capacity_master_df['StoreCode'] == row["SelectedStore"]]['Division'].iloc[0]
                    self.update_capacity(store_code,division)
                
                else:
                    store_code = 'HOLD'
           
            else:
                row['SelectedStore'] = 'NA'
                log_data = [{"LogDetails" : f'UserSelectedStore {store_code} not found in StoreMaster Table', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 1, "Stage" : "T3", "Status" : "INFO"}]
                self.log_ops.save_logs(log_data)

                # store_code = row["SelectedStore"]
                # if division == 'Others':
                 
                #     try:
                #         division = self.mdm_table[self.mdm_table['StoreCode'] == store_code]["Division"].iloc[0]
                #         division = division.replace("HHP" , "IM")
          
                #     except Exception as e:
                #         store_code = 'NA'
                #         log_data = [{"LogDetails" : f'{store_code} not found in MDM Table', 
                # "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                # "SeverityLevel" : 1, "Stage" : "T3", "Status" : "INFO"}]
                #         self.log_ops.save_logs(log_data)

                #         raise(f'{store_code} not found in MDM Table')
                                     

        elif campaign_config["SmartDost"] == 'Y' and row['ShopperChannelPreference'] !='ONLINE' :    
            filtered_store_master ,temp_store_capacity_table, tier = self.filter_store(tier,campaign_config,self.store_capacity_master_df,self.store_master_df,division,pincode)
            
            if not filtered_store_master.empty:
                for name, grouped_df in filtered_store_master.groupby("Tier"):
                    store_list = grouped_df["StoreCode"].unique().tolist()

                    temp_store_capacity_table = temp_store_capacity_table[
                                    temp_store_capacity_table["StoreCode"].isin(store_list)
                                    ].sort_values("CurrentCapacity", ascending=False)

                    if not temp_store_capacity_table.empty:
                        for i, row in temp_store_capacity_table.iterrows():
                           
                            store_code = row["StoreCode"]
                            tier = LeadAssignment.STORE_TIER_MAPPING[f'{name}']     #updating from dict
                           
                            if division == 'Others':     #assigning division here
                                division = temp_store_capacity_table[temp_store_capacity_table['StoreCode'] == store_code]['Division'].iloc[0]
                            
                            self.update_capacity(store_code,division)
                          
                            break

                    if store_code != "NA":
                        break
                if store_code == 'NA': 
                    tier = 'Threshold Overflow'

        if campaign_config["EPromoter"] != "N" and store_code == "NA" and (row["SelectedStore"] == "NA" or row["SelectedStore"] == ""): # else:
            if division != "Others":
                temp_store_capacity_table = self.store_capacity_master_df[(self.store_capacity_master_df["StoreCode"] == 'EP')\
                                                                            & (self.store_capacity_master_df["Division"] == division)]
                temp_store_capacity_table = temp_store_capacity_table[temp_store_capacity_table["CurrentCapacity"] > 0 ]
                if not temp_store_capacity_table.empty:
                    store_code = temp_store_capacity_table["StoreCode"].values[0]
                    self.update_capacity(store_code,division)
                    
                    if tier == "NA":
                        tier = "EP"
            else:
                
                temp_store_capacity_table = self.store_capacity_master_df[(self.store_capacity_master_df["StoreCode"] == 'EP')].sort_values(["CurrentCapacity"],ascending=False)
                temp_store_capacity_table = temp_store_capacity_table[temp_store_capacity_table['CurrentCapacity'] > 0]
                
                ep_division_configured = campaign_config['EPromoter'].replace("MX","IM")
                temp_store_capacity_table = temp_store_capacity_table[temp_store_capacity_table['Division'] ==ep_division_configured ]

                if not temp_store_capacity_table.empty:

                    #store_code = temp_store_capacity_table["StoreCode"].iloc[0]
                    store_code = temp_store_capacity_table[temp_store_capacity_table['Division'] == ep_division_configured]["StoreCode"].iloc[0]

                    if division == 'Others':     #assigning division here
                        division  = ep_division_configured
                        self.update_capacity(store_code,division)
                    if tier == "NA":
                        tier = "EP"
                else:
                    log_data = [{"LogDetails" : f'Epromoter Capacity Exhausted for {str(ep_division_configured)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 1, "Stage" : "T3", "Status" : "INFO"}]
                    self.log_ops.save_logs(log_data)

        print(f"pincode {pincode} assigned to {store_code}")

        return store_code,tier,division
    
    
    
    def get_campaign_config(self):
        query = f"SELECT CampaignName,LeadDistributionStartDate,TakeAStoreListForEachCampaign,ApplicableStore, \
                  EPromoter,SmartDost,RadiusDistance FROM {LeadAssignment.CAMPAIGN_DETAILS_TABLE} WITH (NOLOCK) \
                  WHERE isActive = 'Y' "    #+++ LeadDistributionFlag =Y # and LeadDistributionStartDate <= '{self.current_date}'
       
        campaign_config_df = self.db_ops.execute_dataframe_query(self.connection, query)
        return campaign_config_df

    

    def run(self):
        try:
            #import pdb;pdb.set_trace()
            # dedupe_conf_di = self.read_json(LeadAssignment.DEDUPE_CONF_FILE)
            
            query = f"SELECT B.LeadID, B.UUID, B.LeadCreationTimestamp, A.FullName, B.PhoneNumber, B.Email, A.City,\
                    A.PinCode, B.Division, B.Category, B.Product, B.LeadScore, B.CustomerTier,\
                    A.MasterCampaign ,A.SelectedStore , A.ShopperChannelPreference FROM {LeadAssignment.ALL_LEADS_TABLE} A WITH (NOLOCK)\
                    JOIN {LeadAssignment.GCDM_TABLE} B WITH (NOLOCK)\
                     ON A.UUID = B.UUID  \
                    JOIN {LeadAssignment.CAMPAIGN_DETAILS_TABLE} C WITH (NOLOCK) ON A.MasterCampaign = C.CampaignName \
                    WHERE B.PendingForAction = 1 AND C.IsActive = 'Y' "
            
            df = self.db_ops.execute_dataframe_query(self.connection, query)

            
            if df.empty:
                log_data = [{"LogDetails" : f'No new Leads Founds for allocation....', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 1, "Stage" : "T3", "Status" : "INFO"}]
                self.log_ops.save_logs(log_data)
                raise Exception(f'No new Leads Founds {df.shape}')

            # df = df.head(100)
            df["LeadPushed"] = 0
            df["PinCode"] =  df["PinCode"].apply(lambda x :int(x.replace(" ","")) if x.replace(" ","").strip().isdigit() else 0)
            df["PinCode"].fillna(0, inplace=True)
            #df.loc[df['PinCode']=='NA','PinCode'] = 0

            
            for i, row in df.iterrows():
                try:
                    store_code,tier,division = self.select_store(row)

                    df.loc[i, "StoreCode"] = store_code
                    df.loc[i, "AllocationType"] = tier
                    df.loc[i, "Division"] = division
                except :
                    df.loc[i, "StoreCode"] = 'NA'
                    print(f"Error in Allocating {row}")

            df = df[(df["StoreCode"] != 'NA') & (df["StoreCode"] != "HOLD")]
            df["LeadModifiedTimestamp"] = datetime.datetime.now(LeadAssignment.IST).strftime("%Y-%m-%d %H:%M:%S")
            #import pdb;pdb.set_trace()
            if df.empty:
                raise Exception("No New Leads to Ingest")
            
            df = df[LeadAssignment.COLS_LI] 
            self.db_ops.ingest_dataframe_to_sql(self.connection, "LeadsAssignment", df)
            uuid_li = df["UUID"].tolist()

            batch_size = 10000
            buckets = list(range(0, len(uuid_li) + batch_size, batch_size))
            buckets = [buckets[i:i+2] for i,e in enumerate(buckets) if len(buckets[i:i+2]) == 2]
            #import pdb;pdb.set_trace()
            for batch in buckets:
                tmp_uuid_li = uuid_li[batch[0] : batch[1]]
                tmp_uuid_tup = f'({tmp_uuid_li[0]})' if len(tmp_uuid_li) == 1 else tuple(tmp_uuid_li)
                query = f"UPDATE {LeadAssignment.GCDM_TABLE} SET PendingForAction = 0 WHERE UUID IN {tmp_uuid_tup}"
                self.db_ops.execute_query(self.connection, query)
           
            
            tmp_df = self.store_capacity_master_df[["CurrentCapacity", "StoreCode", "Division"]]
            values = list(tmp_df.itertuples(index=False, name=None))
            
            batch_size = 10000
            buckets = list(range(0, len(values) + batch_size, batch_size))
            buckets = [buckets[i:i+2] for i,e in enumerate(buckets) if len(buckets[i:i+2]) == 2]
            for batch in buckets:
                b_vals = values[batch[0] : batch[1]]
                query = f"UPDATE {LeadAssignment.STORE_CAPACITY_MASTER_TABLE} SET CurrentCapacity = ? WHERE StoreCode = ? and Division = ?" 
                flag = self.db_ops.update_table(self.connection, query, b_vals)
                
            log_data = [{"LogDetails" : f"Lead Allocation completed Successfully for {df.shape[0]} leads...", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T3", "Status" : "SUCCESS"}]
            self.log_ops.save_logs(log_data)
        except Exception as e:
            log_data = [{"LogDetails" : f"Error in lead allocation run : {str(e)}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T3", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f"Error in run : {e}")
        finally:
            self.connection.close()


if __name__ == "__main__":
    obj = LeadAssignment()
    obj.run()

