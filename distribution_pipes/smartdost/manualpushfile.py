import pandas as pd
import requests ,pytz,datetime,time
from utils.db_ops import DBOps


class PushSDLeads(object):
    SRC_TABLE = 'LeadsAssignment'
    IST = pytz.timezone('Asia/Kolkata')
    SD_TIERING_TABLE = "dbo.SdTierMapping"


    def __init__(self):
        self.dbops = DBOps()
        self.connection = self.dbops.create_connection()



    def fetch_leads_db(self):
        try:
            query = "SELECT LeadID, UUID, LeadCreationTimestamp, LeadModifiedTimestamp, FullName,\
            PhoneNumber, Email, City, PinCode, Division, Category, Product, StoreCode, AllocationType,\
            LeadScore, CustomerTier, MasterCampaign,LeadPushed FROM LeadsAssignment WHERE LeadPushed = 0 and StoreCode != 'EP'  ORDER BY LeadCreationTimestamp asc "
            df = self.dbops.execute_dataframe_query(self.connection, query)
        except Exception as e:
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


    def run(self):
        try:
            df = self.fetch_leads_db()
            timestamp = datetime.datetime.now(PushSDLeads.IST).strftime("%Y-%m-%d %H:%M:%S").replace(" ", "_").replace(":", "_")
            

            tiering_query = f"SELECT Division, Type, Score, SdTier FROM {PushSDLeads.SD_TIERING_TABLE}"
            self.tier_mapping_df = self.dbops.execute_dataframe_query(self.connection,tiering_query)

            for index , row in df.iterrows():
                df.iloc[index] = self.apply_tiering(row)
            #import pdb;pdb.set_trace()


            df.to_excel(f"data/Allocated_SmartDost_{timestamp}.xlsx", index=False)
            
            print(f"Wrote Excel File at data/Allocated_SmartDost_{timestamp}.xlsx")
            success_uuid_li = df['UUID'].tolist()
            if success_uuid_li:
                tmp_uuid_tup = f'({success_uuid_li[0]})' if len(success_uuid_li) == 1 else tuple(success_uuid_li)
                query = f"UPDATE dbo.LeadsAssignment SET LeadPushed = 1 WHERE UUID IN {tmp_uuid_tup}"
                try:
                    self.dbops.execute_query(self.connection, query)
                    print(f"Smartdost MANUAL EXPORT {len(success_uuid_li)} uuids successfully at index {index+1}.....")
                except Exception as e:
                    log_data = [{"LogDetails" :f'Error in Updating flag in LeadsAssignment {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T4", "Status" : "ERROR"}]
                    self.log_ops.save_logs(log_data)


        
        finally:
            self.connection.close()




if __name__ == "__main__":
    obj = PushSDLeads()
    obj.run()