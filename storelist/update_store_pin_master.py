import os, json,datetime
import pandas as pd
import pytz
from math import sin, cos, sqrt, atan2, radians
from utils.db_ops import DBOps
from utils.log_ops import LogOps

''''ADD 2K LEFT PINCODES IN PINCODELATLONG TABLE
DONE - SERVICABLE_PINCODE T0  Mapping is left'''



class TierTableCreator(object):
    PINCODE_MASTER_TABLE = "dbo.PincodeLatLonMaster"
    STORE_DETAILS = "dbo.StoreDetails"
    STORE_PIN_MASTER_TABLE = "dbo.StorePinMaster"
        FINAL_DEST_COLS = ['Division','StoreCode' ,'ServicablePinCode','Tier']
    #STORE_PATH = "tables/Samsung_CE_Store_Details.xlsx"
    #STORE_T0_PATH = "tables/Samsung_Store_PinCode_Mapping.xlsx"
    COL_MAPPER = { "Lat": "PinCodeLat", "Long": "PinCodeLon", "StoreLat" : "StoreLat",
     "StoreLong" : "StoreLong" , "PinCode":"ServicablePinCode","StoreID":"StoreCode"} 
     #, "Category" : "Division"
    T0_COL_MAPPER = {"Pin code" : "PinCode", "Lat" : "StoreLat", "Long" : "StoreLong", "DummyStoreCode": "StoreCode"}
    DATE_FMT = "%Y-%m-%d %H:%M:%S"
    IST = pytz.timezone('Asia/Kolkata')
    R = 6373.0
    TIER_LI = [(5.0, "T1"), (10.0, "T2"), (15.0, "T3")]


    def __init__(self):
        self.db_ops = DBOps()
        self.log_ops = LogOps()
        self.connection = self.db_ops.create_connection()

    def tier_calculator(self, x):
        try:
            tier = "NA"
            pin_lat, pin_long, store_lat, store_long = x["Lat"], x["Long"], x["StoreLat"], x["StoreLong"]
            lat1 = radians(float(pin_lat))
            lon1 = radians(float(pin_long))
            lat2 = radians(float(store_lat))
            lon2 = radians(float(store_long))
            dlon = lon2 - lon1
            dlat = lat2 - lat1
            a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
            c = 2 * atan2(sqrt(a), sqrt(1 - a))
            distance = TierTableCreator.R * c
            for tier_tup in TierTableCreator.TIER_LI:
                if distance <= tier_tup[0]:
                    tier = tier_tup[1]
                    break
            else:
                tier = "NO_TIER"
        except Exception as e:
            log_data = [{"LogDetails" : f'Error in Tier Calculator while Updating StorPinMaster {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T6", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)     
            raise Exception(f"Error in tier calculator : {e}")
        return tier
    
    def fetch_current_mapping(self):
        query = F'SELECT  Division ,StoreCode,ServicablePinCode FROM {TierTableCreator.STORE_PIN_MASTER_TABLE}'
        df = self.db_ops.execute_dataframe_query(self.connection ,query)

        return df
    

    def run(self):
            
            try:
                #import pdb;pdb.set_trace()
                query = f'SELECT PinCode, Lat,Long FROM {TierTableCreator.PINCODE_MASTER_TABLE}'
                pincode_master_df = self.db_ops.execute_dataframe_query(self.connection,query)
                print(f'Found {len(pincode_master_df)} PinCodes')
                pincode_master_df = pincode_master_df[(pincode_master_df["Lat"] != "BLANK") | (pincode_master_df["Long"] != "BLANK")]
                pincode_master_df = pincode_master_df[~((pincode_master_df["Lat"].str.contains("HTTPS")) | (pincode_master_df["Long"].str.contains("HTTPS")))]
                pincode_master_df["key"] = 1

                query = f'SELECT Division,StoreID , StoreLat, StoreLong FROM {TierTableCreator.STORE_DETAILS}'
                store_master_df = self.db_ops.execute_dataframe_query(self.connection , query)
                print(f'Found {len(store_master_df)} StoreCode')
                #store_master_df = self.read_excel(TierTableCreator.STORE_PATH)
                store_master_df = store_master_df[(store_master_df["StoreLat"] != "-") | (store_master_df["StoreLong"] != "-")]
                store_master_df["key"] = 1

                #store_master_df = store_master_df.head(120)
                #pincode_master_df = pincode_master_df.head(5000)
                #store_t0_df = self.read_excel(TierTableCreator.STORE_T0_PATH)
                #store_t0_df = store_t0_df[store_t0_df["Tier"] == "T0"]

                pincode_store_cross_df = pd.merge(pincode_master_df, store_master_df, on='key').drop(columns = ["key"])    #.drop("key", 1)
                
                if pincode_store_cross_df.empty:
                    raise Exception("pincode_store_cross_df si empty")

                pincode_store_cross_df["Tier"] = pincode_store_cross_df.apply(lambda x : self.tier_calculator(x), axis=1)
                pincode_store_cross_df = pincode_store_cross_df[pincode_store_cross_df["Tier"] != "NO_TIER"]
                #import pdb; pdb.set_trace()
                pincode_store_cross_df.rename(columns=TierTableCreator.COL_MAPPER, inplace=True)
                fresh_master_df = pincode_store_cross_df[TierTableCreator.FINAL_DEST_COLS]

                old_master_df = self.fetch_current_mapping()
                fresh_master_df['ServicablePinCode'] = fresh_master_df['ServicablePinCode'].astype(int)   #added to debug merge
                # import pdb;pdb.set_trace()
                merged_df = pd.merge(old_master_df , fresh_master_df ,on = ['StoreCode','ServicablePinCode' ] , how = "outer" ,indicator=True)
                
                update_df = merged_df[merged_df["_merge"] == 'both']
                delete_df = merged_df[merged_df["_merge"] == 'left_only']
                insert_df = merged_df[merged_df["_merge"] == 'right_only']
                
                #store_t0_df.rename(columns=TierTableCreator.T0_COL_MAPPER, inplace=True)

                #final_df = pd.concat([store_t0_df, pincode_store_cross_df])
                #final_df.fillna("NA", inplace = True)
                #final_df.to_excel("tables/store_master_table.xlsx", index=False)
                del merged_df ,update_df,delete_df

                if not insert_df.empty:
                    insert_df.drop(columns =  ['Division_x' , '_merge'] , inplace = True)
                    insert_df.rename(columns= {'Division_y':'Division'} , inplace = True)
                    insert_df['DateCreated'] = datetime.datetime.now(TierTableCreator.IST).strftime(TierTableCreator.DATE_FMT)
                    insert_df.rename(columns = {})
                    self.db_ops.ingest_dataframe_to_sql(self.connection,TierTableCreator.STORE_PIN_MASTER_TABLE,insert_df)
                    
                    log_data = [{"LogDetails" : f'Succefully Ingested StoreTier mapping for {str(len(insert_df))} Pincodes ', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T6", "Status" : "SUCCESS"}]
                    self.log_ops.save_logs(log_data)

                else:
                    log_data = [{"LogDetails" : f'No New Store Tier mapping to Ingest', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T6", "Status" : "INFO"}]
                    self.log_ops.save_logs(log_data)
                    
                    raise Exception('No New Store Tier mapping to Ingest')
            
      
            except Exception as e:
                log_data = [{"LogDetails" : f'Error in updating StorePinMaster {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T6", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                
                raise Exception(f'Error in updating StorePinMaster {e}')
            
            finally:
                self.connection.close()


if __name__ == "__main__":
    obj = TierTableCreator()
    obj.run()


