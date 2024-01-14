import json
import requests
import pandas as pd 
import numpy as np 
# dbconnection = ""
import pytz
import datetime
from utils.etl_ops import ETLOps 
from utils.db_ops import DBOps
from utils.log_ops import LogOps


class StoreData(object):  
    CAFE_AUTH_KEY = 'bbe22cc2a9bf0b734877a26a458865cabe679b3a'
    CAFE_KRY_KEY = '30f7c21935e1fab5f1de0d7c553649c1lOpa98'
    CAFE_LIMIT = 10000
    PLAZA_LIMIT = 10000
    PLAZA_AUTH_KEY = "8e3213d03552cdf9515f0f2703d77bac4e0500ff"
    PLAZA_KRY_KEY = "e9d7d3b48add2f6bc81edada5484d644lOpa98"
    COL_MAP_DI = {
                    "zip" : "PinCode", "business_name" : "StoreName",
                    "city" : "City", "longitude" : "StoreLong", "latitude" : "StoreLat",
                    "enterprise_actual_client_store_id" : "StoreID", "Division" : "Division"
                    }
    TABLE_NAME = "StoreDetails"
    DATE_FMT = "%Y-%m-%d %H:%M:%S"
    IST = pytz.timezone('Asia/Kolkata')


    def __init__(self):
        self.IST = pytz.timezone('Asia/Kolkata')
        self.db_ops = DBOps()
        self.log_ops = LogOps()
        self.connection = self.db_ops.create_connection()
        # self.filter_db_fields = ['UUID','FullName','PhoneNumber','Email','PinCode','Campaign','LeadCreationTimestamp','Division','Category','LeadScore','CustomerTier','Product']
 

    def get_smartcafe_store_list(self, cnt):
        try:
            cnt += 1
            df = pd.DataFrame()
            #&master_outlet_id=78795
            url = f'https://api.singleinterface.com/RestfulMicrosites/store_locator/78795?\
            auth_key={StoreData.CAFE_AUTH_KEY}&\
            krykey={StoreData.CAFE_KRY_KEY}&no_redirect=1&limit={StoreData.CAFE_LIMIT}'
            resp = requests.get(url)
            if resp.status_code == 200:
                jsn = resp.json()
                df = pd.DataFrame(jsn['content']['arrThemeData']['arrOutlets'])
                df = pd.DataFrame(df["Outlet"].to_list())
                #df["Division"] = df["business_name"].apply(lambda x : "IM" if "SmartCafé" in x else "CE")
                df["Division"] = "IM"
            else:
                raise Exception(resp)        
        except Exception as e:
            if cnt <= 3:
                df = self.get_smartcafe_store_list(cnt)
            else:
                log_data = [{"LogDetails" : f'Error in SmartCafe API {str(e)} status code {str(resp.status_code)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T6", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
        return df

    def get_smartplaza_store_list(self, cnt):
        try:
            cnt += 1
            df = pd.DataFrame()
            url = f'https://api.singleinterface.com/RestfulMicrosites/store_locator/109650?\
            auth_key={StoreData.PLAZA_AUTH_KEY}&\
            krykey={StoreData.PLAZA_KRY_KEY}&no_redirect=1&limit={StoreData.PLAZA_LIMIT}'
            resp = requests.get(url)
            if resp.status_code == 200:
                jsn = resp.json()
                df = pd.DataFrame(jsn['content']['arrThemeData']['arrOutlets'])
                df = pd.DataFrame(df["Outlet"].to_list())
                df["Division"] = "CE"
            else:
                
                raise Exception(resp)  
        except Exception as e:
            if cnt <= 3:
                df = self.get_smartplaza_store_list(cnt)
            else:
                log_data = [{"LogDetails" : f'Error in SmartCafe API {str(e)} status code {str(resp.status_code)}', 
                    "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                    "SeverityLevel" : 4, "Stage" : "T6", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
        return df     

    def get_store_list(self):
        query = f"SELECT distinct(StoreID) ,Division FROM {StoreData.TABLE_NAME}"
        old_store_df = self.db_ops.execute_dataframe_query(self.connection, query)
        return old_store_df 


    def runner(self):
        try:
            smartcafe_df = self.get_smartcafe_store_list(cnt=1)
            smartplaza_df = self.get_smartplaza_store_list(cnt=1)
            df = pd.concat([smartcafe_df, smartplaza_df])

            if df.empty:
                log_data = [{"LogDetails" : f'Empty data from StoreList Single Interface API', 
                    "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                    "SeverityLevel" : 4, "Stage" : "T6", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                raise Exception("Df with 0 Stores")
            df = df[list(StoreData.COL_MAP_DI.keys())]
            df.rename(columns = StoreData.COL_MAP_DI, inplace = True)
            old_store_df = self.get_store_list()
            merged_df = old_store_df.merge(df, on = 'StoreID', how = 'outer', indicator = True)
            del df
            del old_store_df    
            update_df = merged_df[merged_df["_merge"] == 'both']
            delete_df = merged_df[merged_df["_merge"] == 'left_only']
            insert_df = merged_df[merged_df["_merge"] == 'right_only']
            del merged_df
            
            if not delete_df.empty:
                delete_df.rename(columns = {"Division_x":"Division"} , inplace = True)
                delete_df.drop(columns = ["_merge","Division_y"], inplace = True)
                delete_df["DateModified"] = datetime.datetime.now(StoreData.IST).strftime(StoreData.DATE_FMT)
                delete_df["IsActive"] = "N"
                store_id = delete_df["StoreID"].to_list()
                delete_df = delete_df[["IsActive", "DateModified", "StoreID", "Division"]]
                vals = list(delete_df.itertuples(index=False, name=None))

                query = f"UPDATE {StoreData.TABLE_NAME} SET IsActive = ?, DateModified = ?\
                        WHERE StoreID = ? AND Division = ?"
                flag = self.db_ops.update_table(self.connection, query, vals)

            if not update_df.empty:
                update_df.rename(columns = {"Division_y":"Division"},inplace = True)
                update_df.drop(columns = ["_merge","Division_x"], inplace = True)
                update_df["isActive"] = 'Y'
                update_df["DateModified"] = datetime.datetime.now(StoreData.IST).strftime(StoreData.DATE_FMT)
                cols = [col for col in update_df.columns if col not in ["StoreID", "Division"]]
                cols.extend(["StoreID", "Division"])
                update_df = update_df[cols]
                vals = list(update_df.itertuples(index=False, name=None))
                set_cols_str = ", ".join([f" {col} = ?" for col in update_df.columns if col not in ["StoreID", "Division"]])
                where_col_str = " StoreID = ? AND Division = ?"
                query = f"UPDATE {StoreData.TABLE_NAME} SET {set_cols_str} WHERE {where_col_str}"

                flag = self.db_ops.update_table(self.connection, query, vals)
            #data = self.clen_jsn_data()
            if not insert_df.empty:
                insert_df.rename(columns = {"Division_y":"Division"},inplace = True)
                insert_df.drop(columns = ["_merge","Division_x"], inplace = True)
                insert_df['DateCreated'] = datetime.datetime.now(StoreData.IST).strftime(StoreData.DATE_FMT)
                insert_df['DateModified'] = datetime.datetime.now(StoreData.IST).strftime(StoreData.DATE_FMT)
                insert_df["IsActive"] = "Y"
                self.db_ops.ingest_dataframe_to_sql(self.connection, StoreData.TABLE_NAME, insert_df)

            log_data = [{"LogDetails" : f'Updated Storelist Successfuly , Found {str(len(insert_df))} new Stores and Updated {str(len(update_df))} Stores Info', 
                    "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                    "SeverityLevel" : 5, "Stage" : "T6", "Status" : "SUCCESS"}]
            self.log_ops.save_logs(log_data)

        except Exception as e:
            log_data = [{"LogDetails" : f'Error in Updating Storelist {str(e)} ', 
                    "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                    "SeverityLevel" : 4, "Stage" : "T6", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f"Error in update storelist runner : {str(e)} ")
        finally:
            self.connection.close()
        return 1

if __name__ == "__main__":
    obj = StoreData()
    obj.runner()

