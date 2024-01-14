import pandas as pd
import pytz
import datetime
from utils.etl_ops import ETLOps 
from utils.db_ops import DBOps
from utils.log_ops import LogOps

'''
1. Joining Store Details with MDM on StoreCode only , not on division.
2. Add Logging logic - DONE
'''


class UpdateStoreMaster(object):
  
    STORE_DETAILS_TABLE = 'dbo.StoreDetails'
    SEC_DETAILS_MDM_TABLE = 'dbo.SECDealerMapping'
    STORE_MASTER_TABLE = 'dbo.StoreDetailsMaster' 
    CALLING_CAPACITY_TABLE = 'dbo.CallingCapacityConfig'
    STORE_MASTER_COLS = ['StoreCode','MaxCapacity','CurrentCapacity','CurrentWorkload','SecCount']
    COLS_MAPPER = {'MaxCapacity_y': 'MaxCapacity' ,'SecCount_y': 'SecCount'}
    DEST_COLS = ['DateCreated','DateModified','StoreCode','City','PinCode','Division','StoreLat','StoreLong','StoreStatus',
                 'MaxCapacity','CurrentWorkload','SecCount']
    DATE_FMT = "%Y-%m-%d %H:%M:%S"
    IST = pytz.timezone('Asia/Kolkata')


    def __init__(self):
        self.db_ops = DBOps()
        self.log_ops = LogOps()
        self.connection = self.db_ops.create_connection()


    def fetch_calling_config(self):
        query = f"SELECT CallingDivision , DialerCount , CapacityPerDay ,CapacityMultiplyFactor FROM {UpdateStoreMaster.CALLING_CAPACITY_TABLE}"
        df = self.db_ops.execute_dataframe_query(self.connection,query)
        return df



    def fetch_updated_sec(self ,sec_capacity):
        query = f"WITH SecMapping as \
               (select StoreCode,Division,count(*) as SecCount from {UpdateStoreMaster.SEC_DETAILS_MDM_TABLE} \
                WHERE SECMobile is not NULL GROUP BY StoreCode ,Division) \
                \
                SELECT A.DateCreated,A.DateModified, A.StoreID as StoreCode ,A.City ,A.PinCode,A.Division,\
                A.StoreLat,A.StoreLong,A.IsActive as StoreStatus,(case when secmapping.seccount is NULL then 0 else  secmapping.seccount end)  as SecCount, \
                (case when secmapping.seccount is NULL then 0 else  secmapping.seccount end) *{sec_capacity} as MaxCapacity \
                FROM {UpdateStoreMaster.STORE_DETAILS_TABLE} A \
                left join SecMapping ON \
                SecMapping.StoreCode = A.StoreID WHERE A.IsActive = 'Y'"
        
        updated_sec_df = self.db_ops.execute_dataframe_query(self.connection,query)
        updated_sec_df.fillna({'SecCount':0,'MaxCapacity':sec_capacity},inplace = True)
        return updated_sec_df
    

    def fetch_store_master(self):
        query = f"SELECT  DateCreated ,DateModified,StoreType,StoreCode,City,PinCode,Division,StoreLat,StoreLong,StoreStatus ,\
                  MaxCapacity ,CurrentCapacity ,CurrentWorkload,SecCount FROM {UpdateStoreMaster.STORE_MASTER_TABLE} \
                "
        store_master_df = self.db_ops.execute_dataframe_query(self.connection,query)

        return store_master_df
    
    
    def update_ep_capacity(self, capacity_df , store_master_ep):
        capacity_df = capacity_df[capacity_df["CallingDivision"] != "SD"]
        capacity_df.rename(columns = {"CallingDivision":"StoreCode","DialerCount":"SecCount","CapacityPerDay":"MaxCapacity"},inplace = True)
        capacity_df["MaxCapacity"] = capacity_df["MaxCapacity"] * capacity_df["SecCount"] * capacity_df["CapacityMultiplyFactor"]
       
        merged_df = store_master_ep.merge(capacity_df ,how = "inner" ,left_on = "Division",right_on = "StoreCode",indicator = True)
        merged_df["CapacityDiff"] = merged_df["MaxCapacity_y"] - merged_df["MaxCapacity_x"]
        merged_df['CurrentCapacity'] = (merged_df['CurrentCapacity'] + merged_df['CapacityDiff']).apply(lambda x : x if x > 0 else 0) 
        merged_df.rename(columns = {"StoreCode_x":"StoreCode","SecCount_y":"SecCount","MaxCapacity_y":"MaxCapacity"},inplace = True)
        merged_df["DateModified"] = datetime.datetime.now(UpdateStoreMaster.IST).strftime(UpdateStoreMaster.DATE_FMT)
        merged_df = merged_df[["DateModified","MaxCapacity","CurrentCapacity","SecCount","StoreCode","Division"]]
       
        vals = list(merged_df.itertuples(index=False, name=None))
        query = f"UPDATE {UpdateStoreMaster.STORE_MASTER_TABLE} SET DateModified= ? ,MaxCapacity = ?,CurrentCapacity=?,SecCount=? \
                 WHERE StoreCode = ? and Division = ?"
        try:
            flag = self.db_ops.update_table(self.connection, query, vals)
        except Exception as e:
            log_data = [{"LogDetails" : f'Error in pdating EP-Capaicty {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T6", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f"Error in Updating EP-Capaicty {e}")

        return flag


    def run(self):

        try:
            import pdb;pdb.set_trace()
            store_master_df = self.fetch_store_master()
            store_master_ep = store_master_df[ store_master_df["StoreCode"] == "EP" ]
            store_master_df = store_master_df[store_master_df["StoreCode"] != "EP"]

            capacity_df = self.fetch_calling_config()
            self.update_ep_capacity(capacity_df,store_master_ep)

            sec_capacity = capacity_df[capacity_df['CallingDivision'] == 'SD']['CapacityPerDay'].iloc[0] * capacity_df[capacity_df['CallingDivision'] == 'SD']['CapacityMultiplyFactor'].iloc[0]
            updated_sec_df = self.fetch_updated_sec(sec_capacity)
        
            if updated_sec_df.empty:
                log_data = [{"LogDetails" : f'SecDealerMapping is empty df', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T6", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                raise Exception('StoreSecTable is empty df')

            #store_master_df['CurrentWorkload'] = store_master_df['MaxCapacity'] - store_master_df['CurrentCapacity']
            store_master_df = store_master_df[UpdateStoreMaster.STORE_MASTER_COLS]
            final_df = store_master_df.merge(updated_sec_df , on = 'StoreCode', how = 'outer' ,indicator=True)

            del updated_sec_df
            del store_master_df

            final_df['CapacityDiff'] = final_df['MaxCapacity_y'] - final_df['MaxCapacity_x'] 
            final_df['CurrentCapacity'] = (final_df['CurrentCapacity'] + final_df['CapacityDiff']).apply(lambda x : x if x > 0 else 0) 
            final_df = final_df.rename(columns = UpdateStoreMaster.COLS_MAPPER)
            final_df = final_df.drop(columns = ['MaxCapacity_x','SecCount_x','CapacityDiff'])

            update_df = final_df[final_df["_merge"] == 'both']
            delete_df = final_df[final_df["_merge"] == 'left_only']
            insert_df = final_df[final_df["_merge"] == 'right_only']

            del final_df
            if not delete_df.empty:

                delete_df["DateModified"] = datetime.datetime.now(UpdateStoreMaster.IST).strftime(UpdateStoreMaster.DATE_FMT)
                delete_df["StoreStatus"] = "N"
                delete_df = delete_df[["StoreStatus", "DateModified", "StoreCode"]]
                vals = list(delete_df.itertuples(index=False, name=None))
                query = f"UPDATE {UpdateStoreMaster.STORE_MASTER_TABLE} SET StoreStatus = ?, DateModified = ?\
                            WHERE StoreCode = ?"
                flag = self.db_ops.update_table(self.connection, query, vals)

            if not update_df.empty:
                update_df.drop(columns = ["_merge"], inplace = True)
                update_df["StoreStatus"] = "Y"
                update_df["DateModified"] = datetime.datetime.now(UpdateStoreMaster.IST).strftime(UpdateStoreMaster.DATE_FMT)
                cols = [col for col in update_df.columns if col not in ["StoreCode"]]
                cols.extend(["StoreCode"])
                update_df = update_df[cols]
                vals = list(update_df.itertuples(index=False, name=None))
                set_cols_str = ", ".join([f" {col} = ?" for col in update_df.columns if col not in ["StoreCode"]])
                where_col_str = " StoreCode = ? "
                query = f"UPDATE {UpdateStoreMaster.STORE_MASTER_TABLE} SET {set_cols_str} WHERE {where_col_str}"
                flag = self.db_ops.update_table(self.connection, query, vals)


            if not insert_df.empty:
                insert_df.drop( columns = ["_merge"], inplace = True)
                insert_df["CurrentCapacity"] = insert_df["MaxCapacity"]
                insert_df['CurrentWorkload'] = 0
                insert_df["DateCreated"] = datetime.datetime.now(UpdateStoreMaster.IST).strftime(UpdateStoreMaster.DATE_FMT)
                insert_df["DateModified"] = datetime.datetime.now(UpdateStoreMaster.IST).strftime(UpdateStoreMaster.DATE_FMT)
                insert_df['StoreStatus'] = "Y"
                self.db_ops.ingest_dataframe_to_sql(self.connection,UpdateStoreMaster.STORE_MASTER_TABLE,insert_df)

            log_data = [{"LogDetails" : f'Updated StoreMaster Successfuly , Found {str(len(insert_df))} new Stores and Updated {str(len(update_df))} Stores Info', 
                    "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                    "SeverityLevel" : 5, "Stage" : "T6", "Status" : "SUCCESS"}]
            self.log_ops.save_logs(log_data)

        except Exception as e:
            log_data = [{"LogDetails" : f'Error in Updating StoreMaster {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" :5, "Stage" : "T6", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            
            raise Exception(f"Error in update storelist runner : {e} ")
        
        
        finally:
            self.connection.close()


if __name__ == '__main__':
    obj = UpdateStoreMaster()
    obj.run()
