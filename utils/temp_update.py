import pandas , numpy
from utils.db_ops import DBOps
from lead_status.smartdost.smartdost_pipe import LeadStatus


class TempDump(object):
    FINAL_COLS = ['Level1','Level2','Level3','FinalDisposition',
         'Status','IngestionTimestamp','CallStart','UUID','Division']

    def __init__(self):
        self.DB_ops = DBOps()
        self.LeadStatus = LeadStatus()
        self.connection = self.DB_ops.create_connection()
        query = f"SELECT Division ,Response,Level1,Level2,Level3,FinalStatus FROM dbo.LeadStatusMappingSD"
        self.status_mapper = self.DB_ops.execute_dataframe_query(self.connection , query)
        self.status_mapper['Division'].replace("MX" ,"IM" ,inplace = True)
        pass

    def fetch_status(self,row):
        #import pdb;pdb.set_trace()
        try:
            status_di = {"Level1" : "NA" , "Level2" : "NA" , "Level3" : "NA" , "FinalStatus" :"NA"}
            
            resp = self.status_mapper[(self.status_mapper['Division'] == row['Division']) & (self.status_mapper['Response'] == row['LeadStatus'])].iloc[0]
            resp = resp[["Level1" , "Level2" , "Level3" , "FinalStatus"]]
            resp_di = resp.to_dict()
            for key in status_di:
                status_di[key] = resp_di[key]
        except:
            print('error')
    
        return status_di


    def run(self):

        lead_status_query = "with StatusTable as \
        (select * from [dbo].[SD_Galaxybook_Status_Temp] where LeadStatus is not NULL) \
        \
        select A.UUID , B.LeadStatus from LeadsAssignment A \
        inner join StatusTable B \
        on A.PhoneNumber = B.PhoneNumber"

        df_lead_status = self.DB_ops.execute_dataframe_query(self.connection , lead_status_query)

        query = f"select * from LeadStatus where Product  = 'Galaxy Book3 Series' "
        import pdb;pdb.set_trace()
        Fetch_From_Status_Table = self.DB_ops.execute_dataframe_query(self.connection , query)
        Fetch_From_Status_Table['UUID'] = Fetch_From_Status_Table['UUID'].astype(str)
        final_df = df_lead_status.merge(Fetch_From_Status_Table , how='left' , on = 'UUID')
        final_df = final_df[['UUID','LeadStatus','Division']]
        final_df['TEMP_DI'] = {"Level1" : "NA" , "Level2" : "NA" , "Level3" : "NA" , "FinalDisposition" :"NA"}
        final_df["TEMP_DI"] = final_df.apply(lambda x : self.fetch_status(x),axis = 1)
        final_df["Level1"] = final_df["TEMP_DI"].apply(lambda x : x["Level1"])
        final_df["Level2"] = final_df["TEMP_DI"].apply(lambda x : x["Level2"])
        final_df["Level3"] = final_df["TEMP_DI"].apply(lambda x : x["Level3"])
        final_df["FinalDisposition"] = final_df["TEMP_DI"].apply(lambda x : x["FinalStatus"])
        final_df["Status"] = final_df["TEMP_DI"].apply(lambda x : x["FinalStatus"])
        final_df.drop(["TEMP_DI"], axis=1, inplace=True)
        final_df["CallStart"]= pandas.to_datetime('1990/12/19 00:00:00',format='%Y/%m/%d %H:%M:%S')
        final_df['IngestionTimestamp'] = pandas.to_datetime('2023/07/07 00:16:42',format='%Y/%m/%d %H:%M:%S')
        final_df = final_df[final_df['Division'] != 'NA']
        final_df = final_df[TempDump.FINAL_COLS]
        values , buckets = self.LeadStatus.batch_update(final_df)

        for batch in buckets:
            b_vals = values[batch[0] : batch[1]]
            query_upd = f"UPDATE {LeadStatus.DEST_TABLE} SET \
            Level1 = ? , Level2 = ? , Level3 = ? , FinalDisposition = ?, Status = ? \
            ,IngestionTimestamp = ? ,CallStart = ?\
            WHERE UUID = ? and Division = ?"
            flag = self.DB_ops.update_table(self.connection, query_upd, b_vals)
       

if __name__ == '__main__':
    obj = TempDump()
    obj.run()


