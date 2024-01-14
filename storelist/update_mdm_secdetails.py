import pandas as pd
import numpy ,datetime ,requests ,eventlet
from utils.log_ops import LogOps
from utils.db_ops import DBOps


class GetMdmData():
    START_DATE = '20230709'
    END_DATE = ''
    TABLE = 'dbo.SECDealerMapping'
    TIMEOUT = 10

    #@GET_SEC_API =f'https://www.samsungindiamarketing.com/LMSWebAPI/Api/SECDealerMappingDetailsById?MinId={min_id}&MaxId={max_id}'
    API_CRED = {"Username" : "samsungwebapi" , "Password":"4436612f0e11c5f0be7b70e6efe8d13c41af183f31435819c058edddc2b806d5"}
    
    HEADERS_DI = {
                'Authorization': 'Basic c2Ftc3VuZ3dlYmFwaTo0NDM2NjEyZjBlMTFjNWYwYmU3YjcwZTZlZmU4ZDEzYzQxYWYxODNmMzE0MzU4MTljMDU4ZWRkZGMyYjgwNmQ1',  
                'Host': 'www.samsungindiamarketing.com',
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/46.0.2490.80',
                }



    def __init__(self):
        self.db_ops = DBOps()
        self.log_ops = LogOps()
        self.connection = self.db_ops.create_connection()
        

    def GetLastIngestTime(self):
        query = f"SELECT  CASE WHEN MAX(InsertedDate) IS NULL THEN cast('01-01-2000 00:00' as date) ELSE MAX(InsertedDate) END  from {GetMdmData.TABLE}"
        try:
            df = self.db_ops.execute_dataframe_query(self.connection ,query)
            if df:
                max_ingest_time = df.iloc[0]            
        except Exception as e:
            log_data = [{"LogDetails" : f"Error in GetLastIngestTime {str(e)}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T6", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)

            raise Exception
        
        return max_ingest_time
    

    def fetch_id(self ,start_date , end_date ,cnt):
        
        GET_ID_API = f'https://www.samsungindiamarketing.com/LMSWebAPI/Api/SECDealerMappingMinAndMaxId?StartDate={start_date}&EndDate={end_date}'    
        
        try:
            
            try:
                with eventlet.Timeout(GetMdmData.TIMEOUT) as e:
                    resp = requests.get(GET_ID_API, headers = GetMdmData.HEADERS_DI )

            except eventlet.Timeout :
                raise TimeoutError("TESTING--Timeout Error in MDM GetID API")
           
            if resp.status_code != 200:
                log_data = [{"LogDetails" : f"Error in lead id min max api response status code : {str(resp.status_code)}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T6", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)
                raise Exception(f"Api response other than 200 {resp.status_code} retry number {cnt}")
        

        except Exception as e:
            print(f"Error in GetLeadID API {e}")
            if cnt < 3:
                resp = self.fetch_api_resp(start_date, end_date, cnt)
            else:
                log_data = [{"LogDetails" : f"Error in lead id min max api response status code retry count {str(cnt)}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T6", "Status" : "ERROR"}]
                self.log_ops.save_logs(log_data)

        return resp
    
    def fetch_sec_details(self,minid,maxid):
        try:
            API = f'https://www.samsungindiamarketing.com/LMSWebAPI/Api/SECDealerMappingDetailsById?MinId={minid}&MaxId={maxid}'

            try:
                with eventlet.Timeout(GetMdmData.TIMEOUT) as e:
                    resp = requests.get(API, headers = GetMdmData.HEADERS_DI )

            except eventlet.Timeout :
                raise TimeoutError("TESTING--Timeout Error in MDM GetID API")
        
        except Exception as e:
            raise Exception('Error in Fetching SEC Details API')

        return resp
    
    def run(self):
        startdate = '20230715'
        enddate= '20230717'
        resp = self.fetch_id(startdate,enddate,3)
        # import pdb;pdb.set_trace()
        
        if not resp:
            raise Exception(f"No LeadID Found Between {startdate , enddate}")
        else:
            resp = resp.json()
        min_id = int(resp[0]['MinId'])
        max_id = int(resp[0]['MaxId']) #2142640
#
        df_lst = []
        
        for i in range(min_id ,max_id ,500):
            print(i ,min_id+499)
            try:
                sec_resp = self.fetch_sec_details(i ,min_id+500)
            
            except Exception:
                pass

            min_id = min_id + 500
            if min_id > max_id:
                break
            
            if sec_resp.status_code != 200:
                continue
            elif len(sec_resp.json()) == 0:
                continue
            else:
                df = pd.DataFrame(sec_resp.json())
                df_lst.append(df)
                print('found sec')


        final_df = pd.concat(df_lst,ignore_index= True)
        import pdb;pdb.set_trace()
        final_df.drop_duplicates(subset=['MappingId'])
        #final_df.drop_duplicates(subset = ['SECMobile','StoreCode'])
        final_df['MappingId'] = final_df['MappingId'].astype(int)
        final_df['InsertedDate'] = pd.to_datetime(final_df['InsertedDate'])
        #final_df.drop_duplicates(subset=['SECMobile','StoreCode'])
        final_df.to_csv('SecDataMDM.csv')
        self.db_ops.ingest_dataframe_to_sql(self.connection,'dbo.SECDealerTemp',final_df)

           


if __name__ == '__main__':
    obj = GetMdmData()
    obj.run()
    
