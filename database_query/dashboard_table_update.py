from utils.db_ops import DBOps
from utils.log_ops import LogOps





class UpdateDashboardTable(object):
   # QUERY =  "EXEC dbo.spUpdateDashboardTableDetails"
    QUERY =  "INSERT INTO DashboardTable (RowID, LeadID, LeadCollectionDateTime, PinCode, Campaign, Platform,Business,Category)\
    SELECT RowID, LeadID, LeadCreationTimestamp, PinCode,Campaign, Platform,Division, Category \
    FROM dbo.RawLeads WITH (NOLOCK) \
    WHERE RowID > (SELECT isnull(MAX(RowID),0) FROM DashboardTable WITH (NOLOCK));\
    \
    Update a \
    Set \
    a.Business = b.Division, \
    a.Category = b.Category, \
    a.FormID = b.FormID \
    from dbo.RawLeads  as b inner join DashboardTable as a \
    on b.RowID = a.RowID; \
    \
    Update a \
    Set \
    a.Business = b.Division \
    from dbo.AllLeads  as b inner join DashboardTable as a \
    on b.RowID = a.RowID \
    where a.Business = 'NA'; \
    \
    Update a \
    Set \
    a.Category = b.Category \
    from dbo.AllLeads  as b inner join DashboardTable as a \
    on b.RowID = a.RowID \
    where a.Category = 'NA'; \
    \
    \
    Update a \
    Set \
    a.UUID = b.UUID, \
    a.EncryptedPhoneNo = master.dbo.fn_varbintohexstr(HASHBYTES('SHA2_256',b.PhoneNumber)) \
    from dbo.AllLeads  as b inner join DashboardTable as a \
    on b.RowID = a.RowID \
    WHERE b.LeadQuality = 1\
    ; \
    \
    \
    Update a \
    Set \
    a.MasterCampaign = b.CampaignName \
    from dbo.SubCampaignMaster  as b inner join DashboardTable as a \
    on b.FormId = a.FormId; \
    \
    \
    Update a \
    Set \
    a.Score1 = case when b.LeadScore = 'UNKNOWN' then '0' else  b.LeadScore END, \
    a.Score2 = case when b.CustomerTier  = 'UNKNOWN' then '0' else b.CustomerTier END \
    from dbo.CustomerScoreGCDM as b inner join DashboardTable as a \
    on b.UUID = a.UUID ; \
    \
    UPDATE a \
    SET \
        a.LeadAllocationDateTime = b.LeadModifiedTimestamp, \
        a.AllocatedToFlag = case when b.StoreCode = 'EP' then 'EP' else 'SD' end , \
        a.SDAllocationStore = b.StoreCode, \
        a.SDAllocationTier = b.AllocationType \
    FROM \
        dbo.LeadsAssignment AS b \
    INNER JOIN \
        DashboardTable AS a ON b.UUID = a.UUID \
    WHERE \
        a.AllocatedToFlag IS NULL; \
    \
    \
    Update a \
    Set \
    a.LeadFirstContactDateTime = b.CallStart, \
    a.LeadStatus = b.Status \
    from dbo.LeadStatus as b inner join DashboardTable as a \
    on b.UUID = a.UUID; \
    \
    \
    \
    Update [dbo].[DashboardTable] \
    Set [LeadFirstContactFlag] = \
    Case \
    WHEN LeadFirstContactDateTime IS NULL THEN NULL \
    when DATEDIFF(hour, LeadAllocationDateTime, LeadFirstContactDateTime) <0 then 'NA' \
    when DATEDIFF(hour, LeadAllocationDateTime, LeadFirstContactDateTime) > 0 and DATEDIFF(hour, LeadAllocationDateTime, LeadFirstContactDateTime) <=6 then 'LESS THAN 6 HOURS' \
    when DATEDIFF(hour, LeadAllocationDateTime, LeadFirstContactDateTime) > 6 and DATEDIFF(hour, LeadAllocationDateTime, LeadFirstContactDateTime) <=12 then '6 - 12 HOURS' \
    when DATEDIFF(hour, LeadAllocationDateTime, LeadFirstContactDateTime) > 12 and DATEDIFF(hour, LeadAllocationDateTime, LeadFirstContactDateTime) <=24 then '12 - 24 HOURS' \
    when DATEDIFF(hour, LeadAllocationDateTime, LeadFirstContactDateTime) > 24 and DATEDIFF(hour, LeadAllocationDateTime, LeadFirstContactDateTime) <=48 then '24 - 48 HOURS' \
    ELSE 'GREATER THAN 48 HOURS' END \
    \
    \
    Update a \
    Set \
    a.City = b.City, \
    a.State = b.State \
    from dbo.PincodeLatLonMaster as b inner join DashboardTable as a \
    on b.PinCode = a.PinCode \
    where a.City is NULL; "


    CAPACITY_QUERY = " declare @UpdatedStoreMasterTemp as table \
    ( \
        City nvarchar(255), \
        Division varchar(50), \
        MaxCapacityThreshold INT \
    ) \
    \
    insert INTO @UpdatedStoreMasterTemp \
    ( \
        City, \
        Division,  \
        MaxCapacityThreshold \
    ) \
    \
    select A.City , B.Division ,sum(B.MaxCapacity) as  MaxCapacityThreshold \
    from PincodeLatLonMaster A \
    join StoreDetailsMaster B on                          \
    A.PinCode = B.PinCode  \
    WHERE B.StoreStatus = 'Y' \
    group by A.City , B.Division                            \
    \
    Update A \
    set A.MaxCapacity = B.MaxCapacityThreshold \
    From DashboardTable A \
    join @UpdatedStoreMasterTemp B \
    on A.City = B.City and A.Business = B.Division \
    where A.AllocatedToFlag= 'SD' and A.Business != 'Others' \
    \
    Update A \
    set A.MaxCapacity = B.MaxCapacityThreshold \
    From DashboardTable A \
    join @UpdatedStoreMasterTemp B \
    on A.City = B.City \
    where A.AllocatedToFlag= 'SD' and A.Business = 'Others' \
    \
    update A      \
    set A.MaxCapacity = B.MaxCapacity     \
    From DashboardTable A    \
    join StoreDetailsMaster B       \
    on A.AllocatedToFlag = B.StoreCode            \
    join CampaignDetails C                     \
    on              \
    (Case   \
    WHEN C.EPromoter = 'MX' then 'IM' ELSE  \
    (CASE WHEN C.Epromoter = 'Y' then 'CE' ELSE C.Epromoter END) END )= B.Division     \
    where C.CampaignName = A.MasterCampaign and B.StoreCode ='EP'  "


    def __init__(self):
        self.dbops = DBOps() 
        self.connection = self.dbops.create_connection()
        self.log_ops = LogOps()
        #self.save_dashboard_lead_management = self.read_json(DBOps.ENV_CONFIG)['SaveLogsToLeadManagement']
        # if self.save_logs_lead_management == "YES":
        #     try:
        #         self.leadmanagement_connection = self.create_connection()
        #     except Exception as e:
        #         raise(f"Error in Connecting to LeadManagement {e}")



    
    def run(self):
        try:           
            #print(UpdateDashboardTable.QUERY) 
            flag = self.dbops.execute_query(self.connection,UpdateDashboardTable.QUERY)
            print("Successfully Updated the dashboard table")
            #flag = self.dbops.execute_query(self.connection , UpdateDashboardTable.CAPACITY_QUERY)
            #print("Succesfully Updated Capacity For Dashboard table")
            log_data = [{"LogDetails" : f'Succesffuly Updated Dashboard Table', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T7", "Status" : "SUCCESS"}]
            self.log_ops.save_logs(log_data)


        except Exception as e :
            log_data = [{"LogDetails" : f'Error in Running Dashboard Table Query {str(e)}', 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 5, "Stage" : "T7", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception
        
        finally:
            self.connection.close()
    

if __name__ == "__main__":
    obj = UpdateDashboardTable()
    obj.run()
   
 

#where a.LeadStatus != 'CLOSED' ; \