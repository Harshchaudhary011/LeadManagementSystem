from utils.db_ops import DBOps
from utils.log_ops import LogOps




class ConversionMatching():
    SRC_TABLE = 'dbo.ConversionHistoryDummy'
    DEST_TABLE  = 



    QUERY = '''select A.UUID,B.FullName ,B.PhoneNumber,B.LeadCreationTimestamp , B.Division as LeadDivision  , A.CAT1 as Purchase_Category ,A.REG_DATE as PurchaseDate , A.Dealer_Price 
From ConversionHistoryDummy A
join LeadsAssignment B
on A.PhoneNumber = replace(B.PhoneNumber ,'+' , '')
where A.REG_DATE > B.LeadCreationTimestamp
'''