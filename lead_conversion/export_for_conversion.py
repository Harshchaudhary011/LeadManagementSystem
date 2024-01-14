import pandas as pd
import numpy ,datetime ,requests,base64,pytz
from utils.log_ops import LogOps
from utils.db_ops import DBOps
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import hashlib
import pyDes
import binascii
from Crypto.Util import Padding


class LeadConversionExport():
    ALL_LEADS_TABLE = 'dbo.AllLeads'
    DASHBOARD_TABLE = 'dbo.DashboardTable'
    DEST_TABLE = 'dbo.ConversionLeads'
    IST = pytz.timezone('Asia/Kolkata')

    def __init__(self):
        self.dbops = DBOps()
        self.log_ops = LogOps()
        self.connection = self.dbops.create_connection()
        pass

    
    def fetch_leads(self):
        query = f"SELECT UUID , PhoneNumber ,Email as EmailID ,LeadCreationTimestamp from {LeadConversionExport.ALL_LEADS_TABLE} WHERE UUID IN \
            (SELECT UUID FROM {LeadConversionExport.DASHBOARD_TABLE} where UUID is not NUll ) and PendingForAction = 0"
        try:
            df = self.dbops.execute_dataframe_query(self.connection,query)
        except Exception as e:
            raise Exception("Error in Fetching Data for Conversion")
        return df
    

    def pad(self,st , block_size):
        padding = ((block_size-len(st)) % block_size)
    
        return st + bytes([padding]*padding)
    
    def encrypt_data(self,data_to_encrypt):
        try:
            iv = "CCE@GG@dm1n99887"
            key = 'CCE@MG@dm1n99887'
            key_en = bytes(key,'utf-8')
            iv_en = bytes(iv,'utf-8')
            cipher = AES.new(key_en,AES.MODE_CBC,iv_en)
            encrypt_data = cipher.encrypt(self.pad(data_to_encrypt.encode(), AES.block_size))
            cipher_text_en = base64.b64encode(encrypt_data).decode()
            hex_string = cipher_text_en.encode('utf-16le').hex().upper()
        except Exception as e:
            log_data = [{"LogDetails" : f"Error in encrypt data {str(e)}", 
                "ScriptName" : str(__file__), "ModuleName" : str(self.__class__.__name__), 
                "SeverityLevel" : 4, "Stage" : "T2", "Status" : "ERROR"}]
            self.log_ops.save_logs(log_data)
            raise Exception(f"Error in encrypt data {e}")
        return hex_string
    
    def run(self):

        df = self.fetch_leads()
        #df['EmailID'] = df['EmailID'].apply(lambda x : self.encrypt_data(x)).astype(str)
        #df['PhoneNumber'] = df['PhoneNumber'].apply(lambda x : self.encrypt_data(x))
        df['InsertedOn'] =  datetime.datetime.now(LeadConversionExport.IST).strftime("%Y-%m-%d %H:%M:%S")
        #import pdb;pdb.set_trace()
        self.dbops.ingest_dataframe_to_sql(self.connection ,LeadConversionExport.DEST_TABLE,df)
        print("Successfully updated Table -ConversionLeads")


if __name__ =='__main__':
    obj = LeadConversionExport()
    obj.run()