# -*- coding: utf-8 -*-
"""
Created on Wed Jun  9 17:26:31 2021

@author: Hari kishan Chand Perugu PhD
"""

from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy.sql import text
import pdb



class dbSetup():
    def __init__(self,filename):
        self.filename = filename
        
    """ To create a connection engine for different database server"""
    
    def creatEng(self,conType):
        self.conType = conType
        if conType == 'SQLite':
            #engine = create_engine('sqlite:///:memory:', echo=True)
            eng = create_engine('sqlite:///C:\\Users\\wb580236\\sqlite3\\Transport.db', echo=True)
        elif conType == 'MariaDB':
            eng = create_engine("mariadb+pymysql://<user>:<password>@<some_mariadb_host>[:<port>]/<dbname>?charset=utf8mb4", echo=False)
        elif conType == 'MySQL':
            eng = create_engine("mysql+mysqlconnector://<user>:<password>@<host>[:<port>]/<dbname>")
        else:
            print ("Connection Type Not Specified. Eg: SQLite, MySQl etc.")
            pass
        return eng
    
    """ To Check a Table exists"""
    def checkTabl(self,conType,table_name):
         eng = self.creatEng(conType)
         self.table_name = table_name

         if eng.has_table(table_name):
             chk_status = 'yes'
         else :
             chk_status = 'no'
                    
         return chk_status
    
    """ Read IEA data dump in Xml format"""
    def readData(self,conType,data,sqlite_table):
        self.conType = conType
        eng = self.creatEng(conType)
        self.data = data
        self.sqlite_table = sqlite_table
        sqlite_connection = eng.connect()
        if isinstance(data,pd.DataFrame):
            # convert read df into a variable
            newDf = data
        elif isinstance(data, str):
            # convert file into a data frame
            newDf = self.readCSV(data)
        else:
            print ("Cannot identify the type of data structure")
            pass
        try:  
            newDf.to_sql(sqlite_table, sqlite_connection, if_exists='fail')
        except:
            print ("The table" + sqlite_table+ " already exists! ")
            pass
        sqlite_connection.close()
     
        
    """ Read CSV file """ 
    def readCSV(self,filename):
        self.filename = filename
        newDf = pd.read_csv(filename,header="infer")
        return newDf
    
    
    """ Preprocess the data"""
    def Preprocess(self, conType):
        self.conType = conType
        eng = self.creatEng(conType)
       
        # Start the session 
        with eng.begin() as conn:
            # Create the PRIMARY KEY for Emission rate  Table, if does not exist
            conn.execute(text("BEGIN TRANSACTION;"))
            conn.execute(text("DROP TABLE IF EXISTS FuelAll_old"))
            conn.execute(text("ALTER TABLE FuelAll RENAME TO FuelAll_old;"))
            conn.execute(text("CREATE TABLE FuelAll (ix BIGINT NOT NULL ,COUNTRY VARCHAR(50) NOT NULL,\
                              FLOW VARCHAR(50) NULL, PRODUCT  VARCHAR(50) NULL, TIME INT NULL, OBS FLOAT NULL,\
                              OBS_STATUS VARCHAR(50) NULL, CONSTRAINT country_index \
                              PRIMARY KEY (ix,COUNTRY, PRODUCT, FLOW));"))
            conn.execute(text("INSERT INTO FuelAll(ix, COUNTRY, FLOW,PRODUCT,TIME, OBS, OBS_STATUS) \
                              SELECT \"index\", COUNTRY, FLOW,PRODUCT,cast(TIME as INTEGER), cast(OBS as FLOAT), \
                              OBS_STATUS FROM FuelAll_old;"))
            conn.execute(text ("COMMIT;"))
    
    
