# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 16:18:30 2021

@author: Hari Kishan Chand Perugu PhD 
"""

"""
Import Libraries 
"""
#import mysql.connector as mysql
import pandas as pd
from xmlProcess import xmlProcess
from dataBase_upload import dbSetup
from sqlalchemy.sql import text
from openpyxl_Output import openpyxl_Output
import pdb

     
class TransGHG():
    """Intialize the class"""
    def __init__(self, conType):
       self.conType = conType
        
                   
    
    """Querying the Emission rates/Fuel data to Data Frame"""
    def qryData(self, table_name):
        eng = dbSetup.creatEng(self,self.conType)
        self.table_name = table_name
        # Start the session 
        new_sqlite_conn = eng.connect()
        # Run the query 
        with new_sqlite_conn.begin() as conn:
            conn.execute(text("SELECT * FROM "+ table_name +";"))
        sql_data = pd.DataFrame(conn.fetchall())
        sql_data.columns = conn.column_names

        return sql_data
        
    """ Fuel Data Conversion"""
    def dataConvert(self, data_frame):
        self.data_frame = data_frame
        eng = dbSetup.creatEng(self,self.conType)
        # join two data frames based on fuel units
        
        # mutiply qunatities with conv factor and read that into a new column
        return 0
        
        
    """Calculate Fuel Consumption from CO2 Emissions"""
    def FuelCalc(self):
        eng = dbSetup.creatEng(self,self.conType)
        # Start the session 
        # Create a new Fuel Consumption Table from CO2 consumption and Emission Rates Table        
        with eng.begin() as conn:
            conn.execute(text("BEGIN TRANSACTION;"))
            conn.execute(text("DROP TABLE IF EXISTS country_trans_fuel;"))
            conn.execute(text("CREATE TABLE country_trans_fuel AS  SELECT l.COUNTRY, l.FLOW, l.PRODUCT,l.TIME , \
                              (l.OBS / r.Value) AS FUELQ  from FuelAll l inner join EmisRatesFinal r ON \
                                  l.PRODUCT = r.PRODUCT and l.FLOW = r.FLOW WHERE r.POLLUTANT = 'CO2';"))
            conn.execute(text ("COMMIT;"))
    
    """get Country Code"""
    def getCntryCode(self,country):
        self.country = country
        eng = dbSetup.creatEng(self,self.conType)
        
        with eng.connect() as conn:
            result = conn.execute(text("SELECT COUNTRY FROM CountryLkup WHERE Country_Long_Name = :x"), {"x":country}).scalar()
        
        return result
    
    """ Generate accurate Emission Rates Table"""
   # Generate accurate Emission Rates Table#
    def genERtabl(self,ERtype):
        eng = dbSetup.creatEng(self,self.conType)
        self.ERtype = ERtype
        #self.cntry = cntry
        #### Get  avalue to adjust the emisison rates based on their development index
        #cntryCode = self.getCntryCode(cntry)
        cntryCode = 0
       
        if ERtype == 'Default':
            with eng.begin() as conn:
                conn.execute(text("BEGIN TRANSACTION;"))
                conn.execute (text("DROP TABLE IF EXISTS EmisRatesFinal;"))
                conn.execute(text("CREATE TABLE EmisRatesFinal AS SELECT l.FLOW, l.PRODUCT,l.POLLUTANT, \
                                  l.VALUE FROM EmisRates l WHERE l.Range = 'Default';"))
                conn.execute(text ("COMMIT;"))
        else:
            with eng.begin() as conn:
                conn.execute(text("BEGIN TRANSACTION;")) 
                conn.execute(text("DROP TABLE IF EXISTS EmisRatesFinal;"))
                conn.execute(text("CREATE TABLE EmisRates_Up AS SELECT l.FLOW. l.PRODUCT,l.POLLUTANT, \
                                  l.VALUE FROM EmisRates l WHERE l.Range = 'Upper';"))
                conn.execute(text("CREATE TABLE EmisRates_Low AS SELECT l.FLOW. l.PRODUCT,l.POLLUTANT, \
                                  l.VALUE FROM EmisRates l WHERE l.Range = 'Lower';"))
                qrys =  text("CREATE TABLE EmisRatesFinal AS SELECT l.FLOW, l.PRODUCT,l.POLLUTANT\
                                  (l.VALUE - r.VALUE)/10 * " +str(cntryCode)+"FROM EmisRates_Up l inner join EmisRates_Low r \
                                      ON l.PRODUCT = r.PRODUCT and l.FLOW = r.FLOW;")              
                conn.execute(qrys)
                conn.execute(text ("COMMIT;"))
                                  
                


    # Calculate GHG Emissions from Fuel Consumption #
    def EmisCalc(self):
        eng = dbSetup.creatEng(self,self.conType)
        
        
        # Create a new Fuel Consumption Table from CO2 consumption and Emission Rates Table        
        with eng.begin() as conn:
            conn.execute(text("BEGIN TRANSACTION;"))
            # Run the query for three different pollutants
            conn.execute(text("DROP TABLE IF EXISTS country_trans_emis;"))
            qrystr1 = text("CREATE TABLE country_trans_emis AS  SELECT l.COUNTRY, l.FLOW, l.PRODUCT,l.TIME,l.FUELQ,\
                           (l.FUELQ * r.Value ) AS CO2_emis from country_trans_fuel l inner join EmisRatesFinal r ON l.PRODUCT = r.PRODUCT\
                               AND l.FLOW = r.FLOW WHERE r.POLLUTANT = 'CO2';")
            conn.execute(qrystr1)
            for pol in ['CH4', 'N2O']:
                
                newqrystr1 = text("ALTER TABLE country_trans_emis ADD COLUMN "+pol+'_emis'+" FLOAT")
                conn.execute(newqrystr1 )
                newqrystr2 = text("UPDATE country_trans_emis \
                                  SET "+ pol+'_emis'+ " = FUELQ * Emis.Value \
                                  FROM (SELECT * FROM EmisRatesFinal) AS Emis \
                                  WHERE country_trans_emis.FLOW = Emis.FLOW AND \
                                  country_trans_emis.PRODUCT = Emis.PRODUCT AND \
                                  Emis.POLLUTANT = :y ;")
                                  
                conn.execute(newqrystr2,{"y":pol})
            conn.execute(text("ALTER TABLE country_trans_emis ADD COLUMN Tot_GHG_emis FLOAT;"))
            conn.execute(text("UPDATE country_trans_emis \
                              SET Tot_GHG_emis = (CO2_emis + CH4_emis *28 +N2O_emis *265); "))
            conn.execute(text("COMMIT;"))
            
             
    #Querying the Emission rates/Fuel data to Data Frame#
    def qryAggData(self,cntry,yearlist, agg_type):
        eng = dbSetup.creatEng(self,self.conType)
        cntryCode = self.getCntryCode(cntry)
        self.agg_type = agg_type
        self.yearlist = yearlist
        if agg_type == 'Detailed':
            yearstr =  'AND e.TIME > 2010 AND e.TIME < 2019'
        if len(yearlist) == 1:
            yearstr = 'AND e.TIME = ' + str(yearlist[0])
        elif len(yearlist) == 8:
            yearstr = 'AND e.TIME > 2010 AND e.TIME < 2019'
        else:
            yearstr = ''
            for year in yearlist:
                yearstr += 'AND e.year = '+ str(year)
        # Start the session 
        if agg_type == 'Detailed':
            # Run the query 
            with eng.connect() as conn:
                qrystr = text("SELECT c.Country_Long_Name AS CNTRY, f.Flow_Long_Name AS Mode, p.Product_Long_Name AS Fuel,\
                              e.TIME AS YEAR, sum(e.CO2_Emis) AS CO2_kT,sum(e.CH4_Emis) AS CH4_kT, sum(e.N2O_Emis) AS N2O_kT,\
                              sum(e.CO2_Emis) AS CO2_kT_CO2_eq,sum(e.CH4_Emis*28) AS CH4_kT_CO2_eq, sum(e.N2O_Emis*265) AS N2O_kT_CO2_eq,\
                              sum(e.Tot_GHG_emis) AS Tot_GHG_kT_CO2_eq \
                              FROM country_trans_emis e \
                              LEFT JOIN ProductLkup p ON p.PRODUCT = e.PRODUCT \
                              LEFT JOIN FlowLkup f ON f.FLOW = e.FLOW \
                              LEFT JOIN CountryLkup c ON c.COUNTRY = e.COUNTRY \
                              WHERE e.COUNTRY = :x " + yearstr +\
                            " AND e.PRODUCT <> 'TOTAL' \
                              GROUP BY e.COUNTRY,e.FLOW, e.PRODUCT, e.TIME \
                              ORDER BY  e.TIME, e.PRODUCT, e.FLOW;")
                results = conn.execute(qrystr, {"x":cntryCode}).fetchall()
                try:  
                    sql_data = pd.DataFrame(results, columns = results[0].keys())
                    
                except:
                    data =  []
                    for yr in range(2011,2019):
                        data.append([cntry,'N/A', 'N/A', str(yr), 0.0,0.0,0.0, 0.0,0.0,0.0,0.0])
                   
                    sql_data = pd.DataFrame(data, columns = ['CNTRY', 'Mode','Fuel', 'YEAR', 'CO2_kT', \
                                                             'CH4_kT','N2O_kT','CO2_kT_CO2_eq', 'CH4_kT_CO2_eq',\
                                                             'N2O_kT_CO2_eq', 'Tot_GHG_kT_CO2_eq'])                                                      
          
            
        elif agg_type == 'YearlyAgg':
            # Run the query 
            with eng.connect() as conn:
                qrystr = text("SELECT c.Country_Long_Name AS CNTRY, f.Flow_Long_Name AS Mode, \
                              e.TIME AS YEAR, sum(e.CO2_Emis) AS CO2,sum(e.CH4_Emis) AS CH4, sum(e.N2O_Emis) AS N2O, sum(e.Tot_GHG_emis) AS Tot_GHG \
                              FROM country_trans_emis e \
                              LEFT JOIN ProductLkup p ON p.PRODUCT = e.PRODUCT \
                              LEFT JOIN FlowLkup f ON f.FLOW = e.FLOW \
                              LEFT JOIN CountryLkup c ON c.COUNTRY = e.COUNTRY \
                              WHERE e.COUNTRY = :x  "+ yearstr + 
                              " AND e.PRODUCT <> 'TOTAL' \
                              GROUP BY e.COUNTRY,e.FLOW, e.TIME \
                              ORDER BY e.COUNTRY,e.FLOW, e.TIME;")
                results = conn.execute(qrystr, {"x":cntryCode}).fetchall()
                try:  
                    sql_data = pd.DataFrame(results)
                    sql_data.columns = results[0].keys()
                except:
                    data =  []
                    for yr in range(2011,2019):
                        data.append([cntry,'N/A', str(yr), 0.0,0.0,0.0, 0.0])
                   
                    sql_data = pd.DataFrame(data, columns = ['CNTRY', 'Mode', 'YEAR', 'CO2_kT', \
                                                             'CH4_kT','N2O_kT','Tot_GHG_kT_CO2_eq'])  
            
           
        elif agg_type == 'CAITCompatible':
            # Run the query 
            with eng.connect() as conn:
                qrystr = text("SELECT c.Country_Long_Name AS CNTRY,\
                              e.TIME AS YEAR,sum(e.CO2_Emis) AS CO2_kT_CO2_eq,sum(e.CH4_Emis*28) AS CH4_kT_CO2_eq,\
                              sum(e.N2O_Emis*265) AS N2O_kT_CO2_eq,\
                              sum(e.Tot_GHG_emis) AS Tot_GHG_kT_CO2_eq \
                              FROM country_trans_emis e \
                              LEFT JOIN CountryLkup c ON c.COUNTRY = e.COUNTRY \
                              WHERE e.COUNTRY = :x " + yearstr +
                             " AND e.PRODUCT <> 'TOTAL' \
                              GROUP BY e.COUNTRY, e.TIME \
                              ORDER BY  e.TIME;")
                results = conn.execute(qrystr, {"x":cntryCode}).fetchall()
                try:  
                    sql_data = pd.DataFrame(results)
                    sql_data.columns = results[0].keys()
                except:
                    data =  []
                    for yr in range(2011,2019):
                        data.append([cntry, str(yr), 0.0,0.0,0.0, 0.0])
                    sql_data = pd.DataFrame(data, columns = ['CNTRY', 'YEAR', 'CO2_kT', \
                                                             'CH4_kT_CO2_eq','N2O_kT_CO2_eq','Tot_GHG_kT_CO2_eq'])  
            
            
        else:
            with eng.connect() as conn:
                qrystr = text("SELECT c.Country_Long_Name AS CNTRY, f.Flow_Long_Name AS Mode,\
                              e.TIME AS YEAR, sum(e.CO2_Emis) AS CO2,sum(e.CH4_Emis) As CH4, sum(e.N2O_Emis) AS N2O, sum(e.Tot_GHG_emis) AS Tot_GHG \
                              FROM country_trans_emis e \
                              LEFT JOIN FlowLkup f ON f.FLOW = e.FLOW \
                              LEFT JOIN CountryLkup c ON c.COUNTRY = e.COUNTRY \
                              WHERE e.COUNTRY = :x "+ yearstr +
                             " AND e.PRODUCT <> 'TOTAL' \
                              GROUP BY e.COUNTRY,  e.TIME,e.FLOW \
                              ORDER BY  e.TIME,e.FLOW;")
                results = conn.execute(qrystr, {"x":cntryCode}).fetchall()
                try:  
                    sql_data = pd.DataFrame(results)
                    sql_data.columns = results[0].keys()
                except:
                    data =  []
                    for yr in range(2011,2019):
                        data.append([cntry, 'N/A',  str(yr), 0.0,0.0,0.0, 0.0])
                    sql_data = pd.DataFrame(data, columns = ['CNTRY', 'Mode', 'Year', 'CO2_kT', \
                                                             'CH4_kT','N2O_kT','Tot_GHG_kT_CO2_eq']) 
                

        return sql_data    
       
    def qryCAITData(self,cntry,yearlist, agg_type):
        eng = dbSetup.creatEng(self,self.conType)
        cntryCode = self.getCntryCode(cntry)
        self.agg_type = agg_type
        self.yearlist = yearlist
        if len(yearlist) == 1:
            yearstr = 'AND e.year = ' + str(yearlist[0])
        elif len(yearlist) == 8:
            yearstr = 'AND e.year > 2010 AND e.year < 2019'
        else:
            yearstr = ''
            for year in yearlist:
                yearstr += 'AND e.year = '+ str(year)
                
        #self.year = year
        # Start the session 
        if agg_type == 'YearlyAgg':
            # Run the query 
            with eng.connect() as conn:
                qrystr = text("SELECT c.Country_Long_Name AS CNTRY, e.year AS YEAR,\
                              TOT_GHG_MT FROM CAIT e \
                              LEFT JOIN CountryLkup c ON c.Country = e.country \
                              WHERE e.country = :x " + yearstr + " AND e.sector = 'Transportation';")
                results = conn.execute(qrystr, {"x":cntryCode}).fetchall()
                try:  
                    sql_data = pd.DataFrame(results)
                    sql_data.columns = results[0].keys()
                except: 
                    data =  []
                    for yr in range(2011,2019):
                        data.append([cntry, 'N/A',  str(yr), 0.0])
                    sql_data = pd.DataFrame(data, columns = ['CNTRY', 'YEAR', 'Tot_GHG_kT_CO2_eq']) 
        
        
        return sql_data
                    
    def qryCntryClass(self,cntry):
        eng = dbSetup.creatEng(self,self.conType)
        cntryCode = self.getCntryCode(cntry)
        with eng.connect() as conn:
            qrystr = text("SELECT WB_Region_Lend FROM WorldClass WHERE CountryCode = :x;")
            results = conn.execute(qrystr,{"x":cntryCode}).fetchall()
       
        region = results[0].values()[0]
       
        return region
    
    def qryCompCntryL(self,cntryRegion):
        eng = dbSetup.creatEng(self,self.conType)
        self.cntryRegion = cntryRegion
        with eng.connect() as conn:
            qrystr = text("SELECT  CountryCode FROM WorldClass WHERE WB_Region_Lend = :x;")
            results = conn.execute(qrystr,{"x":cntryRegion}).fetchall()
        countries = []
        for cntry in results:
            countries.append(cntry[0])
       
        
        return countries
                          
    
    """ A Method to read Country specific fuel consumption data from User"""
    def readFuelCSV(self,cntryFuel):
        self.cntryFuel = cntryFuel
        
        return 0
    
    
    """ A method to read Road Actvity Data"""
    def readRoadActiv(self, roadActiv):
        self.roadActiv = roadActiv
        return 0
    
    """To read Rail Activity Data"""
    
    
    """ For average Rail Actvity/Fuel consumption Data"""
    
    
    """ For Landing and Take Off Data"""
    
    
    """ For Aviation Cruising Data"""
    
    
    """ A Method to read Maritime Data"""
    
    
    """ For Surrogate for Maritime Data"""
    
    
    """ For Surrogate for Off-Road Sector"""
    
           
    """  Export the output into a new Table"""  
    def exportData(self,df,tablName):
       self.out_df = df
       self.tablName = tablName
       
       return 0
        
        
        
        
def main():
    """Get list of countries from Users """
  
    ModelLevel = 'Baseline'
    caltype = 'individual'
    CountryList =['North Macedonia']
    EAP = ["Cambodia","China","Fiji","Indonesia","Kiribati","Laos","Malaysia",\
           "Marshall Islands","Micronesia","Mongolia","Myanmar","Nauru","Palau",\
           "Papua New Guinea","Philippines","Samoa","Solomon Islands","Thailand",\
           "Timor-Leste","Tonga","Tuvalu","Vanuatu","Vietnam"]
    ECA = ["Albania","Armenia","Azerbaijan","Belarus","Bosnia and Herzegovina",\
           "Bulgaria","Croatia","Georgia","Kazakhstan","Kosovo","Kyrgyzstan",\
           "Moldova","Montenegro","North Macedonia","Poland","Romania",\
           "Russian Federation","Serbia","Tajikistan","Turkey","Turkmenistan","Ukraine","Uzbekistan"]
    LAC = ["Antigua and Barbuda","Argentina","Belize","Bolivia","Brazil","Chile","Colombia",\
           "Costa Rica","Dominica","Dominican Republic","Ecuador","El Salvador",\
           "Grenada","Guatemala","Guyana","Haiti","Honduras","Jamaica","Mexico",\
           "Nicaragua","Panama","Paraguay","Peru","Saint Kitts and Nevis","Saint Lucia",\
           "Saint Vincent and the Grenadines","Suriname","Trinidad and Tobago","Uruguay","Venezuela"]
  
    subSahara = ["Angola","Benin","Botswana","Burkina Faso","Burundi","Cameroon",\
                 "Cape Verde","Central African Republic","Chad","Comoros",\
                 "Congo,Democratic Republic of","Congo,Republic of","Côte d'Ivoire",\
                 "Equatorial Guinea","Eritrea","Eswatini","Ethiopia","Gabon","Gambia","Ghana",\
                 "Guinea","Guinea-Bissau","Kenya","Lesotho","Liberia","Madagascar","Malawi","Mali",\
                 "Mauritania","Mauritius","Mozambique","Namibia","Niger","Nigeria","Rwanda",\
                 "Sao Tome and Principe","Senegal","Seychelles","Sierra Leone","Somalia",\
                 "South Africa","South Sudan","Sudan","Tanzania","Togo","Uganda"]
    MENA = ["Algeria","Djibouti","Egypt","Iran","Iraq","Jordan","Lebanon","Libya","Morocco","Syria","Tunisia"]
    SA = ["Afghanistan","Bangladesh","Bhutan","India","Maldives","Nepal","Pakistan","Sri Lanka"]
    AvgCountryList = SA
    yearlist = ['2011','2012','2013','2014','2015','2016','2017','2018']
    
    
    
    """Default files used  """
    RootDir = "C:\\Users\\wb580236\\OneDrive - WBG\\TransportDecarbonization_Lit\\"
    OutDir = "C:\\Users\\wb580236\\OneDrive - WBG\\TransportDecarbonization_Lit\\Output\\"
    """ Data dump from IEA Database """
    dumpFile = RootDir + "data_dumps\\DataGeneric.xml"
    """ Default Emission Rates from IPCC report """
    ERfile =  RootDir + "Emission_Rates_V2.csv"
    #ConvFile =  RootDir + "Conv_factors.csv"
    """ Fuel Lookup Values """
    ProductLookupFile = RootDir + "Product.csv"
    """Mode Lookup Values """
    FlowLookupFile = RootDir + "Flow.csv"
    """ Country Lookup Values """
    CntryLookupFile = RootDir +"Country_Codes.csv"
    #Development Classification Lookup
    #DevelopLookupFile = RootDir + "DevelopLookUp.csv"
    #Development Classification Lookup
    #DevelopLookupFile = RootDir + "DevelopLookUp.csv"
    #All ModesRoadway Passenger and Freight Split 
    SplitLookupFile = RootDir + "All_Modes_Split.csv"

    """ Setting up DB """
    """ Read Default Emission Rates file  generated from IPCC Report """
    newERtabl = dbSetup(ERfile)
    newERtabl.readData('SQLite',ERfile, 'EmisRates')
    
    
    """ Read Gigantic XML dump file from IEA website """
    if newERtabl.checkTabl('SQLite','FuelAll') == 'no':
        pdb.set_trace()
        newxml = xmlProcess(dumpFile)
        newxmlDf = newxml.convDf()
        newFueltabl = dbSetup(newxmlDf)
        newFueltabl.readData('SQLite',newxmlDf, 'FuelAll')
        newFueltabl.Preprocess('SQLite')
    
    newProdtabl = dbSetup(ProductLookupFile)
    newProdtabl.readData('SQLite',ProductLookupFile, 'ProductLkup')
    newFlowtabl = dbSetup(FlowLookupFile)
    newFlowtabl.readData('SQLite',FlowLookupFile, 'FlowLkup')
    newCntrytabl = dbSetup(CntryLookupFile)
    newCntrytabl.readData('SQLite', CntryLookupFile, 'CountryLkup')
    newSplittabl = dbSetup(CntryLookupFile)
    newSplittabl.readData('SQLite',SplitLookupFile,'Split')
    
    """ Read Conversion Rates file  """
          
     
    """ Read World Bank Classification file"""
    
    
    newTransGHG = TransGHG('SQLite')
    newTransGHG.genERtabl('Default')
    ##### Don't run this step if this table already exists
    if ModelLevel == 'Baseline':
        if newERtabl.checkTabl('SQLite','country_trans_fuel') == 'no':
            pdb.set_trace()
            newTransGHG.FuelCalc()
            newTransGHG.EmisCalc()
    else:
        if newERtabl.checkTabl('SQLite','country_trans_fuel') == 'no':
            pdb.set_trace()
            newTransGHG.FuelCalc()
            newTransGHG.EmisCalc()
            newTransGHG.PassFreightCalc()
            
       
   
    
    #### Extract single country every country
   
    
    if caltype == 'individual':
        for cntry in CountryList:
             ## Define a new object
            cntryDet_df = newTransGHG.qryAggData(cntry,yearlist, 'Detailed')
            cntryFilename = OutDir+cntry+'.xlsx'
            #cntryAgg_df = newTransGHG.qryAggData(cntry,'Aggregated')
            #cntryYrly_df = newTransGHG.qryAggData(cntry,'YearlyAgg')
           
            
            newOutput =  openpyxl_Output(cntryFilename)
            newOutput.detailOutput(cntryDet_df,cntryFilename )
            
    else:
        
        simcntryFilename = OutDir+'SA_Mean.xlsx'  
        
        for idx,simCntry in enumerate(AvgCountryList) :
            simCntry_df = newTransGHG.qryAggData(simCntry,yearlist,'Detailed')
            
            if idx == 0:
                simDet_df = simCntry_df
            else:
                simDet_df = simDet_df.append(simCntry_df)
            
            
        simDet_df_sum = simDet_df.groupby(['Mode','Fuel','YEAR']).mean()
        simDet_df_sum.reset_index(inplace = True)
        simDet_df_sum.insert(loc = 0,column = 'country',value = 'SA_Mean')
        newOutput =  openpyxl_Output(simcntryFilename)
        #newOutput.detailOutput(cntryDet_df,cntryFilename )
        newOutput.detailOutput(simDet_df_sum,simcntryFilename )
        
    
    ### To create a detailed output  
    #  newOutput.sheetOutput(cntryDet_df,cntryAgg_df,cntryYrly_df,simAgg_df,cntry,filename )  
    # Extract the data and conversion
    # Do the same for Emission Rates
    # Do the calculation and output data as data frame
        

if __name__ == "__main__":
    main()
