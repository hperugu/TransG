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
#from xmlProcess import xmlProcess
from scripts_bank.baseline.xmlProcess import xmlProcess
from scripts_bank.baseline.dataBase_upload import dbSetup
from sqlalchemy.sql import text
import pdb
from flask import render_template, request, Blueprint, jsonify

     
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
        
        # mutiply qunatities with conv factor and reda that into a new column
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
                                  
                


    """ Calculate GHG Emissions from Fuel Consumption """
    def EmisCalc(self):
        eng = dbSetup.creatEng(self,self.conType)
        # Start the session 
        
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
            
             
    """Querying the Emission rates/Fuel data to Data Frame"""
    def qryAggData(self,cntry,agg_type):
        eng = dbSetup.creatEng(self,self.conType)
        cntryCode = self.getCntryCode(cntry)
        self.agg_type = agg_type
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
                              WHERE e.COUNTRY = :x \
                              AND e.TIME <> 2019 \
                              AND e.PRODUCT <> 'TOTAL' \
                              GROUP BY e.COUNTRY,e.FLOW, e.PRODUCT, e.TIME \
                              ORDER BY  e.TIME, e.PRODUCT, e.FLOW;")
                results = conn.execute(qrystr, {"x":cntryCode}).fetchall()
            
          
            
        elif agg_type == 'YearlyAgg':
            # Run the query 
            with eng.connect() as conn:
                qrystr = text("SELECT c.Country_Long_Name AS CNTRY, f.Flow_Long_Name AS Mode, \
                              e.TIME AS YEAR, sum(e.CO2_Emis) AS CO2,sum(e.CH4_Emis) AS CH4, sum(e.N2O_Emis) AS N2O, sum(e.Tot_GHG_emis) AS Tot_GHG \
                              FROM country_trans_emis e \
                              LEFT JOIN ProductLkup p ON p.PRODUCT = e.PRODUCT \
                              LEFT JOIN FlowLkup f ON f.FLOW = e.FLOW \
                              LEFT JOIN CountryLkup c ON c.COUNTRY = e.COUNTRY \
                              WHERE e.COUNTRY = :x \
                              AND e.TIME <> 2019 \
                              AND e.PRODUCT <> 'TOTAL' \
                              GROUP BY e.COUNTRY,e.FLOW, e.TIME \
                              ORDER BY e.COUNTRY,e.FLOW, e.TIME;")
                results = conn.execute(qrystr, {"x":cntryCode}).fetchall()
            
           
        elif agg_type == 'CAITCompatible':
            # Run the query 
            with eng.connect() as conn:
                qrystr = text("SELECT c.Country_Long_Name AS CNTRY,\
                              e.TIME AS YEAR,sum(e.CO2_Emis) AS CO2_kT_CO2_eq,sum(e.CH4_Emis*28) AS CH4_kT_CO2_eq,\
                              sum(e.N2O_Emis*265) AS N2O_kT_CO2_eq,\
                              sum(e.Tot_GHG_emis) AS Tot_GHG_kT_CO2_eq \
                              FROM country_trans_emis e \
                              LEFT JOIN CountryLkup c ON c.COUNTRY = e.COUNTRY \
                              WHERE e.COUNTRY = :x \
                              AND e.TIME <> 2019 \
                              AND e.PRODUCT <> 'TOTAL' \
                              GROUP BY e.COUNTRY, e.TIME \
                              ORDER BY  e.TIME;")
                results = conn.execute(qrystr, {"x":cntryCode}).fetchall()
            
            
        else:
            with eng.connect() as conn:
                qrystr = text("SELECT c.Country_Long_Name AS CNTRY, f.Flow_Long_Name AS Mode,\
                              e.TIME AS YEAR, sum(e.CO2_Emis) AS CO2,sum(e.CH4_Emis) As CH4, sum(e.N2O_Emis) AS N2O, sum(e.Tot_GHG_emis) AS Tot_GHG \
                              FROM country_trans_emis e \
                              LEFT JOIN FlowLkup f ON f.FLOW = e.FLOW \
                              LEFT JOIN CountryLkup c ON c.COUNTRY = e.COUNTRY \
                              WHERE e.COUNTRY = :x \
                              AND e.TIME = 2015 \
                              AND e.PRODUCT <> 'TOTAL' \
                              GROUP BY e.COUNTRY,  e.TIME,e.FLOW \
                              ORDER BY  e.TIME,e.FLOW;")
                results = conn.execute(qrystr, {"x":cntryCode}).fetchall()
            
        sql_data = pd.DataFrame(results)
        sql_data.columns = results[0].keys()

        return sql_data           
    def qryCAITData(self,cntry,agg_type):
        eng = dbSetup.creatEng(self,self.conType)
        cntryCode = self.getCntryCode(cntry)
        self.agg_type = agg_type
        year = 2015
        #self.year = year
        # Start the session 
        if agg_type == 'YearlyAgg':
            # Run the query 
            with eng.connect() as conn:
                qrystr = text("SELECT c.Country_Long_Name AS CNTRY, e.year AS YEAR,\
                              TOT_GHG_MT FROM CAIT e \
                              LEFT JOIN CountryLkup c ON c.Country = e.country \
                              WHERE e.country = :x \
                              AND e.year > 2010 \
                              AND e.year < 2019 \
                              AND e.sector = 'Transportation';")
                results = conn.execute(qrystr, {"x":cntryCode}).fetchall()
            
        elif agg_type == 'Yealy':
             with eng.connect() as conn:
                qrystr = text("SELECT c.Country_Long_Name AS CNTRY, e.year AS YEAR,\
                              TOT_GHG_MT FROM CAIT e \
                              LEFT JOIN CountryLkup c ON c.Country = e.country \
                              WHERE e.country = :x \
                              AND e.year = :y \
                              AND e.sector = 'Transportation';")
                results = conn.execute(qrystr, {"x":cntryCode,"y":year}).fetchall()   
            
        sql_data = pd.DataFrame(results)
        sql_data.columns = results[0].keys()
        
        return sql_data
                    
        
    
    """ A Method to read Country specific fuel consumption data from User"""
    def readFuelCSV(self,cntryFuel):
        self.cntryFuel = cntryFuel
        
        return 0
    
    
    """ A method to read Road Actvity Data"""
    def readRoadActiv(self, roadActiv):
        self.roadActiv = roadActiv
        return 0
    
 
    def exportData(self,df,tablName):
       self.out_df = df
       self.tablName = tablName
       
       return 0
        
        
        
        
def main(args):
    """Get list of countries from Users """
  
    """ List of Directories"""
    RootDir = "C:\\Users\\wb580236\\OneDrive - WBG\\TransportDecarbonization_Lit\\"
    OutDir = "C:\\Users\\wb580236\\OneDrive - WBG\\TransportDecarbonization_Lit\\Output\\"
    
    
    """Input files used  """
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
    

    """ Setting up DB 
        Using many input files"""
    
    """ Read Default Emission Rates file  generated from IPCC Report """
    newERtabl = dbSetup(ERfile)
    newERtabl.readData('SQLite',ERfile, 'EmisRates')
    """ Read Gigantic XML dump file from IEA website """
    if newERtabl.checkTabl('SQLite','FuelAll') == 'no':
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
  
    
    """ Read Conversion Rates file  """
     
     
     
    """ Read World Bank Classification file"""
    
    
    newTransGHG = TransGHG('SQLite')
    newTransGHG.genERtabl('Default')
    newTransGHG.FuelCalc()
    newTransGHG.EmisCalc()


    """ Country Listings"""
    ECA = ['Albania','Armenia','Austria','Azerbaijan','Belarus','Bosnia and Herzegovina','Bulgaria','Croatia',\
           'Cyprus','Czech Republic','Estonia','Georgia','Hungary','Kazakhstan','Kyrgyz Republic',\
           'Kosovo','Latvia','Lithuania','Moldova','Montenegro','North Macedonia','Poland','Romania',\
           'Russian Federation','Serbia','Slovak Republic','Slovenia','Tajikistan','Turkey',\
           'Turkmenistan','Ukraine','Uzbekistan']
    WestBalkans = ['North Macedonia', 'Serbia', 'Albania', 'Bosnia and Herzegovina','Montenegro','Kosovo']
    caltype = 'combined'
    AvgCountryList = WestBalkans
    CountryList = WestBalkans
    simCntryDict = {'Kazakhstan':ECA}
    
    
    """Iterate the list of countries to export data to the output table """
    cntry = args['Country']
    try:
        cntryAgg_df = newTransGHG.qryAggData(cntry,'Aggregated')
        country_emis_dict['collected_output'] = cntryAgg_df.to_json(orient = 'columns')
        #cntryYrly_df = newTransGHG.qryAggData(cntry,'YearlyAgg')
    except:
        country_emis_dict['error'] = '<div class="alert alert-danger" role="alert"><strong>ERROR:</strong> Could not get output from model</div>'
           
    return country_emis_dict
    
####FLASK
Trans_GHG_Tool_bp = Blueprint('baseline', __name__, template_folder='templates', static_folder='static')

@Trans_GHG_Tool_bp.route('/baseline', methods=['GET','POST'])

#def index():
#    return render_template("/main/child.html")
#@Trans_GHG_Tool_bp.route('/baseline', methods=['GET','POST'])
def index():
    if request.method == 'GET':
        return render_template('/main/child.html')

    #handle POST method from JQuery
    elif request.method == 'POST':
        
        print(request.form)
        getmodel_args = {'Country': request.form['country'],
                          'Year': request.form['year']}
        
        global country_emis_dict
        country_emis_dict = {'collected_output': '',
                     'error': ''}
        #output = main(getmodel_args)
        output = {"col_1":"val_11", "col_2":"val_12", "col_3":"val_13"},\
                    {"col_1":"val_21", "col_2":"val_22", "col_3":"val_23"},\
                    {"col_1":"val_31", "col_2":"val_32", "col_3":"val_33"}
  
        #pdb.set_trace()   
        return jsonify(output)

