# -*- coding: utf-8 -*-
"""
Created on Sat Jun  5 20:25:30 2021

@author: Hari kishan Chand Perugu PhD
"""

import pdb
import xml.etree.ElementTree as ET
import pandas as pd
import sqlite3 as db
# Parsing the XML file
#file_xml = 'C:\\Users\\wb580236\\OneDrive - WBG\\TransportDecarbonization_Lit\\DataGeneric.xml'


class xmlProcess:
    """Intialize the class"""
    def __init__(self,file_xml):
        self.file_xml =file_xml
        
        
    def convDict(self,file_xml):
        
        self.file_xml = file_xml
        
        ## Getting Tree and Root items
        tree = ET.parse(file_xml)
        root = tree.getroot()
        
        xmlDict = {} 
        ### To iterate root item  
        for item in root.iter():
            if item.tag == '{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic}Series':
                for child in item:
                    if child.tag == '{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic}SeriesKey':
                        for subchild in child:
                            if subchild.attrib['concept'] == 'COUNTRY':
                                country = subchild.attrib['value']
                                if not country in xmlDict:
                                    xmlDict[country] = {}
                            if subchild.attrib['concept'] == 'FLOW':
                                flow = subchild.attrib['value']
                                if not flow in xmlDict[country]:
                                    xmlDict[country][flow] = {}
                            if subchild.attrib['concept'] == 'PRODUCT':
                                product = subchild.attrib['value']
                                if not product in xmlDict[country][flow]:
                                    xmlDict[country][flow][product] = {}
                    if child.tag == '{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic}Obs':
                        for subchild in child:
                            if subchild.tag == '{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic}Time':     
                                year = subchild.text
                                if not year in xmlDict[country][flow][product]:
                                    xmlDict[country][flow][product][year] = {}
                
                            if subchild.tag == '{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic}ObsValue':
                                obsval = subchild.attrib['value'] 
                                xmlDict[country][flow][product][year]['obs'] =  obsval
                            if subchild.tag == '{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic}Attributes':
                                for grandchild in subchild:
                                    obs_status = grandchild.attrib['value'] 
                                    xmlDict[country][flow][product][year]['obs_status'] = obs_status
                                    
                                    
        return xmlDict                            
                                    
    def grab_children(self,father):
        self.father = father
        big_list = []
        for cntry, nvalue in iter(father.items()):
            for flow,nvalu in iter(father[cntry].items()):
                for prod, nval in iter(father[cntry][flow].items()):
                    for yr, nva in iter(father[cntry][flow][prod].items()):
                        if not 'obs_status' in nva.keys():
                            obs_status = 'N/A'
                        else:
                            obs_status = nva['obs_status']
                        obsVal = nva['obs']
                        big_list.append([cntry,flow, prod, yr,obsVal,obs_status])
                       
        return big_list
    
    def convDf(self):
        cols = ["COUNTRY", "FLOW", "PRODUCT", "TIME", "OBS", "OBS_STATUS"]
        
        newxmlDict = self.convDict(self.file_xml)
        Dict2List = self.grab_children(newxmlDict)
        xml_Df = pd.DataFrame(Dict2List, columns= cols) 
  
        return xml_Df
    

    
                 

  