# -*- coding: utf-8 -*-
"""
Created on Sun Jul  4 21:42:53 2021

@author:  Hari Kishan Chand Perugu
"""
from scripts_bank.baseline.Trans_GHG_Tool import TransGHG
from scripts_bank.baseline.dataBase_upload import dbSetup
from sqlalchemy.sql import text
import pdb
from flask import render_template, request, Blueprint, jsonify
import pandas as pd 


def comparListAggOutput(compListName, compList,typeAgg,sum_mean):
    newTransGHG = TransGHG('SQLite')
    for idx,simCntry in enumerate(compList) :
        simCntry_df = newTransGHG.qryAggData(simCntry,typeAgg)
        if idx == 0:
            simDet_df = simCntry_df
        else:
            simDet_df = simDet_df.append(simCntry_df)
         
        if typeAgg == 'CAITCompatible':
            if sum_mean =='sum':
                simDet_df = simDet_df.groupby(['YEAR']).sum()
            elif sum_mean == 'mean':
                simDet_df = simDet_df.groupby(['YEAR']).mean()
        else:
            if sum_mean == 'sum':
                simDet_df = simDet_df.groupby(['Mode','YEAR']).sum()
            elif sum_mean == 'mean':
                simDet_df = simDet_df.groupby(['Mode','YEAR']).mean()
        
            
        simDet_df.reset_index(inplace = True)
        simDet_df.insert(loc = 0,column = 'country',value = compListName)
    
    return simDet_df
 

   
    
def main(args):
    ECA = ['Albania','Armenia','Austria','Azerbaijan','Belarus','Bosnia and Herzegovina','Bulgaria','Croatia',\
      'Cyprus','Czech Republic','Estonia','Georgia','Hungary','Kazakhstan','Kyrgyz Republic',\
      'Kosovo','Latvia','Lithuania','Moldova','Montenegro','North Macedonia','Poland','Romania',\
      'Russian Federation','Serbia','Slovak Republic','Slovenia','Tajikistan','Turkey',\
      'Turkmenistan','Ukraine','Uzbekistan']
        
    WestBalkans = ['North Macedonia', 'Serbia', 'Albania', 'Bosnia and Herzegovina','Montenegro','Kosovo']
    ###### get arguments from form
    cntryList = args['Compare'].split(',')
    cntryListName = 'Not Defined'
    cntry = args['Country']
    
    newTransGHG = TransGHG('SQLite')
    #pdb.set_trace()
    typeofOutput = 'CAITCompatible'
    cntryAgg_df = newTransGHG.qryAggData(cntry,'CAITCompatible')
    cait_df = newTransGHG.qryCAITData(cntry,'YearlyAgg')
    cait_df = cait_df.drop(['CNTRY','YEAR'],axis =1)
    
    cait_df.columns = ['CAIT_Tot_GHG']
    cait_df = cait_df['CAIT_Tot_GHG'] * 1000
    listAgg_df = comparListAggOutput(cntryListName, cntryList,'CAITCompatible','sum')
    concat_df = pd.concat([cntryAgg_df,listAgg_df,cait_df], axis =1 )
       
    if typeofOutput == 'CAITCompatible':
        concat_df.columns = ['Country', 'Cntry_Year','Cntry_CO2','Cntry_CH4',\
                         'Cntry_N2O','Cntry_Tot','Comp','Comp_Year','Comp_CO2','Comp_CH4',\
                         'Comp_N2O','Comp_Tot', 'CAIT_Tot_GHG']
        concat_df = concat_df.drop(['Cntry_CO2','Cntry_CH4','Cntry_N2O','Comp_Year','Comp_CO2','Comp_CH4',\
                         'Comp_N2O'], axis=1)
    else:
        concat_df.columns = ['Country','Cntry_Mode', 'Cntry_Year','Cntry_CO2','Cntry_CH4',\
                         'Cntry_N2O','Cntry_Tot','Comp','Comp_Mode', 'Comp_Year','Comp_CO2','Comp_CH4',\
                         'Comp_N2O','Comp_Tot', 'CAIT_Tot_GHG']
        concat_df = concat_df.drop(['Cntry_CO2','Cntry_CH4','Cntry_N2O','Comp_Mode', 'Comp_Year','Comp_CO2','Comp_CH4',\
                         'Comp_N2O'], axis=1)
        
    
    round_cols = ['Cntry_Tot', 'Comp_Tot', 'CAIT_Tot_GHG']
    concat_df[round_cols] = concat_df[round_cols].round(1)
    country_emis_dict = concat_df.to_json(orient = 'records')
    return country_emis_dict
    


####FLASK
Trans_GHG_Tool_bp = Blueprint('baseline', __name__, template_folder='templates', static_folder='static')

@Trans_GHG_Tool_bp.route('/baseline', methods=['GET','POST'])


def index():
    if request.method == 'GET':
        return render_template('/main/child.html')

    #handle POST method from JQuery
    elif request.method == 'POST':
        
        print(request.form)
        getmodel_args = {'Country': request.form['country'],
                          'Year': request.form['year'],
                          'Compare':request.form['Comparator']}
        
        
        #global country_emis_dict
        #country_emis_dict = {'collected_output': ''}
        
       
        output = main(getmodel_args)
                  
        return output

    