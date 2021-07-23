# -*- coding: utf-8 -*-
"""
Created on Tue Jun 15 13:25:02 2021

@author: Hari Kishan Chand Perugu PhD
"""


import pandas as pd 
import pdb
#from vincent.colors import brews

class xlsOutput():
    
    def __init__(self,filename):
        self.filename = filename

    def detailSheet(self,data_frame,filename):
        self.data_frame = data_frame
        self.filename = filename
        writer = pd.ExcelWriter(filename, engine='xlsxwriter')
        data_frame.to_excel(writer, sheet_name='Detailed', na_rep='', header=True, index=True, index_label=None, startrow=0, startcol=0)
        writer.save()
        
    def columnChart(self, cntry_df, region_df, filename):
        self.cntry_df = cntry_df
        self.region_df = region_df
        avg_reg_df = region_df.groupby('Mode').mean()
        pdb.set_trace()
        # Create a Pandas Excel writer using XlsxWriter as the engine.
        excel_file = filename
        writer = pd.ExcelWriter(excel_file, engine='xlsxwriter')

        sheet_name = 'Charts'
        cntry_df.set_index('Mode')
        cntry_df.to_excel(writer, sheet_name=sheet_name, startrow=0, startcol=0)
        #region_df.set_index('Mode')
        # Access the XlsxWriter workbook and worksheet objects from the dataframe.
        workbook = writer.book
        worksheet = writer.sheets[sheet_name]
        
        # Create a chart object.
        chart1 = workbook.add_chart({'type': 'column', 'subtype': 'stacked'})
        workbook = writer.book
        worksheet = writer.sheets[sheet_name]
        chart1 = workbook.add_chart({'type': 'column', 'subtype': 'percent_stacked'})
        # Configure the series of the chart from the dataframe data.
        for row_num in range(0, 11):
            chart1.add_series({
                'name':       ['Charts',row_num,2],
                'values':     ['Charts', row_num, 6, row_num, 6],
                'gap':        100,
                'data_labels': {'value': True}
                })
        
        # Configure the chart axes.
        chart1.set_y_axis({'major_gridlines': {'visible': False}})
        chart1.set_x_axis({'num_font':  {'name': 'Calibri', 'size': 10}})
        worksheet.insert_chart('K2', chart1)
        chart1.set_legend({'position': 'right'})
        chart1.set_legend({
        'layout': {
            'x':      0.80,
            'y':      0.37,
            'width':  0.12,
            'height': 0.25,
            }
        })
        # Add X axis Label
        
        chart1.set_title({
        'name': 'Title',
        'overlay': True,
        'layout': {
            'x': 0.42,
            'y': 0.14,
            }
        })
        
        avg_reg_df.to_excel(writer, sheet_name=sheet_name, startrow=20, startcol=0)
        
        # Create another chart object.
        chart2 = workbook.add_chart({'type': 'column', 'subtype': 'percent_stacked'})
 
        # Configure the series of the chart from the dataframe data.
        for row_num in range(20, 31):
            chart2.add_series({
                'name':       ['Charts',row_num,2],
                'values':     ['Charts', row_num, 6, row_num, 6],
                'gap':        100,
                'data_labels': {'value': True}
                })
        
        # Configure the chart axes.
        chart2.set_y_axis({'major_gridlines': {'visible': False}})
        
        # Insert the chart into the worksheet.
        worksheet.insert_chart('K20', chart2)
        
        # Close the Pandas Excel writer and output the Excel file.
        writer.save()
        
    def lineChart(self, cntry_df, region_df, filename):
        self.cntry_df = cntry_df
        self.region_df = region_df
        #### Summarize the totals by Mode, Year
        excel_file = filename
        writer = pd.ExcelWriter(excel_file, engine='xlsxwriter')
        pdb.set_trace()

        sheet_name = 'TimeSeries'
        cntry_df.set_index('Mode')
        cntry_df.to_excel(writer, sheet_name=sheet_name, startrow=0, startcol=0)
        #region_df.set_index('Mode')
        workbook = writer.book
        worksheet = writer.sheets[sheet_name]
        
        # Create a chart object.
        chart3 = workbook.add_chart({'type': 'column', 'subtype': 'stacked'})

        
        # Create a chart object.
        chart3 = workbook.add_chart({'type': 'line'})
        
        # Configure the series of the chart from the dataframe data.
        for row_num in range(1,136):
            
            chart3.add_series({
                'name':       ['TimeSeries',row_num,2],
                'categories': ['TimeSeries', row_num, 3, row_num+9, 3],
                'values':     ['TimeSeries', row_num, 6, row_num+9, 6],
            })
            
        # Configure the chart axes.
        chart3.set_y_axis({'major_gridlines': {'visible': False}})
        
        # Insert the chart into the worksheet.
        worksheet.insert_chart('K2', chart3)
        
        # Close the Pandas Excel writer and output the Excel file.
        writer.save()
                        
            
            