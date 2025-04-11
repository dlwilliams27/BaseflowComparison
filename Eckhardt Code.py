# -*- coding: utf-8 -*-
"""
Created on Sun Feb 16 21:13:41 2025

@author: Delanie Williams
"""
#required libraries
import baseflow 
import pandas as pd 
from numba import njit
import numpy as np
import os
from baseflow import param_estimate
import matplotlib.pyplot as plt

#directories
folder_path='C:\\Users\\Delanie Williams\\OneDrive - The University of Alabama\\Coding\\NRT Eckhardt Project\\Basin_timeseries_data\\basin_dataset_public_v1p2\\usgs_streamflow\\03'
output_folder='C:\\Users\\Delanie Williams\\OneDrive - The University of Alabama\\Coding\\NRT Eckhardt Project\\Basin_timeseries_data\\basin_dataset_public_v1p2\\usgs_streamflow\\finished baseflow'

#Iteration process
for filename in os.listdir(folder_path):
    if filename.endswith(".txt"):
        #updating directory
        os.chdir('c:\\Users\\Delanie Williams\\OneDrive - The University of Alabama\\Coding\\NRT Eckhardt Project')
        path=r'C:\Users\Delanie Williams\OneDrive - The University of Alabama\Coding\NRT Eckhardt Project\Basin_timeseries_data\basin_dataset_public_v1p2\usgs_streamflow\01\01013500_streamflow_qc.txt'
        trial=pd.read_csv(path, sep=" ",header=None,on_bad_lines='skip')
        columns=[1,2,3,6]
        extracted_df=trial.iloc[:,columns]
        extracted_df.columns=['year','month','day','q']
        dates=pd.to_datetime(extracted_df[['year','month','day']], errors='coerce')
        working_df=pd.concat([dates,extracted_df['q']],axis=1)
        working_df.columns=['date','q']
        #turning into a series
        working_df['q']=pd.to_numeric(working_df['q'],errors='coerce')
        working_df.dropna(subset=['q'], inplace=True)
        working_series=working_df.set_index('date')['q']
        #creation of baseflow value array
        new=baseflow.single(working_series,method='Eckhardt',return_kge=True)
        baseflow_series, kge_value=new
        #convert to m3/s
        baseflow_series=baseflow_series*(.3048**3)
        new_filename=filename.replace(".txt","_processed.csv")
        new_file_path=os.path.join(output_folder, new_filename)
        baseflow_series.to_csv(new_file_path)

        
        
        