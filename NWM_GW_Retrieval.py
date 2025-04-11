# -*- coding: utf-8 -*-
"""
Created on Thu Mar 27 10:35:30 2025

@author: Delanie Williams
"""

import pandas as pd
import dask.dataframe as dd
import dask
import os
from dask.diagnostics import ProgressBar

#Conversion of USGS ID to COMID
def USGS_to_NWM(USGS_gage, crosstable_path):
    df = pd.read_csv(crosstable_path)  
    gage_no=df['Gage_no']
    COMID=df['COMID']
    if USGS_gage in gage_no.values:
        row_number=gage_no.index[gage_no == USGS_gage][0]
        return COMID[row_number]
    else:
        print("the USGS ID %s does not have a COMID match" %(USGS_gage))
        return None
    
#def extract_baseflow(COMID_individ, dataset):
#.compute() is for intermediate saving, only when necessary or crash memory 
def extract_baseflow_NWM(COMID_individ, ds):
    qBucket=ds["qBucket"]
    baseflow_timeseries=qBucket.sel(feature_id=COMID_individ)
    with ProgressBar():
        df=baseflow_timeseries.to_dataframe().reset_index()
        ddf=dd.from_pandas(df, npartitions=10).compute()
    return ddf

#convert to parquet instead of .csv for efficiency
def clean(ddf,USGS_gage,output_filename):
    file_path=os.path.join("NMW_GW_data",f"NWM_gage_{USGS_gage}.parquet")
    ddf=ddf.drop(['elevation','latitude','longitude','order'], axis=1)
    ddf.to_parquet(output_filename, engine="pyarrow", compression="snappy")
    
#gonna have the NWM files by USGS id for comparison
def process_one_gage(USGS_gage, crosstable_path, ds):
    COMID_individ=USGS_to_NWM(USGS_gage, crosstable_path)
    if COMID_individ:
        ddf=extract_baseflow_NWM(COMID_individ, ds)
        clean(ddf, USGS_gage, f"NWM_gage_{USGS_gage}.parquet")
        
def process_gages_parallel(USGS_gages, crosstable_path, ds):
    tasks=[]

    for gage in USGS_gages:
        task=dask.delayed(process_one_gage)(gage,crosstable_path,ds)
        tasks.append(task)
    dask.compute(*tasks)
    
