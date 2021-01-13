#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import geopandas as gpd
import pandas as pd
from graphFilterStages import GraphFilterStages
from inundation import inundate
import os
from itertools import product

flows100=pd.read_csv('/data/test_cases/validation_data_ble/12090301/100yr/ble_huc_12090301_flows_100yr.csv')
flows500=pd.read_csv('/data/test_cases/validation_data_ble/12090301/500yr/ble_huc_12090301_flows_500yr.csv')

flows100.set_index('feature_id',inplace=True)
flows500.set_index('feature_id',inplace=True)

streamNetwork=gpd.read_file('/data/outputs/default_12090301/12090301/demDerived_reaches_split_clipped_addedAttributes_crosswalked.gpkg')

streamNetwork.set_index('HydroID',inplace=True)


flows100.rename(columns={'discharge':"discharge1"},inplace=True)
flows500.rename(columns={'discharge':"discharge5"},inplace=True)

streamNetwork = streamNetwork.join(flows100,on='feature_id')
streamNetwork = streamNetwork.join(flows500,on='feature_id')

streamNetwork.fillna(value={'discharge1':streamNetwork.discharge1.median(),'discharge5':streamNetwork.discharge5.median()},inplace=True)


bench_mark_100 = pd.read_csv('/data/test_cases/12090301_ble/performance_archive/development_versions/default_12090301/100yr/total_area_stats.csv')
#bench_mark_500 = pd.read_csv('/data/test_cases/12090301_ble/performance_archive/development_versions/default_12090301/500yr/total_area_stats.csv')

bench_mark_100.rename(columns={'Unnamed: 0' : 'metrics','value' : 'benchmark'},inplace=True)
#bench_mark_500.rename(columns={'Unnamed: 0' : 'metrics','value' : 'benchmark'},inplace=True)

bench_mark_100.set_index('metrics',inplace=True,drop=True)
#bench_mark_500.set_index('metrics',inplace=True,drop=True)

merged_results_100 = bench_mark_100

### GRAPH FILTERING ###

"""
filterNames = ["Abspline","HalfCosine","Itersine","MexicanHat","Meyer","SimpleTight","Heat",
               "Expwin","Regular","Held","Simoncelli","Papadakis"]
filterParameter_domain = { 
                     'Abspline' : { 'lpfactor' : range(1,41,2) },
                     'Itersine' : { 'overlap' : range(1,5)},
                     'MexicanHat' : { 'lpfactor' : range(1,41,2)},
                     'Heat' : { 'tau' : range(1,21,2)},
                     'Expwin' : {'bmax' : (i/100 for i in range(0,100,10)), 'a' : (i/100 for i in range(0,210,10))}
                     'Regular' : { 'd' : range(1,7)},
                     'Held' : { 'a' : (i/100 for i in range(0,200,10))},
                     'Simoncelli' : { 'a' : (i/100 for i in range(0,200,10))},
                     'Papdakis' : { 'a' : (i/100 for i in range(0,200,10))} 
                   }
"""
filterNames = ['Itersine']
filterParameter_domain = {  'Itersine' : {'overlap' : range(1,11)}  }

for filterName in filterNames:

    try:
        filterParameter_generators = filterParameter_domain[filterName]    
        parameter_names = [k for k in filterParameter_generators.keys()]
        params = list(zip(*product(*[filterParameter_generators[p] for p in parameter_names])))
        temp = [None] * len(params)
        for i,p in enumerate(parameter_names):
            temp[i] = list(zip([p] * len(params[i]),params[i]))
        all_filterParameters = [dict(t) for t in zip(*temp)]
    except KeyError:
        all_filterParameters = [None]

    for filterParameters in all_filterParameters:
        filter_param_string =  filterName
        if filterParameters is not None:
            filter_param_string += "_" + "_".join([str(p) for i in filterParameters.items() for p in i])
        print("\n ########### {}  ########## \n".format(filter_param_string))
        
        streamNetwork,weightMatrix = GraphFilterStages(streamNetwork,extractedStages_columnName='discharge1',filteredStages_columnName='filtered1',filterName=filterName,filterParameters=filterParameters,verbose=True)
        #streamNetwork,weightMatrix = GraphFilterStages(streamNetwork,extractedStages_columnName='discharge5',filteredStages_columnName='filtered5',filterName='Abspline',verbose=True)

        # bring to zero
        streamNetwork.loc[streamNetwork['filtered1'] < 0,'filtered1'] = 0
        #streamNetwork.loc[streamNetwork['filtered5'] < 0,'filtered5'] = 0

        streamNetwork.to_file('/data/temp/STREAMNETWORK_GSP_{}.gpkg'.format(filter_param_string),driver='GPKG')

        filtered_forecast100 = streamNetwork.loc[:,['feature_id','filtered1']]
        #filtered_forecast500 = streamNetwork.loc[:,['feature_id','filtered5']]

        filtered_forecast100.rename(columns={'filtered1':"discharge"},inplace=True)
        #filtered_forecast500.rename(columns={'filtered5':"discharge"},inplace=True)

        filtered_forecast100.to_csv('/data/temp/FILTERED_FORECAST_100yr.csv',index=False)
        #filtered_forecast500.to_csv('/data/temp/FILTERED_FORECAST_500yr.csv',index=False)

        os.system('foss_fim/tests/run_test_case.py -r default_12090301/12090301 -b default_12090301_filtered -t 12090301_ble -y 100yr')

        filtered_results_100 = pd.read_csv('/data/test_cases/12090301_ble/performance_archive/development_versions/default_12090301_filtered/100yr/total_area_stats.csv')
        #filtered_results_500 = pd.read_csv('/data/test_cases/12090301_ble/performance_archive/development_versions/default_12090301_filtered/500yr/total_area_stats.csv')

        filtered_results_100.rename(columns={'Unnamed: 0' : 'metrics','value' : "{}".format(filter_param_string)},inplace=True)
        #filtered_results_500.rename(columns={'Unnamed: 0' : 'metrics','value' : "_{}".format(filter_param_string)},inplace=True)

        filtered_results_100.set_index('metrics',inplace=True,drop=True)
        #filtered_results_500.set_index('metrics',inplace=True,drop=True)

        merged_results_100 = pd.merge(merged_results_100,filtered_results_100,left_index=True,right_index=True,how='outer')
        #merged_results_500 = pd.merge(bench_mark_500,filtered_results_500,left_index=True,right_index=True,how='outer',suffixes=('_benchmark','_filtered'))

        pd.set_option('display.float_format', lambda x: '%.5f' % x)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        print(merged_results_100.loc[['CSI','TPR','FAR'],:])

merged_results_100.to_csv("/data/temp/MERGED_RESULTS_V2_100.csv",index=True)
