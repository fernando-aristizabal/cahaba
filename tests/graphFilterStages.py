#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import geopandas as gpd

from pygsp import graphs, filters
#from dbfread import DBF

__filter_dictionary = { 'Abspline' : filters.Abspline, 'Gabor' : filters.Gabor, 'HalfCosine': filters.HalfCosine,
                        'Itersine' : filters.Itersine, 'MexicanHat' : filters.MexicanHat, 'Meyer': filters.Meyer,
                        'SimpleTight' : filters.SimpleTight, 'Regular' : filters.Regular, 'Held': filters.Held,
                        'Simoncelli' : filters.Simoncelli, 'Papadakis' : filters.Papadakis, 'Heat': filters.Heat,
                        'Expwin' : filters.Expwin
                      }

def GraphFilterStages(streamNetwork,extractedStages_columnName,filteredStages_columnName,filterName='Abspline',
                      filterParameters = {'Nf': 1},streamNetwork_vaa_fileName=None,useWeights=False,verbose=False):

    if verbose:
        print("Filtering stages ...")

    # initialize weight matrix
    numberOfCOMIDS = len(streamNetwork)
    weightMatrix = np.zeros((numberOfCOMIDS,numberOfCOMIDS),dtype=np.int8)

    # generate weighted weight matrix
    if useWeights:

        ## generate weight matrix ##
        for reachID,current_comid in enumerate(streamNetwork["HydroID"]):

            current_comid_index = streamNetwork.index[streamNetwork["HydroID"] == current_comid].to_list()

            # get to node
            toNode = streamNetwork.loc[current_comid_index,"To_Node"]
            # fromNode = streamNetwork.loc[current_comid,"From_Node"]

            toMatches = (streamNetwork.loc[:,"From_Node"] == toNode).astype(np.int8)
            # fromMatches = (streamNetwork.loc[:,"To_Node"] == fromNode).astype(np.int8)

            booleanOf_toMatches = toMatches == 1

            if booleanOf_toMatches.sum() > 0:
                toMatches_DA = np.array([],dtype=np.float32)
                for tm in toMatches[booleanOf_toMatches].index.values:
                    print(current_comid,tm)
                    current_tm_index = streamNetwork.index[streamNetwork["HydroID"] == tm].to_list()
                    toMatches_DA = np.append(toMatches_DA,[1/abs(streamNetwork.loc[current_comid_index,'TotDASqKM']-streamNetwork.loc[current_tm_index,'TotDASqKM'])])

                # toMatches_normalized_DA = toMatches_DA / np.max(toMatches_DA)
                # weightMatrix[rowIndex,booleanOf_toMatches] = toMatches_normalized_DA

                weightMatrix[rowIndex,booleanOf_toMatches] = toMatches_DA

    else:
        ## generate weight matrix as adjacency ##
        for i,(reachID,row) in enumerate(streamNetwork.iterrows()):
            
            # get to node
            toNode = row['To_Node']

            # assign adjacency to weight matrix
            weightMatrix[i,:] = (streamNetwork.loc[:,"From_Node"] == toNode).astype(np.int8)

    # make undirected
    weightMatrix = np.maximum(weightMatrix,weightMatrix.T)
    
    # np.set_printoptions(threshold=np.inf)
    # print(np.where(weightMatrix))

    # intialize graph and signals
    graphInstance = graphs.Graph(weightMatrix,coords=None)
    signal = streamNetwork[extractedStages_columnName].values

    # estimate lmax
    graphInstance.estimate_lmax()

    # graph filters
    filterClass = __filter_dictionary[filterName]
    if filterParameters is not None:
        filterInstance = filterClass(graphInstance,**filterParameters)
    else:    
        filterInstance = filterClass(graphInstance)


    # filter signal
    filteredSignal = filterInstance.filter(signal)

    # sum across eigenvalues
    if len(filteredSignal.shape) == 2:
        filteredSignal = filteredSignal.sum(axis=1)
        # filteredSignal = filteredSignal[:,:].sum(axis=1)

    # add filtered
    streamNetwork[filteredStages_columnName] = filteredSignal

    return(streamNetwork,weightMatrix)
