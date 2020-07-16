#!/usr/bin/env python3


    
def compute_stats_from_contingency_table(true_negatives, false_negatives, false_positives, true_positives, cell_area=None):
    """
    This generic function takes contingency table metrics as arguments and returns a dictionary of contingency table statistics.
    Much of the calculations below were taken from older Python files. This is evident in the inconsistent use of case.
    
    Args:
        true_negatives (int): The true negatives from a contingency table.
        false_negatives (int): The false negatives from a contingency table.
        false_positives (int): The false positives from a contingency table.
        true_positives (int): The true positives from a contingency table.
        cell_area (float or None): This optional argument allows for area-based statistics to be calculated, in the case that
                                   contingency table metrics were derived from areal analysis.
    
    Returns:
        stats_dictionary (dict): A dictionary of statistics. Statistic names are keys and statistic values are the values.
                                 Refer to dictionary definition in bottom of function for statistic names.
    
    """
    
    
    import numpy as np
    
    total_population = true_negatives + false_negatives + false_positives + true_positives
        
    # Basic stats.
    percent_correct = (true_positives + true_negatives) / total_population
    pod             = true_positives / (true_positives + false_negatives)
    far             = false_positives / (true_positives + false_positives)
    csi             = true_positives / (true_positives + false_positives + false_negatives)
    bias            = (true_positives + false_positives) / (true_positives + false_negatives)
    
    # Compute equitable threat score (ETS) / Gilbert Score. 
    a_ref = ((true_positives + false_positives)*(true_positives + false_negatives)) / total_population
    equitable_threat_score = (true_positives - a_ref) / (true_positives - a_ref + false_positives + false_negatives)

    total_population = true_positives + false_positives + true_negatives + false_negatives
    TP_perc = (true_positives / total_population) * 100
    FP_perc = (false_positives / total_population) * 100
    TN_perc = (true_negatives / total_population) * 100
    FN_perc = (false_negatives / total_population) * 100
    
    predPositive = true_positives + false_positives
    predNegative = true_negatives + false_negatives
    obsPositive = true_positives + false_negatives
    obsNegative = true_negatives + false_positives
    
    # This checks if a cell_area has been provided, thus making areal calculations possible.
    if cell_area != None:
        TP_area = true_positives * cell_area
        FP_area = false_positives * cell_area
        TN_area = true_negatives * cell_area
        FN_area = false_negatives * cell_area
        total_area = total_population * cell_area
        
        predPositive_area = predPositive * cell_area
        predNegative_area = predNegative * cell_area
        obsPositive_area =  obsPositive * cell_area
        obsNegative_area =  obsNegative * cell_area
        positiveDiff_area = predPositive_area - obsPositive_area
        MCC = (TP_area*TN_area - FP_area*FN_area)/ np.sqrt((TP_area+FP_area)*(TP_area+FN_area)*(TN_area+FP_area)*(TN_area+FN_area))

    # If no cell_area is provided, then the contingeny tables are likely not derived from areal analysis.
    else:
        TP_area = None
        FP_area = None
        TN_area = None
        FN_area = None
        total_area = None
        
        predPositive_area = None
        predNegative_area = None
        obsPositive_area =  None
        obsNegative_area =  None
        positiveDiff_area = None
        MCC = None
        
    total_population = true_positives + false_positives + true_negatives + false_negatives

    predPositive_perc = predPositive / total_population
    predNegative_perc = predNegative / total_population
    obsPositive_perc = obsPositive / total_population
    obsNegative_perc = obsNegative / total_population
    

    positiveDiff_perc = predPositive_perc - obsPositive_perc
    
    prevalence = (true_positives + false_negatives) / total_population
    PPV = true_positives / predPositive
    NPV = true_negatives / predNegative
    TPR = true_positives / obsPositive
    TNR = true_negatives / obsNegative
    ACC = (true_positives + true_negatives) / total_population
    Bal_ACC = np.mean([TPR,TNR])
    F1_score = (2*true_positives) / (2*true_positives + false_positives + false_negatives)

    stats_dictionary = {'true_negatives': int(true_negatives),
                        'false_negatives': int(false_negatives),
                        'true_positives': int(true_positives),
                        'false_positives': int(false_positives),
                        'percent_correct': round(percent_correct, 3),
                        'pod': round(pod, 3),
                        'far': round(far, 3),
                        'csi': round(csi, 3),
                        'bias': round(bias, 3),
                        'equitable_threat_score': round(equitable_threat_score, 3), 
                        'TP_perc': TP_perc,
                        'FP_perc': FP_perc,
                        'TN_perc': TN_perc,
                        'FN_perc': FN_perc,
                        'total_area': total_area,
                        'prevalence': prevalence,
                        'predPositive_perc': predPositive_perc,
                        'predNegative_perc': predNegative_perc,
                        'obsPositive_perc': obsPositive_perc,
                        'obsNegative_perc': obsNegative_perc,
                        'predPositive_area': predPositive_area,
                        'predNegative_area': predNegative_area,
                        'obsPositive_area': obsPositive_area,
                        'obsNegative_area': obsNegative_area,
                        'positiveDiff_area': positiveDiff_area,
                        'positiveDiff_perc': positiveDiff_perc,
                        'PPV': PPV,
                        'NPV': NPV,
                        'TPR': TPR,
                        'TNR': TNR,
                        'ACC': ACC,
                        'F1_score': F1_score,
                        'Bal_ACC': Bal_ACC,
                        'MCC': MCC
                        }

    return stats_dictionary


def get_contingency_table_from_binary_rasters(benchmark_raster_path, predicted_raster_path, agreement_raster=None, mask_values=None, additional_layers_dict={}):
    """
    Produces contingency table from 2 rasters and returns it. Also exports an agreement raster classified as:
        0: True Negatives
        1: False Negative
        2: False Positive
        3: True Positive
        
    Args:
        benchmark_raster_path (str): Path to the binary benchmark raster. 0 = phenomena not present, 1 = phenomena present, NoData = NoData.
        predicted_raster_path (str): Path to the predicted raster. 0 = phenomena not present, 1 = phenomena present, NoData = NoData.
    
    Returns:
        contingency_table_dictionary (dict): A Python dictionary of a contingency table. Key/value pair formatted as:
                                            {true_negatives: int, false_negatives: int, false_positives: int, true_positives: int}
    
    """
    
    import rasterio
    import numpy as np
    import os
        
    # Load rasters.
    benchmark_src = rasterio.open(benchmark_raster_path)    
    benchmark_array = benchmark_src.read(1)
    predicted_src = rasterio.open(predicted_raster_path)
    predicted_array = predicted_src.read(1)
    
    predicted_array_raw = predicted_src.read(1)

    # WILL NOT STAY--JUST DEALING WITH DIFFERENT SHAPES OF INPUT DATA:
    benchmark_array = benchmark_array[:, :-1]
    benchmark_array = np.where(predicted_array==predicted_src.nodata, 10, benchmark_array)
            
    # Ensure zeros and ones for binary comparison. Assume that positive values mean flooding and 0 or negative values mean dry. 
    predicted_array = np.where(predicted_array==predicted_src.nodata, 10, predicted_array)  # Reclassify NoData to 10
    predicted_array = np.where(predicted_array<0, 0, predicted_array)
    predicted_array = np.where(predicted_array>0, 1, predicted_array)
    
    benchmark_array = np.where(benchmark_array==benchmark_src.nodata, 10, benchmark_array)  # Reclassify NoData to 10

    # Create agreement_array in memory.
    agreement_array = np.add(benchmark_array, 2*predicted_array)
    
    # Mask agreement array according to mask catchments.
    print("Masking...")
    for value in mask_values:
        agreement_array = np.where(np.absolute(predicted_array_raw) == int(value), 4, agreement_array)
        
    agreement_array = np.where(agreement_array>4, 10, agreement_array)
    
    del benchmark_src, benchmark_array, predicted_array, predicted_array_raw

    contingency_table_dictionary = {}
    
    # Only write the agreement raster if user-specified.
    if agreement_raster != None:
        with rasterio.Env():
            profile = predicted_src.profile
            profile.update(nodata=10)
            with rasterio.open(agreement_raster, 'w', **profile) as dst:
                dst.write(agreement_array, 1)
                          
    # Store summed pixel counts in dictionary.
    contingency_table_dictionary.update({'total_area':{'true_negatives': int((agreement_array == 0).sum()),
                                                      'false_negatives': int((agreement_array == 1).sum()),
                                                      'false_positives': int((agreement_array == 2).sum()),
                                                      'true_positives': int((agreement_array == 3).sum())
                                                      }})                               
    
    # Parse through dictionary of other layers and create contingency table metrics for the desired area. Layer must be raster with same shape as agreement_raster.
    if additional_layers_dict != {}:
        for layer_name in additional_layers_dict:
            layer_path = additional_layers_dict[layer_name]
            layer_src = rasterio.open(layer_path)
            layer_array = layer_src.read(1)
            layer_array = layer_array[:, :-1]  # TEMPORARY UNTIL SHAPE IS FIXED BY GDAL UPGRADE
            
            # Omit all areas that spatially disagree with the layer_array.
            layer_agreement_array = np.where(layer_array>0, agreement_array, 10)
            
            # Write the layer_agreement_raster.
            layer_agreement_raster = os.path.join(os.path.split(agreement_raster)[0], layer_name + '_agreement.tif')
            with rasterio.Env():
                profile = predicted_src.profile
                profile.update(nodata=10)
                with rasterio.open(layer_agreement_raster, 'w', **profile) as dst:
                    dst.write(layer_agreement_array, 1)
            
            # Store summed pixel counts in dictionary.
            contingency_table_dictionary.update({layer_name:{'true_negatives': int((layer_agreement_array == 0).sum()),
                                                             'false_negatives': int((layer_agreement_array == 1).sum()),
                                                             'false_positives': int((layer_agreement_array == 2).sum()),
                                                             'true_positives': int((layer_agreement_array == 3).sum())
                                                              }})
            del layer_agreement_array

    return contingency_table_dictionary
    
